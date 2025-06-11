import asyncio
import os
import random
import re
import string
import urllib
from pprint import pprint
from typing import Any

import aiofiles
import aiohttp

import utils
from logger import get_logger

_logger = get_logger("ia uploader")


class IAClient:
    """
    Async implementation of the IAS3 and metadata protocol, multipart upload allowed.
    S3 API is stateless, and mulitple clients are allowed to coexist.

    https://archive.org/developers/metadata-schema/index.html
    https://github.com/vmbrasseur/IAS3API#internet-archive-s3-api-documentation

    To simply the client, only essential methods were

    Predefined Metadata tags:
    - title : str
    - collection : str
    - creator : str
    - date [YYYY-MM-DD]
    - description : str (html / css)
    - notes: str
    - identifier : str (required)

    Mulitpart is definitely possible, but since there's a concurrency limit (12 currently),
    Enable mulitpart means me can't have concurrent upload tasks which is a waste imo

    All operations in the class is stateless.
    """

    _semaphore_cache = {}
    global_bytes_uploaded = 0

    def __init__(self, access_key: str, secret_key: str, https_proxy=None):
        self._access_key = access_key
        self._secret_key = secret_key
        self._proxy = https_proxy

        if not self._access_key:
            return
        if access_key not in IAClient._semaphore_cache:
            IAClient._semaphore_cache[access_key] = asyncio.Semaphore(12)
        self._connection_semaphore = IAClient._semaphore_cache[access_key]

    @staticmethod
    def safe_headers(header: str) -> str:
        """
        Convert headers to uri encoded safe headers
        """
        header = header.replace("\r", "")
        encoded = urllib.parse.quote(header.encode("utf-8"))
        return f"uri({encoded})"

    async def get_info(self, bucket: str) -> dict[str, Any] | None:
        """
        metadata in resp['metadata']
        filelist in resp['files']
        """
        url = f"https://archive.org/metadata/{bucket}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, proxy=self._proxy) as resp:
                if resp.status == 200:
                    return await resp.json()
                utils.log_error_and_raise(
                    _logger,
                    f"Get metadata of '{
                        bucket}' failed with status {resp.status}",
                )
                return None

    async def create_bucket(
        self,
        bucket: str,
        filepaths: list[str],
        # for simplicity sake, filename is same as local fliename
        meta_mediatype: str,
        meta_title: str,
        meta_description: str,
        meta_collection: str,
        # test_collection if to be deleted in 30 days
        custom_metadata: dict = None,  # metadata otherthan those required as params
        # custom metadata cannot contain _, use - instead
        option_keep_old_version=False,
        option_delete_derived_files=True,
        option_skip_derive_process=False,
    ):
        # metadata and headers
        accepte_mediatypes = [
            "texts",
            "etree",
            "audio",
            "movies",
            "software",
            "image",
            "data",
            "web",
        ]
        if meta_mediatype not in accepte_mediatypes:
            utils.log_error_and_raise(
                _logger, f"'{meta_mediatype}' not in '{accepte_mediatypes}'"
            )
        if not (
            5 <= len(bucket) <= 100
            and re.fullmatch(r"[A-Za-z0-9._-]+", bucket)
            and (bucket[0].isalpha() or bucket.isnumeric())
        ):
            utils.log_error_and_raise(_logger, f"Invalid identifier '{bucket}'")

        required_metadata = {
            "identifier": bucket,
            "mediatype": meta_mediatype,
            "title": meta_title,
            "description": meta_description,
            "collection": meta_collection,
        }

        for k in list(custom_metadata.keys()):
            v = custom_metadata[k]
            if k in required_metadata:
                if v != required_metadata[k]:
                    _logger.error(
                        f"parameter['{k}'] = '{
                            required_metadata[k]}', custom['{k}'] = '{v}'"
                    )
                    utils.log_error_and_raise(
                        _logger, "Discrepancy between parameter and custom metadata."
                    )
                else:
                    del custom_metadata[k]
        total_bytes = 0
        for filepath in filepaths:
            if not os.path.exists(filepath):
                utils.log_error_and_raise(_logger, f"File '{filepath}' does not exist")
            total_bytes += os.path.getsize(filepath)

        headers = {
            "x-archive-meta-mediatype": meta_mediatype,
            "x-archive-meta-title": self.safe_headers(meta_title),
            "x-archive-meta-description": self.safe_headers(meta_description),
            "x-archive-meta01-collection": meta_collection,
            "x-amz-auto-make-bucket": "1",
            "x-archive-size-hint": total_bytes,
            "x-archive-interactive-priority": 1,
            "authorization": f"LOW {self._access_key}:{self._secret_key}",
        }

        option_keep_old_version and headers.update({"x-archive-keep-old-version": 1})
        option_delete_derived_files and headers.update({"x-archive-cascade-delete": 1})
        option_skip_derive_process and headers.update({"x-archive-queue-derive": 1})

        if custom_metadata:
            for key, value in custom_metadata.items():
                if type(value) != list:
                    headers[f"x-archive-meta-{key}"] = value
                else:
                    for i, v in enumerate(value, start=1):
                        headers[f"x-archive-meta{str(i).zfill(2)}-{key}"] = v

        headers = {k: str(v) for k, v in headers.items() if v is not None}

        # from rich.pretty import pprint
        #
        # pprint(headers)
        # from pprint import pprint
        # pprint(headers)

        # use a smallest flie to init the bucket, then upload in parallel
        smallest_file = min(filepaths, key=lambda f: os.path.getsize(f))
        await self.upload_file(
            bucket, smallest_file, headers
        )  # upload first to create the bucket
        await asyncio.gather(
            *(
                self.upload_file(bucket, filepath)
                for filepath in filepaths
                if filepath != smallest_file
            )
        )

        _logger.info(f"Uploaded {len(filepaths)} files to internet archive.")

    async def upload_file(
        self,
        bucket: str,
        filepath: str,
        headers: dict[str, Any] | None = None,
        attempts=3,
    ):
        """
        Upload file to an exsiting bucket, or create a bucket then upload, depending on the headers
        """
        url = f"https://s3.us.archive.org/{
            bucket}/{os.path.basename(filepath)}"
        if not headers:
            headers = {
                "x-amz-auto-make-bucket": "1",
                "x-archive-interactive-priority": "1",
                "authorization": f"LOW {self._access_key}:{self._secret_key}",
            }
        headers["Content-Length"] = str(os.path.getsize(filepath))

        async def file_chunker(path, chunk_size=1024**2):
            async with aiofiles.open(path, "rb") as af:
                while True:
                    chunk = await af.read(chunk_size)
                    if not chunk:
                        break
                    IAClient.global_bytes_uploaded += chunk_size
                    yield chunk

        last_exception = None
        last_resptext = None
        for _ in range(attempts):
            try:
                async with self._connection_semaphore:
                    async with aiohttp.ClientSession() as session:
                        async with session.put(
                            url,
                            headers=headers,
                            data=file_chunker(filepath),
                            proxy=self._proxy,
                        ) as resp:
                            last_resptext = await resp.text()
                            if resp.status >= 400:
                                raise Exception("Bad status code ")
                            _logger.info(f"Successfully uploaded '{filepath}'.")
                return
            except Exception as e:
                last_exception = e
                _logger.debug(
                    f"Upload '{filepath}' failed with exception '{
                        str(e)}' and message '{last_resptext}', retrying."
                )
                await asyncio.sleep(attempts * 2)
        else:
            utils.log_error_and_raise(
                _logger,
                f"Upload '{filepath}' failed after {
                    attempts} attempts, last exception {last_exception}, last message {last_resptext}",
            )
            return None

    async def download_file(
        self, bucket: str, filename: str, output_dir: str, fast_get=False, attempts=3
    ) -> str | None:
        """
        if fast_get is set to true, use ia web instead of s3. Maybe faster?
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        filepath = os.path.join(output_dir, filename)

        url = (
            f"https://s3.us.archive.org/{bucket}/{filename}"
            if not fast_get
            else (f"https://archive.org/download/" f"{bucket}/{filename}")
        )

        for _ in range(attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, proxy=self._proxy) as resp:
                        if resp.status >= 400:
                            raise Exception("Bad status code.")
                        async with aiofiles.open(filepath, "wb") as f:
                            async for chunk in resp.content.iter_chunked(n=1024 * 64):
                                await f.write(chunk)
                        return filepath
            except Exception as e:
                _logger.warning(
                    f"Download '{bucket}/{filename}' failed: '{e}', retrying."
                )
        else:
            utils.log_error_and_raise(
                _logger,
                f"Download '{
                    bucket}/{filename}' failed after {attempts} attempts.",
            )
            os.remove(filepath)
            return None

    async def head_bucket(self, bucket: str, attempts=3) -> bool:
        """
        Check if bucket exists.
        """
        last_exception = None
        for _ in range(attempts):
            try:
                url = f"https://s3.us.archive.org/{bucket}/"
                async with aiohttp.ClientSession() as session:
                    async with session.head(url, proxy=self._proxy) as resp:
                        if resp.status == 404:
                            return False
                        return True
            except Exception as e:
                last_exception = e
                continue
        else:
            utils.log_error_and_raise(
                _logger,
                f"Head bucket '{bucket}' encountered error '{
                    str(last_exception)}'",
            )

    async def check_limits(self, bucket: str) -> dict | None:
        """
        Check limits does not need bucket to be pre-existing.
        What matters:
        resp["over_limit"] : int
        resp["detail"]["limit_reason"] : str
        """
        url = (
            f"https://s3.us.archive.org/?check_limit=1&"
            f"accesskey={self._access_key}&bucket={bucket}"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url, proxy=self._proxy) as resp:
                if resp.status == 200:
                    return await resp.json()
                utils.log_error_and_raise(
                    _logger, f"Check limits failed with status {resp.status}"
                )
                return None

    async def verify_bucket(
        self,
        bucket: str,
        filepaths: list[str] = None,
        md5_dict: dict[str, str] = None,
        timeout=180,
    ) -> list[str]:
        """
        Verify each file in filepaths is present and has same checksum in the bucket.
        Returns filepaths in the bucket that either not exist or differ from local files.

        Will use md5_dict if provided, otherwise will calculate md5 for each file in filepaths.
        """
        if not filepaths and not md5_dict:
            utils.log_error_and_raise(_logger, "No filepaths or md5_dict provided.")

        if md5_dict:
            md5_items = md5_dict.items()
            filepaths = [item[0] for item in md5_items]
            files_local_hash = [item[1] for item in md5_items]
        else:
            files_local_hash = await asyncio.gather(
                *(utils.async_hash(filepath) for filepath in filepaths)
            )

        _logger.debug(
            f"Verifying bucket {
                bucket}, wait up to {timeout} seconds."
        )
        start_time = asyncio.get_running_loop().time()
        while asyncio.get_running_loop().time() - start_time < timeout:
            info = await self.get_info(bucket)
            # from rich.pretty import pprint

            # pprint(info)
            try:
                files_bucket_hash = [d["md5"] for d in info["files"]]
                result = [
                    filepaths[idx]
                    for idx, local_hash in enumerate(files_local_hash)
                    if local_hash not in files_bucket_hash
                ]

                if result == [] or (
                    result != []
                    and ("pending_tasks" not in info or info["pending_tasks"] == False)
                ):
                    return result
            except KeyError:
                _logger.debug("Pending tasks not found in info.")
                _logger.debug(info)
            await asyncio.sleep(10)
        else:
            msg = f"Bucket '{bucket}' not ready after {
                timeout} seconds."
            _logger.debug(msg)
            raise Exception(msg)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


async def main():
    config = utils.read_config()
    async with IAClient(
        access_key=config["ia"]["s3_access_key"],
        secret_key=config["ia"]["s3_secret_key"],
    ) as client:
        bucket = "".join(random.choices(string.ascii_letters, k=16))
        print(bucket)

        exists = await client.head_bucket("manualzz-id-3422")

        pprint(exists)
        return

        datadir = utils.proj_path("data/")
        filepaths = []
        for filename in os.listdir(datadir):
            filepath = os.path.join(datadir, filename)
            if os.path.isfile(filepath):
                filepaths.append(filepath)
        #
        await client.create_bucket(
            bucket=bucket,
            filepaths=filepaths,
            meta_mediatype="data",
            meta_title=filename,
            meta_description="<p> Hello <h1> Ha! </h1> </p>",
            meta_collection="test_collection",
            custom_metadata={"Hello": "world", "multitag": ["A", "B"]},
        )

        info = await client.get_info(bucket)
        pprint(info)
        verify = await client.verify_bucket(bucket, filepaths)
        pprint(verify)


if __name__ == "__main__":
    asyncio.run(main())
