import asyncio
import math
import os
import random
import re
import tempfile
import urllib
from typing import Any
from xml.etree import ElementTree

import aiofiles
import aiohttp
from rich.pretty import pprint
from tenacity import retry, stop_after_attempt, wait_fixed

import utils
from logger import get_logger
from utils import proj_path

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

    If multipart upload is enabled the verification may take a really long time to process.
    Multipart upload is not recommend for files < 256 MB

    https://www.alibabacloud.com/help/en/oss/user-guide/multipart-upload

    All operations in the class is stateless.
    """

    _semaphore_cache = {}
    global_bytes_uploaded = 0

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        https_proxy=None,
        # not larger than 256MB, adjust according to filesize
        multipart_chunksize=1024**2 * 16,
    ):
        self._access_key = access_key
        self._secret_key = secret_key
        self._proxy = https_proxy
        self._mulitpart_chunksize = multipart_chunksize

        if not self._access_key:
            return
        if access_key not in IAClient._semaphore_cache:
            IAClient._semaphore_cache[access_key] = asyncio.Semaphore(8)
        self._connection_semaphore = IAClient._semaphore_cache[access_key]

        self._default_headers = {
            "x-amz-auto-make-bucket": "1",
            "x-archive-interactive-priority": "1",
            "authorization": f"LOW {self._access_key}:{self._secret_key}",
        }

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
                    f"Get metadata of '{bucket}' failed with status "
                    f""
                    f"{resp.status}",
                )
                return None

    async def create_bucket(
        self,
        bucket: str,  # for simplicity sake, filename is same as local fliename
        filepaths: list[str],
        meta_mediatype: str,
        meta_title: str,
        meta_description: str,
        meta_collection: str,  # test_collection if to be deleted in 30 days
        custom_metadata: dict = {},  # metadata otherthan those required as params
        # custom metadata cannot contain _, use - instead
        option_keep_old_version=False,
        option_delete_derived_files=True,
        option_skip_derive_process=False,
        multipart=False,
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
                _logger,
                f"'{meta_mediatype}' not in '{accepte_mediatypes}'",
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
                        f"parameter['{k}'] = '{required_metadata[k]}', custom['{k}'] = "
                        f"'{v}'"
                    )
                    utils.log_error_and_raise(
                        _logger, "Discrepancy between parameter and custom metadata."
                    )
                else:
                    del custom_metadata[k]
        total_bytes = 0
        for filepath in filepaths:
            if not os.path.exists(filepath):
                utils.log_error_and_raise(_logger, f"File {filepath} does not exist")
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

        placeholder_filepath = await IAClient.generate_placeholder()
        await self.upload_file(bucket, placeholder_filepath, headers=headers)

        if not multipart:
            # upload first to create the bucket
            await asyncio.gather(
                *(self.upload_file(bucket, filepath) for filepath in filepaths)
            )
        else:
            for fp in filepaths:
                if os.path.getsize(fp) < 1024**2 * 8:
                    await self.upload_file(bucket, fp, headers)
                else:
                    await self.mulitpart_upload_file(
                        bucket, fp, headers, chunk_size=self._mulitpart_chunksize
                    )

        os.remove(placeholder_filepath)
        await self.delete_file(bucket, os.path.basename(placeholder_filepath))

        _logger.info(f"Uploaded {len(filepaths)} files to internet archive.")

    # delete bucket not allowed, but we can delete

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def delete_file(self, bucket: str, filename: str):
        url = f"https://s3.us.archive.org/{bucket}/{filename}"
        async with aiohttp.ClientSession() as session:
            async with session.delete(
                url, headers=self._default_headers, proxy=self._proxy
            ) as resp:
                body = await resp.text()
                _logger.debug(f"delete request body: {body}")
                resp.raise_for_status()
                return

    @staticmethod
    async def _file_chunker(path, chunk_size=1024**2, start=0, end=None):
        async with aiofiles.open(path, "rb") as af:
            await af.seek(start)
            pos = start

            while True:
                if end is not None:
                    rem = end - pos
                    if rem <= 0:
                        break
                    read_size = min(chunk_size, rem)
                else:
                    read_size = chunk_size

                chunk = await af.read(read_size)
                if not chunk:
                    break
                IAClient.global_bytes_uploaded += chunk_size
                yield chunk

                pos += len(chunk)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def _multipart_init(
        self, bucket: str, filepath: str, headers: dict[str, Any]
    ):
        filename = os.path.basename(filepath)
        url = f"https://s3.us.archive.org/{bucket}/{filename}?uploads"
        if not headers:
            headers = self._default_headers
        headers |= {"Content-Length": str(os.path.getsize(filepath))}
        async with aiohttp.ClientSession() as session:
            async with self._connection_semaphore:
                async with session.post(
                    url, headers=headers, proxy=self._proxy
                ) as resp:
                    body = await resp.text()
                    _logger.debug(f"Mulitpart init resp : {body} ")
                    if resp.status >= 400:
                        raise Exception(f"Bad status code {resp.status}")
                    tree = ElementTree.fromstring(body)
                    ns = {"ns": "http://s3.amazonaws.com/doc/2006-03-01/"}
                    upload_id = tree.find("ns:UploadId", ns).text
                    return upload_id

    # @retry(stop=stop_after_attempt(20), wait=wait_fixed(1))
    async def _multipart_upload_part(
        self,
        bucket: str,
        filepath: str,
        upload_id: str,
        part_number: int,
        start: int,
        end: int,  # [start, end)
    ):
        filename = os.path.basename(filepath)
        for _ in range(20):
            try:
                url = (
                    f"https://s3.us.archive.org/{bucket}/{filename}?uploadId="
                    f"{upload_id}&partNumber={part_number}"
                )
                header = self._default_headers | {"Content-Length": str(end - start)}
                async with aiohttp.ClientSession() as session:
                    async with self._connection_semaphore:
                        async with session.put(
                            url,
                            data=self._file_chunker(filepath, start=start, end=end),
                            headers=header,
                            proxy=self._proxy,
                        ) as resp:
                            if resp.status >= 400:
                                _logger.warning(
                                    f"Upload part '{filename}' part {part_number} to "
                                    f"bucket {bucket} failed with statu"
                                    f"s code {resp.status} and message '"
                                    f"{await resp.text()}'"
                                )
                                raise Exception(f"Bad status code {resp.status}")
                            etag = resp.headers.get("ETag")
                            _logger.debug(
                                f"{part_number} is done, etag {etag}, chunksize "
                                f"{end - start}"
                            )
                            return resp.headers.get("ETag")
            except Exception as e:
                _logger.warning(
                    f"Multipart upload {filename} part {part_number} failed, trying."
                )

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def _multipart_complete(
        self, bucket: str, filepath: str, upload_id: str, parts: dict[str, Any]
    ):
        filename = os.path.basename(filepath)
        url = f"https://s3.us.archive.org/{bucket}/{filename}?uploadId={upload_id}"
        xml = "<CompleteMultipartUpload>"
        for part in parts:
            xml += (
                f"<Part><PartNumber>{part['part_number']}</PartNumber><ETag>"
                f"{part['etag']}</ETag></Part>"
            )
        xml += "</CompleteMultipartUpload>"

        headers = self._default_headers | {"Content-Type": "application/xml"}
        async with aiohttp.ClientSession() as session:
            async with self._connection_semaphore:
                async with session.post(
                    url, data=xml, headers=headers, proxy=self._proxy
                ) as resp:
                    text = await resp.text()
                    _logger.debug(
                        f"Complete upload request resp : {text} with status "
                        f"{resp.status}"
                    )
                    if resp.status >= 400:
                        raise Exception(f"Bad status code {resp.status}")
                    return text

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def multipart_abort(self, bucket, filename, upload_id):
        url = f"https://s3.us.archive.org/{bucket}/{filename}?uploadId={upload_id}"
        async with aiohttp.ClientSession() as session:
            async with self._connection_semaphore:
                async with session.delete(
                    url, proxy=self._proxy, headers=self._default_headers
                ) as resp:
                    _logger.info(
                        f"Mulitpart upload aborted with msg : '{await resp.text()}'"
                    )
                    if resp.status >= 400:
                        raise Exception(f"Bad status code {resp.status}")
                    return await resp.text()

    async def mulitpart_upload_file(
        self,
        bucket: str,
        filepath: str,
        headers: dict[str, Any] = None,
        chunk_size=1024**2 * 8,
    ):
        filename = os.path.basename(filepath)

        upload_id = await self._multipart_init(bucket, filepath, headers)

        try:
            # parts = []
            # part_num = 1
            # upload_tasks = []

            # async for chunk in self._file_chunker(filepath, chunk_size=chunk_size):
            #     # etag = await self._multipart_upload_part(
            #     #     bucket, filename, upload_id, part_num, chunk
            #     # )
            #     task = asyncio.create_task(
            #         self._multipart_upload_part(
            #             bucket=bucket,
            #             filename=filename,
            #             upload_id=upload_id,
            #             part_number=part_num,
            #             data=chunk,
            #         )
            #     )
            #     upload_tasks.append((part_num, task))
            #     part_num += 1
            #
            # results = await asyncio.gather(*(task for _, task in upload_tasks))
            # for (num, _), etag in zip(upload_tasks, results):
            #     parts.append({"part_number": num, "etag": etag})

            parts = []
            part_num = 1

            total_size = os.path.getsize(filepath)
            num_chunks = math.ceil(total_size / chunk_size)

            for start, end in utils.divide_into_chunks(total_size, num_chunks):
                # etag = await self._multipart_upload_part(
                #     bucket, filename, upload_id, part_num, chunk
                # )
                etag = await self._multipart_upload_part(
                    bucket=bucket,
                    filepath=filepath,
                    upload_id=upload_id,
                    part_number=part_num,
                    start=start,
                    end=end,
                )
                parts.append({"part_number": part_num, "etag": etag})
                part_num += 1

            await self._multipart_complete(bucket, filepath, upload_id, parts)
            _logger.info(f"Mulitpart upload {filepath} to bucket '{bucket}' finished.'")
        except Exception as e:
            try:
                await self.multipart_abort(bucket, filename, upload_id)
            except Exception as f:  # there's nothing we can do about it
                _logger.exception(f"Abort failed. {f}")
            _logger.exception(f"Error when multipart uploading file '{filepath}': {e}")
            raise Exception("Multipart upload failed.")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def upload_file(
        self, bucket: str, filepath: str, headers: dict[str, Any] | None = None
    ):
        """
        Upload file to an exsiting bucket, or create a bucket then upload, depending on the headers
        """
        url = f"https://s3.us.archive.org/{bucket}/{os.path.basename(filepath)}"
        if not headers:
            headers = self._default_headers
        headers |= {"Content-Length": str(os.path.getsize(filepath))}

        async with self._connection_semaphore:
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    url,
                    headers=headers,
                    data=self._file_chunker(filepath),
                    proxy=self._proxy,
                ) as resp:
                    if resp.status >= 400:
                        text = await resp.text()
                        raise Exception(
                            f"Bad status code {resp.status}, resp {text}, "
                            f"when uploading {filepath}"
                        )
                    _logger.info(f"Successfully uploaded {filepath}.")
        return

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
                            raise Exception(f"Bad status code {resp.status}")
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
                f"Download '{bucket}/{filename}' failed after {attempts} " f"attempts.",
            )
            os.remove(filepath)
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    async def head_bucket(self, bucket: str) -> bool:
        """
        Check if bucket exists.
        """
        url = f"https://s3.us.archive.org/{bucket}/"
        async with aiohttp.ClientSession() as session:
            async with session.head(url, proxy=self._proxy) as resp:
                if resp.status == 404:
                    return False
                return True

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
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
    ) -> tuple[list[str], dict]:
        """
        Returns bad_filelist, metainfo

        Verify each file in filepaths is present and has same checksum in the bucket.
        Returns filepaths in the bucket that either not exist or differ from local files.
        Also return the metainfo from ia.

        Will use md5_dict if provided, otherwise will calculate md5 for each file in filepaths.
        """
        if not filepaths and not md5_dict:
            utils.log_error_and_raise(
                _logger, f"No filepaths or md5_dict provided, bucket '{bucket}'"
            )

        if md5_dict:
            md5_items = md5_dict.items()
            filepaths = [item[0] for item in md5_items]
            files_local_hash = [item[1] for item in md5_items]
        else:
            files_local_hash = await asyncio.gather(
                *(utils.async_hash(filepath) for filepath in filepaths)
            )

        if not await self.head_bucket(bucket):
            return filepaths, {}

        _logger.debug(f"Verifying bucket {bucket}, wait up to {timeout} seconds.")
        start_time = asyncio.get_running_loop().time()
        empty_json = True
        while asyncio.get_running_loop().time() - start_time < timeout:
            info = await self.get_info(bucket)
            if info == {} and empty_json:
                empty_json == True
                await asyncio.sleep(1)
                continue
            else:
                empty_json = False
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
                    return result, info["metadata"]
            except KeyError:
                _logger.debug("Pending tasks not found in info.")
                _logger.debug(info)
            await asyncio.sleep(10)
        else:
            # if empty_json:
            #     return ["Bucket likely not found."]
            msg = f"Bucket '{bucket}' not ready after {timeout} seconds."
            _logger.debug(msg)
            raise Exception(msg)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @staticmethod
    async def generate_placeholder():
        """
        The code logic requires a placeholder to be uploaded in order to
        create the bucket and init the whole upload procedure.
        However, a blank file or if too simple would trigger IA's spam filter
        and make bucket creation unsuccessful.
        This function solves that, by randomize AiW and Bible and fill the placeholder.
        """
        async with aiofiles.open(proj_path("data/aiw.txt"), mode="r") as f:
            aiw_text = await f.read()
        async with aiofiles.open(proj_path("data/pg10.txt"), mode="r") as f:
            pg10_text = await f.read()
        combined = aiw_text + pg10_text
        paragraphs = combined.split("\n\n")
        random.shuffle(paragraphs)
        paragraphs = random.choices(
            paragraphs, k=int(len(paragraphs) * random.uniform(0.6, 0.9))
        )
        random_text = "\n\n".join(paragraphs)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="ph_", suffix=".dat")
        os.close(tmp_fd)  # Close the os-level file descriptor
        async with aiofiles.open(tmp_path, mode="wb") as f:
            await f.write(random_text.encode("utf-8"))
        return tmp_path


async def main():
    # 1. Generate a 1GB dummy file
    dummy_path = "/tmp/dummy.zip"

    # 2. Upload it using multipart upload to the 'test_collection' bucket

    config = utils.read_config()
    async with IAClient(
        access_key=config["ia"]["s3_access_key"],
        secret_key=config["ia"]["s3_secret_key"],
    ) as client:
        bucket = "nvgpu_NVIDIA-GRID-Windows-418.197.02-427.33.zip"
        res = await client.get_info(bucket)
        pprint(res)
        return


if __name__ == "__main__":
    asyncio.run(main())
