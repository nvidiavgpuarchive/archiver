# downloader accepts things and download them
import asyncio
import os
import pathlib
import re
import urllib.parse
from typing import List, Tuple

import aiofiles
import aiohttp

import utils
from logger import get_logger

_logger = get_logger(__name__)


class AsyncChunkDownloader:
    """
    Aynchronous downloader, that supports chunk based downloads and auto resume
    within each chunk if error happens
    Each downloader instance is responsible for one download tasks and
    should be deleted once download is done or failed.
    Mulitple downloader can run at the same time.

    Since nvidia don't require downlodas to be loggedin, current implementation
    doesn't require passing through cookies.

    async with AsyncChunkDownloader(url, output_dir) as downloader:
        path = await downloader.download()

    For simplicity's sake, each chunk is downloaded using its own session.
    """

    global_bytes_downloaded = 0

    def __init__(self, url: str, output_dir: str, num_chunks: int = 32, proxy=None):
        self._url = url
        self._output_dir = output_dir
        if not os.path.isdir(self._output_dir):
            os.makedirs(self._output_dir)
        self._num_chunks = num_chunks
        self._proxy = proxy  # https://user:pass@proxyserver:port

        self._state = "uninitialised"  # fetching_metadata, downloading, done
        self._total_bytes = 0  # filled by fetch metadata
        self._support_range = False  # filled by fetch matadata
        self._filename = None  # filled by fetch matadata

    async def download(self) -> str:
        """
        start downloading, returns final filepath if completed
        return "" if failed
        """
        if not self._state == "uninitialised":
            utils.log_error_and_raise(_logger, "Downloader reuse forbidden.")

        await self._fetch_file_metadata()
        if not utils.get_free_space(self._output_dir) >= self._total_bytes * 2:
            utils.log_error_and_raise(
                _logger, f"Not enough free space in {self._output_dir}."
            )
        if self._total_bytes == 0:
            _logger.warning(f"{self._filename} length is 0!")
            final_path = os.path.join(self._output_dir, self._filename)
            pathlib.Path(final_path).touch()
            return final_path

        self._state = "downloading"
        if (
            not self._support_range or self._total_bytes <= (1024**2) * 128
        ):  # basic downloading, no point to use chunks for file <= 128MB
            _logger.debug(
                f"File {
                    self._filename} does not support multipart downloading or is too small."
            )
            _logger.info(
                f"Downloading '{self._filename}' with one chunk, total size "
                f"{utils.human_readable_size_str(self._total_bytes)} "
            )
            self._num_chunks = 1
            final_path = await self._basic_download()
            return final_path
        else:  # chunked downloading
            chunks = utils.divide_into_chunks(
                self._total_bytes, self._num_chunks)
            _logger.info(
                f"Downloading '{self._filename}' with {
                    self._num_chunks} chunks,"
                f" total size {
                    utils.human_readable_size_str(self._total_bytes)} "
            )

            download_tasks = [
                self._chunk_download(start, end, idx)
                for idx, (start, end) in enumerate(chunks)
            ]
            chunk_filelist = await asyncio.gather(*download_tasks)

            for chunk in chunk_filelist:
                if not os.path.exists(chunk):
                    await asyncio.to_thread(utils.remove_files, chunk_filelist)
                    _logger.error(f"{chunk} not found.")
                    raise Exception("Chunks incomplete.")

            chunk_total_size = sum(os.path.getsize(f) for f in chunk_filelist)
            if chunk_total_size != self._total_bytes:
                await asyncio.to_thread(utils.remove_files, chunk_filelist)
                _logger.error(
                    f"Final size verification error. Expect {
                        self._total_bytes} bytes, "
                    f"got {chunk_total_size} bytes"
                )
                raise Exception("Size mismatch.")

            final_path = await asyncio.to_thread(self._merge_chunks, chunk_filelist)
            self._state = "done"
            _logger.info(f"Done with '{final_path}'")

            return final_path

    async def _is_url_supports_range(self, attempts=3) -> bool:
        last_exception = None
        for _ in range(attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(self._url, proxy=self._proxy) as resp:
                        if resp.headers.get("Accept-Ranges", "") == "bytes":
                            return True
            except Exception as e:
                _logger.debug(f"Head request error with {str(e)}, retrying.")
                last_exception = e
                continue
        else:
            _logger.warning(
                f"Check if url supports range failed after 3 attempts, assume false."
            )
            _logger.warning(f"Last exception: {last_exception}")
            return False

        headers = {"Range": "bytes=0-99"}
        async with session.get(url, headers=headers, proxy=self._proxy) as resp:
            if resp.status not in (200, 206):
                return False
            total = 0
            async for chunk in resp.content.iter_chunked(n=64):
                total += len(chunk)
                if total > 100:
                    return False
            return total == 100

    async def close(self):
        pass

    async def _fetch_file_metadata(self, attempts=3):
        """
        HEAD request to determine total file size and range support.
        Populates internal metadata.
        """
        self._state = "fetching_metadata"

        last_exception = None
        for _ in range(attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(self._url, proxy=self._proxy) as resp:
                        if resp.status >= 400:
                            utils.log_error_and_raise(
                                _logger,
                                f"Failed to fetch file metadata for {
                                    self._url}: {resp.status}",
                            )

                        content_length = resp.headers.get("content-length")
                        if content_length is None:
                            utils.log_error_and_raise(
                                _logger,
                                f"Failed to fetch content length for {
                                    self._url}",
                            )
                        self._total_bytes = int(content_length)

                        # get filename if avail
                        filename = None
                        content_disp = resp.headers.get(
                            "Content-Disposition", "")
                        if "filename" in content_disp:
                            match = re.search(
                                r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)',
                                content_disp,
                            )
                            if match:
                                filename = match.group(1)
                        if not filename:
                            path = urllib.parse.urlparse(self._url).path
                            filename = os.path.basename(path)
                        self._filename = utils.sanitize_filename(filename)
                        break
            except Exception as e:
                _logger.debug(
                    f"Fetching metadata failed with error {str(e)}, retrying."
                )
                last_exception = e
                continue
        else:
            utils.log_error_and_raise(
                _logger,
                f"Unable to fetch metadata for url {
                    self._url}, last exceptino was : str{last_exception}",
            )

        # self._support_range = True
        self._support_range = await self._is_url_supports_range()

    async def _basic_download(self, attempts=3) -> str | None:
        """
        Download the whole file in series . If error occurs, startover.
        Returns one single file, which is final.
        """

        final_filepath = os.path.join(self._output_dir, self._filename)
        for _ in range(attempts):
            try:
                bytes_downloaded = 0
                async with aiohttp.ClientSession() as session:
                    async with session.get(self._url, proxy=self._proxy) as resp:
                        resp.raise_for_status()
                        async with aiofiles.open(final_filepath, "wb") as f:
                            async for piece in resp.content.iter_chunked(n=1024 * 64):
                                await f.write(piece)
                                bytes_downloaded += len(piece)
                                AsyncChunkDownloader.global_bytes_downloaded += len(
                                    piece
                                )
                if bytes_downloaded != self._total_bytes:
                    utils.log_error_and_raise(
                        _logger,
                        f"Basic download size verification error, expect {self._total_bytes}, got {
                            bytes_downloaded}.",
                    )
                return final_filepath
            except Exception as e:
                _logger.warning(f"Download error: {str(e)}, try again.")
        else:
            if os.path.exists(final_filepath):
                os.remove(final_filepath)  # clean up if failed
            utils.log_error_and_raise(
                _logger, f"Failed to download {final_filepath}")
            return None

    async def _chunk_download(
        self, start: int, end: int, part_index: int, attempts=5
    ) -> str | None:
        """
        Advanced, download one chunk only. If any error occurs, simply resume from the last success byte downloaded.
        Fail after 3 consecutive times of no bytes received. (Meaning retry - no bytes received - retry)

        In http header, start and end is inclusive, and start from 0. Meaning if we want to download the first byte,
        we do bytes = 0-0
        For this method, by python convention we start from 0 inclusive and end non inclusive. So to download first byte
        we do start = 0, end = 1
        """

        chunk_filepath = os.path.join(
            self._output_dir, f"{self._filename}.{part_index}.chunk"
        )
        chunk_bytes_downloaded = 0
        chunk_bytes_last_downloaded = 0
        last_exception = None

        cons_fail = 0
        while cons_fail < attempts:
            current_start = start + chunk_bytes_downloaded
            headers = {"Range": f"bytes={current_start}-{end - 1}"}

            connector = aiohttp.TCPConnector(
                force_close=True, enable_cleanup_closed=True
            )

            try:
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        self._url, headers=headers, proxy=self._proxy
                    ) as resp:
                        if resp.status not in (200, 206):
                            raise Exception(
                                f"Chunk download failed with status {
                                    resp.status}, headers: {headers}"
                            )

                        async with aiofiles.open(chunk_filepath, "ab") as f:
                            async for chunk in resp.content.iter_chunked(n=1024 * 64):
                                await f.write(chunk)
                                chunk_bytes_downloaded += len(chunk)
                                AsyncChunkDownloader.global_bytes_downloaded += len(
                                    chunk
                                )

                # success → break
                break

            except (aiohttp.ClientConnectionError, ConnectionResetError) as e:
                _logger.debug(
                    f"Chunk {part_index} connection error: {
                        str(e)}, retrying..."
                )
                last_exception = e

            except Exception as e:
                last_exception = e
                _logger.debug(
                    f"Chunk {part_index} download error: {str(e)}, retrying..."
                )

            # Retry logic — exponential backoff if no progress
            if chunk_bytes_last_downloaded == chunk_bytes_downloaded:
                await asyncio.sleep(2**cons_fail)
                cons_fail += 1
            else:
                cons_fail = 0
                chunk_bytes_last_downloaded = chunk_bytes_downloaded

        else:
            if os.path.exists(chunk_filepath):
                os.remove(chunk_filepath)  # clean up if failed
            utils.log_error_and_raise(
                _logger,
                f"After {attempts} attempts failed to download part {
                    part_index} from {self._url}, "
                f"last exception: {last_exception}",
            )
            return None

        # Final verification
        if chunk_bytes_downloaded != end - start:
            utils.log_error_and_raise(
                _logger,
                f"Chunk download failed, expect {
                    end - start} bytes, got {chunk_bytes_downloaded} bytes",
            )

        _logger.debug(
            f"Chunk {part_index} downloaded to {chunk_filepath}, "
            f"size {utils.human_readable_size_str(chunk_bytes_downloaded)}"
        )
        return chunk_filepath

    def _merge_chunks(self, chunk_filelist: List[str]) -> str:
        """
        await asycncio.to_thread(self._merge_chunks)
        returns merged path
        """
        final_path = os.path.join(self._output_dir, self._filename)

        # if there's only one chunk, simply rename it
        if len(chunk_filelist) == 1:
            os.rename(chunk_filelist[0], final_path)
            return final_path

        self._state = "merging_chunks"
        _logger.debug(f"Start to merge {self._filename} into {final_path}")
        with open(final_path, "wb") as outfile:
            for chunk_file in chunk_filelist:
                with open(chunk_file, "rb") as infile:
                    while True:
                        chunk = infile.read(1024 * 64)
                        if not chunk:
                            break
                        outfile.write(chunk)
                    os.remove(chunk_file)
        return final_path

    async def __aenter__(self):
        # Optionally, perform async setup here (if needed)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


if __name__ == "__main__":
    pass
