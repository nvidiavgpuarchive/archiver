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
    Aynchronous downloader, that supports chunk based downloads
    Each downloader instance is responsible for one download tasks and
    should be deleted once download is done or failed.
    Mulitple downloader can run at the same time.

    Since nvidia don't require downlodas to be loggedin, current implementation
    doesn't require passing through cookies.

    async with AsyncChunkDownloader(url, output_dir) as downloader:
        path = await downloader.download()

    """

    global_bytes_downloaded = 0

    def __init__(self, url: str, output_dir: str, num_chunks: int = 32, proxy=None):
        self._url = url
        self._output_dir = output_dir
        if not os.path.isdir(self._output_dir):
            os.makedirs(self._output_dir)
        self._num_chunks = num_chunks
        self._proxy = proxy  # https://user:pass@proxyserver:port

        self._session = aiohttp.ClientSession()

        self._state = "uninitialised"  # fetching_metadata, downloading, done
        self._total_bytes = 0  # filled by fetch metadata
        self._support_range = False  # filled by fetch matadata
        self._filename = None  # filled by fetch matadata
        self._bytes_downloaded = 0  # filled by download

    def status(self):
        return {
            "state": self._state,
            "total_bytes": self._total_bytes,
            "support_range": self._support_range,
            "filename": self._filename,
            "bytes_downloaded": self._bytes_downloaded
        }

    async def download(self) -> str:
        """
        start downloading, returns final filepath if completed
        return "" if failed
        """
        if not self._state == "uninitialised":
            utils.log_error_and_raise(_logger, "Downloader reuse forbidden.")

        await self._fetch_file_metadata()
        if not utils.get_free_space(self._output_dir) >= self._total_bytes * 2:
            utils.log_error_and_raise(_logger, f"Not enough free space in {self._output_dir}.")
        if self._total_bytes == 0:
            _logger.warning(f"{self._filename} length is 0!")
            final_path = os.path.join(self._output_dir, self._filename)
            pathlib.Path(final_path).touch()
            return final_path

        self._state = "downloading"
        if not self._support_range or self._total_bytes <= 1024:
            _logger.warning(f"File {self._filename} does not support multipart downloading or is too small.")
            self._num_chunks = 1
        chunks = self._divide_into_chunks()

        _logger.info(
            f"Downloading {self._filename} with {self._num_chunks} chunks,"
            f" total size {utils.human_readable_size_str(self._total_bytes)} ")

        download_tasks = [
            self._download_chunk(start, end, idx)
            for idx, (start, end) in enumerate(chunks)
        ]
        chunk_filelist = await asyncio.gather(*download_tasks)
        if self._bytes_downloaded != self._total_bytes:
            for f in chunk_filelist:
                os.remove(f)
            _logger.error(f"Error. Expect {self._bytes_downloaded} bytes, "
                          f"got {self._bytes_downloaded} bytes")

        final_path = await asyncio.to_thread(self._merge_chunks, chunk_filelist)
        self._state = "done"
        _logger.info(f"Done with {final_path}")

        return final_path

    async def _is_url_supports_range(self, url) -> bool:
        async with self._session.head(url, proxy=self._proxy) as resp:
            if resp.headers.get("Accept-Ranges", "") == "bytes":
                return True

        headers = {"Range": "bytes=0-99"}
        async with self._session.get(url, headers=headers, proxy=self._proxy) as resp:
            if resp.status not in (200, 206):
                return False
            total = 0
            async for chunk in resp.content.iter_chunked(n=64):
                total += len(chunk)
                if total > 100:
                    return False
            return total == 100

    async def close(self):
        await self._session.close()

    async def _fetch_file_metadata(self):
        """
        HEAD request to determine total file size and range support.
        Populates internal metadata.
        """
        self._state = "fetching_metadata"
        async with self._session.head(self._url, proxy=self._proxy) as resp:
            if resp.status >= 400:
                utils.log_error_and_raise(_logger, f"Failed to fetch file metadata for {self._url}: {resp.status}")

            content_length = resp.headers.get("content-length")
            if content_length is None:
                utils.log_error_and_raise(_logger, f"Failed to fetch content length for {self._url}")
            self._total_bytes = int(content_length)

            # get filename if avail
            filename = None
            content_disp = resp.headers.get("Content-Disposition", "")
            if "filename" in content_disp:
                match = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', content_disp)
                if match:
                    filename = match.group(1)
            if not filename:
                path = urllib.parse.urlparse(self._url).path
                filename = os.path.basename(path)
            self._filename = utils.sanitize_filename(filename)

        self._support_range = await self._is_url_supports_range(self._url)
        # self._support_range = True

    def _divide_into_chunks(self) -> List[Tuple]:

        base = self._total_bytes // self._num_chunks
        reminder = self._total_bytes % self._num_chunks

        res = []
        start = 0
        for i in range(self._num_chunks):
            # chatgpt says it's smart to do this
            chunk_size = base + (1 if i < reminder else 0)
            end = start + chunk_size
            res.append((start, end))
            start = end
        return res

    async def _download_chunk(self, start: int, end: int, part_index: int, attempts=3) -> str | None:
        """
        downlodas chunk, return full chunk filepath
        [start, end)
        For a range of 100 bytes, by convention start = 0 and end = 100
        """
        headers = {
            "Range": f"bytes={start}-{end - 1}",
            # for a total of 100 bytes, range is from 0 to 99 inclusive
        }
        headers = headers if not (self._num_chunks == 1) else None

        chunk_filepath = os.path.join(self._output_dir, f"{self._filename}.{part_index}.chunk")
        for _ in range(attempts):
            try:
                chunk_bytes_downloaded = 0
                async with self._session.get(self._url, headers=headers, proxy=self._proxy) as resp:
                    if resp.status not in (200, 206):
                        raise Exception(f"Chunk download failed with status {resp.status}")
                    async with aiofiles.open(chunk_filepath, "wb") as f:
                        async for chunk in resp.content.iter_chunked(n=1024 * 64):
                            await f.write(chunk)
                            self._bytes_downloaded += len(chunk)
                            chunk_bytes_downloaded += len(chunk)
                            AsyncChunkDownloader.global_bytes_downloaded += len(chunk)

                if chunk_bytes_downloaded != end - start:
                    utils.log_error_and_raise(
                        _logger,
                        f"Chunk download failed. "
                        f"Expect {end - start} bytes, got {chunk_bytes_downloaded} bytes")
                _logger.debug(
                    f"Chunk {part_index} downloaded to {chunk_filepath}, "
                    f"size {utils.human_readable_size_str(chunk_bytes_downloaded)}")
                return chunk_filepath
            except Exception as e:
                _logger.warning(f"{self._filename} part {part_index} failed, retrying. \n {e}")
        else:
            os.remove(chunk_filepath)  # clean up if failed
            utils.log_error_and_raise(_logger, f"Failed to download part {part_index} from {self._url}")
            return None

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
                        if not chunk: break
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
