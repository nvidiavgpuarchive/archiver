import filecmp
import http.server
import os
import tempfile
import threading
import time
from http.cookies import SimpleCookie

import pytest
from ranged_handler import RangeRequestHandler

import utils
from downloader import AsyncChunkDownloader

FILE_SIZES = [
    ("0B", 0),
    ("1B", 1),
    ("1KB", 1024),
    ("64KB", 64 * 1024),
    ("256MB", 256 * 1024 * 1024),
]


class CookieRequiredMixin:
    REQUIRED_COOKIE_NAME = "test_session"
    REQUIRED_COOKIE_VALUE = "letmein"

    def send_head(self):
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        if (
            self.REQUIRED_COOKIE_NAME not in cookie
            or cookie[self.REQUIRED_COOKIE_NAME].value != self.REQUIRED_COOKIE_VALUE
        ):
            self.send_error(403, "Forbidden")
            return None
        return super().send_head()


class CookieRequiredRangeHandler(CookieRequiredMixin, RangeRequestHandler):
    pass


class CookieRequiredSimpleHandler(
    CookieRequiredMixin, http.server.SimpleHTTPRequestHandler
):
    pass


def run_http_server(
    directory,
    port,
    stop_event: threading.Event,
    ranged_support=True,
    require_cookie=False,
):
    os.chdir(directory)
    if require_cookie:
        handler = (
            CookieRequiredRangeHandler
            if ranged_support
            else CookieRequiredSimpleHandler
        )
    else:
        handler = (
            RangeRequestHandler
            if ranged_support
            else http.server.SimpleHTTPRequestHandler
        )
    httpd = http.server.ThreadingHTTPServer(("localhost", port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    stop_event.wait()
    httpd.shutdown()
    thread.join()


def start_server(tempdir, ranged_support=True, require_cookie=False):
    stop_event = threading.Event()
    port = utils.find_free_port()
    server_thread = threading.Thread(
        target=run_http_server,
        args=(tempdir, port, stop_event, ranged_support, require_cookie),
    )
    server_thread.start()
    time.sleep(1)  # wait for server
    return port, stop_event, server_thread


@pytest.fixture(scope="module", params=FILE_SIZES, ids=[name for name, _ in FILE_SIZES])
def testfile(request):
    size_name, size_bytes = request.param
    with tempfile.TemporaryDirectory() as tempdir:
        file_path = os.path.join(tempdir, "testfile.bin")
        with open(file_path, "wb") as f:
            if size_bytes > 0:
                f.write(os.urandom(size_bytes))
        yield tempdir, file_path


async def run_test(url, source_file, cookies=None):
    with tempfile.TemporaryDirectory() as output_dir:
        downloader = AsyncChunkDownloader(
            url, output_dir, num_chunks=8, cookies=cookies
        )
        output_file = await downloader.download()
        await downloader.close()

        files = [
            f
            for f in os.listdir(output_dir)
            if os.path.isfile(os.path.join(output_dir, f))
        ]
        assert len(files) == 1, f"Expected 1 file, found {len(files)}: {files}"
        assert os.path.isfile(output_file)
        assert filecmp.cmp(source_file, output_file)


@pytest.mark.asyncio
async def test_chunked_downloader_with_range(testfile):
    tempdir, source_file = testfile
    port, stop_event, server_thread = start_server(tempdir, ranged_support=True)
    url = f"http://localhost:{port}/testfile.bin"

    try:
        await run_test(url, source_file)
    finally:
        stop_event.set()
        server_thread.join()


@pytest.mark.asyncio
async def test_chunked_downloader_without_range(testfile):
    tempdir, source_file = testfile
    port, stop_event, server_thread = start_server(tempdir, ranged_support=False)
    url = f"http://localhost:{port}/testfile.bin"

    try:
        await run_test(url, source_file)
    finally:
        stop_event.set()
        server_thread.join()


@pytest.mark.asyncio
@pytest.mark.parametrize("ranged_support", [True, False])
async def test_downloader_sends_cookies(ranged_support):
    cookies = {
        CookieRequiredMixin.REQUIRED_COOKIE_NAME: (
            CookieRequiredMixin.REQUIRED_COOKIE_VALUE
        )
    }

    with tempfile.TemporaryDirectory() as tempdir:
        source_file = os.path.join(tempdir, "testfile.bin")
        with open(source_file, "wb") as f:
            f.write(os.urandom(1024))

        port, stop_event, server_thread = start_server(
            tempdir, ranged_support=ranged_support, require_cookie=True
        )
        url = f"http://localhost:{port}/testfile.bin"

        try:
            await run_test(url, source_file, cookies=cookies)
        finally:
            stop_event.set()
            server_thread.join()


@pytest.mark.asyncio
async def test_chunked_downloader_sends_cookies():
    cookies = {
        CookieRequiredMixin.REQUIRED_COOKIE_NAME: (
            CookieRequiredMixin.REQUIRED_COOKIE_VALUE
        )
    }

    with tempfile.TemporaryDirectory() as tempdir:
        source_file = os.path.join(tempdir, "large-testfile.bin")
        with open(source_file, "wb") as f:
            f.truncate(129 * 1024 * 1024)

        port, stop_event, server_thread = start_server(
            tempdir, ranged_support=True, require_cookie=True
        )
        url = f"http://localhost:{port}/large-testfile.bin"

        try:
            await run_test(url, source_file, cookies=cookies)
        finally:
            stop_event.set()
            server_thread.join()
