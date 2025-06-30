import filecmp
import http.server
import os
import tempfile
import threading
import time

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


def run_http_server(directory, port, stop_event: threading.Event, ranged_support=True):
    os.chdir(directory)
    handler = (
        RangeRequestHandler if ranged_support else http.server.SimpleHTTPRequestHandler
    )
    httpd = http.server.ThreadingHTTPServer(("localhost", port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    stop_event.wait()
    httpd.shutdown()
    thread.join()


def start_server(tempdir, ranged_support=True):
    stop_event = threading.Event()
    port = utils.find_free_port()
    server_thread = threading.Thread(
        target=run_http_server, args=(tempdir, port, stop_event, ranged_support)
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


async def run_test(url, source_file):
    with tempfile.TemporaryDirectory() as output_dir:
        downloader = AsyncChunkDownloader(url, output_dir, num_chunks=8)
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
