# misc functions
import asyncio
import hashlib
import json
import logging
import os
import re
import socket
import sys
import zipfile
from contextlib import closing
from typing import Dict, Tuple, List

import aiofiles
import aiohttp
from playwright.async_api import Page


def proj_path(filepath: str) -> str:
    """
    The script runs in src folder, this function converts path
    to be based on the parent folder of src.
    """
    utils_dir = os.path.dirname(__file__)
    return os.path.join(utils_dir, "..", filepath)


def read_config() -> dict:
    with open(proj_path("config/config.json"), "r") as f:
        return json.load(f)


def log_error_and_raise(logger: logging.Logger, errormsg: str):
    logger.error(errormsg)
    raise Exception(errormsg)


def get_free_space(dirpath: str) -> int:
    """
    Get free space in bytes
    """

    stat = os.statvfs(dirpath)
    return stat.f_bavail * stat.f_frsize


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def playwright_wait_for_any(page: Page, urls: Dict[str, str], timeout=30):
    """
    Wait for any of the urls to be loaded, return when any of them is loaded.
    Raise timeout if all timeout
    """

    tasks = {asyncio.create_task(page.wait_for_url(url, timeout=timeout * 1000)): tag for tag, url in urls.items()}

    done, pending = await asyncio.wait(tasks.keys(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()

    for finished_task in done:
        if finished_task.exception() is None:
            return tasks[finished_task]
    raise TimeoutError("Timeout, no url matches the criteria.")


def run_async_blocking(awaitable_func, *args, **kwargs):
    """
    example:
    atexit.register(partial(run_async_blocking, session.close))
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No event loop, safe to create a fresh one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(awaitable_func(*args, **kwargs))
        loop.close()
    else:
        # If we're in a running loop, use run_coroutine_threadsafe
        f = asyncio.run_coroutine_threadsafe(awaitable_func(*args, **kwargs), loop)
        f.result()


def sanitize_filename(filename: str, replacement: str = "_", max_length: int = 255) -> str:
    """
    Replace invalid filename characters with a safe replacement.

    Args:
        filename (str): The original filename.
        replacement (str): The character to replace invalid characters with.
        max_length (int): Maximum filename length.

    Returns:
        str: A safe, sanitized filename.
    """

    invalid_chars = r'[\\/*?:"<>|\r\n\t]'
    sanitized = re.sub(invalid_chars, replacement, filename)

    # Remove leading/trailing whitespace, dots, etc.
    sanitized = sanitized.strip().strip(".")

    # Limit length (preserving extension, if any)
    if len(sanitized) > max_length:
        base, dot, ext = sanitized.partition(".")
        ext = (dot + ext) if dot else ""
        trimmed = base[:max_length - len(ext)]
        sanitized = trimmed + ext

    # Fallback if result is empty
    if not sanitized:
        sanitized = "file"

    return sanitized


def human_readable_size(num_bytes: int) -> Tuple[float, str]:
    scale = ["bytes", "kilobytes", "megabytes", "gigabytes", "terabytes", "petabytes"]
    for idx, word in enumerate(scale[::-1]):
        power = 1024 ** (len(scale) - idx - 1)
        if num_bytes >= power:
            return round(num_bytes / power, 2), word
    return num_bytes, scale[0]


def human_readable_size_str(num_bytes: int) -> str:
    t = human_readable_size(num_bytes)
    return f"{t[0]} {t[1]}"


def zip_listfiles(zippath: str) -> List[str]:
    with zipfile.ZipFile(zippath) as z:
        return z.namelist()


async def is_link_alive(url, timeout=10):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.head(url, allow_redirects=True) as resp:
                return resp.status == 200
    except Exception as e:
        # Could log the exception here if desired
        return False


async def async_md5(filepath, bufsize=1024 ** 2):
    md5 = hashlib.md5()
    async with aiofiles.open(filepath, "rb") as f:
        while True:
            chunk = await f.read(bufsize)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()


def is_console_interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


class AsyncCounter:
    def __init__(self):
        self.value = 0
        self._lock = asyncio.Lock()

    async def increment(self):
        async with self._lock:
            async with self._lock:
                self.value += 1
                return self.value

    async def decrement(self):
        async with self._lock:
            self.value -= 1
            return self.value

    async def reset(self):
        async with self._lock:
            self.value = 0
            return self.value


if __name__ == "__main__":
    print(find_free_port())
    print(human_readable_size(99999999999999999999))
    print(is_console_interactive())
