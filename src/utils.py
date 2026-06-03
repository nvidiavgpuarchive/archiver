# misc functions
import asyncio
import gc
import hashlib
import html
import inspect
import logging
import os
import re
import shutil
import socket
import sys
import threading
import time
import types
import zipfile
from collections.abc import Callable, Coroutine, Iterator
from contextlib import closing
from io import TextIOBase
from typing import Any

import aiofiles
import aiohttp
import psutil

## Logging
##


def log_error_and_raise(logger: logging.Logger, errormsg: str) -> None:
    logger.error(errormsg)
    raise Exception(errormsg)


## Project and File Management
##


def proj_path(filepath: str) -> str:
    """
    The script runs in src folder, this function converts path
    to be based on the parent folder of src.
    """
    utils_dir = os.path.dirname(__file__)
    return os.path.join(utils_dir, "..", filepath)


def sanitize_filename(
    filename: str, replacement: str = "_", max_length: int = 255
) -> str:
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
    sanitized = sanitized.replace(" ", "_").strip(".")

    # Limit length (preserving extension, if any)
    if len(sanitized) > max_length:
        base, dot, ext = sanitized.partition(".")
        ext = (dot + ext) if dot else ""
        trimmed = base[: max_length - len(ext)]
        sanitized = trimmed + ext

    # Fallback if result is empty
    if not sanitized:
        sanitized = "file"

    return sanitized


def zip_listfiles(zippath: str) -> list[str]:
    with zipfile.ZipFile(zippath) as z:
        return z.namelist()


async def zip_verify_crc(zippath: str) -> bool:
    if shutil.which("7z") is None:
        raise Exception("7z not found, please install it")
    try:
        process = await asyncio.create_subprocess_exec(
            "7z",
            "t",
            zippath,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        stdout_text = stdout.decode()
        # Optional: stderr_text = stderr.decode()
        return "Everything is Ok" in stdout_text
    except Exception as e:
        return False


async def zip_decompress(zippath: str, dest_folder: str) -> list[str]:
    """
    Decompresses a zip file using 7-zip asynchronously into the specified destination folder.
    """

    if shutil.which("7z") is None:
        raise Exception("7z not found, please install it.")

    # Ensure destination folder exists
    os.makedirs(dest_folder, exist_ok=True)

    try:
        # Execute 7-zip extraction command asynchronously
        process = await asyncio.create_subprocess_exec(
            "7z",
            "x",
            zippath,
            f"-o{dest_folder}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise Exception(f"7z extraction failed: {stderr.decode().strip()}")

        # Collect file paths of extracted files
        decompressed_files = []
        for root, _, files in os.walk(dest_folder):
            for file in files:
                decompressed_files.append(os.path.abspath(os.path.join(root, file)))

        return decompressed_files

    except Exception as e:
        raise Exception(f"Async extraction failed: {e}")


def check_non_exist(filepaths: list[str]) -> list[str]:
    """
    Accepts a list of absolute filepaths, return a list of str
    of files that doesn't exist in the file system
    """
    non_exist = []
    for fp in filepaths:
        if not os.path.exists(fp):
            non_exist.append(fp)
    return non_exist


def remove_files(filepaths: list[str]) -> list[str]:
    """
    Remove files in the list, will just continue if files non exist
    Returns a list of files successfully removed
    """
    rmed = []
    for fp in filepaths:
        try:
            os.remove(fp)
            rmed.append(fp)
        except:
            continue
    return rmed


def rm_dir(dirpath: str) -> None:
    try:
        shutil.rmtree(dirpath)
    except:
        return


class TouchAndOpen:
    def __init__(self, filepath: str, mode: str = "w", encoding: str = "utf-8") -> None:
        self.filepath = filepath
        self.mode = mode
        self.encoding = encoding
        self.file: TextIOBase | None = None

    def __enter__(self) -> TextIOBase:
        # Ensure all the directories in the filepath exist
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        # Open the file and return the file object
        self.file = open(self.filepath, self.mode, encoding=self.encoding)
        return self.file

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        # Close the file when exiting
        if self.file:
            self.file.close()


## Network and Async Operations
##


def divide_into_chunks(total_size: int, num_chunks: int) -> list[tuple[int, int]]:
    base = total_size // num_chunks
    reminder = total_size % num_chunks

    res = []
    start = 0
    for i in range(num_chunks):
        # chatgpt says it's smart to do this
        chunk_size = base + (1 if i < reminder else 0)
        end = start + chunk_size
        res.append((start, end))
        start = end
    return res


def run_async_blocking(
    awaitable_func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any
) -> None:
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


async def is_link_alive(
    url: str,
    timeout: int = 10,
    proxy: str | None = None,
    cookies: dict[str, str] | None = None,
) -> bool:
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout), cookies=cookies
        ) as session:
            async with session.head(url, allow_redirects=True, proxy=proxy) as resp:
                return resp.status == 200
    except Exception as e:
        # Could log the exception here if desired
        return False


async def async_hash(
    filepath: str, hashfunc: Callable[[], Any] = hashlib.md5, bufsize: int = 1024**2
) -> str:
    hashis = hashfunc()
    async with aiofiles.open(filepath, "rb") as f:
        while True:
            chunk = await f.read(bufsize)
            if not chunk:
                break
            hashis.update(chunk)
    return hashis.hexdigest()


async def async_multihash(
    filepath: str, hashfuncs: list[Callable[[], Any]], bufsize: int = 1024**2
) -> dict[str, str]:
    """
    Compute mulitple hashes at once, more efficient than calling async_hash multiple times.
    await asyicio.to_thread(...)
    """
    hashiss = [hashfunc() for hashfunc in hashfuncs]
    async with aiofiles.open(filepath, "rb") as f:
        while True:
            chunk = await f.read(bufsize)
            if not chunk:
                break
            for i in hashiss:
                i.update(chunk)
    return {i.name: i.hexdigest() for i in hashiss}


def sync_multihash(filepath: str, hashfuncs: list[Callable[[], Any]]) -> dict[str, str]:
    """
    Synchronously load the entire file into memory, then compute multiple hashes in parallel,
    one per thread.
    Requires a lot of memory significantly faster than aysnc mulithash.
    """
    with open(filepath, "rb") as f:
        data = f.read()

    results = {}
    threads = []

    def compute_hash(hashfunc: Callable[[], Any]) -> None:
        h = hashfunc()
        h.update(data)
        results[h.name] = h.hexdigest()

    for hashfunc in hashfuncs:
        t = threading.Thread(target=compute_hash, args=(hashfunc,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    return results


async def run_with_shutdown(
    c: Coroutine[Any, Any, Any], e: asyncio.Event
) -> Any | None:
    """
    Return when e is set.
    If e is never set, behaves like that coroutine
    """

    coroutine_task = asyncio.create_task(c)
    shutdown_task = asyncio.create_task(e.wait())

    done, pending = await asyncio.wait(
        {coroutine_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        try:
            task.cancel()
        except:
            pass
    if coroutine_task in done:
        return coroutine_task.result()
    else:
        return None


def run_async_in_thread(coro: Coroutine[Any, Any, Any]) -> threading.Thread:
    """
    Run an async coroutine in a separate thread with its own event loop.
    The thread and event loop shut down automatically when the coroutine is done.
    """

    def thread_entry() -> None:
        # Create and bind a new event loop to this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    t = threading.Thread(target=thread_entry)
    t.start()
    return t


class AsyncCounter:
    def __init__(self) -> None:
        self.value = 0
        self._lock = asyncio.Lock()

    async def increment(self) -> int:
        async with self._lock:
            self.value += 1
            return self.value

    async def decrement(self) -> int:
        async with self._lock:
            self.value -= 1
            return self.value

    async def set(self, new_val: int) -> int:
        async with self._lock:
            self.value = new_val
            return self.value

    async def reset(self) -> int:
        async with self._lock:
            self.value = 0
            return self.value

    def get_value(self) -> int:
        return self.value


def simple_timer(interval: int) -> Iterator[bool]:
    """
    Returns true and reset timer if have elapsed >= interval from last time calling
    Returns false and do nothing if not
    Triggers instantly the first time

    timer = simple_timer(30)
    if next(timer):
        do(something)
    """

    last_time = 0.0
    while True:
        current_time = time.time()
        if current_time - last_time >= interval:
            last_time = current_time
            yield True
        else:
            yield False


## System and Process Utilities
##


def get_free_space(dirpath: str) -> int:
    """
    Get free space in bytes
    """

    stat = os.statvfs(dirpath)
    return stat.f_bavail * stat.f_frsize


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def is_console_interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def count_active_coroutines() -> int:
    """
    Returns the number of coroutine objects currently tracked by the garbage collector.
    """
    return sum(1 for obj in gc.get_objects() if isinstance(obj, types.CoroutineType))


def get_memory_usage() -> int:
    """
    Returns the maximum resident set size used in bytes
    """
    process = psutil.Process(os.getpid())
    mem_bytes = process.memory_info().rss  # Resident Set Size: memory in RAM
    return mem_bytes


def where_am_i() -> tuple[str, int]:
    frame = inspect.currentframe().f_back
    filename = frame.f_code.co_filename
    line_number = frame.f_lineno
    return (filename, line_number)


## Data Formatting
##


def human_readable_size(num_bytes: int, long: bool = True) -> tuple[float, str]:
    scale = (
        ["bytes", "kilobytes", "megabytes", "gigabytes", "terabytes", "petabytes"]
        if long
        else ["B", "KB", "MB", "GB", "TB", "PB"]
    )
    for idx, word in enumerate(scale[::-1]):
        power = 1024 ** (len(scale) - idx - 1)
        if num_bytes >= power:
            return round(num_bytes / power, 2), word
    return num_bytes, scale[0]


def human_readable_size_str(num_bytes: int, long: bool = True) -> str:
    t = human_readable_size(num_bytes, long)
    return f"{t[0]} {t[1]}"


def text_to_html_code_block(text: str) -> str:
    escaped_text = html.escape(text)
    return f"<pre><code>{escaped_text}</code></pre>"


def dict_remove_empty_values(data: Any) -> Any:
    """
    Recursively remove keys with empty string values from a nested JSON-like dictionary.

    :param data: The dictionary to process
    :return: A new dictionary with empty string values removed
    """
    if isinstance(data, dict):
        return {
            k: dict_remove_empty_values(v)
            for k, v in data.items()
            if v and (not isinstance(v, (dict, list)) or dict_remove_empty_values(v))
        }
    elif isinstance(data, list):
        return [dict_remove_empty_values(item) for item in data if item != ""]
    else:
        return data


def is_number(s: str) -> bool:
    try:
        float(s.strip())  # Try to convert the string to a float
        return True
    except ValueError:
        return False
