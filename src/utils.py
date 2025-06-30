# misc functions
import asyncio
import gc
import hashlib
import html
import inspect
import logging
import os
import random
import re
import shutil
import socket
import sys
import tempfile
import threading
import time
import types
import zipfile
from contextlib import closing
from typing import Coroutine, Dict, Iterator, List, Tuple

import aiofiles
import aiohttp
import psutil
import yaml
from playwright.async_api import Page


def proj_path(filepath: str) -> str:
    """
    The script runs in src folder, this function converts path
    to be based on the parent folder of src.
    """
    utils_dir = os.path.dirname(__file__)
    return os.path.join(utils_dir, "..", filepath)


def read_config() -> dict:
    with open(proj_path("config/config.yaml"), "r") as f:
        return yaml.safe_load(f)


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
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def playwright_wait_for_any(page: Page, urls: Dict[str, str], timeout=30):
    """
    Wait for any of the urls to be loaded, return when any of them is loaded.
    Raise timeout if all timeout
    """

    tasks = {
        asyncio.create_task(page.wait_for_url(url, timeout=timeout * 1000)): tag
        for tag, url in urls.items()
    }

    done, pending = await asyncio.wait(
        tasks.keys(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )

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


def human_readable_size(num_bytes: int, long=True) -> Tuple[float, str]:
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


def human_readable_size_str(num_bytes: int, long=True) -> str:
    t = human_readable_size(num_bytes, long)
    return f"{t[0]} {t[1]}"


def zip_listfiles(zippath: str) -> list[str]:
    with zipfile.ZipFile(zippath) as z:
        return z.namelist()


# use external tools for this one because all
# python libs fails miserably when encourtering
# some compression formats
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


async def is_link_alive(url, timeout=10):
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            async with session.head(url, allow_redirects=True) as resp:
                return resp.status == 200
    except Exception as e:
        # Could log the exception here if desired
        return False


async def async_hash(
    filepath, hashfunc: callable = hashlib.md5, bufsize=1024**2
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
    filepath, hashfuncs: list[callable], bufsize=1024**2
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


def sync_multihash(filepath: str, hashfuncs: list[callable]) -> dict[str, str]:
    """
    Synchronously load the entire file into memory, then compute multiple hashes in parallel,
    one per thread.
    Requires a lot of memory significantly faster than aysnc mulithash.
    """
    with open(filepath, "rb") as f:
        data = f.read()

    results = {}
    threads = []

    def compute_hash(hashfunc):
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


def is_console_interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


class AsyncCounter:
    def __init__(self):
        self.value = 0
        self._lock = asyncio.Lock()

    async def increment(self):
        async with self._lock:
            self.value += 1
            return self.value

    async def decrement(self):
        async with self._lock:
            self.value -= 1
            return self.value

    async def set(self, new_val):
        async with self._lock:
            self.value = new_val
            return self.value

    async def reset(self):
        async with self._lock:
            self.value = 0
            return self.value

    def get_value(self):
        return self.value


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


async def run_with_shutdown(c: Coroutine, e: asyncio.Event) -> any:
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


def text_to_html_code_block(text: str) -> str:
    escaped_text = html.escape(text)
    return f"<pre><code>{escaped_text}</code></pre>"


def dict_remove_empty_values(data):
    """
    Recursively remove keys with empty string values from a nested JSON-like dictionary.

    :param data: The dictionary to process
    :return: A new dictionary with empty string values removed
    """
    if isinstance(data, dict):
        return {
            k: dict_remove_empty_values(v)
            for k, v in data.items()
            if v != ""
            and (not isinstance(v, (dict, list)) or dict_remove_empty_values(v))
        }
    elif isinstance(data, list):
        return [dict_remove_empty_values(item) for item in data if item != ""]
    else:
        return data


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


def where_am_i():
    frame = inspect.currentframe().f_back
    filename = frame.f_code.co_filename
    line_number = frame.f_lineno
    return (filename, line_number)


async def generate_placeholder():
    """
    The code logic requires a placeholder to be uploaded in order to
    create the bucket and init the whole upload procedure.
    However, a blank file or if too simple would trigger IA's spam filter
    and make bucket creation unsuccessful.
    This function solves that, by randomize AiW and fill the placeholder.
    """
    async with aiofiles.open(proj_path("data/aiw.txt"), mode="r") as f:
        aiw_text = await f.read()
    aiw_paragraphs = aiw_text.split("\n\n")
    random.shuffle(aiw_paragraphs)
    aiw_paragraphs = random.choices(
        aiw_paragraphs, k=int(len(aiw_paragraphs) * random.uniform(0.6, 0.9))
    )
    random_text = "\n\n".join(aiw_paragraphs)
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="placeholder_", suffix=".txt")
    os.close(tmp_fd)  # Close the os-level file descriptor
    async with aiofiles.open(tmp_path, mode="w") as f:
        await f.write(random_text)
    return tmp_path


def divide_into_chunks(total_size: int, num_chunks: int) -> List[Tuple]:
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


def run_async_in_thread(coro: Coroutine) -> threading.Thread:
    """
    Run an async coroutine in a separate thread with its own event loop.
    The thread and event loop shut down automatically when the coroutine is done.
    """

    def thread_entry():
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


class TouchAndOpen:
    def __init__(self, filepath, mode="w", encoding="utf-8"):
        self.filepath = filepath
        self.mode = mode
        self.encoding = encoding
        self.file = None

    def __enter__(self):
        # Ensure all the directories in the filepath exist
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        # Open the file and return the file object
        self.file = open(self.filepath, self.mode, encoding=self.encoding)
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close the file when exiting
        if self.file:
            self.file.close()


def is_number(s: str) -> bool:
    try:
        float(s.strip())  # Try to convert the string to a float
        return True
    except ValueError:
        return False
