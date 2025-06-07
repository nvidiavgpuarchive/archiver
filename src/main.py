import asyncio
import json
import os
import random
import shutil
import time
from typing import TypedDict

import aiofiles
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, ProgressColumn, Task
from rich.text import Text

import utils
from downloader import AsyncChunkDownloader
from gmail_client import GmailClient
from ia import IAClient
from logger import get_logger
from portal import NvidiaWebPortal, MetaInfo, DownloadInfo

_logger = get_logger(__name__)

fail_counter = utils.AsyncCounter()
idle_workers: dict[int, bool] = {}

shutdown_event = asyncio.Event()
state_filelock = asyncio.Lock()


class SpeedColumnBase(ProgressColumn):
    def __init__(self, get_value, icon, color):
        super().__init__()
        self.get_value = get_value  # callable that returns current total bytes
        self.icon = icon
        self.color = color
        self._last_time = None
        self._last_bytes = None
        self._last_speed = 0.0

    def render(self, task: Task) -> Text:
        current_time = time.time()
        total_bytes = self.get_value()

        if self._last_time is None:
            self._last_time = current_time
            self._last_bytes = total_bytes
            speed = 0.0
        else:
            elapsed = current_time - self._last_time
            bytes_diff = total_bytes - self._last_bytes
            if elapsed > 0.5:
                speed = bytes_diff / elapsed
                self._last_time = current_time
                self._last_bytes = total_bytes
                self._last_speed = speed
            else:
                speed = self._last_speed

        speed_str = f"{utils.human_readable_size_str(int(speed))}/s"
        return Text(f"{self.icon} {speed_str}", style=self.color)


DownloadSpeedColumn = lambda: SpeedColumnBase(get_value=lambda: AsyncChunkDownloader.global_bytes_downloaded, icon="⬇",
                                              color="green")

UploadSpeedColumn = lambda: SpeedColumnBase(get_value=lambda: IAClient.global_bytes_uploaded, icon="⬆", color="cyan")

progress_bar = Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn(), DownloadSpeedColumn(),
                        UploadSpeedColumn(), refresh_per_second=10, transient=True)


class TaskDict(TypedDict):
    meta: MetaInfo
    download: DownloadInfo


# in the json file use bucket name for key
# and this for value
class StateDict(TypedDict):
    meta: MetaInfo
    hash_dict: dict[str, str]
    time_added: float
    upload_verified: bool  # will set to true if ia done processing
    # no matter the file is actually good or not
    is_complete: bool  # will set to true only if passed verification


# handles the actual logic
async def worker(worker_id: int, config: dict, queue: asyncio.Queue):
    _logger.info(f"Worker {worker_id} started.")
    global idle_workers
    while not shutdown_event.is_set():
        try:
            task: TypedDict = await asyncio.wait_for(queue.get(), timeout=1)
            _logger.info(f"Worker {worker_id} got task: {task['name']}")
            idle_workers[worker_id] = False
        except:
            idle_workers[worker_id] = True
            continue

        if not utils.is_link_alive(task["download"]["url"]):
            _logger.info(f"Worker {worker_id} download link expired, skipping. ")
            continue

        download_dir = config["global"]["download_dir"]
        https_proxy = config["global"]["https_proxy"]
        num_chunks = config["downloader"]["num_chunks"]

        filepath_list = []
        try:
            async with AsyncChunkDownloader(task["download"]["url"], download_dir, num_chunks=num_chunks,
                                            proxy=https_proxy) as downloader:
                filepath_list.append(await downloader.download())
                main_filename = os.path.basename(filepath_list[0])  # use for metadata
            if task["download"]["checksumUrl"] != "":
                async with AsyncChunkDownloader(task["download"]["checksumUrl"], download_dir,
                                                proxy=https_proxy) as downloader:
                    filepath_list.append(await downloader.download())
        except Exception as e:
            await fail_counter.increment()
            _logger.warning(f"Worker {worker_id} download failed with exception {str(e)}, skipping.")
            continue

        hash_list = await asyncio.gather(*(utils.async_md5(filepath) for filepath in filepath_list))
        hash_dict = {os.path.basename(filepath_list[i]): hash_list[i] for i in range(len(hash_list))}

        _logger.info(f"Worker {worker_id} start uploading to IA.")
        async with IAClient(config["ia"]["s3_access_key"], config["ia"]["s3_secret_key"],
                            https_proxy=https_proxy) as ia:
            bucket_name = config["ia"]["bucket_prefix"] + main_filename
            if await ia.head_bucket(bucket_name):
                _logger.warning(f"Bucket {bucket_name} already exists, skipping upload.")
                continue

            try:
                await ia.create_bucket(bucket=bucket_name, filepaths=filepath_list, meta_mediatype="data",
                                       meta_title=task["meta"]["description"],
                                       meta_description=config["ia"]["common_description"],
                                       meta_collection=config["ia"]["collection"],
                                       # open_source_software, test_collection
                                       custom_metadata=task["meta"], )
            except Exception as e:
                await fail_counter.increment()
                _logger.warning(f"Upload to IA possibly unsuccessful with exception {str(e)}, proceed anyway. ")

        _logger.info(f"Worker {worker_id} upload to IA finished, record and remove any leftover files.")

        state_dict: StateDict = {"meta": task["meta"], "hash_dict": hash_dict, "time_added": time.time(),
                                 "upload_verified": False, "is_complete": False}

        state_filepath = utils.proj_path("config/state.json")
        async with state_filelock:
            async with aiofiles.open(state_filepath, "r+") as f:
                state_json = json.loads(await f.read())
                state_json[bucket_name] = state_dict
                await f.seek(0)
                await f.write(json.dumps(state_json, indent=4))
                await f.truncate()

        for fp in filepath_list:
            os.remove(fp)

    _logger.info(f"Worker {worker_id} stopped.")


async def verification_worker(config, delay=10, n=8):
    """
    Randomly selects n unverified entires from state.json every delay seconds
    and attempts to verify them.
    """

    _logger.info("Verification worker started.")
    state_filepath = utils.proj_path("config/state.json")

    while not shutdown_event.is_set():
        await asyncio.sleep(delay)

        async with state_filelock:
            async with aiofiles.open(state_filepath, "r") as f:
                state_dict = json.loads(await f.read())
        unverified = [item for item in state_dict.items() if not item["upload_verified"]]
        if not unverified:
            continue

        to_verify = random.choices(unverified, k=min(len(unverified), n))
        async with IAClient(  # for verification purpose access key is not needed
                "", "", config["global"]["https_proxy"]) as ia:
            results = await asyncio.gather(
                *(ia.verify_bucket(item[0], md5_dict=item[1]["hash_dict"]) for item in to_verify),
                return_exceptions=True)

        async with state_filelock:
            async with aiofiles.open(state_filepath, "r") as f:
                state_dict = json.loads(await f.read())
            # result is a list of bucket names, or exception
            for idx, result in enumerate(results):
                if isinstance(result, Exception):  # timeout
                    continue
                bucket = to_verify[idx][0]
                state_dict[bucket]["upload_verified"] = True
                state_dict[bucket]["is_complete"] = not bool(result)
            async with aiofiles.open(state_filepath, "w") as f:
                json_str = json.dumps(state_dict, indent=4)
                await f.write(json_str)


async def main():
    # config and state file setup
    config = utils.read_config()
    state_filepath = utils.proj_path("config/state.json")
    if not os.path.exists(state_filepath):
        with open(state_filepath, "w") as f:
            f.write("{}")
    download_dir = config["global"]["download_dir"]
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)
    elif os.listdir(download_dir):
        _logger.fatal(f"Download dir {download_dir} not empty, exiting.")
        exit(-1)

    progress_bar.start()
    progress_bar_task = progress_bar.add_task("Processing", total=0, start=False)

    # init workers
    queue = asyncio.Queue()

    async_workers = [asyncio.create_task(worker(i, config, queue, )) for i in range(config["global"]["num_workers"])]
    async_verification = asyncio.create_task(verification_worker(config, delay=10, n=8))

    # main routine
    gmail_client = GmailClient(config["imap"]["host"], config["imap"]["port"], config["imap"]["username"],
                               config["imap"]["password"])
    portal = NvidiaWebPortal(username=config["portal"]["nvidia_username"], password=config["portal"]["nvidia_password"],
                             https_proxy=config["global"]["https_proxy"], gmail_client=gmail_client)
    asyncio.create_task(gmail_client.connect())

    meta_list: list[MetaInfo] = []  # cache for all meta info
    meta_added_list: list[MetaInfo] = []  # meta that already queued

    def same_meta(meta1: MetaInfo, meta2: MetaInfo):
        return meta1["downloadId"] == meta2["downloadId"] or meta1["description"] == meta2["description"]

    try:
        while fail_counter.value < 20:  # hardcoded for now
            await asyncio.sleep(1)
            if not await portal.is_loggedin():  # login and refresh download list
                await portal.login()
                meta_list = await portal.list_downloads()
                progress_bar.update(progress_bar_task, total=len(meta_list))

            idle_cnt = len([k for k, v in idle_workers.items() if v])
            if not idle_cnt:
                continue

            async with state_filelock:
                async with aiofiles.open(state_filepath, "r") as f:
                    state_json = json.loads(await f.read())

            meta_to_download = [meta for meta in meta_list if
                                not any(same_meta(meta, m) for m in meta_added_list) and not any(
                                    same_meta(meta, m) for m in state_json.values())]
            if not meta_to_download: break
            progress_bar.update(progress_bar_task, advance=len(meta_to_download))

            meta_to_queue = random.choices(meta_to_download, k=min(len(meta_to_download), idle_cnt))
            try:
                download_to_queue = await asyncio.gather(
                    *(portal.get_download_url(m["downloadId"]) for m in meta_to_queue))
            except Exception as e:
                await fail_counter.increment()
                _logger.warning(f"Failed to get download url with exception {str(e)}, retrying.")
                continue

            for meta, download in zip(meta_to_queue, download_to_queue):
                if not download:
                    continue
                meta_added_list.append(meta)
                task: TaskDict = {"meta": meta, "download": download}
                await queue.put(task)
                _logger.info(f"Task {meta['description']} queued.")
    except KeyboardInterrupt:
        _logger.warning("Initiating graceful shutdown...")
    finally:
        progress_bar.stop()
        shutdown_event.set()
        await asyncio.gather(*async_workers)
        await asyncio.gather(*async_verification)

        shutil.rmtree(download_dir)
        os.mkdir(download_dir)
        _logger.info("Program stoppped.")


if __name__ == "__main__":
    exit(asyncio.run(main()))
