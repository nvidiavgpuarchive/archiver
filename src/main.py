import asyncio
import hashlib
import json
import os
import random
import shutil
import signal
import tempfile
import threading
import time
import traceback
from datetime import datetime
from typing import Text, TypedDict

import aiofiles
from rich.live import Live
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table

import utils
from downloader import AsyncChunkDownloader
from gmail_client import GmailClient
from ia import IAClient
from logger import get_logger
from portal import DownloadInfo, MetaInfo, NvidiaWebPortal, same_meta
from tui_elements import CounterColumn, SpeedColumnBase, VarTextColumn
from utils import proj_path, sync_multihash, zip_verify_crc

_logger = get_logger(__name__)

# various counters and state trackers

idle_workers: dict[int, bool] = {}
verification_idle = False
fail_counter = utils.AsyncCounter()  # main loop returns if a number of failures
complete_counter = utils.AsyncCounter()
incomplete_counter = utils.AsyncCounter()

shutdown_event = asyncio.Event()  # once triggered all coroutines must return
state_filelock = asyncio.Lock()

ctrl_c_counter = 0

# TextUI setup

indicator_column = VarTextColumn("Running", "green")
verified_column = CounterColumn(
    complete_counter.get_value, label="✔", color="bright_green"
)
incomplete_column = CounterColumn(
    incomplete_counter.get_value, label="✗", color="bright_red"
)
coroutines_column = CounterColumn(
    utils.count_active_coroutines, label="Coroutines", color="cyan"
)
threads_column = CounterColumn(threading.active_count, label="Threads", color="yellow")
mem_column = CounterColumn(
    utils.get_memory_usage, label="Mem", color="magenta", bytes_conv=True
)
download_speed_column = SpeedColumnBase(
    get_value=lambda: AsyncChunkDownloader.global_bytes_downloaded,
    icon="⬇",
    color="green",
)
upload_speed_column = SpeedColumnBase(
    get_value=lambda: IAClient.global_bytes_uploaded, icon="⬆", color="blue"
)

# upper progress bar
progress_bar = Progress(
    indicator_column,
    BarColumn(),
    TextColumn("{task.completed}/{task.total}"),
    verified_column,
    incomplete_column,
    refresh_per_second=10,
    transient=True,
)

status_bar = Progress(
    download_speed_column,
    upload_speed_column,
    TextColumn("[bold]|[/bold]"),
    threads_column,
    coroutines_column,
    mem_column,
)
progress_bar_task = progress_bar.add_task("Processing", total=0, start=False)
status_bar_task = status_bar.add_task("Status", total=0, start=False)
progress_bar_task_started = False


def live_display_render():
    table = Table.grid(padding=(0, 1))
    # Add a separator row at the top (using dashes, adjust width as needed)
    separator = Text("")
    table.add_row(separator)
    table.add_row(progress_bar)
    table.add_row(status_bar)
    return table


live_display = Live(live_display_render(), refresh_per_second=10, transient=True)
live_display.start()


# TypeDicts for IDE hint


class TaskDict(TypedDict):
    meta: MetaInfo
    download: DownloadInfo


# in the json file use bucket name for key
# and this for value
class StateDict(TypedDict):
    meta: MetaInfo
    md5_dict: dict[str, str]
    time_added: float
    upload_verified: bool  # will set to true if ia done processing
    # no matter the file is actually good or not
    is_complete: bool  # will set to true only if passed verification


# Coroutines


# handles the actual logic
async def worker(worker_id: int, config: dict, queue: asyncio.Queue):
    _logger = get_logger(f"worker {worker_id}")
    _logger.info(f"Worker {worker_id} started.")
    global idle_workers
    while not shutdown_event.is_set():
        try:
            task: TypedDict = await asyncio.wait_for(queue.get(), timeout=1)
            _logger.info(f"Got task: '{task["meta"]['description']}'")
            idle_workers[worker_id] = False
        except asyncio.TimeoutError:
            idle_workers[worker_id] = True
            continue
        except Exception as e:
            _logger.error(f"Encountered error: '{e}'")
            idle_workers[worker_id] = False
            continue

        # download
        if not await utils.is_link_alive(task["download"]["url"]):
            _logger.info(f"Download link expired, skipping. ")
            continue

        download_dir = config["global"]["download_dir"]
        https_proxy = config["global"]["https_proxy"]
        num_chunks = config["downloader"]["num_chunks"]

        filepath_list = []
        try:
            async with AsyncChunkDownloader(
                task["download"]["url"],
                download_dir,
                num_chunks=num_chunks,
                proxy=https_proxy,
            ) as downloader:
                main_filepath = await utils.run_with_shutdown(
                    downloader.download(), shutdown_event
                )
                if not main_filepath:
                    continue
                main_filename = os.path.basename(main_filepath)
                filepath_list.append(main_filepath)
            if task["download"]["checksumUrl"] != "":
                async with AsyncChunkDownloader(
                    task["download"]["checksumUrl"], download_dir, proxy=https_proxy
                ) as downloader:
                    checksum_filepath = await utils.run_with_shutdown(
                        downloader.download(), shutdown_event
                    )
                    if not checksum_filepath:
                        continue
                    filepath_list.append(checksum_filepath)
        except Exception as e:
            await fail_counter.increment()
            _logger.warning(
                f"Download failed with exception '{
                    str(e)}', skipping."
            )
            continue

        # hashing, crc check,  verify
        _logger.info(f"Generate checksum for '{main_filename}'")

        crc_task = asyncio.create_task(asyncio.to_thread(zip_verify_crc, main_filepath))

        if config["global"]["hashing_method"] == "sync":
            hash_list = await asyncio.gather(
                *(
                    asyncio.to_thread(
                        sync_multihash,
                        fp,
                        [
                            hashlib.md5,
                            hashlib.sha1,
                            hashlib.sha256,
                            hashlib.sha512,
                            hashlib.blake2b,
                        ],
                    )
                    for fp in filepath_list
                )
            )
        else:
            hash_list = await asyncio.gather(
                *(
                    sync_multihash(
                        fp,
                        [
                            hashlib.md5,
                            hashlib.sha1,
                            hashlib.sha256,
                            hashlib.sha512,
                            hashlib.blake2b,
                        ],
                    )
                    for fp in filepath_list
                )
            )
        hash_dict = {
            os.path.basename(filepath_list[i]): hash_list[i]
            for i in range(len(hash_list))
        }
        md5_dict = {k: v["md5"] for k, v in hash_dict.items()}
        main_checksum_d = hash_dict[main_filename]  # dict[str, str]

        if await crc_task:
            _logger.info(f"CRC checksum passed. '{main_filename}' is good.")
        else:
            _logger.error(
                f"CRC checksum failed, '{
                    main_filename}' corrupted. Skipping"
            )
            await fail_counter.increment()
            continue

        if "checksum_filepath" in locals():
            async with aiofiles.open(checksum_filepath, "r") as f:
                checksum = await f.read()
            for c in main_checksum_d.values():
                if c in checksum:
                    _logger.info(
                        f"Checksum test passed for' {
                            main_filename}' "
                    )
                    break
            else:
                _logger.error(
                    f"Checksum test failed for '{
                        main_filename}', skipping  "
                )
                continue

        # custom metadata & description
        description = config["ia"]["common_description"]
        if main_filename.endswith(".zip"):
            file_list = await asyncio.to_thread(utils.zip_listfiles, main_filepath)
            description += (
                "<br><p><strong>Files</strong></p>"
                + utils.text_to_html_code_block("\n".join(file_list))
                + "<br><hr>"
            )
        custom_metadata = task["meta"] | {
            "checksum-" + k: v for k, v in main_checksum_d.items()
        }
        custom_metadata["description"] = description

        # uploading

        _logger.info("Start uploading to IA.")
        async with IAClient(
            config["ia"]["s3_access_key"],
            config["ia"]["s3_secret_key"],
            https_proxy=https_proxy,
        ) as ia:
            bucket_name = config["ia"]["bucket_prefix"] + main_filename
            try:
                if await ia.head_bucket(bucket_name):
                    _logger.warning(
                        f"Bucket {
                            bucket_name} already exists, skipping upload."
                    )
                    continue
            except Exception as e:
                _logger.error(
                    f"Exception '{
                        e}' happens when trying to head bucket, skip."
                )
                continue

            try:
                await ia.create_bucket(
                    bucket=bucket_name,
                    filepaths=filepath_list,
                    meta_mediatype="data",
                    meta_title=task["meta"]["description"],
                    meta_description=description,
                    meta_collection=config["ia"]["collection"],
                    # open_source_software, test_collection
                    custom_metadata=custom_metadata,
                )
            except Exception as e:
                await fail_counter.increment()
                traceback.print_exc()
                _logger.warning(
                    f"Upload to IA possibly unsuccessful with exception '{
                        str(e)}', proceed anyway. "
                )

        _logger.info(f"Upload to IA finished, record and remove any leftover files.")

        state_dict: StateDict = {
            "meta": task["meta"],
            "md5_dict": md5_dict,
            "time_added": time.time(),
            "upload_verified": False,
            "is_complete": False,
        }

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

    _logger.info(f"Stopped.")


async def verification_worker(config, delay=10, n=8):
    """
    Randomly selects n unverified entire from state.json every delay seconds
    and attempts to verify them.
    """

    _logger = get_logger("verification")
    _logger.info("Verification worker started.")
    state_filepath = utils.proj_path("config/state.json")

    last_report_time = 0.0
    global verification_idle
    while not shutdown_event.is_set():
        await asyncio.sleep(delay)

        async with state_filelock:
            async with aiofiles.open(state_filepath, "r") as f:
                state_dict = json.loads(await f.read())
        unverified = [
            item for item in state_dict.items() if not item[1]["upload_verified"]
        ]

        num_complete = len(
            [item for item in state_dict.items() if item[1]["is_complete"]]
        )
        num_incomplete = len(state_dict) - len(unverified) - num_complete

        await complete_counter.set(num_complete)
        await incomplete_counter.set(num_incomplete)
        if not unverified:
            verification_idle = True
            continue
        verification_idle = False

        to_verify = random.choices(unverified, k=min(len(unverified), n))
        async with IAClient(  # for verification purpose access key is not needed
            "", "", config["global"]["https_proxy"]
        ) as ia:
            results = await asyncio.gather(
                *(
                    ia.verify_bucket(item[0], md5_dict=item[1]["md5_dict"], timeout=5)
                    for item in to_verify
                ),
                return_exceptions=True,
            )

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
                _logger.info(
                    f"Bucket '{bucket}' verified to be {
                        not bool(result)}"
                )
            async with aiofiles.open(state_filepath, "w") as f:
                json_str = json.dumps(state_dict, indent=4)
                await f.write(json_str)

        current_time = asyncio.get_event_loop().time()
        if current_time - last_report_time > 60:
            _logger.info(
                f"Running, {len(state_dict)} items in total, {
                    len(unverified)} items to verify."
            )
            last_report_time = current_time


# crash handling
def signal_handler(_, frame):
    global ctrl_c_counter
    ctrl_c_counter += 1

    if ctrl_c_counter == 1:
        # First Ctrl+C: Start graceful shutdown
        _logger.warning(
            "Terminating command received, wait for current loop to complete."
        )
        _logger.warning("Note: IA upload task will not be interrupted.")
        fail_counter.value = 11451419191810
        indicator_column.update("Sig Recved", "red3")
    elif ctrl_c_counter == 2:
        # Second Ctrl+C: Emergency shutdown with crash log
        try:
            live_display.stop()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            crash_file = os.path.join(tempfile.gettempdir(), f"crash_{timestamp}.log")

            with open(crash_file, "w") as f:
                f.write(f"Emergency shutdown triggered at {datetime.now()}\n")
                f.write("\nTraceback at point of interrupt:\n\n\n")
                traceback.print_stack(frame, file=f)
            _logger.warning(f"Crash log saved to '{crash_file}'")
        except Exception as e:
            _logger.warning("Another exception occurred when trying to write log.")
            print(e)
            _logger.warning("Quit without saving the log.")
            pass  # If we can't write the crash log, just exit

        _logger.warning("Force quitting...")
        os._exit(1)

    else:
        # Third or more Ctrl+C: Immediate force quit
        os._exit(2)


async def main():
    # signal setup
    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.siginterrupt(signal.SIGINT, False)
    except AttributeError:
        pass  # Not available on all platforms

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
        _logger.fatal(f"Download dir '{download_dir}' not empty, exiting.")
        exit(-1)

    # init workers
    queue = asyncio.Queue()

    async_workers = [
        asyncio.create_task(
            worker(
                i,
                config,
                queue,
            )
        )
        for i in range(config["global"]["num_workers"])
    ]
    async_verification = asyncio.create_task(verification_worker(config, delay=10, n=8))

    # main routine
    gmail_client = GmailClient(
        config["imap"]["host"],
        config["imap"]["port"],
        config["imap"]["username"],
        config["imap"]["password"],
    )
    portal = NvidiaWebPortal(
        username=config["portal"]["nvidia_username"],
        password=config["portal"]["nvidia_password"],
        https_proxy=config["global"]["https_proxy"],
        gmail_client=gmail_client,
    )
    asyncio.create_task(gmail_client.connect())

    meta_list: list[MetaInfo] = []  # cache for all meta info
    meta_added_list: list[MetaInfo] = []  # meta that already queued

    try:
        await portal.load_session_from_cookies()
        if await portal.is_loggedin():
            meta_list = await portal.list_meta()
            progress_bar.update(progress_bar_task, total=len(meta_list))

        global idle_workers
        global progress_bar_task_started
        task_limit = config["global"]["num_tasks"]
        if task_limit == -1:
            task_limit = 1145141919810

        while fail_counter.value < 20:  # hardcoded for now
            await asyncio.sleep(1)
            if not await portal.is_loggedin():  # login and refresh download list
                await portal.login()
                meta_list = await portal.list_meta()

            if not meta_list:
                meta_list = await portal.list_meta()
                async with aiofiles.open(proj_path("config/list.json"), "w") as f:
                    await f.write(json.dumps(meta_list, indent=4))
                continue

            idle_cnt = len([k for k, v in idle_workers.items() if v])
            if not idle_cnt:
                continue

            async with state_filelock:
                async with aiofiles.open(state_filepath, "r") as f:
                    state_json = json.loads(await f.read())

            meta_to_download = [
                meta
                for meta in meta_list
                if not any(same_meta(meta, m) for m in meta_added_list)
                and not any(same_meta(meta, i["meta"]) for i in state_json.values())
            ]

            if not progress_bar_task_started:
                progress_bar_task_started = True
                progress_bar.start_task(progress_bar_task)

            progress_bar.update(
                progress_bar_task, total=len(meta_list), completed=len(state_json)
            )

            if not meta_to_download or task_limit < 1:
                if verification_idle:
                    break
                else:
                    if "last_message_time" not in locals():
                        last_message_time = -1
                    if asyncio.get_event_loop().time() - last_message_time > 120:
                        _logger.info(
                            "No meta to download or reached task limit, waiting for verification to finish."
                        )
                        last_message_time = asyncio.get_event_loop().time()
                    continue

            meta_to_queue = random.choices(
                meta_to_download, k=min(len(meta_to_download), idle_cnt, task_limit)
            )
            task_limit -= len(meta_to_queue)
            try:
                download_to_queue = await asyncio.gather(
                    *(portal.get_download_url(m["downloadId"]) for m in meta_to_queue)
                )
            except Exception as e:
                await fail_counter.increment()
                _logger.warning(
                    f"Failed to get download url with exception '{
                        str(e)}', retrying."
                )
                continue

            for meta, download in zip(meta_to_queue, download_to_queue):
                if not download:
                    continue
                meta_added_list.append(meta)
                task: TaskDict = {"meta": meta, "download": download}
                await queue.put(task)
                _logger.info(f"Task '{meta['description']}' queued.")

            idle_workers = {k: False for k in idle_workers}
    except Exception as e:
        _logger.fatal(f"Unexpected exception '{e}', quitting.")
        traceback.print_exc()
    finally:
        indicator_column.update("Stopping", "dark_orange3")

        shutdown_event.set()
        await asyncio.gather(*async_workers)
        await asyncio.gather(async_verification)

        progress_bar.stop()
        status_bar.stop()
        live_display.stop()

        try:
            shutil.rmtree(download_dir)
        except:
            pass
        os.mkdir(download_dir)
        _logger.info("Program stopped.")


if __name__ == "__main__":
    exit(asyncio.run(main()))
