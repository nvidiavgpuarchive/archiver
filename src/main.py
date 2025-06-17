import asyncio
import hashlib
import os
import shutil
import signal
import tempfile
import traceback
from datetime import datetime
from typing import TypedDict

from pony.orm import db_session, select

from db import (
    ArchiveEntry,
    DriverMeta,
    FileChecksum,
    VerificationState,
    sync_meta_to_db,
)
from gmail_client import GmailClient
from logger import get_logger
from main_tui import *
from portal import DownloadInfo, MetaInfo, NvidiaWebPortal
from utils import sync_multihash, zip_verify_crc

_logger = get_logger(__name__)

worker_states: dict[str, str] = {}
verification_idle = False

fail_counter = utils.AsyncCounter()  # main loop returns if a number of failures
shutdown_event = asyncio.Event()  # once triggered all coroutines must return
# ui worker owns a seperate shutdown event
uiworker_shutdown_event = asyncio.Event()
ctrl_c_counter = 0


class TaskDict(TypedDict):
    meta: DriverMeta
    download: DownloadInfo


# ui worker takes care of progress bar
# this worker must be run in an seperate thread
async def ui_worker(config: dict, delay=1, report_interval=30):
    # AyncCounter used by counters
    _logger = get_logger("ui")
    _logger.debug("UI Worker started.")
    live_display.start()

    progress_bar_started = False
    while not uiworker_shutdown_event.is_set():
        await asyncio.sleep(delay)

        # update counters and progress
        with db_session:
            complete_cnt = select(
                a
                for a in ArchiveEntry
                if a.verificationState == VerificationState.COMPLETE
            ).count()
            incomplete_cnt = select(
                a
                for a in ArchiveEntry
                if a.verificationState == VerificationState.INCOMPLETE
            ).count()
            not_verified_cnt = select(
                a
                for a in ArchiveEntry
                if a.verificationState == VerificationState.NOT_VERIFIED
            ).count()
            total_tasks = DriverMeta.select().count()

        await complete_counter.set(complete_cnt)
        await incomplete_counter.set(incomplete_cnt)
        await not_verified_counter.set(not_verified_cnt)

        # report status via logger
        # msg = "".join([f"[{k}:{v}]" for k, v in worker_states.items()])
        # _logger.info(msg)

        # progress bar related

        if not progress_bar_started:
            progress_bar_started = True
            progress_bar.start_task(progress_bar_task)
            status_bar.start_task(status_bar_task)

        progress_bar.update(
            progress_bar_task, total=total_tasks, completed=complete_cnt
        )

    progress_bar.stop()
    status_bar.stop()
    live_display.stop()
    _logger.debug("UI worker stopped.")


# handles the actual logic
async def worker(worker_id: int, config: dict, queue: asyncio.Queue):
    _logger = get_logger(f"worker {worker_id}")
    _logger.info(f"Worker {worker_id} started.")
    global worker_states
    while not shutdown_event.is_set():
        try:
            task: TypedDict = await asyncio.wait_for(queue.get(), timeout=1)
            _logger.info(f"Got task: '{task["meta"].description}'")
            worker_states[f"{worker_id}"] = "busy"
        except asyncio.TimeoutError:
            worker_states[f"{worker_id}"] = "idle"
            continue
        except Exception as e:
            _logger.exception(f"An error occured.")
            worker_states[f"{worker_id}"] = "busy"
            continue

        try:
            # download
            worker_states[f"{worker_id}"] = "D"
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

            # check if all files exist first
            worker_states[f"{worker_id}"] = "C"
            if utils.check_non_exist(filepath_list):
                await asyncio.to_thread(utils.remove_files, filepath_list)
                _logger.error("Some file download failed, skip the task.")
                continue

            # hashing, crc check,  verify
            _logger.info(f"Generate checksum for '{main_filename}'")

            crc_task = asyncio.create_task(zip_verify_crc(main_filepath))

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
            hash_dict = {
                os.path.basename(filepath_list[i]): hash_list[i]
                for i in range(len(hash_list))
            }
            main_checksum_d = hash_dict[main_filename]  # dict[str, str]

            if await crc_task:
                _logger.info(
                    f"CRC checksum passed. '{
                        main_filename}' is good."
                )
            else:
                _logger.error(
                    f"CRC checksum failed, '{
                        main_filename}' corrupted. Skipping"
                )
                await fail_counter.increment()
                continue

            # custom metadata & description
            ia_description = config["ia"]["common_description"]
            if main_filename.endswith(".zip"):
                file_list = await asyncio.to_thread(utils.zip_listfiles, main_filepath)
                ia_description += (
                    "<br><p><strong>Files</strong></p>"
                    + utils.text_to_html_code_block("\n".join(file_list))
                    + "<br><hr>"
                )
            custom_metadata = task["meta"].to_json() | {
                "checksum-" + k: v for k, v in main_checksum_d.items()
            }
            custom_metadata["description"] = ia_description

            # uploading

            _logger.info("Start uploading to IA.")
            worker_states[f"{worker_id}"] = "U"
            async with IAClient(
                config["ia"]["s3_access_key"],
                config["ia"]["s3_secret_key"],
                https_proxy=https_proxy if config["ia"]["use_proxy"] else None,
                multipart_chunksize=1024**2 * 256,  # 256MB
            ) as ia:
                # determine bucket name
                bucket_name = config["ia"]["bucket_prefix"] + main_filename
                if await ia.head_bucket(bucket_name):
                    info = await ia.get_info(bucket_name)
                    if info["metadata"]["downloadid"] != task["meta"].downloadId:
                        _logger.warning(
                            f"{bucket_name} conflicted, downloadId mismatch, skip."
                        )
                        await asyncio.to_thread(utils.remove_files, filepath_list)
                        continue

                try:
                    await ia.create_bucket(
                        bucket=bucket_name,
                        filepaths=filepath_list,
                        meta_mediatype="data",
                        meta_title=task["meta"].description,
                        meta_description=ia_description,
                        meta_collection=config["ia"]["collection"],
                        # open_source_software, test_collection
                        custom_metadata=custom_metadata,
                        multipart=os.path.getsize(main_filepath) > 1024**2 * 256,
                    )
                except Exception as e:
                    await fail_counter.increment()
                    traceback.print_exc()
                    _logger.warning(
                        f"Upload to IA possibly unsuccessful with exception '"
                        f"{str(e)}', proceed anyway. "
                    )

            _logger.info(
                f"Upload to IA finished, record and remove any leftover files."
            )

            # update db

            def _update_db():  # run with asyncio.to_thread
                with db_session():
                    existing_entry = ArchiveEntry.get(identifier=bucket_name)
                if not existing_entry:  # if existing, simply wait for verification
                    with db_session():
                        main_dbentry = FileChecksum.get(md5=main_checksum_d["md5"])
                        if main_dbentry:
                            main_dbentry.update_from_hash_dict(
                                main_filepath, main_checksum_d
                            )
                        else:
                            main_dbentry = FileChecksum.from_hash_dict(
                                main_filepath, main_checksum_d
                            )

                        meta = DriverMeta.get(downloadId=task["meta"].downloadId)

                        archive_entry = ArchiveEntry.get(identifier=bucket_name)
                        if archive_entry:
                            archive_entry.meta = meta
                            archive_entry.files = [main_dbentry]
                            archive_entry.verificationState = (
                                VerificationState.NOT_VERIFIED
                            )
                        else:
                            ArchiveEntry(
                                identifier=bucket_name,
                                meta=meta,
                                files=[main_dbentry],
                                verificationState=VerificationState.NOT_VERIFIED,
                            )

            await asyncio.to_thread(_update_db)
            _logger.info(f"'{bucket_name}' updated in db.")

            if fail_counter.value > 0:
                await fail_counter.decrement()  # reduce consequtive fail count
        except Exception as e:
            _logger.exception(f"Worker exception {e}:")
            continue
        finally:
            await asyncio.to_thread(utils.remove_files, filepath_list)

    worker_states[f"{worker_id}"] = "dead"
    _logger.info(f"Stopped.")


async def verification_worker(config, delay=10, n=8):
    """
    Randomly selects n unverified entire from state.json every delay seconds
    and attempts to verify them.

    Verificatoin worker have very long blocking db session, so this must not
    run in the same async loop as the main program.
    """
    global verification_idle

    _logger = get_logger("verification")
    _logger.info("Verification worker started.")

    worker_states["veri"] = "good"
    last_report_time = 0
    while not shutdown_event.is_set():
        await asyncio.sleep(delay)

        with db_session:
            unverified_cnt = select(
                a
                for a in ArchiveEntry
                if a.verificationState == VerificationState.NOT_VERIFIED
            ).count()
            _logger.debug(
                f"Verification worker running, {
                    unverified_cnt} to verify."
            )

            if asyncio.get_event_loop().time() - last_report_time > 30:
                _logger.info(
                    f"Verification running, {
                        unverified_cnt} waiting."
                )
                last_report_time = asyncio.get_event_loop().time()

            if not unverified_cnt:
                verification_idle = True
                continue
            verification_idle = False

        with db_session:
            sample_cnt = min(unverified_cnt, n)
            toverify_archives = select(
                a
                for a in ArchiveEntry
                if a.verificationState == VerificationState.NOT_VERIFIED
            )[:sample_cnt]
            toverify_checksums = [a.files for a in toverify_archives]
            args = [
                {
                    "bucket": toverify_archives[i].identifier,
                    "md5_dict": {f.filename: f.md5 for f in toverify_checksums[i]},
                    "timeout": 5,
                }
                for i in range(sample_cnt)
            ]

            async with IAClient(  # for verification purpose access key is not needed
                "", "", config["global"]["https_proxy"]
            ) as ia:
                # results are tuples of filelist, meta
                results = await asyncio.gather(
                    *(ia.verify_bucket(**args[i]) for i in range(sample_cnt)),
                    return_exceptions=True,
                )

            # result is a list of bucket names, or exception
            for idx, result in enumerate(results):
                if isinstance(result, Exception):  # timeout, unverified
                    continue
                result_fl, ia_meta = result
                if result_fl:  # incomplete
                    toverify_archives[idx].verificationState = (
                        VerificationState.INCOMPLETE
                    )
                else:
                    toverify_archives[idx].verificationState = (
                        VerificationState.COMPLETE
                    )
                    toverify_archives[idx].ia_meta = ia_meta
                _logger.info(
                    f"Bucket '{
                        toverify_archives[idx].identifier}' verified to be "
                    f"{not bool(result_fl)}"
                )

    worker_states["veri"] = "dead"
    _logger.info("Verification worker stopped.")


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
        _logger.warning("Force terminating...")
        try:
            live_display.stop()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            crash_file = os.path.join(
                tempfile.gettempdir(), f"vgpucrash_{timestamp}.log"
            )

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
    global worker_states

    # signal setup
    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.siginterrupt(signal.SIGINT, False)
    except AttributeError:
        pass  # Not available on all platforms

    # config and db setup
    config = utils.read_config()

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
    async_ui_thread = utils.run_async_in_thread(ui_worker(config))

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

    meta_added_list: list[MetaInfo] = []  # meta that already queued

    try:
        await portal.load_session_from_cookies()
        while fail_counter.value < 5:  # hardcoded for now
            await asyncio.sleep(1)
            if not await portal.is_loggedin():  # login and refresh download list
                await portal.login()
                sync_meta_to_db(await portal.list_meta())

            idle_cnt = len(
                [k for k, v in worker_states.items() if k.isnumeric() and v == "idle"]
            )
            if not idle_cnt:
                continue

            # get meta tasks
            def _db_task():
                with db_session:
                    unarchived_metas = select(
                        m
                        for m in DriverMeta
                        if not ArchiveEntry.select(lambda a: a.meta == m)
                    )[:]
                    incomplete_archives = select(
                        a
                        for a in ArchiveEntry
                        if a.verificationState == VerificationState.INCOMPLETE
                    )[:]
                    meta_to_download = [
                        a.meta for a in incomplete_archives
                    ] + unarchived_metas
                    meta_to_download = [
                        m for m in meta_to_download if m not in meta_added_list
                    ]

            await asyncio.to_thread(_db_task)

            # TODO: ui worker report each worker state periodically
            if not meta_to_download or not config["global"]["num_workers"]:
                if verification_idle:
                    break
                else:
                    indicator_column.update("Verifying", "bright_yellow")

            try:
                meta_to_queue = meta_to_download[: min(len(meta_to_download), idle_cnt)]
                download_to_queue = await asyncio.gather(
                    *(portal.get_download_url(m.downloadId) for m in meta_to_queue)
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
                _logger.info(f"Task '{meta.description}' queued.")

            worker_states = {k: "busy" for k in worker_states if k.isnumeric()}

    except Exception as e:
        _logger.fatal(f"Unexpected exception '{e}', quitting.")
        traceback.print_exc()
    finally:
        indicator_column.update("Stopping", "dark_orange3")

        shutdown_event.set()
        await asyncio.gather(*async_workers)
        await asyncio.gather(async_verification)
        uiworker_shutdown_event.set()
        async_ui_thread.join()

        try:
            shutil.rmtree(download_dir)
        except:
            pass
        os.mkdir(download_dir)
        _logger.info("Program stopped.")


if __name__ == "__main__":
    exit(asyncio.run(main()))
