# worker and verification worker logic extracted from main
import asyncio
import hashlib
import os
import random
import re
import string
import traceback
from enum import Enum
from typing import Optional, TypedDict

from pony.orm import db_session, select

import db
import utils
from db import ArchiveEntry, DriverMeta, VerificationState, FileChecksum
from downloader import AsyncChunkDownloader
from ia import IAClient
from logger import get_logger
from portal import MetaInfo, DownloadInfo


class WorkerState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    DEAD = "dead"


class QueueItem(TypedDict):
    meta: MetaInfo
    download: DownloadInfo


class Worker:

    def __init__(self, worker_id: int, queue: asyncio.Queue):
        self._worker_id = worker_id
        self._queue = queue

        self._config = utils.read_config()
        self._logger = get_logger(f"worker {worker_id}")
        self._state = WorkerState.IDLE
        self._task: Optional[asyncio.Task] = None
        self._shutdown: Optional[asyncio.Event] = None

    ## Lifetime Control
    ##

    def is_idle(self) -> bool:
        return self._state == WorkerState.IDLE

    async def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._run())
        self._shutdown = asyncio.Event()  # worker manages its own shutdown event
        return self

    async def stop(self):
        if self._task and not self._task.done():
            self._shutdown.set()
            await self._task

    async def _run(self):
        """Main worker loop."""
        self._logger.info(f"Worker {self._worker_id} started.")

        try:
            while not self._shutdown.is_set():
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=1)
                    self._logger.info(f"Got task: '{item['meta']['description']}'")
                    self._state = WorkerState.RUNNING
                except asyncio.TimeoutError:
                    self._state = WorkerState.IDLE
                    continue
                except Exception as e:
                    self._logger.exception(f"Error getting task from queue: {e}")
                    self._state = WorkerState.RUNNING
                    continue

                # Create archive entry first to reserve identifier
                try:
                    identifier = await self._create_pending_archive_entry(item)
                except Exception as e:
                    self._logger.exception(
                        f"Failed to create pending archive entry: {e}"
                    )
                    continue

                # Process the task
                working_dir = self._create_working_dir()
                filepath_list = []
                try:
                    filepath_list = await self._handle_item(
                        item, identifier, working_dir
                    )
                except Exception as e:
                    self._logger.error(f"Task processing failed: {e}")
                    await self._mark_archive_incomplete(identifier)
                finally:
                    await self._cleanup_files(filepath_list, working_dir)
        except Exception as e:
            self._logger.exception(f"Worker {self._worker_id} crashed: {e}")
        finally:
            self._state = WorkerState.DEAD
            self._logger.info("Stopped.")

    ## Actual Business Logic
    ##

    async def _handle_item(
        self, item: QueueItem, identifier: str, working_dir: str
    ) -> list[str]:
        """Handle a single task and return working_dir and filepath_list for cleanup."""

        # Create working directory

        # Download files
        filepath_list = await self._download_files(item, working_dir)
        if not filepath_list:
            raise Exception("Download failed.")

        if self._shutdown.is_set():  # incase shutdown come
            raise Exception("Shutdown received.")

        # Verify files
        if not await self._verify_files(filepath_list):
            raise Exception("CRC checksum failed.")

        # Process checksums and metadata
        main_filepath = filepath_list[0]
        main_filename = os.path.basename(main_filepath)
        hash_dict = await self._compute_hashes(filepath_list)
        main_checksum_d = hash_dict[main_filename]

        # Upload to IA
        if not await self._upload_to_ia(
            item, filepath_list, identifier, main_checksum_d
        ):
            raise Exception("Upload to IA failed.")

        # Update database with file checksums
        await self._update_db_on_complete(identifier, main_filepath, main_checksum_d)

        return filepath_list

    async def _download_files(self, item: QueueItem, working_dir: str) -> list[str]:
        """Download files and return list of file paths."""
        if not await utils.is_link_alive(item["download"]["url"]):
            self._logger.info(
                f"Download link {item["download"]["url"]} expired, skipping."
            )
            return []

        https_proxy = self._config["global"]["https_proxy"]
        num_chunks = self._config["downloader"]["num_chunks"]
        filepath_list = []

        try:
            # Download main file
            async with AsyncChunkDownloader(
                item["download"]["url"],
                working_dir,
                num_chunks=num_chunks,
                proxy=https_proxy,
            ) as downloader:
                main_filepath = await utils.run_with_shutdown(
                    downloader.download(), self._shutdown
                )
                if not main_filepath:
                    return []
                filepath_list.append(main_filepath)

            # Download checksum file if available
            if item["download"]["checksumUrl"]:
                async with AsyncChunkDownloader(
                    item["download"]["checksumUrl"], working_dir, proxy=https_proxy
                ) as downloader:
                    checksum_filepath = await utils.run_with_shutdown(
                        downloader.download(), self._shutdown
                    )
                    if checksum_filepath:
                        filepath_list.append(checksum_filepath)

        except Exception as e:
            self._logger.warning(f"Download {item["download"]["url"]} failed: {e}")
            return []

        # Verify all files exist
        if utils.check_non_exist(filepath_list):
            self._logger.error(
                f"Some file download failed for {item["download"]["url"]}"
            )
            return []

        return filepath_list

    async def _verify_files(self, filepath_list: list[str]) -> bool:
        """Verify downloaded files (CRC check for zip files)."""
        main_filepath = filepath_list[0]
        main_filename = os.path.basename(main_filepath)

        self._logger.info(f"Generate checksum for {main_filepath}")

        # CRC verification for zip files
        if main_filepath.endswith(".zip"):
            if not await utils.zip_verify_crc(main_filepath):
                self._logger.warning(f"CRC checksum failed, {main_filepath} corrupted.")
                return False
            self._logger.info(f"CRC checksum passed. '{main_filename}' is good.")

        return True

    async def _compute_hashes(
        self, filepath_list: list[str]
    ) -> dict[str, dict[str, str]]:
        """Compute hashes for all files."""
        hash_algorithms = [
            hashlib.md5,
            hashlib.sha1,
            hashlib.sha256,
            hashlib.sha512,
            hashlib.blake2b,
        ]

        hash_list = await asyncio.gather(
            *(
                asyncio.to_thread(utils.sync_multihash, fp, hash_algorithms)
                for fp in filepath_list
            )
        )

        return {
            os.path.basename(filepath_list[i]): hash_list[i]
            for i in range(len(hash_list))
        }

    async def _upload_to_ia(
        self,
        task: QueueItem,
        filepath_list: list[str],
        identifier: str,
        main_checksum_d: dict[str, str],
    ) -> bool:
        """Upload files to Internet Archive using the pre-allocated identifier."""
        self._logger.info(f"Start uploading '{identifier}' to IA.")

        https_proxy = self._config["global"]["https_proxy"]
        main_filepath = filepath_list[0]
        main_filename = os.path.basename(main_filepath)

        # Prepare metadata
        ia_description = self._config["ia"]["common_description"]
        if main_filename.endswith(".zip"):
            file_list = await asyncio.to_thread(utils.zip_listfiles, main_filepath)
            ia_description += (
                "<br><p><strong>Files</strong></p>"
                + utils.text_to_html_code_block("\n".join(file_list))
                + "<br><hr>"
            )

        custom_metadata = task["meta"] | {
            "checksum-" + k: v for k, v in main_checksum_d.items()
        }
        custom_metadata["description"] = ia_description

        try:
            async with IAClient(
                self._config["ia"]["s3_access_key"],
                self._config["ia"]["s3_secret_key"],
                https_proxy=https_proxy if self._config["ia"]["use_proxy"] else None,
                multipart_chunksize=1024**2 * 256,  # 256MB
            ) as ia:
                # Upload using pre-allocated identifier
                await ia.create_bucket(
                    bucket=identifier,
                    filepaths=filepath_list,
                    meta_mediatype="data",
                    meta_title=task["meta"]["description"],
                    meta_description=ia_description,
                    meta_collection=self._config["ia"]["collection"],
                    custom_metadata=custom_metadata,
                    multipart=os.path.getsize(main_filepath) > 1024**2 * 256,
                )

        except Exception as e:
            traceback.print_exc()
            self._logger.warning(
                f"Upload to IA possibly unsuccessful: {e}, proceed anyway."
            )

        self._logger.info(f"Upload '{task["download"]["url"]}' to IA finished.")
        return True

    async def _cleanup_files(
        self, filepath_list: list[str], working_dir: Optional[str]
    ):
        """Clean up downloaded files and working directory."""
        if filepath_list:
            await asyncio.to_thread(utils.remove_files, filepath_list)
        if working_dir:
            await asyncio.to_thread(utils.rm_dir, working_dir)

    ## DB Tools
    ##

    async def _create_pending_archive_entry(self, task: QueueItem) -> Optional[str]:
        """
        Create archive entry with PENDING state and return unique identifier.
        If entry exists (matching meta), change state to PENDING and return identifier
        """

        filename = self._extract_filename(task["download"]["url"])
        if not filename:
            raise Exception(
                f"Can't extract filenamef from url {task["download"]["url"]}"
            )

        # Generate unique identifier
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        identifier = f"{self._config['ia']['bucket_prefix']}{filename}_{random_suffix}"

        def _create_archive():
            with db_session():
                meta = DriverMeta.get(downloadId=task["meta"]["downloadId"])
                if not meta:
                    utils.log_error_and_raise(
                        self._logger,
                        f"Meta not found for downloadId: "
                        f"'{task['meta']['downloadId']}'",
                    )

                existing_ar = ArchiveEntry.get(meta=meta.id)
                if existing_ar:
                    existing_ar.verificationState = VerificationState.PENDING
                    for file in existing_ar.files:
                        file.delete()
                    existing_ar.files = []
                    return existing_ar.identifier
                else:
                    ArchiveEntry(
                        identifier=identifier,
                        meta=meta,
                        files=[],
                        verificationState=VerificationState.PENDING,
                    )
                    return identifier

        result = await asyncio.to_thread(_create_archive)
        if result:
            self._logger.info(f"Mark archive entry pending: '{identifier}'")
            return result
        else:
            utils.log_error_and_raise(
                self._logger, f"Failed to create unique identifier for task"
            )

    async def _update_db_on_complete(
        self, identifier: str, main_filepath: str, main_checksum_d: dict[str, str]
    ):
        """Update archive entry to complete with file checksums."""

        def _db_update():
            with db_session():
                # Get or create file checksum entry
                main_dbentry = FileChecksum.get(md5=main_checksum_d["md5"])
                if main_dbentry:
                    main_dbentry.update_from_hash_dict(main_filepath, main_checksum_d)
                else:
                    main_dbentry = FileChecksum.from_hash_dict(
                        main_filepath, main_checksum_d
                    )

                # Update archive entry
                archive_entry = ArchiveEntry.get(identifier=identifier)
                if archive_entry:
                    archive_entry.files = [main_dbentry]
                    archive_entry.verificationState = VerificationState.NOT_VERIFIED
                else:
                    self._logger.error(
                        f"Archive entry {identifier} not found during completion"
                    )

        await asyncio.to_thread(_db_update)
        self._logger.info(f"ArchiveEntry {identifier} updated to not veriified in db.")

    async def _mark_archive_incomplete(self, identifier: str):
        """Mark archive entry as incomplete on failure."""

        def _db_update():
            with db_session():
                archive_entry = ArchiveEntry.get(identifier=identifier)
                if archive_entry:
                    archive_entry.verificationState = VerificationState.INCOMPLETE

        await asyncio.to_thread(_db_update)
        self._logger.info(f"ArchiveEntry {identifier} marked as incomplete.")

    ## Other Tools
    ##

    def _extract_filename(self, url: str) -> Optional[str]:
        """Extract filename from URL."""
        match = re.search(r"/([^/?#]+?)(?:\?.*)?$", url)
        if not match:
            self._logger.warning(f"Unable to determine filename from url {url}")
            return None
        return match.group(1)

    def _create_working_dir(self) -> str:
        """Create and return working directory path."""
        working_dir = os.path.join(
            self._config["global"]["download_dir"],
            "".join(random.choices(string.ascii_lowercase, k=6)),
        )
        os.mkdir(working_dir)
        return working_dir


class VerificationWorker:

    def __init__(self, delay=10, batch_size=8):
        self._delay = delay
        self._batch_size = batch_size

        self._config = utils.read_config()
        self._logger = get_logger(f"verification")
        self._state = WorkerState.IDLE
        self._task: Optional[asyncio.Task] = None
        self._shutdown: Optional[asyncio.Event] = None

    ## Lifetime Control
    ##

    def is_idle(self) -> bool:
        return self._state == WorkerState.IDLE

    async def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._run())
        self._shutdown = asyncio.Event()
        self._logger.info("Verification worker started.")
        return self

    async def stop(self):
        if self._task and not self._task.done():
            self._shutdown.set()
            await self._task

    async def _run(self):
        """
        Randomly selects n unverified entire from state.json every delay seconds
        and attempts to verify them.

        Verificatoin worker have very long blocking db session, so this must not
        run in the same async loop as the main program.
        """

        logmsg_timer = utils.simple_timer(30)
        while not self._shutdown.is_set():
            await asyncio.sleep(self._delay)

            states_count = await asyncio.to_thread(db.get_states_count)
            unverified_cnt = states_count[VerificationState.NOT_VERIFIED]
            if next(logmsg_timer):
                self._logger.info(
                    f"Verification worker running, " f"{unverified_cnt} to verify."
                )

            if not unverified_cnt:
                self._state = WorkerState.IDLE
                continue
            self._state = WorkerState.RUNNING

            with db_session:
                sample_cnt = min(unverified_cnt, self._batch_size)
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

                async with (
                    IAClient(  # for verification purpose access key is not needed
                        "", "", self._config["global"]["https_proxy"]
                    ) as ia
                ):
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
                    self._logger.info(
                        f"Bucket '{toverify_archives[idx].identifier}' verified to "
                        f"be "
                        f"{not bool(result_fl)}"
                    )

        self._logger.info("Verification worker stopped.")
