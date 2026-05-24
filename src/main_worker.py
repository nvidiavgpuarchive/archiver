# worker and verification worker logic extracted from main
import asyncio
import hashlib
import os
import random
import re
import string
import tempfile
import traceback
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, TypedDict

from pony.orm import db_session

import utils
from db import ArchiveEntry, DriverMeta, FileChecksum, VerificationState
from downloader import AsyncChunkDownloader
from ia import IAClient
from logger import get_logger
from portal import DownloadInfo, MetaInfo


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

    # Lifetime Control
    ##

    def is_idle(self) -> bool:
        return self._state == WorkerState.IDLE

    async def start(self) -> "Worker":
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
                    self._logger.warning(f"Task processing failed: {e}")
                    await self._mark_archive_incomplete(identifier)
                finally:
                    await self._cleanup_files(filepath_list, working_dir)
        except Exception as e:
            self._logger.exception(f"Worker {self._worker_id} crashed: {e}")
        finally:
            self._state = WorkerState.DEAD
            self._logger.info("Stopped.")

    # Actual Business Logic
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

        if self._shutdown.is_set():  # incase shutdown comes
            raise Exception("Shutdown received.")

        # Verify files
        if not await self._verify_files(filepath_list):
            raise Exception("CRC checksum failed.")

        # decompress zip and compute hashes
        main_filepath = filepath_list[0]
        zip_content = await self._hash_zip_content(main_filepath)

        # Process checksums and metadata
        main_filename = os.path.basename(main_filepath)
        hash_dict = await self._compute_hashes(filepath_list)
        main_checksum_d = hash_dict[main_filename]

        # Upload to IA
        if not await self._upload_to_ia(item, filepath_list, identifier, hash_dict):
            raise Exception("Upload to IA failed.")

        # Update a database with file checksums
        await self._update_db_on_complete(
            identifier, main_filepath, main_checksum_d, zip_content
        )

        return filepath_list

    async def _download_files(self, item: QueueItem, working_dir: str) -> list[str]:
        """Download files and return a list of file paths."""
        https_proxy = self._config["global"]["https_proxy"]
        download_cookies = item["download"].get("cookies")
        self._logger.debug("Downloading with cookies.")

        if not await utils.is_link_alive(
            item["download"]["url"], proxy=https_proxy, cookies=download_cookies
        ):
            self._logger.info(
                f"Download link {item["download"]["url"]} expired, skipping."
            )
            return []

        num_chunks = self._config["downloader"]["num_chunks"]
        filepath_list = []

        try:
            # Download the main file
            async with AsyncChunkDownloader(
                item["download"]["url"],
                working_dir,
                num_chunks=num_chunks,
                proxy=https_proxy,
                cookies=download_cookies,
            ) as downloader:
                main_filepath = await utils.run_with_shutdown(
                    downloader.download(), self._shutdown
                )
                if not main_filepath:
                    return []
                filepath_list.append(main_filepath)

            # Download a checksum file if available
            if item["download"]["checksumUrl"]:
                checksum_filepath = None
                for attempt in range(3):
                    try:
                        async with AsyncChunkDownloader(
                            item["download"]["checksumUrl"],
                            working_dir,
                            proxy=https_proxy,
                            cookies=download_cookies,
                        ) as downloader:
                            checksum_filepath = await utils.run_with_shutdown(
                                downloader.download(), self._shutdown
                            )
                            if checksum_filepath:
                                filepath_list.append(checksum_filepath)
                                break
                    except Exception as e:
                        self._logger.debug(
                            f"Checksum download attempt {attempt+1} failed for {item['download']['checksumUrl']}: {e}"
                        )
                if not checksum_filepath:
                    self._logger.warning(
                        f"Checksum file {item['download']['checksumUrl']} failed after 3 attempts. Skipping."
                    )

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
                self._logger.warning(
                    f"CRC checksum failed, {
                                     main_filepath} corrupted."
                )
                return False
            self._logger.info(
                f"CRC checksum passed. '{
                              main_filename}' is good."
            )

        return True

    async def _hash_zip_content(self, main_filepath: str) -> list[dict[str, str]]:
        """
        Supplies zipfile path, returns dict of filepath in zip : md5 checksum
        """
        self._logger.info(f"Hashing zip content for {main_filepath}")
        if not main_filepath.endswith(".zip"):
            return []
        temp_dirpath = os.path.join(self._config["virustotal"]["tempdir"])
        with tempfile.TemporaryDirectory(dir=temp_dirpath) as temp_dir:
            zip_contents = await utils.zip_decompress(main_filepath, temp_dir)

            # Skipping hash on zip with too many files
            if len(zip_contents) > 8964:
                self._logger.warning(
                    f"Zip {main_filepath} contains {len(zip_contents)} files. Skipping."
                )
                return []

            zip_sizes = [os.path.getsize(fp) for fp in zip_contents]
            zip_relpaths = [
                str(Path(fp).relative_to(Path(temp_dir))) for fp in zip_contents
            ]
            zip_hashes = await asyncio.gather(
                *(utils.async_hash(fp) for fp in zip_contents)
            )

            return [
                {"size": zip_sizes[i], "relpath": zip_relpaths[i], "md5": zip_hashes[i]}
                for i, _ in enumerate(zip_contents)
            ]

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
        hash_dict: dict[str, dict[str, str]],
    ) -> bool:
        """Upload files to Internet Archive using the pre-allocated identifier."""
        self._logger.info(f"Start uploading '{identifier}' to IA.")

        https_proxy = self._config["global"]["https_proxy"]
        main_filepath = filepath_list[0]
        main_filename = os.path.basename(main_filepath)
        main_checksum_d = hash_dict[main_filename]

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
                # verify, if all files already exist on ia then skip
                badlist, _ = await ia.verify_bucket(
                    bucket=identifier,
                    md5_dict={k: v["md5"] for k, v in hash_dict.items()},
                    timeout=1,
                )

                if not badlist:
                    self._logger.info(
                        f"Bucket '{
                            identifier}' already exists on IA, skipping upload"
                    )
                    return True

                # Upload using pre-allocated identifier
                await ia.create_bucket(
                    bucket=identifier,
                    filepaths=filepath_list,
                    meta_mediatype="software",
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

        self._logger.info(f"Upload '{identifier}' to IA finished.")
        return True

    async def _cleanup_files(
        self, filepath_list: list[str], working_dir: Optional[str]
    ):
        """Clean up downloaded files and working directory."""
        if filepath_list:
            await asyncio.to_thread(utils.remove_files, filepath_list)
        if working_dir:
            await asyncio.to_thread(utils.rm_dir, working_dir)

    # DB Tools
    ##

    async def _create_pending_archive_entry(self, task: QueueItem) -> Optional[str]:
        """
        Create archive entry with PENDING state and return unique identifier.
        If entry exists (matching meta), change state to PENDING and return identifier
        """

        filename = self._extract_filename(task["download"]["url"])
        if not filename:
            raise Exception(
                f"Can't extract filename from url {task["download"]["url"]}"
            )

        # Generate unique identifier
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        identifier = f"{self._config['ia']['bucket_prefix']}{
            filename}_{random_suffix}"

        def _create_archive():
            with db_session():
                meta = DriverMeta.get(downloadId=task["meta"]["downloadId"])
                if not meta:
                    utils.log_error_and_raise(
                        self._logger,
                        f"Meta not found for downloadId: "
                        f"'{task['meta']['downloadId']}'",
                    )

                attempted_at = datetime.now()
                existing_ar = ArchiveEntry.get(meta=meta.id)
                if existing_ar:
                    existing_ar.verificationState = VerificationState.PENDING
                    existing_ar.lastAttemptAt = attempted_at
                    return existing_ar.identifier
                else:
                    ArchiveEntry(
                        identifier=identifier,
                        meta=meta,
                        verificationState=VerificationState.PENDING,
                        lastAttemptAt=attempted_at,
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
        self,
        identifier: str,
        main_filepath: str,
        main_checksum_d: dict[str, str],
        zip_content: list[dict[str, str]],
    ):
        """Update archive entry to complete with file checksums."""

        def _db_update():
            with db_session():
                # Get or create file checksum entry
                file_entry = FileChecksum.get(md5=main_checksum_d["md5"])
                if not file_entry:
                    file_entry = FileChecksum.from_hash_dict(
                        main_filepath, main_checksum_d
                    )
                this_filename = os.path.basename(main_filepath)
                if this_filename not in file_entry.filenames:
                    file_entry.filenames.append(this_filename)
                file_entry.zip_content = zip_content

                # Update archive entry
                archive_entry = ArchiveEntry.get(identifier=identifier)
                if archive_entry:
                    archive_entry.file = file_entry
                    archive_entry.verificationState = VerificationState.NOT_VERIFIED
                    archive_entry.lastAttemptAt = None
                else:
                    self._logger.error(
                        f"Archive entry {
                            identifier} not found during completion"
                    )

        await asyncio.to_thread(_db_update)
        self._logger.info(f"ArchiveEntry '{identifier}' updated in db.")

    async def _mark_archive_incomplete(self, identifier: str):
        """Mark archive entry as incomplete on failure."""

        def _db_update():
            with db_session():
                archive_entry = ArchiveEntry.get(identifier=identifier)
                if archive_entry:
                    archive_entry.verificationState = VerificationState.INCOMPLETE

        await asyncio.to_thread(_db_update)
        self._logger.info(f"ArchiveEntry '{identifier}' marked as incomplete.")

    # Other Tools
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
