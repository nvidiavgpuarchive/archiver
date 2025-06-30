import asyncio
from dataclasses import dataclass
from typing import List, Optional

from pony.orm import commit, db_session, select

import db
import utils
from db import ArchiveEntry, VerificationState
from ia import IAClient
from logger import get_logger
from main_worker import WorkerState


@dataclass
class VerificationArchive:
    identifier: str
    verification_state: VerificationState
    file_md5: Optional[str] = None
    filenames: Optional[List[str]] = None
    ia_meta: Optional[dict] = None


class VerificationWorker:

    def __init__(self, delay=10, batch_size=8):
        self._delay = delay
        self._batch_size = batch_size

        self._config = utils.read_config()
        self._logger = get_logger(f"verification")
        self._state = WorkerState.IDLE
        self._task: Optional[asyncio.Task] = None
        self._shutdown: Optional[asyncio.Event] = None
        self._logmsg_timer = utils.simple_timer(30)

    # Lifetime Control
    ##

    def is_idle(self) -> bool:
        return self._state == WorkerState.IDLE

    async def start(self) -> "VerificationWorker":
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
        Randomly selects n unverified entries from the database every delay seconds
        and attempts to verify them.

        Verification worker has very long blocking db sessions, so this must not
        run in the same async loop as the main program.
        """

        while not self._shutdown.is_set():
            await asyncio.sleep(self._delay)

            # Check if there's work to do
            unverified_cnt = await self._check_unverified_count()
            if not unverified_cnt:
                self._state = WorkerState.IDLE
                continue

            self._state = WorkerState.RUNNING

            await self._cleanup_orphaned_archives()

            archives_to_verify = await self._get_archives_to_verify(unverified_cnt)
            if not archives_to_verify:
                continue

            try:
                verification_results = await self._perform_verification(
                    archives_to_verify
                )
            except SystemError as e:
                self._logger.warning(
                    f"System error {e}, most likely can be safely ignored."
                )
                continue

            await self._update_verification_results(
                archives_to_verify, verification_results
            )

        self._logger.info("Verification worker stopped.")

    async def _check_unverified_count(self) -> int:
        """Check how many archives need verification and log if needed."""
        states_count = await asyncio.to_thread(db.get_states_count)
        unverified_cnt = states_count[VerificationState.NOT_VERIFIED]

        if next(self._logmsg_timer):
            self._logger.info(
                f"Verification worker running, {unverified_cnt} to verify."
            )

        return unverified_cnt

    async def _cleanup_orphaned_archives(self):
        """Mark archives without associated files as incomplete."""

        def _cleanup_db():
            with db_session:
                failed_archives = select(
                    a
                    for a in ArchiveEntry
                    if a.verificationState
                    in [VerificationState.COMPLETE, VerificationState.NOT_VERIFIED]
                    and not a.file
                )[:]

                for a in failed_archives:
                    a.verificationState = VerificationState.INCOMPLETE
                    self._logger.warning(
                        f"Archive '{a.identifier}' marked incomplete because no "
                        f"file associated. This is NOT normal."
                    )
                commit()

        await asyncio.to_thread(_cleanup_db)

    async def _get_archives_to_verify(
        self, unverified_cnt: int
    ) -> List[VerificationArchive]:
        """Get a batch of archives that need verification."""

        def _get_archives():
            with db_session:
                sample_cnt = min(unverified_cnt, self._batch_size)
                toverify_archives = select(
                    a
                    for a in ArchiveEntry
                    if a.verificationState == VerificationState.NOT_VERIFIED
                )[:sample_cnt]

                # Convert database objects to dataclass instances
                return [
                    VerificationArchive(
                        identifier=a.identifier,
                        verification_state=a.verificationState,
                        file_md5=a.file.md5 if a.file else None,
                        filenames=a.file.filenames if a.file else None,
                    )
                    for a in toverify_archives
                ]

        return await asyncio.to_thread(_get_archives)

    async def _perform_verification(
        self, archives_to_verify: List[VerificationArchive]
    ) -> List:
        """Perform verification of archives via IA API."""
        toverify_checksums = [a.file_md5 for a in archives_to_verify]

        async with IAClient("", "", self._config["global"]["https_proxy"]) as ia:
            # results are tuples of filelist, meta
            results = await asyncio.gather(
                *(
                    ia.verify_bucket(
                        bucket=archives_to_verify[i].identifier,
                        md5_dict=(
                            {archives_to_verify[i].filenames[0]: toverify_checksums[i]}
                            if archives_to_verify[i].filenames
                            else {}
                        ),
                        timeout=5,
                    )
                    for i in range(len(archives_to_verify))
                ),
                return_exceptions=True,
            )

        return results

    async def _update_verification_results(
        self, archives_to_verify: List[VerificationArchive], results: List
    ):
        """Update database with verification results."""

        def _update_db():
            with db_session:
                for idx, result in enumerate(results):
                    if isinstance(result, Exception):  # timeout, unverified
                        continue

                    result_fl, ia_meta = result
                    archive_data = archives_to_verify[idx]

                    # Fetch corresponding db object by identifier
                    archive = ArchiveEntry.get(identifier=archive_data.identifier)
                    if not archive:
                        continue

                    # Update state
                    if result_fl:  # incomplete
                        archive.verificationState = VerificationState.INCOMPLETE
                    else:
                        archive.verificationState = VerificationState.COMPLETE
                        archive.ia_meta = ia_meta

                    self._logger.info(
                        f"Bucket '{archive.identifier}' verified to be {not bool(result_fl)}"
                    )

        await asyncio.to_thread(_update_db)
