import argparse
import json
import os
import signal
import tempfile
import traceback
from collections.abc import Callable
from datetime import datetime

from pony.orm import select
from rich_argparse import RichHelpFormatter

import app_config
from aws_s3 import AwsS3
from db import ArchiveEntry, DriverMeta, sync_meta_to_db
from gmail_client import GmailClient
from main_tui import *
from main_verification import VerificationWorker
from main_worker import Worker
from nvidia_portal import NvidiaWebPortal

_logger = get_logger(__name__)


class SignalHandler:
    def __init__(self, shutdown_ctrl: asyncio.Event, ui_worker: AppTUI) -> None:
        self._shutdown_ctrl = shutdown_ctrl
        self._ui_worker = ui_worker

        self._ctrl_c_counter = 0
        self._logger = get_logger("signal handler")

    def __call__(self, _: int, frame: object) -> None:
        self._ctrl_c_counter += 1

        if self._ctrl_c_counter == 1:
            # First Ctrl+C: Start graceful shutdown
            self._logger.warning(
                "Terminating command received, wait for current loop to complete."
            )
            self._logger.warning("Note: IA upload task will not be interrupted.")
            self._ui_worker.update_progress_bar_text_column("Sig Recved", "red3")

            self._shutdown_ctrl.set()
        elif self._ctrl_c_counter == 2:
            # Second Ctrl+C: Emergency shutdown with crash log
            self._logger.warning("Force terminating...")
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                crash_file = os.path.join(
                    tempfile.gettempdir(), f"vgpucrash_{timestamp}.log"
                )

                with open(crash_file, "w") as f:
                    f.write(f"Emergency shutdown triggered at {datetime.now()}\n")
                    f.write("\nTraceback at point of interrupt:\n\n\n")
                    traceback.print_stack(frame, file=f)
                self._logger.warning(f"Crash log saved to '{crash_file}'")
            except Exception as e:
                self._logger.exception(
                    f"Another exception occurred when trying to write log: {e}"
                )
                self._logger.warning("Quit without saving the log.")

            _logger.warning("Force quitting...")
            os._exit(1)
        else:
            # Third or more Ctrl+C: Immediate force quit
            os._exit(2)


async def main(source: str) -> None:
    ui_worker = AppTUI()

    queue = asyncio.Queue()
    shutdown_ctrl = asyncio.Event()  # controlled by signal handler, control mainloop

    # signal setup
    signal.signal(signal.SIGINT, SignalHandler(shutdown_ctrl, ui_worker))
    signal.siginterrupt(signal.SIGINT, False)

    # config and db setup
    config = app_config.load_config()

    download_dir = config.global_.download_dir
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)
    elif os.listdir(download_dir):
        _logger.fatal(f"Download dir '{download_dir}' not empty, exiting.")
        raise SystemExit(-1)

    db.mark_all_pending_incomplete()
    await ui_worker.start()

    # init workers

    workers = await asyncio.gather(
        *[Worker(id, queue).start() for id in range(config.global_.num_workers)]
    )
    verification = await VerificationWorker().start()

    # main routine
    if source == "nvidia":
        gmail_client = GmailClient(
            config.imap.host,
            config.imap.port,
            config.imap.username,
            config.imap.password,
        )
        portal = NvidiaWebPortal(
            username=config.nvidia_portal.nvidia_username,
            password=config.nvidia_portal.nvidia_password,
            https_proxy=config.global_.https_proxy,
            gmail_client=gmail_client,
        )
        asyncio.create_task(gmail_client.connect())
    elif source == "aws":
        portal = AwsS3()
        sync_meta_to_db(await portal.list_meta())
    else:
        raise ValueError(f"Unknown sync source: {source}")

    try:
        await main_loop(
            config=config,
            portal=portal,
            queue=queue,
            is_worker_idle=lambda: any(w.is_idle() for w in workers),
            shutdown=shutdown_ctrl,
        )
    except Exception as e:
        _logger.fatal(f"Unexpected exception '{e}', quitting.")
        traceback.print_exc()
    finally:
        ui_worker.update_progress_bar_text_column("Stopping", "dark_orange3")

        shutdown_ctrl.set()
        await asyncio.gather(*[worker.stop() for worker in workers])
        await verification.stop()
        await ui_worker.stop()

        utils.rm_dir(download_dir)
        os.mkdir(download_dir)
        _logger.info("Program stopped.")


async def main_loop(
    config: app_config.AppConfig,
    portal: NvidiaWebPortal | AwsS3,
    queue: asyncio.Queue,
    is_worker_idle: Callable[[], bool],
    shutdown: asyncio.Event,
) -> None:
    """
    Mainloop, add tasks to worker one task at a time
    """

    timer = utils.simple_timer(30)
    while not shutdown.is_set():  # hardcoded for now
        await asyncio.sleep(1)
        if not await portal.is_loggedin():  # login and refresh download list
            await portal.login()
            sync_meta_to_db(await portal.list_meta())

        # mainloop lazy
        if not is_worker_idle():
            if next(timer):
                _logger.info(f"No idle workers, standby.")
            continue

        state_cnt = await asyncio.to_thread(db.get_states_count)
        meta_cnt = await asyncio.to_thread(db.get_meta_count)
        if state_cnt[VerificationState.COMPLETE] >= meta_cnt:  # all tasks done
            _logger.info(f"All tasks verified, quitting.")
            break
        elif (
            state_cnt[VerificationState.COMPLETE]
            + state_cnt[VerificationState.PENDING]
            + state_cnt[VerificationState.NOT_VERIFIED]
            >= meta_cnt
        ):
            if next(timer):
                _logger.info("No new tasks to queue, standby.")
            continue

        # get meta tasks
        # TODO: rough patch, if in future have more than aws and nvidia, add proper type
        # in db instead of monkey patching it.
        def _db_task() -> dict | None:
            with db_session:
                # New / never-attempted tasks first.
                for meta in DriverMeta.select():
                    meta_is_s3 = bool(
                        meta.extra and meta.extra.get("type") == "S3 Gaming"
                    )
                    if isinstance(portal, AwsS3) and not meta_is_s3:
                        continue
                    if isinstance(portal, NvidiaWebPortal) and meta_is_s3:
                        continue
                    if not meta.archive:
                        return meta.to_json()

                for archive in select(
                    a
                    for a in ArchiveEntry
                    if a.verificationState == VerificationState.INCOMPLETE
                    and a.lastAttemptAt == None
                ):
                    meta_is_s3 = bool(
                        archive.meta.extra
                        and archive.meta.extra.get("type") == "S3 Gaming"
                    )
                    if isinstance(portal, AwsS3) and meta_is_s3:
                        return archive.meta.to_json()
                    if isinstance(portal, NvidiaWebPortal) and not meta_is_s3:
                        return archive.meta.to_json()

                for archive in select(
                    a
                    for a in ArchiveEntry
                    if a.verificationState == VerificationState.INCOMPLETE
                ).order_by(lambda a: a.lastAttemptAt):
                    meta_is_s3 = bool(
                        archive.meta.extra
                        and archive.meta.extra.get("type") == "S3 Gaming"
                    )
                    if isinstance(portal, AwsS3) and meta_is_s3:
                        return archive.meta.to_json()
                    if isinstance(portal, NvidiaWebPortal) and not meta_is_s3:
                        return archive.meta.to_json()

                return None

        #                 if state_cnt[VerificationState.INCOMPLETE]:
        #                     meta_json = (
        #                         select(
        #                             a
        #                             for a in ArchiveEntry
        #                             if a.verificationState == VerificationState.INCOMPLETE
        #                         )
        #                         .first()
        #                         .meta.to_json()
        #                     )
        #                 else:
        #                     meta_json = (
        #                         select(
        #                             m
        #                             for m in DriverMeta
        #                             if not ArchiveEntry.select(lambda a: a.meta == m)
        #                         )
        #                         .first()
        #                         .to_json()
        #                     )
        #             return meta_json

        try:
            meta_json = await asyncio.to_thread(_db_task)
            if not meta_json:
                if next(timer):
                    _logger.info(
                        f"No {portal.__class__.__name__} tasks to queue, standby."
                    )
                continue
            download_json = await portal.get_download_url(meta_json["downloadId"])
            if not download_json:
                _logger.warning(
                    f"Failed to resolve download url for {portal.__class__.__name__} "
                    f"downloadId '{meta_json['downloadId']}', skipping."
                )
                continue
        except Exception as e:
            _logger.exception(
                f"Failed to get download url with exception '{str(e)}', retrying."
            )
            continue

        await queue.put({"meta": meta_json, "download": download_json})
        _logger.info(f"Task '{meta_json["description"]}' queued.")


def parse_arguments() -> argparse.ArgumentParser:
    """
    Parse command-line arguments using rich argparse.
    """
    parser = argparse.ArgumentParser(
        description="Download and upload archived driver files.",
        formatter_class=RichHelpFormatter,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sync-nvidia",
        action="store_true",
        help="Sync NVIDIA enterprise portal downloads.",
    )
    group.add_argument(
        "--sync-aws",
        action="store_true",
        help="Sync AWS S3 gaming driver downloads.",
    )
    group.add_argument(
        "--docgen",
        type=str,
        metavar="FOLDER",
        help="Generate documentation in the specified folder.",
    )
    group.add_argument(
        "--dump-json",
        type=str,
        metavar="FILENAME",
        help="Dump database to json file.",
    )
    group.add_argument(
        "--load-json",
        type=str,
        metavar="FILENAME",
        help="Initiate db from dumped json file.",
    )
    # group.add_argument(
    #     "--verify-ia",
    #     action="store_true",
    #     help="Mark all entry status to pending, and start the verification process.",
    # )
    return parser


if __name__ == "__main__":
    parser = parse_arguments()
    args = parser.parse_args()

    # Display help if no arguments are provided
    if not any(vars(args).values()):
        parser.print_help()
        exit(1)

    if args.sync_nvidia:
        exit(asyncio.run(main("nvidia")))
    elif args.sync_aws:
        exit(asyncio.run(main("aws")))
    elif args.docgen:
        from docgen import DocGen  # Assuming DocGen is implemented in docgen module

        docgen = DocGen(docdir=args.docgen)
        docgen.generate()
    elif args.dump_json:
        d = db.dump_completed_to_json()
        with open(args.dump_json, "w") as f:
            json.dump(d, f, indent=4)
        print(
            f"Dumped completed entries to '{args.dump_json}'. "
            f"Total {len(d)} entries."
        )
    elif args.load_json:
        db.load_from_json(args.load_json)
