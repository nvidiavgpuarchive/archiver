import argparse
import os
import signal
import tempfile
import traceback
from datetime import datetime

from pony.orm import select
from rich_argparse import RichHelpFormatter

from db import ArchiveEntry, DriverMeta, sync_meta_to_db
from gmail_client import GmailClient
from main_tui import *
from main_verification import VerificationWorker
from main_worker import Worker
from portal import NvidiaWebPortal

_logger = get_logger(__name__)


class SignalHandler:
    def __init__(self, shutdown_ctrl: asyncio.Event, ui_worker: AppTUI):
        self._shutdown_ctrl = shutdown_ctrl
        self._ui_worker = ui_worker

        self._ctrl_c_counter = 0
        self._logger = get_logger("signal handler")

    def __call__(self, _, frame):
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


async def main():
    ui_worker = await AppTUI().start()

    queue = asyncio.Queue()
    shutdown_ctrl = asyncio.Event()  # controlled by signal handler, control mainloop

    # signal setup
    signal.signal(signal.SIGINT, SignalHandler(shutdown_ctrl, ui_worker))
    signal.siginterrupt(signal.SIGINT, False)

    # config and db setup
    config = utils.read_config()
    db.mark_all_pending_incomplete()

    download_dir = config["global"]["download_dir"]
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)
    elif os.listdir(download_dir):
        _logger.fatal(f"Download dir '{download_dir}' not empty, exiting.")
        exit(-1)

    # init workers

    workers = await asyncio.gather(
        *[Worker(id, queue).start() for id in range(config["global"]["num_workers"])]
    )
    verification = await VerificationWorker().start()

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

    try:
        await portal.load_session_from_cookies()
        await ui_worker.start()
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
    config, portal, queue, is_worker_idle: callable, shutdown: asyncio.Event
):
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
        def _db_task():
            with db_session:
                if state_cnt[VerificationState.INCOMPLETE]:
                    meta_json = (
                        select(
                            a
                            for a in ArchiveEntry
                            if a.verificationState == VerificationState.INCOMPLETE
                        )
                        .first()
                        .meta.to_json()
                    )
                else:
                    meta_json = (
                        select(
                            m
                            for m in DriverMeta
                            if not ArchiveEntry.select(lambda a: a.meta == m)
                        )
                        .first()
                        .to_json()
                    )
            return meta_json

        try:
            meta_json = await asyncio.to_thread(_db_task)
            download_json = await portal.get_download_url(meta_json["downloadId"])
        except Exception as e:
            _logger.exception(
                f"Failed to get download url with exception '{str(e)}', retrying."
            )
            continue

        await queue.put({"meta": meta_json, "download": download_json})
        _logger.info(f"Task '{meta_json["description"]}' queued.")


def parse_arguments():
    """
    Parse command-line arguments using rich argparse.
    """
    parser = argparse.ArgumentParser(
        description="Download and upload NVIDIA drivers from enterprise portal.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Update meta entries and start the download process.",
    )
    parser.add_argument(
        "--docgen",
        type=str,
        metavar="folder",
        help="Generate documentation in the specified folder.",
    )
    return parser


if __name__ == "__main__":
    parser = parse_arguments()
    args = parser.parse_args()

    # Display help if no arguments are provided
    if not any(vars(args).values()):
        parser.print_help()
        exit(1)

    if args.download:
        exit(asyncio.run(main()))
    elif args.docgen:
        from docgen import DocGen  # Assuming DocGen is implemented in docgen module

        docgen = DocGen(docdir=args.docgen)
        docgen.generate()
