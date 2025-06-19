# rich interface logic and global ui elements are placed here
import asyncio
import threading
import time
from typing import Optional

from pony.orm import db_session
from rich.live import Live
from rich.progress import ProgressColumn, Task, Progress, BarColumn, TextColumn
from rich.table import Table
from rich.text import Text

import db
import utils
from db import VerificationState
from downloader import AsyncChunkDownloader
from ia import IAClient
from logger import get_logger


class CounterColumn(ProgressColumn):
    def __init__(self, get_value, label, color, bytes_conv=False):
        super().__init__()
        self.get_value = get_value  # a callable that returns the current count
        self.label = label
        self.color = color
        self.bytes_conv = bytes_conv

    def render(self, task: Task) -> Text:
        count = self.get_value()
        if self.bytes_conv:
            count = utils.human_readable_size_str(count, long=False)
        return Text(f"{self.label} {count}", style=self.color)


class VarTextColumn(ProgressColumn):
    def __init__(self, init_text, color):
        super().__init__()
        self.text = init_text
        self.color = color

    def render(self, task: Task) -> Text:
        return Text(self.text, style=self.color)

    def update(self, text, color):
        self.text = text
        self.color = color


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

        speed_str = f"{utils.human_readable_size_str(int(speed), long=True)}/s"
        return Text(f"{self.icon} {speed_str}", style=self.color)


class AppTUI:
    def __init__(self):
        self.complete_counter = utils.AsyncCounter()
        self.incomplete_counter = utils.AsyncCounter()
        self.not_verified_counter = utils.AsyncCounter()

        self._indicator_column = VarTextColumn("Running", "green")
        self._progress_bar = self._init_upper_progress_bar()
        self._status_bar = self._init_lower_status_bar()
        self._live_display = Live(
            self._live_display_render(), refresh_per_second=10, transient=True
        )

        self._progress_bar_task = self._progress_bar.add_task(
            "Processing", total=0, start=False
        )
        self._status_bar_task = self._status_bar.add_task(
            "Status", total=0, start=False
        )

        self._logger = get_logger("ui worker")
        self._shutdown: Optional[asyncio.Event] = None
        self._thread: Optional[threading.Thread] = None

    ## Init UI Elements
    ##

    def _init_upper_progress_bar(self) -> Progress:
        complete_column = CounterColumn(
            self.complete_counter.get_value, label="✔", color="bright_green"
        )
        incomplete_column = CounterColumn(
            self.incomplete_counter.get_value, label="✗", color="bright_red"
        )
        not_verified_column = CounterColumn(
            self.not_verified_counter.get_value, label="?", color="bright_yellow"
        )

        # upper progress bar
        return Progress(
            self._indicator_column,
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            not_verified_column,
            complete_column,
            incomplete_column,
            refresh_per_second=10,
            transient=True,
        )

    def _init_lower_status_bar(self) -> Progress:
        coroutines_column = CounterColumn(
            utils.count_active_coroutines, label="Coroutines", color="cyan"
        )
        threads_column = CounterColumn(
            threading.active_count, label="Threads", color="yellow"
        )
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
        return Progress(
            download_speed_column,
            upload_speed_column,
            TextColumn("[bold]|[/bold]"),
            threads_column,
            coroutines_column,
            mem_column,
        )

    def _live_display_render(self):
        table = Table.grid(padding=(0, 1))
        # Add a separator row at the top (using dashes, adjust width as needed)
        separator = Text("")
        table.add_row(separator)
        table.add_row(self._progress_bar)
        table.add_row(self._status_bar)
        return table

    ## Lifetime Control
    ##

    async def start(self):
        self._live_display.start()

        self._shutdown = asyncio.Event()
        self._thread = utils.run_async_in_thread(self._run())
        self._logger.info("UI Worker started.")
        return self

    async def stop(self):
        if self._thread and self._thread.is_alive():
            self._shutdown.set()
            await asyncio.to_thread(self._thread.join)

        self._progress_bar.stop()
        self._status_bar.stop()
        self._live_display.stop()
        self._logger.info("UI Worker stopped.")

    ## Actual UI Logic and exposed methods
    ##

    # this worker must be run in an seperate thread
    async def _run(self):
        progress_bar_started = False
        while not self._shutdown.is_set():
            await asyncio.sleep(1)
            states_cnt = await asyncio.to_thread(db.get_states_count)

            # update counters and progress
            with db_session:
                total_tasks = db.DriverMeta.select().count()

            await self.complete_counter.set(states_cnt[VerificationState.COMPLETE])
            await self.incomplete_counter.set(states_cnt[VerificationState.INCOMPLETE])
            await self.not_verified_counter.set(
                states_cnt[VerificationState.NOT_VERIFIED]
            )

            if not progress_bar_started:
                progress_bar_started = True
                self._progress_bar.start_task(self._progress_bar_task)
                self._status_bar.start_task(self._status_bar_task)

            self._progress_bar.update(
                self._progress_bar_task,
                total=total_tasks,
                completed=states_cnt[VerificationState.COMPLETE],
            )

    def update_progress_bar_text_column(self, new_text: str, new_color: str):
        self._indicator_column.update(new_text, new_color)
