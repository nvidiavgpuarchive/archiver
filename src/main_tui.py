# rich interface logic and global ui elements are placed here
import threading
import time

from rich.live import Live
from rich.progress import ProgressColumn, Task, Progress, BarColumn, TextColumn
from rich.table import Table
from rich.text import Text

import utils
from downloader import AsyncChunkDownloader
from ia import IAClient


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


complete_counter = utils.AsyncCounter()
incomplete_counter = utils.AsyncCounter()
not_verified_counter = utils.AsyncCounter()

indicator_column = VarTextColumn("Running", "green")
complete_column = CounterColumn(
    complete_counter.get_value, label="✔", color="bright_green"
)
incomplete_column = CounterColumn(
    incomplete_counter.get_value, label="✗", color="bright_red"
)
not_verified_column = CounterColumn(
    not_verified_counter.get_value, label="?", color="bright_yellow"
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
    not_verified_column,
    complete_column,
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


def live_display_render():
    table = Table.grid(padding=(0, 1))
    # Add a separator row at the top (using dashes, adjust width as needed)
    separator = Text("")
    table.add_row(separator)
    table.add_row(progress_bar)
    table.add_row(status_bar)
    return table


live_display = Live(live_display_render(), refresh_per_second=10, transient=True)
