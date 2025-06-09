# some custom rich classes are palced here
import time

from rich.progress import ProgressColumn, Task
from rich.text import Text

import utils


class CounterColumn(ProgressColumn):
    def __init__(self, get_value, label, color, bytes_conv=False):
        super().__init__()
        self.get_value = get_value  # a callable that returns the current count
        self.label = label
        self.color = color
        self.bytes_conv = bytes_conv

    def render(self, task: Task) -> Text:
        count = self.get_value()
        if self.bytes_conv: count = utils.human_readable_size_str(count, long=False)
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
