import logging
import os

from rich.logging import RichHandler  # Raises ImportError if not installed

import utils


class CenteredFormatter(logging.Formatter):
    # Class variable to track the longest name length
    longest_name_length = 14  # Initial default width

    def __init__(self, fmt=None, datefmt=None, style="%", initial_width=14):
        super().__init__(fmt, datefmt, style)
        # Set initial width if defined
        CenteredFormatter.longest_name_length = initial_width

    def format(self, record):
        # Update the class variable if a longer name is found
        CenteredFormatter.longest_name_length = max(
            CenteredFormatter.longest_name_length, len(record.name)
        )

        # Calculate the dynamic width based on the longest name + brackets
        dynamic_width = CenteredFormatter.longest_name_length + 2

        # Center the name within the dynamic width (subtract 2 for the brackets)
        record.name = f"{record.name.center(dynamic_width - 2)}"
        return super().format(record)


def get_logger(name=None) -> logging.Logger:
    """
    Creates and returns a logger configured with RichHandler for rich output.
    """
    if name is None:
        name = "Default"
    logger = logging.getLogger(name)
    log_level = logging.DEBUG if os.getenv("DEBUG") else logging.INFO
    logger.setLevel(log_level)

    if not logger.handlers:
        # Create formatter that includes logger name
        format_pattern = "[%(name)s]  %(message)s"
        formatter = CenteredFormatter(format_pattern)

        console_handler = RichHandler(
            show_time=True,
            show_level=True,
            show_path=False,
            rich_tracebacks=True,
            log_time_format="[%X]",
        )
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(utils.proj_path("config/lastlog.txt"))
        file_handler.setFormatter(
            logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s] %(message)s")
        )
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

        logger.propagate = False
        logger.debug(f"Logger for '{name}' initialized with RichHandler.")

    return logger
