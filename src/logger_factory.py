import logging
import os

from rich.logging import RichHandler  # Raises ImportError if not installed


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
        handler = RichHandler(show_time=True, show_level=True, show_path=False, rich_tracebacks=True)
        handler.setLevel(log_level)
        logger.addHandler(handler)
        logger.propagate = False
        logger.debug(f"Logger for '{name}' initialized with RichHandler.")

    return logger
