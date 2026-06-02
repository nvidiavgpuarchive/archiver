from dataclasses import dataclass
from functools import lru_cache
from os import PathLike
from typing import Any

import yaml

from utils import proj_path


@dataclass(frozen=True)
class GlobalConfig:
    download_dir: str
    https_proxy: str
    num_workers: int
    num_tasks: int


@dataclass(frozen=True)
class PortalConfig:
    nvidia_username: str
    nvidia_password: str


@dataclass(frozen=True)
class DownloaderConfig:
    num_chunks: int


@dataclass(frozen=True)
class ImapConfig:
    host: str
    port: int
    username: str
    password: str


@dataclass(frozen=True)
class IAConfig:
    s3_access_key: str
    s3_secret_key: str
    bucket_prefix: str
    collection: str
    common_description: str
    force_uploading: bool
    use_proxy: bool


@dataclass(frozen=True)
class AppConfig:
    global_: GlobalConfig
    portal: PortalConfig
    downloader: DownloaderConfig
    imap: ImapConfig
    ia: IAConfig


def _section(config: dict[str, Any], name: str) -> dict[str, Any]:
    section = config[name]
    if not isinstance(section, dict):
        raise TypeError(f"Config section '{name}' must be a mapping.")
    return section


@lru_cache
def load_config(path: str | PathLike[str] | None = None) -> AppConfig:
    config_path = path if path is not None else proj_path("config/config.yaml")
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise TypeError("Config root must be a mapping.")

    global_config = _section(raw, "global")
    portal_config = _section(raw, "portal")
    downloader_config = _section(raw, "downloader")
    imap_config = _section(raw, "imap")
    ia_config = _section(raw, "ia")

    return AppConfig(
        global_=GlobalConfig(
            download_dir=global_config["download_dir"],
            https_proxy=global_config["https_proxy"],
            num_workers=global_config["num_workers"],
            num_tasks=global_config["num_tasks"],
        ),
        portal=PortalConfig(
            nvidia_username=portal_config["nvidia_username"],
            nvidia_password=portal_config["nvidia_password"],
        ),
        downloader=DownloaderConfig(num_chunks=downloader_config["num_chunks"]),
        imap=ImapConfig(
            host=imap_config["host"],
            port=imap_config["port"],
            username=imap_config["username"],
            password=imap_config["password"],
        ),
        ia=IAConfig(
            s3_access_key=ia_config["s3_access_key"],
            s3_secret_key=ia_config["s3_secret_key"],
            bucket_prefix=ia_config["bucket_prefix"],
            collection=ia_config["collection"],
            common_description=ia_config["common_description"],
            force_uploading=ia_config["force_uploading"],
            use_proxy=ia_config["use_proxy"],
        ),
    )
