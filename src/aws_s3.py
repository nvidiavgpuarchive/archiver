import json
import os
from pathlib import Path
from typing import Any, Literal, TypedDict

import app_config
import utils
from domain import DownloadInfo, MetaInfo
from logger import get_logger

_logger = get_logger(__name__)


class S3GamingExtra(TypedDict):
    type: Literal["S3 Gaming"]
    content: dict[str, Any]


class AwsS3:
    BASE_URL = "https://nvidia-gaming.s3.amazonaws.com/"

    def __init__(self) -> None:
        config = app_config.load_config().aws_s3
        self._entries: list[tuple[str, dict[str, Any]]] = []
        self._meta: list[MetaInfo] = []
        self._entries_by_etag: dict[str, dict[str, Any]] = {}

        self._load_platform(
            config.linux_json_path,
            platform_name="Linux",
        )
        self._load_platform(
            config.windows_json_path,
            platform_name="Windows",
        )
        self._finalize_entries()
        _logger.info(f"Loaded {len(self._meta)} AWS S3 gaming driver entries.")

    async def list_meta(self) -> list[MetaInfo]:
        return self._meta

    async def is_loggedin(self) -> bool:
        return True

    async def login(self) -> None:
        pass

    async def get_download_url(self, download_id: str) -> DownloadInfo | None:
        entry = self._entries_by_etag.get(download_id)
        if not entry:
            return None

        key = entry["Key"]
        return {
            "id": download_id,
            "url": self.BASE_URL + key,
            "checksumUrl": "",
            "cookies": {},
        }

    def _load_platform(self, json_path: str, platform_name: str) -> None:
        with open(self._resolve_path(json_path), "r") as f:
            data = json.load(f)

        for entry in data.get("Contents", []):
            key = entry.get("Key", "")
            if not key or key.endswith("/"):
                continue

            self._entries.append((platform_name, entry))

    def _finalize_entries(self) -> None:
        self._entries.sort(
            key=lambda item: (
                self._etag(item[1]),
                self._filename(item[1].get("Key", "")),
                item[0],
                item[1].get("Key", ""),
            )
        )
        for platform_name, entry in self._entries:
            etag = self._etag(entry)
            if etag in self._entries_by_etag:
                _logger.warning(
                    f"Skipping duplicate AWS S3 ETag '{etag}' for '{entry['Key']}'."
                )
                continue

            self._entries_by_etag[etag] = entry
            self._meta.append(self._to_meta(entry, etag, platform_name))

    @staticmethod
    def _resolve_path(path: str) -> str:
        return path if os.path.isabs(path) else utils.proj_path(path)

    @staticmethod
    def _etag(entry: dict[str, Any]) -> str:
        return str(entry.get("ETag", "")).strip('"')

    @staticmethod
    def _filename(key: str) -> str:
        return Path(key).name

    def _to_meta(
        self, entry: dict[str, Any], etag: str, platform_name: str
    ) -> MetaInfo:
        key = entry["Key"]
        extra: S3GamingExtra = {"type": "S3 Gaming", "content": entry}
        return {
            "linkType": "",
            "category": "GamingDriver",
            "downloadType": "",
            "downloadId": etag,
            "name": self._filename(key),
            "productName": "",
            "releaseDate": str(entry.get("LastModified", ""))[:10],
            "version": "",
            "platformVersion": "",
            "productFamilies": [],
            "platformName": platform_name,
            "checksumFormat": "",
            "description": self._filename(key),
            "extra": extra,
        }
