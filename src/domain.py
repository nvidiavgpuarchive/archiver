from typing import Any, TypedDict


class MetaInfo(TypedDict):
    linkType: str
    category: str
    downloadType: str
    downloadId: str  # unique
    name: str
    productName: str
    releaseDate: str
    version: str
    platformVersion: str
    productFamilies: list[str]
    platformName: str
    checksumFormat: str
    description: str
    extra: dict[str, Any]


class DownloadInfo(TypedDict):
    id: str
    url: str
    checksumUrl: str
    cookies: dict[str, str]


class QueueItem(TypedDict):
    meta: MetaInfo
    download: DownloadInfo


def same_meta(meta1: MetaInfo, meta2: MetaInfo) -> bool:
    return (
        meta1["downloadId"] == meta2["downloadId"]
        or meta1["description"] == meta2["description"]
    )
