# pony orm use rule of thumb:
# 1. use shortliving sessions and don't interrupt them with async
# 2. if a function is full of pony code, isolate it into a seperate thread
import json
import os
from datetime import date
from enum import Enum
from typing import Union

from pony.orm import *

import utils
from logger import get_logger


_logger = get_logger(__name__)


class VerificationState(str, Enum):
    PENDING = "PENDING"  # not yet or upload in progress
    NOT_VERIFIED = "NOT_VERIFIED"  # upload complete, integrity yet to verify
    COMPLETE = "COMPLETE"  # uploaded and verified to be integral
    INCOMPLETE = "INCOMPLETE"  # uploaded, verified to be broken


# Database setup
db = Database()


class DriverMeta(db.Entity):
    id = PrimaryKey(int, auto=True)
    downloadId = Required(str, unique=True, index=True)
    description = Required(str)
    name = Required(str)

    releaseDate = Optional(date)
    version = Optional(str)
    downloadType = Optional(str)
    linkType = Optional(str)
    platformName = Optional(str)
    platformVersion = Optional(str)
    productName = Optional(str)
    category = Optional(str)
    checksumFormat = Optional(str)
    productFamilies = Optional(Json)  # JSON field in Pony ORM

    # Relationships
    archive = Optional("ArchiveEntry", reverse="meta")

    @staticmethod
    def from_meta_info(meta_info: dict):
        release_date_obj = date.fromisoformat(meta_info["releaseDate"])
        for name in [
            "releaseDate",
            "downloadType",
            "linkType",
            "platformName",
            "platformVersion",
            "productName",
            "category",
            "checksumFormat",
            "productFamilies",
        ]:
            if name not in meta_info:
                meta_info[name] = ""
        return DriverMeta(
            downloadId=meta_info["downloadId"],
            description=meta_info["description"],
            name=meta_info["name"],
            releaseDate=release_date_obj,
            version=meta_info["version"],
            downloadType=meta_info.get("downloadType"),
            linkType=meta_info.get("linkType"),
            platformName=meta_info.get("platformName"),
            platformVersion=meta_info.get("platformVersion"),
            productName=meta_info.get("productName"),
            category=meta_info.get("category"),
            checksumFormat=meta_info.get("checksumFormat"),
            productFamilies=meta_info.get("productFamilies"),
        )

    def to_json(self, include_id=False):
        j = {
            "downloadId": self.downloadId,
            "description": self.description,
            "name": self.name,
            "releaseDate": self.releaseDate.isoformat(),
            "version": self.version,
            "downloadType": self.downloadType,
            "linkType": self.linkType,
            "platformName": self.platformName,
            "platformVersion": self.platformVersion,
            "productName": self.productName,
            "category": self.category,
            "checksumFormat": self.checksumFormat,
            "productFamilies": self.productFamilies,
        }
        return j if not include_id else j | {"id": self.id}


class FileChecksum(db.Entity):
    id = PrimaryKey(int, auto=True)
    # TODO: doesn't have to be unique
    # also add logic for duplicatoin check in main
    size = Required(int, size=64)
    md5 = Required(str)
    sha1 = Required(str)
    sha256 = Required(str)
    sha512 = Required(str)
    blake2b = Required(str)

    shake_128 = Optional(str)
    shake_256 = Optional(str)
    sha224 = Optional(str)
    sha384 = Optional(str)
    sha3_224 = Optional(str)
    sha3_256 = Optional(str)
    sha3_384 = Optional(str)
    sha3_512 = Optional(str)
    blake2s = Optional(str)
    crc32 = Optional(str)

    filenames = Required(Json)
    archives = Set("ArchiveEntry", reverse="file")

    @staticmethod
    def from_hash_dict(filepath, hash_dict: dict):
        return FileChecksum(
            filenames=[os.path.basename(filepath)],
            size=os.path.getsize(filepath),
            **hash_dict,
        )

    def to_json(self, include_id=False):
        j = {
            "size": self.size,
            "md5": self.md5,
            "sha1": self.sha1,
            "sha256": self.sha256,
            "sha512": self.sha512,
            "blake2b": self.blake2b,
            "shake_128": self.shake_128,
            "shake_256": self.shake_256,
            "sha224": self.sha224,
            "sha384": self.sha384,
            "sha3_224": self.sha3_224,
            "sha3_256": self.sha3_256,
            "sha3_384": self.sha3_384,
            "sha3_512": self.sha3_512,
            "blake2s": self.blake2s,
            "crc32": self.crc32,
            "filenames": self.filenames,
        }
        return j if not include_id else j | {"id": self.id}


class ArchiveEntry(db.Entity):
    identifier = PrimaryKey(str)

    ia_meta = Optional(Json)  # JSON field in Pony ORM

    # Relationship
    # One file can have multiple archives
    meta = Required(DriverMeta, reverse="archive", unique=True)
    file = Optional("FileChecksum", reverse="archives")

    verificationState = Required(str, default=VerificationState.NOT_VERIFIED)

    def to_json(self, expand=False):
        return {
            "identifier": self.identifier,
            "ia_meta": self.ia_meta,
            "meta": self.meta if not expand else self.meta.to_json(),
            "file": self.file if not expand else self.file.to_json(),
        }


# Initialize the database (SQLite example)
# Initialize the database (SQLite example)
db.bind(provider="sqlite", filename=utils.proj_path("config/db.sqlite"), create_db=True)
db.generate_mapping(create_tables=True)


@db_session
def sync_meta_to_db(meta_list: list[dict]):
    existing_ids = select(m.downloadId for m in DriverMeta)[:]
    updated_cnt = 0
    for meta in meta_list:
        if meta["downloadId"] not in existing_ids:
            DriverMeta.from_meta_info(meta)
            updated_cnt += 1
    _logger.info(f"Synced {updated_cnt} meta entries to database.")

    mismatch_cnt = 0
    for id in existing_ids:
        if id not in {meta["downloadId"] for meta in meta_list}:
            mismatch_cnt += 1
    if mismatch_cnt:
        _logger.warning(
            f"{mismatch_cnt} number of items found in db but not in nvidia portal"
        )


@db_session
def mark_all_pending_incomplete():
    pending_ars = select(
        ar for ar in ArchiveEntry if ar.verificationState == VerificationState.PENDING
    )[:]
    for ar in pending_ars:
        ar.verificationState = VerificationState.INCOMPLETE
    if len(pending_ars):
        _logger.info(f"Marked {len(pending_ars)} entries as incomplete.")


@db_session
def get_states_count():
    return {
        s: count(a for a in ArchiveEntry if a.verificationState == s)
        for s in VerificationState
    }


@db_session
def get_meta_count():
    return DriverMeta.select().count()


@db_session
def dump_completed_to_json() -> dict[str, Union["JinjaEntry", dict]]:
    ar_list = ArchiveEntry.select(verificationState=VerificationState.COMPLETE)[:]
    ar_list_json = {ar.identifier: ar.to_json(expand=True) for ar in ar_list}
    ar_list_json = utils.dict_remove_empty_values(ar_list_json)
    return ar_list_json


async def __debug_remove_404_entires():
    """
    Removes entries and associated files from db if head bucket returns 404
    :return:
    """
    with db_session:
        ar = select(
            a
            for a in ArchiveEntry
            if a.verificationState == VerificationState.INCOMPLETE
        )[:]

        from ia import IAClient

        config = utils.read_config()
        async with IAClient(
            access_key=config["ia"]["s3_access_key"],
            secret_key=config["ia"]["s3_secret_key"],
        ) as client:
            for a in ar:
                bucket = a.identifier
                if not await client.head_bucket(bucket):
                    for file in a.files:
                        file.delete()
                    a.delete()
                    print(f"{bucket} deleted from db.")
                else:
                    print(f"Won't delete {bucket}")


async def main():
    j = dump_completed_to_json()
    with open("/tmp/test.json", "w") as f:
        json.dump(j, f, indent=4)


# Example usage
if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
