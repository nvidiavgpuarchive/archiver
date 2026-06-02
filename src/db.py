# pony orm use rule of thumb:
# 1. use shortliving sessions and don't interrupt them with async
# 2. if a function is full of pony code, isolate it into a seperate thread
import json
import os
from datetime import date, datetime
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
    def from_json(meta_info: dict):
        fields = class_to_fields(DriverMeta, ["id", "archive"])
        meta_info = {k: meta_info.get(k, None) for k in fields}
        meta_info["releaseDate"] = (
            date.fromisoformat(meta_info["releaseDate"])
            if "releaseDate" in meta_info
            else None
        )
        meta_info = utils.dict_remove_empty_values(meta_info)
        return DriverMeta(**meta_info)

    def to_json(self, include_id=False):
        exclusion = ["archive"]
        if not include_id:
            exclusion += ["id"]
        fields = class_to_fields(DriverMeta, exclusion)
        data = {k: getattr(self, k) for k in fields if getattr(self, k)}
        data["releaseDate"] = self.releaseDate.isoformat() if self.releaseDate else None
        return data if not include_id else {**data, "id": self.id}


class FileChecksum(db.Entity):
    id = PrimaryKey(int, auto=True)
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

    extra = Optional(Json)

    @staticmethod
    def from_hash_dict(filepath, hash_dict: dict):
        return FileChecksum(
            filenames=[os.path.basename(filepath)],
            size=os.path.getsize(filepath),
            **hash_dict,
        )

    def to_json(self, include_id=False):
        fields = class_to_fields(FileChecksum, ["id", "archives"])
        data = {k: getattr(self, k) for k in fields if getattr(self, k)}
        return data if not include_id else {**data, "id": self.id}

    @staticmethod
    def from_json(json_data: dict):
        """
        Creates an instance of FileChecksum from the provided JSON data.
        Constructs key-value pairs for all fields, filling missing values with None.
        """
        fields = class_to_fields(FileChecksum, ["id", "archives"])
        data = {k: json_data.get(k, "") for k in fields}
        for k in ["extra"]:  # migration
            if isinstance(data.get(k), str):
                if data[k]:
                    data[k] = json.loads(data[k])
                else:
                    data.pop(k)

        return FileChecksum(**data)


class ArchiveEntry(db.Entity):
    identifier = PrimaryKey(str)

    ia_meta = Optional(Json)  # JSON field in Pony ORM

    # Relationship
    # One file can have multiple archives
    meta = Required(DriverMeta, reverse="archive", unique=True)
    file = Optional("FileChecksum", reverse="archives")

    verificationState = Required(str, default=VerificationState.NOT_VERIFIED)
    lastAttemptAt = Optional(datetime)
    extra = Optional(Json)

    def to_json(self, expand=False):
        return {
            "identifier": self.identifier,
            "ia_meta": self.ia_meta,
            "meta": self.meta if not expand else self.meta.to_json(),
            "file": self.file if not expand else self.file.to_json(),
            "extra": self.extra,
        }


# Initialize the database (SQLite example)
# Initialize the database (SQLite example)
db.bind(provider="sqlite", filename=utils.proj_path("config/db.sqlite"), create_db=True)
db.generate_mapping(create_tables=True)


def class_to_fields(cls, excludes: list[str]) -> list[str]:
    return [
        k
        for k, v in cls.__dict__.items()
        if not k.startswith("_") and not callable(v) and k not in excludes
    ]


@db_session
def sync_meta_to_db(meta_list: list[dict]):
    existing_ids = select(m.downloadId for m in DriverMeta)[:]
    updated_cnt = 0
    for meta in meta_list:
        if meta["downloadId"] not in existing_ids:
            DriverMeta.from_json(meta)
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
    if (
        not DriverMeta.select().count()
        or not FileChecksum.select().count()
        or not ArchiveEntry.select().count()
    ):
        _logger.fatal(
            "Database is empty, refusing to dump to json. Have you just created the database?"
        )
        exit(-1)
    ar_list = ArchiveEntry.select(verificationState=VerificationState.COMPLETE)[:]
    ar_list_json = {ar.identifier: ar.to_json(expand=True) for ar in ar_list}
    ar_list_json = utils.dict_remove_empty_values(ar_list_json)
    return ar_list_json


@db_session
def load_from_json(json_filepath: str):
    if (
        DriverMeta.select().count()
        or FileChecksum.select().count()
        or ArchiveEntry.select().count()
    ):
        _logger.fatal("Database is not empty, refusing to load from json.")
        exit(-1)
    with open(json_filepath, "r") as f:
        json_data = json.load(f)
    for k, v in json_data.items():
        meta = DriverMeta.from_json(v["meta"])
        file = FileChecksum.get(md5=v["file"]["md5"])
        if not file:
            file = FileChecksum.from_json(v["file"])
        ArchiveEntry(
            identifier=k,
            meta=meta,
            file=file,
            ia_meta=v["ia_meta"],
            extra=v.get("extra"),
            verificationState=VerificationState.COMPLETE,
        )
        commit()
        _logger.debug(f"Loaded '{k}' to db.")
    _logger.info(f"Loaded {len(json_data)} entries from json.")


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
    pass


# with open()
# Example usage
if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
