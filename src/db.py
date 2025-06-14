# pony orm use rule of thumb:
# 1. use shortliving sessions and don't interrupt them with async
# 2. if a function is full of pony code, isolate it into a seperate thread
import hashlib
import os
from datetime import date
from enum import Enum

from pony.orm import *

import utils


class VerificationState(str, Enum):
    NOT_VERIFIED = "NOT_VERIFIED"
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


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

    def to_json(self):
        return {
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


class FileChecksum(db.Entity):
    id = PrimaryKey(int, auto=True)
    # TODO: doesn't have to be unique
    # also add logic for duplicatoin check in main
    filename = Required(str, unique=True, index=True)
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

    # Relationships
    archive = Optional("ArchiveEntry", reverse="files")

    @staticmethod
    def from_file(filepath: str):
        hash_dict = utils.sync_multihash(
            filepath,
            hashfuncs=[
                hashlib.md5,
                hashlib.sha1,
                hashlib.sha256,
                hashlib.sha512,
                hashlib.bake2b,
            ],
        )
        return FileChecksum(
            filename=os.path.basename(filepath),
            size=os.path.getsize(filepath),
            **hash_dict,
        )

    @staticmethod
    def from_hash_dict(filepath, hash_dict: dict):
        return FileChecksum(
            filename=os.path.basename(filepath),
            size=os.path.getsize(filepath),
            **hash_dict,
        )

    def update_from_hash_dict(self, filepath, hash_dict: dict):
        self.filename = os.path.basename(filepath)
        self.size = os.path.getsize(filepath)

        for key, value in hash_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)


class ArchiveEntry(db.Entity):
    identifier = PrimaryKey(str)

    ia_meta = Optional(Json)  # JSON field in Pony ORM

    # Relationship: Foreign Key to DriverMeta
    meta = Required(DriverMeta, reverse="archive", unique=True)
    files = Set(FileChecksum, reverse="archive")

    verificationState = Required(str, default=VerificationState.NOT_VERIFIED)


# Initialize the database (SQLite example)
# Initialize the database (SQLite example)
db.bind(provider="sqlite", filename=utils.proj_path("config/db.sqlite"), create_db=True)
db.generate_mapping(create_tables=True)


@db_session
def sync_meta_to_db(meta_list: list[dict]):
    existing_ids = select(m.downloadId for m in DriverMeta)[:]

    for meta in meta_list:
        if meta["downloadId"] not in existing_ids:
            DriverMeta.from_meta_info(meta)


# Example usage
if __name__ == "__main__":
    with db_session:
        ar = select(
            a for a in ArchiveEntry if a.verificationState == VerificationState.COMPLETE
        )[:][0]
        meta = ar.meta
        print(meta.to_json())
