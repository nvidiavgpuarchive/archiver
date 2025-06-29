import hashlib
import json
import os
import shutil
import time
from collections import defaultdict
from datetime import datetime
from math import floor
from os.path import join, exists
from typing import TypedDict, Any

from jinja2 import Environment, FileSystemLoader

import db
import utils
from logger import get_logger
from utils import proj_path


class JinjaEntry(TypedDict):
    identifier: str
    ia_mata: dict[str, Any]
    meta: dict[str, Any]
    file: dict[str, Any]


class JinjaBreadcrumb(TypedDict):
    name: str
    url: str | None


class JinjaListfile(TypedDict):
    entry: JinjaEntry
    url: str


class JinjaIndex(TypedDict):
    option_value: str
    nextlevel_url: str
    result_cnt: int
    newest_entry: JinjaListfile | None


class JinajaReadmeMeta(TypedDict):
    last_updated: str
    total_size: int
    driver_count: int
    non_driver_count: int
    duplicate_ratio: str


class DocGen:

    DRIVER_PARTITION_ORDER = [
        "category",
        "platformName",
        "platformVersion",
        "version",
        "releaseDate",
        "description",
        "name",
    ]
    NON_DRIVER_PARTITION_ORDER = [
        "category",
        "platformName",
        "releaseDate",
        "description",
        "name",
    ]

    def __init__(self, docdir: str):

        self._docdir = docdir

        self._logger = get_logger("docgen")
        self._env = Environment(loader=FileSystemLoader(proj_path("templates")))

        if not exists(docdir):
            os.makedirs(docdir)

        if not os.path.exists(join(docdir, ".docgen")):
            if os.listdir(docdir):
                self._logger.fatal("Docgen directory must be empty upon first running.")
                exit(-1)
        self._update_placeholder(docdir)  # update placeholder

        # delete so we can refresh
        utils.rm_dir(join(docdir, "details"))
        utils.rm_dir(join(docdir, "index"))

    def generate(self):
        # dump db
        self._logger.info("Dumping completed entries to json file... ")
        db_dump: dict[str, JinjaEntry] = db.dump_completed_to_json()
        with open(join(self._docdir, "dump.json"), "w") as f:
            json.dump(db_dump, f, indent=4)

        # index
        self._logger.info("Generating index...")
        driver_index = [
            k for k in db_dump.keys() if db_dump[k]["meta"]["category"] == "Driver"
        ]
        non_driver_index = [
            k for k in db_dump.keys() if db_dump[k]["meta"]["category"] == "NonDriver"
        ]

        driver_parted_index = self._partition_index(
            db_dump,
            driver_index,
            DocGen.DRIVER_PARTITION_ORDER,
            5,
        )
        non_driver_parted_index = self._partition_index(
            db_dump,
            non_driver_index,
            DocGen.NON_DRIVER_PARTITION_ORDER,
            5,
        )

        self._gen_content(
            driver_parted_index, db_dump, DocGen.DRIVER_PARTITION_ORDER, start_level=1
        )
        self._gen_content(
            non_driver_parted_index,
            db_dump,
            DocGen.NON_DRIVER_PARTITION_ORDER,
            start_level=1,
        )

        # readme
        self._logger.info("Generating README and static files...")
        self._gen_readme(driver_parted_index, non_driver_parted_index, db_dump)

        # Copyover static files
        self._copy_static_files()

        self._logger.info("Done.")

    def _partition_index(
        self,
        db_dump: dict[str, JinjaEntry],
        unparted_index: list[str],
        partition_order: list[str],
        level_limit: int,
    ):
        def _rec(index, level):
            if (  # turn nested index into a filelist, if too few items, or linear shape
                len(index) <= 15 or level >= level_limit
            ):
                return sorted(
                    index,
                    key=lambda x: tuple(
                        DocGen._option_cmp_keyfunc(db_dump[x]["meta"].get(o, ""))
                        for o in partition_order
                    ),
                    reverse=True,
                )

            index_options_map = {
                idx: db_dump[idx]["meta"].get(partition_order[level], "OTHER")
                for idx in index
            }
            sorted_options = sorted(
                set(index_options_map.values()),
                key=DocGen._option_cmp_keyfunc,
                reverse=True,
            )

            options_index_map = defaultdict(list)
            for idx, option in index_options_map.items():
                options_index_map[option].append(idx)

            res = {o: _rec(options_index_map[o], level + 1) for o in sorted_options}
            if all(  # dict[list[str]] -> # list[str]
                isinstance(item, list) and len(item) == 1 and isinstance(item[0], str)
                for item in res.values()
            ):
                res = [item[0] for item in res.values()]

            return res

        return _rec(unparted_index, level=0)

    def _gen_readme(
        self,
        parted_driver_index,
        parted_non_driver_index,
        db_dump: dict[str, JinjaEntry],
    ):

        # readme meta
        md5_list = [e["file"]["md5"] for e in db_dump.values()]
        readme_meta = {
            "last_updated": DocGen.index_get_newest_entry(
                parted_driver_index | parted_non_driver_index,
                db_dump,
                DocGen.DRIVER_PARTITION_ORDER,
            )["entry"]["meta"]["releaseDate"],
            "total_size": sum(e["file"]["size"] for e in db_dump.values()),
            "driver_count": DocGen.nested_struct_count_leaves(parted_driver_index, str),
            "non_driver_count": DocGen.nested_struct_count_leaves(
                parted_non_driver_index, str
            ),
            "duplicate_ratio": str(
                round((1 - len(set(md5_list)) / len(md5_list)) * 100, 2)
            )
            + "%",
        }

        # index
        driver_indexes = [
            {
                "option_value": k,
                "nextlevel_url": DocGen._get_index_filepath([k]),
                "result_cnt": DocGen.nested_struct_count_leaves(v, str),
                "newest_entry": DocGen.index_get_newest_entry(
                    v, db_dump, DocGen.DRIVER_PARTITION_ORDER
                ),
            }
            for k, v in parted_driver_index["Driver"].items()
        ]
        non_dirver_indexes = [
            {
                "option_value": k,
                "nextlevel_url": DocGen._get_index_filepath([k]),
                "result_cnt": DocGen.nested_struct_count_leaves(v, str),
                "newest_entry": DocGen.index_get_newest_entry(
                    v, db_dump, DocGen.NON_DRIVER_PARTITION_ORDER
                ),
            }
            for k, v in parted_non_driver_index["NonDriver"].items()
        ]
        with utils.TouchAndOpen(join(self._docdir, "README.md"), "w") as f:
            file_content = self._env.get_template("readme.md").render(
                non_driver_indexes=non_dirver_indexes,
                driver_indexes=driver_indexes,
                readme_meta=readme_meta,
            )
            f.write(file_content)

    def _gen_content(
        self,
        parted_index,
        db_dump: dict[str, JinjaEntry],
        partition_order: list[str],
        start_level: int = 0,
        level_limit: int = 8964,
    ):
        # trail is the sequence of options went through, including current
        # does not include detail file, only works for index and filelists
        def _rec(focus: Any, trail: list[str], level: int):
            if level >= level_limit:
                return
            if isinstance(focus, str):  # generate details file
                jinja_entry = db_dump[focus]
                detail_filepath = DocGen._get_detail_filepath(jinja_entry)
                breadcrumbs = DocGen._get_breadcrumbs(
                    trail, partition_order, jinja_entry, start_level=start_level
                )
                with utils.TouchAndOpen(join(self._docdir, detail_filepath), "w") as f:
                    file_content = self._env.get_template("details.md").render(
                        entry=jinja_entry,
                        breadcrumbs=breadcrumbs,
                        start_level=start_level,
                    )
                    f.write(file_content)
            elif isinstance(focus, list):  # generate filellist file
                jinja_filelists = [
                    {"entry": entry, "url": DocGen._get_detail_filepath(entry)}
                    for entry in map(db_dump.get, focus)
                ]
                breadcrumbs = DocGen._get_breadcrumbs(
                    trail, partition_order, start_level=start_level
                )
                index_filepath = DocGen._get_index_filepath(trail)
                with utils.TouchAndOpen(join(self._docdir, index_filepath), "w") as f:
                    file_content = self._env.get_template("filelist.md").render(
                        filelists=jinja_filelists, breadcrumbs=breadcrumbs
                    )
                    f.write(file_content)
                for item in focus:
                    _rec(item, trail, level + 1)
            else:  # generate index file
                option_name = partition_order[len(trail)]
                jinja_indexes = [
                    {
                        "option_value": k,
                        "nextlevel_url": DocGen._get_index_filepath(trail + [k]),
                        "result_cnt": DocGen.nested_struct_count_leaves(v, str),
                        "newest_entry": DocGen.index_get_newest_entry(
                            v, db_dump, partition_order
                        ),
                    }
                    for k, v in focus.items()
                ]
                breadcrumbs = DocGen._get_breadcrumbs(
                    trail, partition_order, start_level=start_level
                )
                index_filepath = DocGen._get_index_filepath(trail)
                with utils.TouchAndOpen(join(self._docdir, index_filepath), "w") as f:
                    file_content = self._env.get_template("index.md").render(
                        indexes=jinja_indexes,
                        breadcrumbs=breadcrumbs,
                        cur_option=option_name,
                    )
                    f.write(file_content)
                for k, next_focus in focus.items():
                    _rec(next_focus, trail + [k], level + 1)

        _rec(parted_index, [], level=0)

    def _copy_static_files(self):
        src_dir = proj_path("templates/static")
        for item in os.listdir(src_dir):
            src_path = join(src_dir, item)
            dst_path = join(self._docdir, item)
            if os.path.isdir(src_path):
                shutil.copytree(src_path, dst_path)
            elif os.path.isfile(src_path):
                shutil.copy2(src_path, dst_path)  # shutil.copy2 preserves metadata

    @staticmethod
    def _update_placeholder(docdir):
        with utils.TouchAndOpen(os.path.join(docdir, ".docgen"), "w") as f:
            f.write(str(round(time.time(), -5)))

    @staticmethod
    def _get_detail_filepath(entry: JinjaEntry):
        return (
            "details/"
            + hashlib.md5(entry["identifier"].encode("utf-8")).hexdigest()[:6]
            + "_"
            + utils.sanitize_filename(entry["meta"]["description"])
            + ".md"
        )

    @staticmethod
    def _get_index_filepath(trail: list[str]) -> str:
        trail = map(utils.sanitize_filename, trail)
        return "index/" + "/".join(trail) + ".md"

    @staticmethod
    def _get_breadcrumbs(
        trail: list[str], order: list[str], entry=None, start_level=0
    ) -> list[JinjaBreadcrumb]:

        names = [
            (o if not (utils.is_number(o) or o == "OTHER") else f"{order[i]} : {o}")
            for i, o in enumerate(trail)
        ]
        collaspsed_filecrumbs = (
            [
                {"name": names[i], "url": "README.md"}
                for i in range(0, min(len(trail), start_level))
            ]
            if start_level
            else [{"name": "/", "url": "README.md"}]
        )
        breadcrumbs = collaspsed_filecrumbs + [
            {"name": names[i], "url": DocGen._get_index_filepath(trail[: i + 1])}
            for i in range(start_level, len(trail))
        ]
        if entry:
            breadcrumbs.append(
                {
                    "name": entry["meta"]["description"],
                    "url": DocGen._get_detail_filepath(entry),
                }
            )
        return breadcrumbs

    @staticmethod
    def nested_struct_count_leaves(struct: list | dict, objtype: type) -> int:
        if isinstance(struct, objtype):
            return 1
        elif isinstance(struct, list):
            return sum(DocGen.nested_struct_count_leaves(s, objtype) for s in struct)
        elif isinstance(struct, dict):
            return sum(
                DocGen.nested_struct_count_leaves(s, objtype) for s in struct.values()
            )
        else:
            return 0

    # sort options
    @staticmethod
    def _option_cmp_keyfunc(key: str) -> tuple:  # to allow correct versioning
        def _complement_str(s: str):
            if s == "OTHER":  # dirty patch
                return "0" * 10
            return "".join(
                (
                    chr(ord("A") + ord("Z") - ord(c))
                    if c.isupper()
                    else chr(ord("a") + ord("z") - ord(c)) if c.islower() else c
                )
                for c in s
            )

        if not utils.is_number(key):
            return 0, 0, _complement_str(key)
        elif "." not in key:
            return int(key), 0, key
        else:
            return floor(float(key)), float(".".join(key.split(".")[1:])), key

    @staticmethod
    def flatten_neste_struct(struct: list | dict) -> list[str]:
        res = []

        def _rec(s):
            if isinstance(s, list):
                [_rec(item) for item in s]
            elif isinstance(s, dict):
                [_rec(item) for item in s.values()]
            else:
                res.append(s)

        _rec(struct)
        return res

    @staticmethod
    def index_get_newest_entry(
        index: dict, db_dump: dict[str, JinjaEntry], partition_order: list[str]
    ) -> JinjaListfile | None:
        """
        Return one or zero JinjaEntries that has the newest date, and comes first in
        entries that has the same date.
        """
        if not DocGen.nested_struct_count_leaves(index, str):
            return None
        leaves = DocGen.flatten_neste_struct(index)
        entries = [db_dump[idx] for idx in leaves]
        most_recent_date = sorted(
            [x["meta"]["releaseDate"] for x in entries],
            key=lambda d: datetime.strptime(d, "%Y-%m-%d"),
            reverse=True,
        )[0]
        most_recent_entries = [
            e for e in entries if e["meta"]["releaseDate"] == most_recent_date
        ]
        most_recent_entry = sorted(
            most_recent_entries,
            key=lambda x: tuple(
                DocGen._option_cmp_keyfunc(x["meta"].get(o, ""))
                for o in partition_order
            ),
            reverse=True,
        )[0]
        return {
            "entry": most_recent_entry,
            "url": DocGen._get_detail_filepath(most_recent_entry),
        }


if __name__ == "__main__":
    docgen = DocGen(docdir="/tmp/doctest")
    docgen.generate()
