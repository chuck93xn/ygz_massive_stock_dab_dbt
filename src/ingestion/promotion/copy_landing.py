"""Promotion layer: copy dev's Landing Volume into prod's, so prod can run
load_bronze against real, already-fetched data without independently
calling massive.com for the same watchlist - see
plan/records/08_bronze_promotion_process.md for why this exists and why the
copy is file-by-file instead of a single directory-level `dbutils.fs.cp
(..., recurse=True)`.
"""

from __future__ import annotations

from typing import Any

_DBFS_SCHEME = "dbfs:"


def _strip_dbfs_scheme(path: str) -> str:
    """dbutils.fs.ls() returns FileInfo.path values prefixed with `dbfs:`
    even for /Volumes/... paths, but the rest of this codebase (settings.py,
    the promotion job's DEV_LANDING_VOLUME_PATH constant) always uses the
    scheme-less form. Mixing the two broke the relative-path math in
    copy_landing_volume: slicing a `dbfs:`-prefixed path by the length of a
    scheme-less source_path left a stray 5-character remainder ("_data",
    the tail of "raw_massive_data") glued onto every copied path. See
    plan/records/09_bronze_promotion_process.md."""
    return path.removeprefix(_DBFS_SCHEME)


def _list_files(dbutils: Any, path: str) -> list[str]:
    files: list[str] = []
    for entry in dbutils.fs.ls(path):
        entry_path = _strip_dbfs_scheme(entry.path)
        if entry.isDir():
            files.extend(_list_files(dbutils, entry_path))
        else:
            files.append(entry_path)
    return files


def copy_landing_volume(dbutils: Any, *, source_path: str, dest_path: str) -> None:
    """Copies every file under source_path to the same relative path under
    dest_path, file by file - not a single directory-level `dbutils.fs.cp
    (..., recurse=True)`, since every run after the first hits a
    non-empty destination and it's unclear whether that merges into dest
    or nests source inside it. File-to-file cp has no such ambiguity: a
    destination file either gets overwritten with identical bytes (Landing
    files are never modified after being written, so that's a no-op) or
    created fresh."""
    source_path = _strip_dbfs_scheme(source_path).rstrip("/")
    dest_path = _strip_dbfs_scheme(dest_path).rstrip("/")
    for file_path in _list_files(dbutils, source_path):
        relative = file_path[len(source_path):].lstrip("/")
        dbutils.fs.cp(file_path, f"{dest_path}/{relative}")
