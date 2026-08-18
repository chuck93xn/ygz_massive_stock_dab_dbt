from ingestion.promotion.copy_landing import copy_landing_volume


class _FakeFileInfo:
    def __init__(self, path: str, is_dir: bool):
        # Real dbutils.fs.ls() returns FileInfo.path values prefixed with
        # "dbfs:" even for /Volumes/... paths - modeling that here is the
        # whole point of these fakes (see copy_landing._strip_dbfs_scheme's
        # docstring for the bug this caught).
        self.path = f"dbfs:{path}"
        self._is_dir = is_dir

    def isDir(self) -> bool:
        return self._is_dir


class _FakeFs:
    def __init__(self, tree: dict[str, list[_FakeFileInfo]]):
        self._tree = tree
        self.copies: list[tuple[str, str]] = []

    def ls(self, path: str) -> list[_FakeFileInfo]:
        # Real dbutils.fs.ls() accepts scheme-less /Volumes/... paths as
        # input regardless of what scheme it echoes back in FileInfo.path -
        # the fake's tree is keyed the same way _list_files calls it, on
        # scheme-less paths.
        return self._tree[path]

    def cp(self, source: str, dest: str) -> None:
        self.copies.append((source, dest))


class _FakeDbutils:
    def __init__(self, tree: dict[str, list[_FakeFileInfo]]):
        self.fs = _FakeFs(tree)


def test_copy_landing_volume_copies_nested_files_to_matching_relative_paths():
    source_root = "/Volumes/dev/landing/raw_massive_data"
    dest_root = "/Volumes/prod/landing/raw_massive_data"
    tree = {
        source_root: [_FakeFileInfo(f"{source_root}/daily_bars", is_dir=True)],
        f"{source_root}/daily_bars": [
            _FakeFileInfo(f"{source_root}/daily_bars/date=2026-08-17", is_dir=True)
        ],
        f"{source_root}/daily_bars/date=2026-08-17": [
            _FakeFileInfo(f"{source_root}/daily_bars/date=2026-08-17/abc.jsonl", is_dir=False),
            _FakeFileInfo(f"{source_root}/daily_bars/date=2026-08-17/def.jsonl", is_dir=False),
        ],
    }
    dbutils = _FakeDbutils(tree)

    copy_landing_volume(dbutils, source_path=source_root, dest_path=dest_root)

    assert dbutils.fs.copies == [
        (
            f"{source_root}/daily_bars/date=2026-08-17/abc.jsonl",
            f"{dest_root}/daily_bars/date=2026-08-17/abc.jsonl",
        ),
        (
            f"{source_root}/daily_bars/date=2026-08-17/def.jsonl",
            f"{dest_root}/daily_bars/date=2026-08-17/def.jsonl",
        ),
    ]


def test_copy_landing_volume_handles_trailing_slashes():
    source_root = "/Volumes/dev/landing/raw_massive_data"
    dest_root = "/Volumes/prod/landing/raw_massive_data"
    tree = {
        source_root: [_FakeFileInfo(f"{source_root}/splits/x.jsonl", is_dir=False)],
    }
    dbutils = _FakeDbutils(tree)

    copy_landing_volume(dbutils, source_path=f"{source_root}/", dest_path=f"{dest_root}/")

    assert dbutils.fs.copies == [
        (f"{source_root}/splits/x.jsonl", f"{dest_root}/splits/x.jsonl"),
    ]


def test_copy_landing_volume_strips_dbfs_scheme_from_relative_path():
    """Regression test: dbutils.fs.ls() returning `dbfs:`-prefixed
    FileInfo.path values (while source_path/dest_path are scheme-less) used
    to make the relative-path slice keep a stray tail of source_path glued
    onto every destination path (`_data`, the last 5 characters of
    "raw_massive_data" - see copy_landing._strip_dbfs_scheme). Asserting the
    exact destination path (not just "does it not start with _data") is
    what catches that regression."""
    source_root = "/Volumes/ygz_massive_stock_dev/landing/raw_massive_data"
    dest_root = "/Volumes/ygz_massive_stock_test/landing/raw_massive_data"
    tree = {
        source_root: [_FakeFileInfo(f"{source_root}/daily_bars", is_dir=True)],
        f"{source_root}/daily_bars": [
            _FakeFileInfo(f"{source_root}/daily_bars/abc.jsonl", is_dir=False),
        ],
    }
    dbutils = _FakeDbutils(tree)

    copy_landing_volume(dbutils, source_path=source_root, dest_path=dest_root)

    assert dbutils.fs.copies == [
        (f"{source_root}/daily_bars/abc.jsonl", f"{dest_root}/daily_bars/abc.jsonl"),
    ]
