from ingestion.promotion.copy_landing import copy_landing_volume


class _FakeFileInfo:
    def __init__(self, path: str, is_dir: bool):
        self.path = path
        self._is_dir = is_dir

    def isDir(self) -> bool:
        return self._is_dir


class _FakeFs:
    def __init__(self, tree: dict[str, list[_FakeFileInfo]]):
        self._tree = tree
        self.copies: list[tuple[str, str]] = []

    def ls(self, path: str) -> list[_FakeFileInfo]:
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
