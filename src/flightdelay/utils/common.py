import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Tuple

import yaml


def read_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_json(obj: Any, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def month_range(start: str, end: str) -> List[Tuple[int, int]]:
    """'2024-01', '2024-03' -> [(2024,1),(2024,2),(2024,3)]"""
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out, y, m = [], sy, sm
    while (y, m) <= (ey, em):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


@dataclass(frozen=True)
class Paths:
    raw: Path
    interim: Path
    processed: Path
    reference: Path
    artifacts: Path

    @classmethod
    def from_config(cls, cfg: dict) -> "Paths":
        d = cfg["data"]
        paths = cls(*(Path(d[k]) for k in
                      ("raw_dir", "interim_dir", "processed_dir", "reference_dir", "artifacts_dir")))
        for p in (paths.raw, paths.interim, paths.processed, paths.reference, paths.artifacts):
            p.mkdir(parents=True, exist_ok=True)
        return paths


def partition_file(base: Path, name: str, year: int, month: int) -> Path:
    return base / name / f"year={year}" / f"month={month:02d}" / "part.parquet"