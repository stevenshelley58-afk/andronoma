"""Helpers for writing pipeline outputs to CSV."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd


def write_records(path: Path, records: Iterable[Mapping[str, object]]) -> Path:
    df = pd.DataFrame(list(records))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path
