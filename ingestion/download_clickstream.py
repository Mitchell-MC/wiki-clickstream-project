"""
ingestion/download_clickstream.py
──────────────────────────────────
Downloads Wikimedia clickstream TSV.GZ dump files for the months listed in
config.py and saves them to data/raw/.

File naming on the Wikimedia server:
  clickstream-<wiki>-<YYYY-MM>.tsv.gz
  e.g. clickstream-enwiki-2024-10.tsv.gz

Dump index page (browse available months):
  https://dumps.wikimedia.org/other/clickstream/

Usage:
  python ingestion/download_clickstream.py
"""

import gzip
import os
import sys
import time
import hashlib
import urllib.request
import urllib.error
from pathlib import Path

# Allow running from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

# ── helpers ───────────────────────────────────────────────────────────────────

def _build_url(month: str) -> str:
    """Return the full download URL for a given YYYY-MM month string."""
    filename = f"clickstream-{config.CLICKSTREAM_WIKI}-{month}.tsv.gz"
    return f"{config.CLICKSTREAM_BASE_URL}/{month}/{filename}"


def _dest_path(month: str) -> Path:
    raw_dir = Path(config.RAW_DATA_DIR)
    raw_dir.mkdir(parents=True, exist_ok=True)
    filename = f"clickstream-{config.CLICKSTREAM_WIKI}-{month}.tsv.gz"
    return raw_dir / filename


def _is_valid_gzip(path: Path) -> bool:
    """
    Return True if *path* is a complete, readable gzip archive.

    Reads the file in 64 KB chunks so the integrity check does not require
    loading the entire decompressed content into memory.  An incomplete or
    corrupted download raises EOFError, OSError, or gzip.BadGzipFile.
    """
    try:
        with gzip.open(path, "rb") as fh:
            while fh.read(65_536):
                pass
        return True
    except (EOFError, OSError, gzip.BadGzipFile):
        return False


class _ProgressLogger:
    """Simple callback for urllib.request.urlretrieve that logs progress."""

    def __init__(self, filename: str):
        self.filename = filename
        self._last_pct = -1

    def __call__(self, block_num: int, block_size: int, total_size: int):
        if total_size <= 0:
            return
        downloaded = block_num * block_size
        pct = min(int(downloaded * 100 / total_size), 100)
        if pct != self._last_pct and pct % 10 == 0:
            print(f"  {self.filename}: {pct}%")
            self._last_pct = pct


def download_month(month: str, force: bool = False) -> Path:
    """
    Download the clickstream dump for *month* (YYYY-MM).

    Parameters
    ----------
    month : str   e.g. "2024-10"
    force : bool  Re-download even if the file already exists.

    Returns
    -------
    Path  Local path of the downloaded file.

    Raises
    ------
    urllib.error.HTTPError  if the server returns a non-200 status.
    """
    url = _build_url(month)
    dest = _dest_path(month)

    if dest.exists() and not force:
        # Guard against partial downloads from a previous interrupted run.
        # If the gzip file is corrupt, treat it as missing and re-download.
        if _is_valid_gzip(dest):
            print(f"[skip]     {dest.name} already exists and is valid")
            return dest
        print(f"[redownload] {dest.name} exists but is corrupt — re-downloading")

    print(f"[download] {url}")
    tmp = dest.with_suffix(".tmp")
    try:
        urllib.request.urlretrieve(url, tmp, reporthook=_ProgressLogger(dest.name))
    except urllib.error.HTTPError as exc:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"Failed to download {url}: HTTP {exc.code} {exc.reason}"
        ) from exc
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    tmp.rename(dest)
    size_mb = dest.stat().st_size / 1_048_576
    print(f"[done]     {dest.name}  ({size_mb:.1f} MB)")
    return dest


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    months = config.CLICKSTREAM_MONTHS
    print(f"Downloading {len(months)} month(s) of {config.CLICKSTREAM_WIKI} clickstream data")
    print(f"Destination: {Path(config.RAW_DATA_DIR).resolve()}\n")

    errors = []
    for month in months:
        try:
            download_month(month)
        except RuntimeError as exc:
            print(f"[ERROR]    {exc}")
            errors.append(month)
        time.sleep(0.5)  # be polite to the Wikimedia servers

    print()
    if errors:
        print(f"Failed months: {errors}")
        sys.exit(1)
    else:
        print("All downloads complete.")


if __name__ == "__main__":
    main()
