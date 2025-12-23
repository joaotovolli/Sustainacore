from __future__ import annotations

import fcntl
from contextlib import contextmanager

LOCK_PATH = "/tmp/sc_idx_pipeline.lock"


@contextmanager
def run_lock():
    """Global lock to prevent overlapping ingest + calc runs."""

    handle = open(LOCK_PATH, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


__all__ = ["run_lock", "LOCK_PATH"]
