import json

import pytest

from app.index_engine.reconstruction_status import (
    read_reconstruction_status,
    write_reconstruction_status,
)


def test_status_write_is_atomic_and_secrets_free(tmp_path):
    path = tmp_path / "reconstruction_status.json"
    write_reconstruction_status(
        path,
        run_id="ca-1",
        backup_tag="TAG1",
        revision="abc123",
        repair_start="2025-01-02",
        repair_end="2026-07-10",
        stage="portfolio_positions",
        stage_started_at="2026-07-13T00:00:00+00:00",
        last_completed_date="2026-07-10",
        rows_processed=57000,
        status="RUNNING",
        failure_class=None,
        rollback_status=None,
        password="must-not-be-written",
    )

    payload = read_reconstruction_status(path)
    assert payload["run_id"] == "ca-1"
    assert payload["rows_processed"] == 57000
    assert "password" not in payload
    assert "must-not-be-written" not in path.read_text(encoding="utf-8")
    assert not list(tmp_path.glob("*.tmp"))
    json.loads(path.read_text(encoding="utf-8"))


def test_status_rejects_unknown_state(tmp_path):
    with pytest.raises(ValueError, match="invalid_reconstruction_status"):
        write_reconstruction_status(tmp_path / "status.json", status="UNKNOWN")
