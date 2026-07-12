import datetime as dt
from pathlib import Path

import pytest

from tools.db_migrations import repair_sc_idx_corporate_actions as repair

START = dt.date(2025, 1, 2)
END = dt.date(2026, 7, 10)
TAG = "20260712120000ABCD"
RUN_ID = "ca-run-1"


class FakeCursor:
    def __init__(self, *, fail_on: str | None = None, rowcount: int = 1):
        self.sql: list[str] = []
        self.result: list[tuple] = []
        self.fail_on = fail_on
        self.rowcount = rowcount

    def execute(self, sql, binds=None):
        normalized = " ".join(sql.strip().upper().split())
        self.sql.append(normalized)
        if self.fail_on and self.fail_on in normalized:
            raise RuntimeError("forced restoration failure")
        if "WITH P AS" in normalized:
            self.result = [(2, dt.datetime(2025, 11, 4), dt.datetime(2026, 7, 2))]
        elif "MAX(TRADE_DATE)" in normalized:
            self.result = [(END,)]
        else:
            self.result = [(10,)]
        return self

    def executemany(self, sql, binds):
        self.sql.append(" ".join(sql.strip().upper().split()))
        return self

    def fetchone(self):
        return self.result.pop(0)

    def fetchall(self):
        result = list(self.result)
        self.result = []
        return result


class FakeConnection:
    def __init__(self, *, fail_on: str | None = None, rowcount: int = 1):
        self.cur = FakeCursor(fail_on=fail_on, rowcount=rowcount)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def complete_records() -> list[repair.BackupRecord]:
    return [
        repair.BackupRecord(
            RUN_ID,
            TAG,
            target,
            repair.backup_name(TAG, code),
            START,
            END,
            10,
            10,
            "VALIDATED",
        )
        for code, target in repair.BACKUP_OBJECTS.items()
    ]


def test_backup_names_are_unique_timestamp_safe() -> None:
    name = repair.backup_name(TAG, "PC")
    assert name == "SCB_CA_20260712120000ABCD_PC"
    assert len(name) <= 30
    generated = repair.new_backup_tag()
    assert len(generated) == 18
    assert generated[:14].isdigit()


def test_price_file_is_ticker_scoped(tmp_path: Path) -> None:
    path = tmp_path / "prices.csv"
    path.write_text(
        "ticker,trade_date,adjusted_close\nAAA,2026-07-01,100.25\nBBB,2026-07-01,9\n",
        encoding="utf-8",
    )
    assert repair.load_adjusted_prices(path, "AAA", START, END) == {
        dt.date(2026, 7, 1): 100.25
    }


def test_csv_dates_outside_backup_window_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "prices.csv"
    path.write_text("ticker,trade_date,adjusted_close\nAAA,2024-12-31,10\n", encoding="utf-8")
    with pytest.raises(repair.PriceRepairError, match="outside_repair_range"):
        repair.load_adjusted_prices(path, "AAA", START, END)


def test_csv_duplicate_dates_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "prices.csv"
    path.write_text(
        "ticker,trade_date,adjusted_close\nAAA,2026-07-01,10\nAAA,2026-07-01,11\n",
        encoding="utf-8",
    )
    with pytest.raises(repair.PriceRepairError, match="duplicate_adjusted_price_date"):
        repair.load_adjusted_prices(path, "AAA", START, END)


def test_missing_backup_table_causes_zero_deletes_and_zero_commits(monkeypatch) -> None:
    conn = FakeConnection()
    records = complete_records()
    monkeypatch.setattr(repair, "_manifest_rows", lambda *_: records)
    monkeypatch.setattr(
        repair,
        "_object_exists",
        lambda _conn, name: not name.endswith("_PC"),
    )
    monkeypatch.setattr(repair, "_columns", lambda *_: [("ID", "NUMBER")])
    monkeypatch.setattr(repair, "_total_count_and_range", lambda *_: (10, START, END))
    with pytest.raises(repair.BackupValidationError, match="backup_object_missing"):
        repair.rollback(conn, TAG, START, END)
    assert not any(sql.startswith(("DELETE", "INSERT")) for sql in conn.cur.sql)
    assert conn.commits == 0
    assert conn.rollbacks == 0


def test_restoration_failure_rolls_back_whole_transaction(monkeypatch) -> None:
    conn = FakeConnection(fail_on="INSERT INTO SC_IDX_PORTFOLIO_POSITION_DAILY")
    monkeypatch.setattr(repair, "validate_backup_set", lambda *_args, **_kwargs: complete_records())
    with pytest.raises(repair.RestorationError, match="SC_IDX_PORTFOLIO_POSITION_DAILY"):
        repair.rollback(conn, TAG, START, END)
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_validated_complete_backup_set_restores_atomically(monkeypatch) -> None:
    conn = FakeConnection()
    monkeypatch.setattr(repair, "validate_backup_set", lambda *_args, **_kwargs: complete_records())
    restored = repair.rollback(conn, TAG, START, END)
    assert set(restored) == set(repair.BACKUP_OBJECTS.values())
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert sum(sql.startswith("DELETE") for sql in conn.cur.sql) == len(repair.BACKUP_OBJECTS)
    assert sum(sql.startswith("INSERT") for sql in conn.cur.sql) == len(repair.BACKUP_OBJECTS)


def test_colliding_backup_tag_is_rejected(monkeypatch) -> None:
    conn = FakeConnection()
    monkeypatch.setattr(
        repair,
        "_object_exists",
        lambda _conn, name: name.startswith("SCB_CA_"),
    )
    with pytest.raises(repair.BackupCollisionError, match="backup_object_collision"):
        repair.create_backups(conn, TAG, RUN_ID, START, END)
    assert not any(sql.startswith("CREATE TABLE") for sql in conn.cur.sql)
    assert conn.commits == 0


def test_backup_reuse_requires_same_manifest_run(monkeypatch) -> None:
    conn = FakeConnection()
    called = {}

    def validate(_conn, tag, start, end, *, expected_run_id=None):
        called.update(tag=tag, start=start, end=end, run_id=expected_run_id)
        return complete_records()

    monkeypatch.setattr(repair, "validate_backup_set", validate)
    repair.create_backups(conn, TAG, RUN_ID, START, END, reuse=True)
    assert called == {"tag": TAG, "start": START, "end": END, "run_id": RUN_ID}


def test_stale_backup_manifest_run_is_rejected(monkeypatch) -> None:
    conn = FakeConnection()
    monkeypatch.setattr(repair, "_manifest_rows", lambda *_: complete_records())
    with pytest.raises(repair.BackupValidationError, match="run_id_mismatch"):
        repair.validate_backup_set(conn, TAG, START, END, expected_run_id="different-run")
    assert not any(sql.startswith(("DELETE", "INSERT")) for sql in conn.cur.sql)
    assert conn.commits == 0


def test_mismatched_update_count_fails_and_rolls_back() -> None:
    conn = FakeConnection(rowcount=1)
    prices = {dt.date(2026, 6, 30): 100.0, dt.date(2026, 7, 1): 101.0}
    with pytest.raises(repair.PriceRepairError, match="canonical_update_count_mismatch"):
        repair.apply_prices(conn, "AAA", prices)
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_modified_dates_must_exist_in_canonical_backup() -> None:
    conn = FakeConnection()
    conn.cur.result = [(1,)]
    canonical = next(record for record in complete_records() if record.target_object == "SC_IDX_PRICES_CANON")
    with pytest.raises(repair.BackupValidationError, match="canonical_backup_date_count_mismatch"):
        repair.validate_canonical_dates_backed_up(
            conn,
            [canonical],
            "AAA",
            [dt.date(2026, 6, 30), dt.date(2026, 7, 1)],
        )


def test_refresh_requires_material_change_and_split_continuity() -> None:
    before = {dt.date(2026, 7, 1): 400.0, dt.date(2026, 7, 2): 101.0}
    after = {dt.date(2026, 7, 1): 100.0, dt.date(2026, 7, 2): 101.0}
    changed, count, event_return = repair.validate_refreshed_history(
        before,
        after,
        effective_date=dt.date(2026, 7, 2),
        start=START,
        end=END,
    )
    assert changed == dt.date(2026, 7, 1)
    assert count == 1
    assert event_return == pytest.approx(0.01)


@pytest.mark.parametrize("failing_step", ["rebuild", "verify"])
def test_action_remains_confirmed_when_rebuild_or_verification_fails(failing_step: str) -> None:
    calls: list[str] = []

    def step(name):
        def execute():
            calls.append(name)
            if name == failing_step:
                raise RuntimeError(name)
        return execute

    with pytest.raises(repair.ReconstructionError, match=failing_step):
        repair.complete_reconstruction(
            rebuild_fn=step("rebuild"),
            verify_fn=step("verify"),
            mark_applied_fn=step("applied"),
        )
    assert "applied" not in calls


def test_action_becomes_applied_only_after_strict_verification() -> None:
    calls: list[str] = []
    repair.complete_reconstruction(
        rebuild_fn=lambda: calls.append("rebuild"),
        verify_fn=lambda: calls.append("verify"),
        mark_applied_fn=lambda: calls.append("applied"),
    )
    assert calls == ["rebuild", "verify", "applied"]


def test_rerunning_dry_run_performs_zero_oracle_writes() -> None:
    conn = FakeConnection()
    args = repair.parser().parse_args([])
    for _ in range(2):
        end, lines = repair.dry_run(conn, args)
        assert end == END
        assert "oracle_writes=0" in lines
    assert all(sql.startswith(("SELECT", "WITH")) for sql in conn.cur.sql)
    assert conn.commits == 0
    assert conn.rollbacks == 0
