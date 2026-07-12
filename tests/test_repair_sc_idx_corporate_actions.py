import datetime as dt
import subprocess
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
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql, binds=None):
        normalized = " ".join(sql.strip().upper().split())
        self.sql.append(normalized)
        self.calls.append((normalized, binds))
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

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


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


def test_backup_columns_allow_ctas_nullability_relaxation(monkeypatch) -> None:
    target = [("TRADE_DATE", "DATE", 7, None, None, "N")]
    backup = [("TRADE_DATE", "DATE", 7, None, None, "Y")]
    monkeypatch.setattr(
        repair,
        "_columns",
        lambda _conn, name: target if name == "TARGET" else backup,
    )
    assert repair._columns_compatible(object(), "TARGET", "BACKUP")


def test_backup_columns_reject_restore_shape_mismatch(monkeypatch) -> None:
    target = [("TRADE_DATE", "DATE", 7, None, None, "N")]
    backup = [("TRADE_DATE", "TIMESTAMP(6)", 11, None, 6, "Y")]
    monkeypatch.setattr(
        repair,
        "_columns",
        lambda _conn, name: target if name == "TARGET" else backup,
    )
    assert not repair._columns_compatible(object(), "TARGET", "BACKUP")


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


def test_backup_ddl_uses_typed_date_literals_without_binds(monkeypatch) -> None:
    conn = FakeConnection()
    created: set[str] = set()

    def object_exists(_conn, name):
        if name in repair.BACKUP_OBJECTS.values():
            return True
        return name in created

    original_execute = conn.cur.execute

    def execute(sql, binds=None):
        normalized = " ".join(sql.strip().upper().split())
        if normalized.startswith("CREATE TABLE"):
            created.add(normalized.split()[2])
        return original_execute(sql, binds)

    conn.cur.execute = execute
    monkeypatch.setattr(repair, "_object_exists", object_exists)
    monkeypatch.setattr(repair, "_count_and_range", lambda *_: (10, START, END))
    monkeypatch.setattr(repair, "_total_count_and_range", lambda *_: (10, START, END))
    monkeypatch.setattr(repair, "_columns", lambda *_: [("ID", "NUMBER")])
    monkeypatch.setattr(repair, "validate_backup_set", lambda *_args, **_kwargs: complete_records())

    repair.create_backups(conn, TAG, RUN_ID, START, END)

    ddl_calls = [(sql, binds) for sql, binds in conn.cur.calls if sql.startswith("CREATE TABLE")]
    assert len(ddl_calls) == len(repair.BACKUP_OBJECTS)
    assert all(binds is None for _, binds in ddl_calls)
    assert all("DATE '2025-01-02'" in sql and "DATE '2026-07-10'" in sql for sql, _ in ddl_calls)


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


def test_failed_readiness_precedes_schema_and_backup_writes(monkeypatch, tmp_path: Path) -> None:
    conn = FakeConnection()
    writes = []
    price_file = tmp_path / "prices.csv"
    price_file.write_text("ticker,trade_date,adjusted_close\nAAA,2026-07-01,10\n", encoding="utf-8")
    monkeypatch.setattr(repair, "load_env_files", lambda: None)
    monkeypatch.setattr(repair, "get_connection", lambda: conn)
    monkeypatch.setattr(repair, "dry_run", lambda *_: (END, []))
    monkeypatch.setattr(
        repair,
        "run_reconstruction_readiness",
        lambda *_: (_ for _ in ()).throw(RuntimeError("readiness_failed")),
    )
    monkeypatch.setattr(repair, "ensure_action_schema", lambda *_: writes.append("schema"))
    monkeypatch.setattr(repair, "create_backups", lambda *_args, **_kwargs: writes.append("backup"))

    with pytest.raises(RuntimeError, match="readiness_failed"):
        repair.main(
            [
                "--apply",
                "--ticker",
                "AAA",
                "--effective-date",
                "2026-07-02",
                "--ratio",
                "4",
                "--source-reference",
                "authoritative-reference",
                "--adjusted-price-csv",
                str(price_file),
                "--start",
                START.isoformat(),
                "--end",
                END.isoformat(),
            ]
        )
    assert writes == []
    assert conn.commits == 0


def workflow_context(tmp_path: Path) -> repair.ApplyContext:
    return repair.ApplyContext(
        TAG,
        RUN_ID,
        START,
        END,
        f"repair --rollback-tag {TAG} --start {START} --end {END}",
        tmp_path / "apply-report.txt",
    )


def run_controlled(
    tmp_path: Path,
    stages: list[repair.ApplyStage],
    *,
    rollback_fn=None,
    verify_restoration_fn=None,
    action_status_fn=None,
    events: list[str] | None = None,
):
    conn = FakeConnection()
    lines: list[str] = []
    events = events if events is not None else []

    def printer(line, flush=False):
        events.append(f"print:{line}:flush={flush}")

    code = repair.execute_controlled_apply(
        conn,
        workflow_context(tmp_path),
        stages,
        lines,
        rollback_fn=rollback_fn or (lambda: events.append("rollback")),
        verify_restoration_fn=verify_restoration_fn or (lambda: events.append("restore_verified")),
        action_status_fn=action_status_fn or (lambda: "NOT_RECORDED"),
        printer=printer,
    )
    return code, conn, lines, events


def test_csv_post_write_continuity_failure_invokes_atomic_restoration(tmp_path: Path) -> None:
    prices = {"changed": False}

    def update():
        prices["changed"] = True

    def fail_validation():
        raise repair.PriceRepairError("split_discontinuity_unresolved")

    def restore():
        prices["changed"] = False

    code, _, lines, events = run_controlled(
        tmp_path,
        [
            repair.ApplyStage("csv_canonical_update", update, may_mutate=True),
            repair.ApplyStage("validate_csv_history", fail_validation),
        ],
        rollback_fn=restore,
    )
    assert code == 2
    assert not prices["changed"]
    assert "automatic_rollback=PASS" in lines
    assert "restore_verified" in events


def test_automated_ingest_validation_failure_invokes_restoration(tmp_path: Path) -> None:
    state = {"ingested": False}
    events: list[str] = []
    code, _, lines, events = run_controlled(
        tmp_path,
        [
            repair.ApplyStage(
                "automated_price_ingestion",
                lambda: state.update(ingested=True),
                may_mutate=True,
            ),
            repair.ApplyStage(
                "validate_refreshed_history",
                lambda: (_ for _ in ()).throw(repair.PriceRepairError("no_material_change")),
            ),
        ],
        rollback_fn=lambda: (state.update(ingested=False), events.append("rollback")),
        events=events,
    )
    assert code == 2
    assert not state["ingested"]
    assert "automatic_rollback=PASS" in lines


def test_ingestion_subprocess_failure_after_possible_write_attempts_restoration(tmp_path: Path) -> None:
    events: list[str] = []

    def ingest():
        events.append("ingest_may_have_written")
        raise subprocess.CalledProcessError(1, ["ingest"])

    code, _, lines, events = run_controlled(
        tmp_path,
        [repair.ApplyStage("automated_price_ingestion", ingest, may_mutate=True)],
        events=events,
    )
    assert code == 2
    assert "rollback" in events
    assert "failing_stage=automated_price_ingestion" in lines


def test_action_upsert_failure_restores_prices_and_reports_not_recorded(tmp_path: Path) -> None:
    state = {"prices_changed": False}
    code, _, lines, events = run_controlled(
        tmp_path,
        [
            repair.ApplyStage(
                "csv_canonical_update",
                lambda: state.update(prices_changed=True),
                may_mutate=True,
            ),
            repair.ApplyStage(
                "confirm_action",
                lambda: (_ for _ in ()).throw(RuntimeError("upsert failed")),
                may_mutate=True,
                action_status_after="CONFIRMED",
            ),
        ],
        rollback_fn=lambda: state.update(prices_changed=False),
        action_status_fn=lambda: "NOT_RECORDED",
    )
    assert code == 2
    assert not state["prices_changed"]
    assert "action_status_before_rollback=NOT_RECORDED" in lines
    assert "action_status=NOT_RECORDED" in lines
    assert "automatic_rollback=PASS" in lines


def test_rebuild_failure_uses_manifest_compensation(tmp_path: Path) -> None:
    code, _, lines, events = run_controlled(
        tmp_path,
        [
            repair.ApplyStage("confirm_action", lambda: None, may_mutate=True, action_status_after="CONFIRMED"),
            repair.ApplyStage(
                "rebuild_official_index",
                lambda: (_ for _ in ()).throw(RuntimeError("rebuild failed")),
                may_mutate=True,
            ),
        ],
    )
    assert code == 2
    assert "rollback" in events
    assert "automatic_rollback=PASS" in lines


def test_strict_verification_failure_never_marks_applied(tmp_path: Path) -> None:
    events: list[str] = []
    code, _, lines, events = run_controlled(
        tmp_path,
        [
            repair.ApplyStage("confirm_action", lambda: events.append("confirmed"), True, "CONFIRMED"),
            repair.ApplyStage("rebuild_official_index", lambda: events.append("official"), True),
            repair.ApplyStage("rebuild_portfolio_outputs", lambda: events.append("portfolio"), True),
            repair.ApplyStage(
                "strict_verification",
                lambda: (_ for _ in ()).throw(RuntimeError("verification failed")),
            ),
            repair.ApplyStage("mark_action_applied", lambda: events.append("applied"), True, "APPLIED"),
        ],
        events=events,
    )
    assert code == 2
    assert "applied" not in events
    assert "failing_stage=strict_verification" in lines
    assert "action_status_before_rollback=CONFIRMED" in lines


def test_rollback_failure_reports_original_and_rollback_errors(tmp_path: Path) -> None:
    code, _, lines, _ = run_controlled(
        tmp_path,
        [
            repair.ApplyStage(
                "csv_canonical_update",
                lambda: (_ for _ in ()).throw(RuntimeError("original failure")),
                may_mutate=True,
            )
        ],
        rollback_fn=lambda: (_ for _ in ()).throw(RuntimeError("rollback failure")),
        action_status_fn=lambda: "CONFIRMED",
    )
    assert code == 2
    assert "original_error=original failure" in lines
    assert "rollback_error=rollback failure" in lines
    assert "automatic_rollback=FAIL" in lines
    assert "action_status=CONFIRMED" in lines
    assert any(line.startswith("rollback_command=") for line in lines)


def test_backup_context_is_emitted_before_first_mutation(tmp_path: Path) -> None:
    events: list[str] = []

    def mutate():
        events.append("mutation")

    code, _, _, events = run_controlled(
        tmp_path,
        [repair.ApplyStage("repair", mutate, may_mutate=True)],
        events=events,
    )
    assert code == 0
    mutation_index = events.index("mutation")
    required = ("backup_tag=", "backup_run_id=", "repair_start=", "repair_end=", "rollback_command=")
    for prefix in required:
        index = next(i for i, event in enumerate(events) if event.startswith(f"print:{prefix}"))
        assert index < mutation_index
        assert events[index].endswith("flush=True")
    report = (tmp_path / "apply-report.txt").read_text(encoding="utf-8")
    assert f"backup_tag={TAG}" in report
    assert f"backup_run_id={RUN_ID}" in report


def test_failure_before_mutation_does_not_restore(tmp_path: Path) -> None:
    events: list[str] = []
    code, conn, lines, events = run_controlled(
        tmp_path,
        [
            repair.ApplyStage(
                "prepare_csv_repair",
                lambda: (_ for _ in ()).throw(repair.PriceRepairError("scope failed")),
            )
        ],
        events=events,
    )
    assert code == 2
    assert "rollback" not in events
    assert "automatic_rollback=NOT_REQUIRED" in lines
    assert conn.commits == 0


def test_success_order_includes_all_apply_gates(tmp_path: Path) -> None:
    events: list[str] = []
    stages = [
        repair.ApplyStage("repair", lambda: events.append("repair"), True),
        repair.ApplyStage("validate", lambda: events.append("validate")),
        repair.ApplyStage("confirm", lambda: events.append("confirm"), True, "CONFIRMED"),
        repair.ApplyStage("official", lambda: events.append("official"), True),
        repair.ApplyStage("portfolio", lambda: events.append("portfolio"), True),
        repair.ApplyStage("verify", lambda: events.append("verify")),
        repair.ApplyStage("apply", lambda: events.append("apply"), True, "APPLIED"),
    ]
    code, _, lines, events = run_controlled(tmp_path, stages, events=events)
    assert code == 0
    ordered = [event for event in events if not event.startswith("print:")]
    assert ordered == ["repair", "validate", "confirm", "official", "portfolio", "verify", "apply"]
    assert "action_status=APPLIED" in lines
