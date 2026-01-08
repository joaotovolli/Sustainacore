from __future__ import annotations

import datetime as _dt
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from db_helper import get_connection

PIPELINE_STATE_TABLE = "SC_IDX_PIPELINE_STATE"
DEFAULT_PIPELINE_NAME = "sc_idx_pipeline"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATE_PATH = REPO_ROOT / "tools" / "audit" / "output" / "pipeline_state_latest.json"


def _utc_today() -> _dt.date:
    return _dt.datetime.now(_dt.timezone.utc).date()


def _ensure_state_table() -> None:
    sql = (
        "BEGIN "
        "  EXECUTE IMMEDIATE '"
        "    CREATE TABLE SC_IDX_PIPELINE_STATE ("
        "      RUN_ID VARCHAR2(36) NOT NULL,"
        "      PIPELINE_NAME VARCHAR2(64) NOT NULL,"
        "      STAGE_NAME VARCHAR2(64) NOT NULL,"
        "      STAGE_STATUS VARCHAR2(16) NOT NULL,"
        "      STARTED_AT TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,"
        "      ENDED_AT TIMESTAMP,"
        "      DETAILS VARCHAR2(4000),"
        "      CONSTRAINT SC_IDX_PIPELINE_STATE_PK PRIMARY KEY (RUN_ID, STAGE_NAME)"
        "    )'"
        ";"
        "EXCEPTION "
        "  WHEN OTHERS THEN "
        "    IF SQLCODE != -955 THEN "
        "      RAISE; "
        "    END IF; "
        "END;"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def _format_iso(value: _dt.datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)
    return value.astimezone(_dt.timezone.utc).isoformat()


@dataclass
class StageRecord:
    status: str
    started_at: _dt.datetime | None
    ended_at: _dt.datetime | None
    details: str | None


class PipelineStateStore:
    def __init__(
        self,
        *,
        pipeline_name: str = DEFAULT_PIPELINE_NAME,
        state_path: Path | None = None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.state_path = state_path or DEFAULT_STATE_PATH
        self._oracle_ok = False
        try:
            _ensure_state_table()
            self._oracle_ok = True
        except Exception:
            self._oracle_ok = False

    def _load_local_state(self) -> Dict[str, Any]:
        try:
            raw = self.state_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        except Exception:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def _write_local_state(self, payload: Dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _ensure_local_run(self, run_id: str, run_date: _dt.date) -> Dict[str, Any]:
        payload = self._load_local_state()
        if payload.get("pipeline_name") != self.pipeline_name or payload.get("run_date") != run_date.isoformat():
            payload = {"pipeline_name": self.pipeline_name, "run_date": run_date.isoformat(), "run_id": run_id}
        payload.setdefault("stages", {})
        return payload

    def create_run_id(self) -> str:
        return str(uuid.uuid4())

    def fetch_resume_run_id(self, *, run_date: _dt.date | None = None) -> str | None:
        run_date = run_date or _utc_today()
        if self._oracle_ok:
            sql = (
                "SELECT run_id "
                "FROM SC_IDX_PIPELINE_STATE "
                "WHERE pipeline_name = :pipeline_name "
                "AND started_at >= TRUNC(SYSTIMESTAMP) "
                "ORDER BY started_at DESC FETCH FIRST 1 ROWS ONLY"
            )
            try:
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(sql, {"pipeline_name": self.pipeline_name})
                    row = cur.fetchone()
                    if row and row[0]:
                        return str(row[0])
            except Exception:
                self._oracle_ok = False
        payload = self._load_local_state()
        if payload.get("pipeline_name") != self.pipeline_name:
            return None
        if payload.get("run_date") != run_date.isoformat():
            return None
        return payload.get("run_id")

    def fetch_stage_statuses(self, run_id: str) -> Dict[str, StageRecord]:
        stages: Dict[str, StageRecord] = {}
        if self._oracle_ok:
            sql = (
                "SELECT stage_name, stage_status, started_at, ended_at, details "
                "FROM SC_IDX_PIPELINE_STATE "
                "WHERE run_id = :run_id "
                "AND pipeline_name = :pipeline_name"
            )
            try:
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(sql, {"run_id": run_id, "pipeline_name": self.pipeline_name})
                    for stage_name, status, started_at, ended_at, details in cur.fetchall():
                        stages[str(stage_name)] = StageRecord(
                            status=str(status),
                            started_at=started_at,
                            ended_at=ended_at,
                            details=str(details) if details else None,
                        )
                return stages
            except Exception:
                self._oracle_ok = False
        payload = self._load_local_state()
        local_stages = payload.get("stages", {})
        for name, entry in local_stages.items():
            stages[name] = StageRecord(
                status=str(entry.get("status")),
                started_at=_dt.datetime.fromisoformat(entry["started_at"]) if entry.get("started_at") else None,
                ended_at=_dt.datetime.fromisoformat(entry["ended_at"]) if entry.get("ended_at") else None,
                details=entry.get("details"),
            )
        return stages

    def record_stage_start(self, run_id: str, stage_name: str, details: str | None = None) -> None:
        if self._oracle_ok:
            sql = (
                "MERGE INTO SC_IDX_PIPELINE_STATE dst "
                "USING (SELECT :run_id AS run_id, :stage_name AS stage_name FROM dual) src "
                "ON (dst.run_id = src.run_id AND dst.stage_name = src.stage_name) "
                "WHEN MATCHED THEN UPDATE SET "
                "  pipeline_name = :pipeline_name, "
                "  stage_status = 'STARTED', "
                "  started_at = SYSTIMESTAMP, "
                "  ended_at = NULL, "
                "  details = :details "
                "WHEN NOT MATCHED THEN INSERT "
                "  (run_id, pipeline_name, stage_name, stage_status, started_at, details) "
                "VALUES (:run_id, :pipeline_name, :stage_name, 'STARTED', SYSTIMESTAMP, :details)"
            )
            try:
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        sql,
                        {
                            "run_id": run_id,
                            "pipeline_name": self.pipeline_name,
                            "stage_name": stage_name,
                            "details": details,
                        },
                    )
                    conn.commit()
                    return
            except Exception:
                self._oracle_ok = False
        run_date = _utc_today()
        payload = self._ensure_local_run(run_id, run_date)
        payload["stages"][stage_name] = {
            "status": "STARTED",
            "started_at": _format_iso(_dt.datetime.now(_dt.timezone.utc)),
            "ended_at": None,
            "details": details,
        }
        self._write_local_state(payload)

    def record_stage_end(self, run_id: str, stage_name: str, status: str, details: str | None = None) -> None:
        if self._oracle_ok:
            sql = (
                "UPDATE SC_IDX_PIPELINE_STATE "
                "SET stage_status = :status, "
                "    ended_at = SYSTIMESTAMP, "
                "    details = :details "
                "WHERE run_id = :run_id "
                "AND stage_name = :stage_name"
            )
            try:
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        sql,
                        {
                            "run_id": run_id,
                            "stage_name": stage_name,
                            "status": status,
                            "details": details,
                        },
                    )
                    conn.commit()
                    return
            except Exception:
                self._oracle_ok = False
        run_date = _utc_today()
        payload = self._ensure_local_run(run_id, run_date)
        entry = payload["stages"].get(stage_name, {})
        entry.update(
            {
                "status": status,
                "started_at": entry.get("started_at")
                or _format_iso(_dt.datetime.now(_dt.timezone.utc)),
                "ended_at": _format_iso(_dt.datetime.now(_dt.timezone.utc)),
                "details": details,
            }
        )
        payload["stages"][stage_name] = entry
        self._write_local_state(payload)
