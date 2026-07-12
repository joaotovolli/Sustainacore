"""Oracle access for auditable TECH100 corporate actions."""

from __future__ import annotations

import datetime as dt

from db_helper import get_connection

from .corporate_actions import ConfirmedCorporateAction, CorporateActionCandidate

INDEX_CODE = "TECH100"


def _missing_object(exc: Exception) -> bool:
    if "ORA-00942" in str(exc):
        return True
    return bool(exc.args and getattr(exc.args[0], "code", None) == 942)


def fetch_confirmed_action(ticker: str, effective_date: dt.date) -> ConfirmedCorporateAction | None:
    sql = (
        "SELECT ticker,effective_date,action_type,ratio,processing_method "
        "FROM SC_IDX_CORPORATE_ACTIONS WHERE index_code=:index_code AND ticker=:ticker "
        "AND effective_date=:effective_date AND confirmation_status IN ('CONFIRMED','APPLIED')"
    )
    try:
        with get_connection() as conn:
            cur = conn.cursor(); cur.execute(sql, {"index_code": INDEX_CODE, "ticker": ticker, "effective_date": effective_date})
            row = cur.fetchone()
    except Exception as exc:
        if _missing_object(exc):
            return None
        raise
    if not row:
        return None
    day = row[1].date() if isinstance(row[1], dt.datetime) else row[1]
    return ConfirmedCorporateAction(str(row[0]), day, str(row[2]), float(row[3]), str(row[4]))


def record_pending_candidate(candidate: CorporateActionCandidate, run_id: str | None) -> bool:
    sql = (
        "MERGE INTO SC_IDX_CORPORATE_ACTIONS d USING (SELECT :index_code index_code,:ticker ticker,:effective_date effective_date,:action_type action_type FROM dual) s "
        "ON (d.index_code=s.index_code AND d.ticker=s.ticker AND d.effective_date=s.effective_date AND d.action_type=s.action_type) "
        "WHEN NOT MATCHED THEN INSERT (index_code,ticker,action_type,effective_date,ratio,confirmation_status,source_type,detected_at,processing_method,processing_run_id) "
        "VALUES (:index_code,:ticker,:action_type,:effective_date,:ratio,'PENDING','PRICE_ANOMALY',SYSTIMESTAMP,'REFRESH_ADJUSTED_HISTORY',:run_id)"
    )
    binds = {"index_code": INDEX_CODE, "ticker": candidate.ticker, "effective_date": candidate.effective_date,
             "action_type": candidate.action_type, "ratio": candidate.ratio, "run_id": run_id}
    try:
        with get_connection() as conn:
            cur=conn.cursor();cur.execute(sql,binds);conn.commit()
        return True
    except Exception as exc:
        if _missing_object(exc):
            return False
        raise


__all__ = ["fetch_confirmed_action", "record_pending_candidate"]
