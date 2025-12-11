from datetime import datetime
from typing import Any, Dict, List, Optional

import app as flask_app
import db_helper


def _norm_date(val: Any) -> str:
    if isinstance(val, datetime):
        return val.date().isoformat()
    return str(val)


class _FakeCursor:
    def __init__(self, rows: List[Dict[str, Any]], columns: List[str]):
        self._base_rows = rows
        self._rows: List[tuple] = []
        self.description = [(col, None) for col in columns]
        self.sql = ""
        self.binds: Dict[str, Any] = {}
        self.columns = columns

    def execute(self, sql: str, binds: Optional[Dict[str, Any]] = None):
        self.sql = sql
        self.binds = binds or {}
        rows = list(self._base_rows)

        def _row_matches(row: Dict[str, Any]) -> bool:
            if "port_date" in self.binds:
                target = _norm_date(self.binds["port_date"])
                if _norm_date(row["PORT_DATE"]) != target:
                    return False
            if "sector" in self.binds:
                if str(row.get("GICS_SECTOR", "")).lower() != str(self.binds["sector"]).lower():
                    return False
            if "ticker_exact" in self.binds:
                if str(row.get("TICKER", "")).lower() != str(self.binds["ticker_exact"]).lower():
                    return False
            if "search_like" in self.binds:
                needle = str(self.binds["search_like"]).strip("%").lower()
                hay_company = str(row.get("COMPANY_NAME", "")).lower()
                hay_ticker = str(row.get("TICKER", "")).lower()
                if needle not in hay_company and needle not in hay_ticker:
                    return False
            return True

        rows = [r for r in rows if _row_matches(r)]
        limit = int(self.binds.get("limit", len(rows)))
        rows = rows[:limit]
        self._rows = [tuple(r.get(col) for col in self.columns) for r in rows]

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, rows: List[Dict[str, Any]], columns: List[str]):
        self._cursor = _FakeCursor(rows, columns)

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


COLUMNS = [
    "PORT_DATE",
    "RANK_INDEX",
    "COMPANY_NAME",
    "TICKER",
    "PORT_WEIGHT",
    "GICS_SECTOR",
    "TRANSPARENCY",
    "ETHICAL_PRINCIPLES",
    "GOVERNANCE_STRUCTURE",
    "REGULATORY_ALIGNMENT",
    "STAKEHOLDER_ENGAGEMENT",
    "AIGES_COMPOSITE_AVERAGE",
    "SUMMARY",
    "SOURCE_LINKS",
]

BASE_ROWS: List[Dict[str, Any]] = [
    {
        "PORT_DATE": datetime(2024, 7, 31),
        "RANK_INDEX": 1,
        "COMPANY_NAME": "Acme Corp",
        "TICKER": "ACME",
        "PORT_WEIGHT": 0.02,
        "GICS_SECTOR": "Technology",
        "TRANSPARENCY": 88,
        "ETHICAL_PRINCIPLES": 77,
        "GOVERNANCE_STRUCTURE": 66,
        "REGULATORY_ALIGNMENT": 55,
        "STAKEHOLDER_ENGAGEMENT": 44,
        "AIGES_COMPOSITE_AVERAGE": 90,
        "SUMMARY": "Acme leads on governance.",
        "SOURCE_LINKS": "https://example.com/acme",
    },
    {
        "PORT_DATE": datetime(2024, 6, 30),
        "RANK_INDEX": 2,
        "COMPANY_NAME": "Acme Corp",
        "TICKER": "ACME",
        "PORT_WEIGHT": 1.2,
        "PORT_WEIGHT": 0.02,
        "GICS_SECTOR": "Technology",
        "TRANSPARENCY": 70,
        "ETHICAL_PRINCIPLES": None,
        "GOVERNANCE_STRUCTURE": 60,
        "REGULATORY_ALIGNMENT": None,
        "STAKEHOLDER_ENGAGEMENT": 40,
        "AIGES_COMPOSITE_AVERAGE": 80,
        "SUMMARY": None,
        "SOURCE_LINKS": None,
    },
    {
        "PORT_DATE": datetime(2024, 7, 31),
        "RANK_INDEX": 5,
        "COMPANY_NAME": "Bravo Inc",
        "TICKER": "BRVO",
        "PORT_WEIGHT": 0.01,
        "GICS_SECTOR": "Finance",
        "TRANSPARENCY": 65,
        "ETHICAL_PRINCIPLES": 50,
        "GOVERNANCE_STRUCTURE": 45,
        "REGULATORY_ALIGNMENT": 40,
        "STAKEHOLDER_ENGAGEMENT": 35,
        "AIGES_COMPOSITE_AVERAGE": 70,
        "SUMMARY": "Finance leader on ESG.",
        "SOURCE_LINKS": "",
    },
]


def _install_fakes(monkeypatch, rows: Optional[List[Dict[str, Any]]] = None):
    monkeypatch.setattr(db_helper, "_to_plain", lambda v: v)
    monkeypatch.setattr(
        db_helper,
        "get_connection",
        lambda: _FakeConnection(list(rows or BASE_ROWS), COLUMNS),
    )
    flask_app._API_AUTH_TOKEN = "secret"
    flask_app._API_AUTH_WARNED = False


def test_api_tech100_returns_history_and_aliases(monkeypatch):
    _install_fakes(monkeypatch)

    client = flask_app.app.test_client()
    resp = client.get("/api/tech100", headers={"Authorization": "Bearer secret"})

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["count"] == 3

    items = body["items"]
    assert {item["port_date"] for item in items} == {"2024-07-31", "2024-06-30"}

    first = items[0]
    for key in (
        "port_date",
        "rank_index",
        "company_name",
        "ticker",
        "port_weight",
        "weight",
        "sector",
        "transparency",
        "ethical_principles",
        "governance_structure",
        "regulatory_alignment",
        "stakeholder_engagement",
        "aiges_composite",
        "summary",
        "source_links",
        "overall",
        "accountability",
        "updated_at",
    ):
        assert key in first

    acme = next(item for item in items if item["company_name"] == "Acme Corp" and item["rank_index"] == 1)
        "port_weight",
        "weight",
    ):
        assert key in first

    acme = next(
        item for item in items if item["company_name"] == "Acme Corp" and item["rank_index"] == 1
    )
    assert acme["aiges_composite"] == 90.0
    assert acme["overall"] == 90.0
    assert acme["accountability"] == acme["governance_structure"]
    assert acme["updated_at"] == acme["port_date"]
    assert acme["summary"] == "Acme leads on governance."
    assert acme["port_weight"] == 2.0
    assert acme["weight"] == 2.0

    acme_prev = next(item for item in items if item["company_name"] == "Acme Corp" and item["rank_index"] == 2)
    assert acme_prev["port_weight"] == 1.2
    assert acme_prev["weight"] == 1.2
    bravo = next(item for item in items if item["company_name"] == "Bravo Inc")
    assert bravo["port_weight"] == 1.0


def test_api_tech100_filters_port_date(monkeypatch):
    _install_fakes(monkeypatch)
    client = flask_app.app.test_client()

    resp = client.get(
        "/api/tech100",
        headers={"Authorization": "Bearer secret"},
        query_string={"port_date": "2024-06-30"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["count"] == 1
    assert body["items"][0]["port_date"] == "2024-06-30"
    assert body["items"][0]["company_name"] == "Acme Corp"
    assert body["items"][0]["weight"] == 1.2


def test_api_tech100_filters_sector(monkeypatch):
    _install_fakes(monkeypatch)
    client = flask_app.app.test_client()

    resp = client.get(
        "/api/tech100",
        headers={"Authorization": "Bearer secret"},
        query_string={"sector": "Finance"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["count"] == 1
    item = body["items"][0]
    assert item["company_name"] == "Bravo Inc"
    assert item["sector"] == "Finance"
    assert item["aiges_composite"] == 70.0
    assert item["weight"] == 1.0
