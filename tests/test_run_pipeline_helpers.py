import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.orchestration import _derive_terminal_status, _is_oracle_transient_error


def test_is_oracle_transient_error_matches_known_tokens():
    assert _is_oracle_transient_error("ORA-12545: Connect failed")
    assert _is_oracle_transient_error(RuntimeError("ORA-29002: SSL failure"))
    assert not _is_oracle_transient_error("ORA-01017: invalid username/password")


def test_derive_terminal_status_prefers_failed_and_blocked():
    failed_state = {
        "stage_results": {
            "ingest_prices": {"status": "FAILED"},
            "calc_index": {"status": "OK"},
        },
        "warnings": [],
    }
    blocked_state = {
        "stage_results": {
            "preflight_oracle": {"status": "BLOCKED"},
        },
        "warnings": [],
    }
    degraded_state = {
        "stage_results": {
            "completeness_check": {"status": "DEGRADED"},
        },
        "warnings": ["canon_incomplete"],
    }
    skip_state = {
        "stage_results": {
            "determine_target_dates": {"status": "SKIP"},
        },
        "warnings": [],
    }

    assert _derive_terminal_status(failed_state) == "failed"
    assert _derive_terminal_status(blocked_state) == "blocked"
    assert _derive_terminal_status(degraded_state) == "success_with_degradation"
    assert _derive_terminal_status(skip_state) == "clean_skip"
