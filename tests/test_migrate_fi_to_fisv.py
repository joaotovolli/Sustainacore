from tools.db_migrations import migrate_fi_to_fisv as mig


def test_choose_best_row_prefers_ok_status():
    rows = [
        {"RID": "1", "TICKER": "FI", "STATUS": "OK", "ADJ_CLOSE_PX": 10.0, "CLOSE_PX": 10.0},
        {"RID": "2", "TICKER": "FISV", "STATUS": "ERROR", "ADJ_CLOSE_PX": 10.0, "CLOSE_PX": 10.0},
    ]
    best = mig._choose_best_row("SC_IDX_PRICES_RAW", rows)
    assert best["TICKER"] == "FI"


def test_choose_best_row_prefers_fisv_on_tie():
    rows = [
        {"RID": "1", "TICKER": "FI", "STATUS": "OK", "ADJ_CLOSE_PX": 10.0, "CLOSE_PX": 10.0},
        {"RID": "2", "TICKER": "FISV", "STATUS": "OK", "ADJ_CLOSE_PX": 10.0, "CLOSE_PX": 10.0},
    ]
    best = mig._choose_best_row("SC_IDX_PRICES_RAW", rows)
    assert best["TICKER"] == "FISV"
