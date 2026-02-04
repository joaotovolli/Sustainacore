import datetime as dt

from tools.telemetry import related_companies as rc


def test_company_ticker_from_path():
    assert rc._company_ticker_from_path("/tech100/company/MSFT/") == "MSFT"
    assert rc._company_ticker_from_path("/tech100/company/msft") == "MSFT"
    assert rc._company_ticker_from_path("/tech100/company/") is None


def test_transition_scores_with_google_boost():
    now = dt.datetime(2026, 1, 10, 12, 0, tzinfo=dt.timezone.utc)
    rows = [
        (now, "s1", "/tech100/company/AAA/", "https://example.com", "ua", None, None),
        (now + dt.timedelta(minutes=5), "s1", "/tech100/company/BBB/", "https://www.google.com/search?q=bbb", "ua", None, None),
    ]
    scores, reasons = rc._build_transition_scores(rows, google_boost_weight=3.0)
    assert scores["AAA"]["BBB"] == 4.0
    assert reasons[("AAA", "BBB")] == "google_boost"
