from datetime import timedelta

from tools.research_generator.run_generator import _retry_delay_seconds, _next_retry_at


def test_retry_delay_seconds():
    assert _retry_delay_seconds(0) == 120
    assert _retry_delay_seconds(1) == 300
    assert _retry_delay_seconds(2) == 900
    assert _retry_delay_seconds(3) == 1800
    assert _retry_delay_seconds(4) == 3600
    assert _retry_delay_seconds(10) == 3600


def test_next_retry_at_in_future():
    now = _next_retry_at(1)
    later = _next_retry_at(2)
    assert later > now - timedelta(seconds=1)
