from tools.telemetry.report_filters import is_bot_user_agent, is_probe_path


def test_bot_user_agent_detection():
    assert is_bot_user_agent("Mozilla/5.0 (compatible; Googlebot/2.1)")
    assert is_bot_user_agent("curl/7.68.0")
    assert is_bot_user_agent("python-requests/2.31.0")
    assert not is_bot_user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X)")


def test_probe_path_detection():
    assert is_probe_path("/.env")
    assert is_probe_path("/.git/config")
    assert is_probe_path("/wp-login.php")
    assert not is_probe_path("/news/")
