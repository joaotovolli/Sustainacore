from datetime import datetime, timedelta, timezone

from app.auth import login_codes


class FakeCursor:
    def __init__(self, state):
        self.state = state
        self._rows = []
        self.last_sql = ""
        self.last_binds = {}

    def execute(self, sql, binds=None):
        self.last_sql = sql
        self.last_binds = binds or {}
        sql_upper = sql.upper()
        if "SELECT COUNT" in sql_upper:
            if "EMAIL_NORMALIZED" in sql_upper:
                self._rows = [(self.state.get("email_count", 0),)]
            elif "REQUEST_IP" in sql_upper:
                self._rows = [(self.state.get("ip_count", 0),)]
            else:
                self._rows = [(0,)]
        elif "INSERT INTO SC_AUTH_LOGIN_CODES" in sql_upper:
            self.state["insert_binds"] = dict(self.last_binds)
            self._rows = []
        elif "SELECT ID, CODE_HASH" in sql_upper:
            row = self.state.get("select_row")
            self._rows = [row] if row else []
        elif "UPDATE SC_AUTH_LOGIN_CODES" in sql_upper:
            self.state.setdefault("updates", []).append((sql, dict(self.last_binds)))
            self._rows = []
        else:
            self._rows = []

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]


class FakeConnection:
    def __init__(self, state):
        self.state = state
        self.cursor_obj = FakeCursor(state)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_request_code_stores_hash_not_plaintext(monkeypatch):
    state = {"email_count": 0, "ip_count": 0}
    conn = FakeConnection(state)
    monkeypatch.setattr(login_codes.db_helper, "get_connection", lambda: conn)

    captured = {}

    def fake_send_login_email(email, code):
        captured["code"] = code
        return True

    monkeypatch.setattr(login_codes, "send_login_email", fake_send_login_email)

    ok = login_codes.request_login_code("user@example.com", "1.2.3.4")
    assert ok is True
    insert = state.get("insert_binds")
    assert insert is not None
    assert captured["code"] != insert.get("code_hash")
    assert captured["code"] not in insert.values()


def test_verify_rejects_expired(monkeypatch):
    expired = datetime.now(timezone.utc) - timedelta(minutes=5)
    state = {"select_row": (1, "deadbeef", "salt", expired, 0)}
    conn = FakeConnection(state)
    monkeypatch.setattr(login_codes.db_helper, "get_connection", lambda: conn)

    token = login_codes.verify_login_code("user@example.com", "123456", "secret")
    assert token is None
    assert not state.get("updates")


def test_verify_rejects_after_too_many_attempts(monkeypatch):
    future = datetime.now(timezone.utc) + timedelta(minutes=5)
    state = {"select_row": (1, "deadbeef", "salt", future, login_codes.MAX_ATTEMPTS)}
    conn = FakeConnection(state)
    monkeypatch.setattr(login_codes.db_helper, "get_connection", lambda: conn)

    token = login_codes.verify_login_code("user@example.com", "123456", "secret")
    assert token is None
    assert not state.get("updates")


def test_verify_success_returns_token(monkeypatch):
    future = datetime.now(timezone.utc) + timedelta(minutes=5)
    email = "user@example.com"
    code = "123456"
    salt = "abc123"
    code_hash = login_codes.hash_code(email, code, salt)
    state = {"select_row": (1, code_hash, salt, future, 0)}
    conn = FakeConnection(state)
    monkeypatch.setattr(login_codes.db_helper, "get_connection", lambda: conn)

    token = login_codes.verify_login_code(email, code, "secret")
    assert token
    assert token.count(".") == 2
    assert state.get("updates")
