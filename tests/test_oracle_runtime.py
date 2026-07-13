import pytest

from app.index_engine import oracle_runtime


class Connection:
    call_timeout = None


def test_configured_oracle_call_timeout_is_applied(monkeypatch):
    monkeypatch.setenv("SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS", "456789")
    conn = Connection()
    assert oracle_runtime.configure_reconstruction_connection(conn) is conn
    assert conn.call_timeout == 456789


def test_default_oracle_call_timeout_is_conservative(monkeypatch):
    monkeypatch.delenv("SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS", raising=False)
    conn = Connection()
    oracle_runtime.configure_reconstruction_connection(conn)
    assert conn.call_timeout == 300_000


def test_optional_configuration_leaves_scheduled_connection_unchanged(monkeypatch):
    monkeypatch.delenv("SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS", raising=False)
    conn = Connection()
    oracle_runtime.configure_reconstruction_connection_if_enabled(conn)
    assert conn.call_timeout is None


@pytest.mark.parametrize("value", ("0", "-1"))
def test_nonpositive_oracle_call_timeout_is_rejected(monkeypatch, value):
    monkeypatch.setenv("SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS", value)
    with pytest.raises(ValueError, match="must_be_positive"):
        oracle_runtime.configure_reconstruction_connection(Connection())
