import os

from tools.oracle import env_bootstrap


def test_load_env_files_parses_and_strips_quotes(tmp_path, monkeypatch):
    env_file = tmp_path / "db.env"
    env_file.write_text(
        """
        # comment
        DB_USER = wksp_user
        DB_PASSWORD="secret"
        INVALID LINE
        EMPTY=
        DB_DSN=db_high
        TNS_ADMIN='/opt/wallet'
        """,
        encoding="utf-8",
    )

    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_DSN", raising=False)
    monkeypatch.delenv("TNS_ADMIN", raising=False)

    presence = env_bootstrap.load_env_files([str(env_file)])

    assert os.environ["DB_USER"] == "wksp_user"
    assert os.environ["DB_PASSWORD"] == "secret"
    assert os.environ["DB_DSN"] == "db_high"
    assert os.environ["TNS_ADMIN"] == "/opt/wallet"
    assert presence == {
        "DB_USER": True,
        "DB_PASSWORD": True,
        "EMPTY": False,
        "DB_DSN": True,
        "TNS_ADMIN": True,
    }


def test_load_env_files_respects_existing_env(monkeypatch, tmp_path):
    env_file = tmp_path / "db.env"
    env_file.write_text("DB_USER=from_file\n", encoding="utf-8")

    monkeypatch.setenv("DB_USER", "existing")

    presence = env_bootstrap.load_env_files([str(env_file)])

    assert os.environ["DB_USER"] == "existing"
    assert presence == {"DB_USER": True}


def test_required_keys_present_tracks_password(monkeypatch):
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_DSN", raising=False)
    monkeypatch.delenv("TNS_ADMIN", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_PASS", raising=False)
    monkeypatch.delenv("DB_PWD", raising=False)
    monkeypatch.delenv("WALLET_PWD", raising=False)

    presence = env_bootstrap.required_keys_present()
    assert presence == {
        "DB_USER": False,
        "DB_DSN": False,
        "TNS_ADMIN": False,
        "DB_PASSWORD|DB_PASS|DB_PWD": False,
        "WALLET_PWD": False,
    }

    monkeypatch.setenv("DB_PASS", "pw")
    monkeypatch.setenv("DB_USER", "wksp")
    monkeypatch.setenv("DB_DSN", "db_high")
    monkeypatch.setenv("TNS_ADMIN", "/opt/wallet")
    monkeypatch.setenv("WALLET_PWD", "w")

    presence = env_bootstrap.required_keys_present()
    assert presence == {
        "DB_USER": True,
        "DB_DSN": True,
        "TNS_ADMIN": True,
        "DB_PASSWORD|DB_PASS|DB_PWD": True,
        "WALLET_PWD": True,
    }
