from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_sc_idx_pipeline_service_uses_internal_lock_only():
    unit_text = (REPO_ROOT / "infra/systemd/sc-idx-pipeline.service").read_text(encoding="utf-8")

    assert "tools/index_engine/run_pipeline.py" in unit_text
    assert "ExecStart=/usr/bin/flock -n /tmp/sc_idx_pipeline.lock" not in unit_text
    assert "ExecStart=/home/opc/Sustainacore/.venv/bin/python" in unit_text
