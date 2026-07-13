import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "tools/index_engine/run_reconstruction_low_resource.sh"


def test_launcher_detaches_and_prints_one_shot_coordinates(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    recorded = tmp_path / "systemd-run-args.txt"
    fake_sudo = fake_bin / "sudo"
    fake_sudo.write_text(
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$RECORDED_ARGS\"\nexit 0\n",
        encoding="utf-8",
    )
    fake_sudo.chmod(0o755)
    fake_readlink = fake_bin / "readlink"
    fake_readlink.write_text(
        f"#!/usr/bin/env bash\nprintf '%s\\n' '{ROOT}'\n",
        encoding="utf-8",
    )
    fake_readlink.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["RECORDED_ARGS"] = str(recorded)

    result = subprocess.run(
        [str(LAUNCHER), "--start", "2025-01-02", "--end", "2026-07-10"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )

    args = recorded.read_text(encoding="utf-8").splitlines()
    assert "--no-block" in args
    assert "--wait" not in args
    assert "--pipe" not in args
    assert "--property=KillMode=control-group" in args
    assert "launch_status=ACCEPTED" in result.stdout
    assert "unit_name=sc-idx-reconstruction-" in result.stdout
    assert "status_file=/var/lib/sustainacore/sc_idx/reconstruction_status.json" in result.stdout
    assert "status_command=" in result.stdout
    assert "tools/index_engine/reconstruction_status.py" in result.stdout


def test_launcher_has_no_runtime_kill_or_polling_loop():
    source = LAUNCHER.read_text(encoding="utf-8")
    assert "RuntimeMaxSec" not in source
    assert "while " not in source
    assert "until " not in source
    assert "--wait" not in source
    assert "--pipe" not in source
