"""Clean venv: pip install this tree, then import. Run: python tests/test_bare_install.py"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_requires_daily_extra() -> None:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    if "pipecat-ai[daily]" not in text:
        raise RuntimeError("pyproject.toml must depend on pipecat-ai[daily]")
    if '"pipecat-ai>=' in text or "'pipecat-ai>=" in text:
        raise RuntimeError("pyproject.toml still depends on plain pipecat-ai")


def test_bare_pip_install_imports() -> None:
    """Install only this package (and its declared deps). Import must work."""
    with tempfile.TemporaryDirectory(prefix="pipecat-akapulu-bare-") as raw:
        root = Path(raw)
        venv_dir = root / "venv"
        venv.create(venv_dir, with_pip=True)
        if sys.platform == "win32":
            python = venv_dir / "Scripts" / "python.exe"
            pip = venv_dir / "Scripts" / "pip.exe"
        else:
            python = venv_dir / "bin" / "python"
            pip = venv_dir / "bin" / "pip"
        if not python.is_file() or not pip.is_file():
            raise RuntimeError(f"venv tools missing under {venv_dir}")
        subprocess.run([str(pip), "install", "-U", "pip"], check=True)
        subprocess.run([str(pip), "install", str(ROOT)], check=True)
        # Must not have been installed via an editable path that skips deps.
        location = subprocess.check_output(
            [
                str(python),
                "-c",
                "import pipecat_akapulu, pathlib; "
                "print(pathlib.Path(pipecat_akapulu.__file__).resolve())",
            ],
            text=True,
        ).strip()
        if "site-packages" not in location.replace("\\", "/"):
            raise RuntimeError(f"bare install is not site-packages: {location}")
        daily = subprocess.check_output(
            [
                str(python),
                "-c",
                "import importlib.util; "
                "print(importlib.util.find_spec('daily') is not None)",
            ],
            text=True,
        ).strip()
        if daily != "True":
            raise RuntimeError("bare install did not pull daily-python")


if __name__ == "__main__":
    test_pyproject_requires_daily_extra()
    test_bare_pip_install_imports()
    print("ok")
