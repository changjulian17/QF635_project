import sys
from pathlib import Path


def pytest_configure(config):
    venv_python = Path(__file__).parent / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return  # CI or fresh clone without a venv — skip the check
    if Path(sys.executable).resolve() != venv_python.resolve():
        raise RuntimeError(
            f"Wrong Python interpreter.\n"
            f"  Running: {sys.executable}\n"
            f"  Expected: {venv_python}\n\n"
            f"Fix: source .venv/bin/activate && pytest tests/ -v"
        )
