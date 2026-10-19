import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
THIS_FILE = Path(__file__).resolve()
SKIP_DIRS = {".git", ".venv", "data", "__pycache__", ".pytest_cache", ".ruff_cache"}
SCAN_SUFFIXES = {".py", ".bicep", ".yml", ".yaml", ".md", ".json", ".txt", ".sql", ".ini", ".toml"}

PATTERNS = {
    "storage account key": re.compile(r"AccountKey\s*=", re.I),
    "connection string": re.compile(r"DefaultEndpointsProtocol\s*=", re.I),
    "SAS token": re.compile(r"[?&]sig=[A-Za-z0-9%+/=]{20,}"),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "hard-coded password": re.compile(r"password\s*[:=]\s*[\"'][^\"']{4,}[\"']", re.I),
    "databricks token": re.compile(r"\bdapi[0-9a-f]{32}\b"),
}


def test_no_secrets_committed():
    findings = []
    for folder, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            path = Path(folder) / name
            if path.suffix.lower() not in SCAN_SUFFIXES or path.resolve() == THIS_FILE:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for label, pattern in PATTERNS.items():
                if pattern.search(text):
                    findings.append(f"{path.relative_to(ROOT)}: {label}")
    assert not findings, "Possible secrets found:\n" + "\n".join(findings)


def test_gitignore_protects_secrets_and_data():
    entries = (ROOT / ".gitignore").read_text().split()
    assert ".env" in entries
    assert "data/" in entries