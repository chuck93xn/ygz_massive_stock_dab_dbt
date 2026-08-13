# Recreates the local dev venv from scratch.
# Needs Python 3.11 specifically (matches pyproject.toml's
# requires-python = ">=3.10,<3.12"); bare `python` isn't reliable here since
# it resolves to whatever the system default is (e.g. a newer 3.x outside
# that range), which would silently build an incompatible venv.
# Usage: powershell -ExecutionPolicy Bypass -File scripts/setup_env.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

py -3.11 -m venv "$root\.venv"
& "$root\.venv\Scripts\python.exe" -m pip install --upgrade pip
& "$root\.venv\Scripts\pip.exe" install -e "$root[dev]"

Write-Output ".venv ready. Activate with: .\.venv\Scripts\Activate.ps1"
