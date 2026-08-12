# Recreates the local dev venv from scratch.
# Usage: powershell -ExecutionPolicy Bypass -File scripts/setup_env.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

python -m venv "$root\.venv"
& "$root\.venv\Scripts\python.exe" -m pip install --upgrade pip
& "$root\.venv\Scripts\pip.exe" install -e "$root[dev]"

Write-Output "Venv ready. Activate with: .\.venv\Scripts\Activate.ps1"
