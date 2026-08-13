# Recreates venv_dbc (Databricks Connect, serverless) from scratch.
# Kept separate from .venv because databricks-connect and pyspark cannot
# coexist in the same environment.
# Usage: powershell -ExecutionPolicy Bypass -File scripts/setup_env_dbc.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

python -m venv "$root\venv_dbc"
& "$root\venv_dbc\Scripts\python.exe" -m pip install --upgrade pip
& "$root\venv_dbc\Scripts\pip.exe" install -r "$root\requirements-dbc.txt"

Write-Output "venv_dbc ready. Activate with: .\venv_dbc\Scripts\Activate.ps1"
