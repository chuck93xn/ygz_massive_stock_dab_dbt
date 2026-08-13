# Recreates .venv_dbc (Databricks Connect, serverless) from scratch.
# Kept separate from .venv because databricks-connect and pyspark cannot
# coexist in the same environment.
#
# Needs Python 3.12 specifically - the VS Code Databricks extension refuses
# to manage Databricks Connect on 3.11 ("Databricks Connect requires Python
# 3.12"), and databricks-connect 19.x (latest on 3.12+) doesn't support this
# workspace's serverless backend yet, hence the pin in requirements-dbc.txt.
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts/setup_env_dbc.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

py -3.12 -m venv "$root\.venv_dbc"
& "$root\.venv_dbc\Scripts\python.exe" -m pip install --upgrade pip
& "$root\.venv_dbc\Scripts\pip.exe" install -r "$root\requirements-dbc.txt"

Write-Output ".venv_dbc ready. Activate with: .\.venv_dbc\Scripts\Activate.ps1"
