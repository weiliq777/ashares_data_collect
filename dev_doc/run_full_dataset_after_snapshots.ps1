<#
    Production handoff after a complete Research Snapshot series exists.

    The script only reads FROZEN snapshots and then creates, builds, validates,
    and freezes one immutable Backtest Dataset. It never writes Raw tables.
    Baseline/date/count are mandatory so an old historical baseline cannot be
    selected silently after the source boundary or schema changes.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$BaselineTag,

    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 10000)]
    [int]$ExpectedSnapshots,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$WarmupStart,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$EvaluationStart,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$EvaluationEnd,

    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$OutOfSampleStart = "",

    [string]$SchemaVersion = "research-v1.2",
    [string]$DatasetName = "history-baseline",
    [string]$DatasetVersion = "dataset-v3.0",
    [string]$CalendarRuleVersion = "tushare-sse-v1",
    [string]$MarketRuleVersion = "cn-a-share-v2",
    [string]$FeeRuleVersion = "research-fee-assumption-v3",
    [string]$BenchmarkCode = "000300.SH",
    [string]$ArchivePath = "",
    [int]$SnapshotParentPid = 0,
    [string]$LogPath = "F:\a_github\Finance_cn\ashares_data_collect\logs\full_history_dataset.log"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Project Python does not exist: $python"
}

function Write-Log([string]$Message) {
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogPath) | Out-Null
Write-Log "handoff started expected=$ExpectedSnapshots baseline=$BaselineTag schema=$SchemaVersion"

while ($true) {
    $summary = docker exec ashares-postgres psql -U ashares -d ashares -At -F "|" -c @"
SELECT
  COUNT(*) FILTER (WHERE status='FROZEN'),
  COUNT(*) FILTER (WHERE status='FAILED'),
  COUNT(*)
FROM research.snapshot_manifest
WHERE data_baseline_tag='$BaselineTag' AND schema_version='$SchemaVersion';
"@ 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Database status query failed: $summary"
    }
    $parts = ($summary | Select-Object -Last 1).ToString().Trim().Split('|')
    $frozen = [int]$parts[0]
    $failed = [int]$parts[1]
    $total = [int]$parts[2]
    Write-Log "snapshot status frozen=$frozen failed=$failed total=$total"
    if ($failed -gt 0) {
        throw "Failed snapshots exist; Dataset creation is blocked"
    }
    if ($frozen -eq $ExpectedSnapshots -and $total -eq $ExpectedSnapshots) {
        break
    }
    if ($frozen -gt $ExpectedSnapshots -or $total -gt $ExpectedSnapshots) {
        throw "Snapshot count exceeds the explicit contract: frozen=$frozen total=$total expected=$ExpectedSnapshots"
    }
    if ($SnapshotParentPid -le 0) {
        throw "Snapshot series is incomplete and no watcher PID was supplied: frozen=$frozen expected=$ExpectedSnapshots"
    }
    if (-not (Get-Process -Id $SnapshotParentPid -ErrorAction SilentlyContinue)) {
        throw "Snapshot process ended before the expected coverage was reached"
    }
    Start-Sleep -Seconds 60
}

$env:ASHARES_HANDOFF_BASELINE = $BaselineTag
$env:ASHARES_HANDOFF_SCHEMA = $SchemaVersion
$env:ASHARES_HANDOFF_EXPECTED = $ExpectedSnapshots.ToString()
$env:ASHARES_HANDOFF_DATASET_NAME = $DatasetName
$env:ASHARES_HANDOFF_DATASET_VERSION = $DatasetVersion
$env:ASHARES_HANDOFF_WARMUP_START = $WarmupStart
$env:ASHARES_HANDOFF_EVALUATION_START = $EvaluationStart
$env:ASHARES_HANDOFF_EVALUATION_END = $EvaluationEnd
$env:ASHARES_HANDOFF_OOS_START = $OutOfSampleStart
$env:ASHARES_HANDOFF_CALENDAR_VERSION = $CalendarRuleVersion
$env:ASHARES_HANDOFF_MARKET_VERSION = $MarketRuleVersion
$env:ASHARES_HANDOFF_FEE_VERSION = $FeeRuleVersion
$env:ASHARES_HANDOFF_BENCHMARK = $BenchmarkCode
$env:ASHARES_HANDOFF_ARCHIVE = $ArchivePath

Write-Log "all snapshots frozen; starting Dataset create/build/validate/freeze"
$pythonCode = @'
import os
from datetime import date

from data_collect.jobs.backtest_dataset import (
    build_dataset,
    create_dataset,
    freeze_dataset,
    validate_dataset,
)
from data_collect.utils.db import get_connection

baseline = os.environ["ASHARES_HANDOFF_BASELINE"]
schema = os.environ["ASHARES_HANDOFF_SCHEMA"]
expected = int(os.environ["ASHARES_HANDOFF_EXPECTED"])
with get_connection() as conn, conn.cursor() as cur:
    cur.execute(
        """SELECT snapshot_id
             FROM research.snapshot_manifest
            WHERE data_baseline_tag=%s AND schema_version=%s AND status='FROZEN'
            ORDER BY research_cutoff_date""",
        (baseline, schema),
    )
    snapshot_ids = [str(row[0]) for row in cur.fetchall()]
if len(snapshot_ids) != expected:
    raise RuntimeError(f"expected {expected} frozen snapshots, got {len(snapshot_ids)}")

oos_text = os.environ.get("ASHARES_HANDOFF_OOS_START", "")
dataset_id = create_dataset(
    snapshot_ids=snapshot_ids,
    dataset_name=os.environ["ASHARES_HANDOFF_DATASET_NAME"],
    dataset_version=os.environ["ASHARES_HANDOFF_DATASET_VERSION"],
    baseline_tag=baseline,
    warmup_start=date.fromisoformat(os.environ["ASHARES_HANDOFF_WARMUP_START"]),
    evaluation_start=date.fromisoformat(os.environ["ASHARES_HANDOFF_EVALUATION_START"]),
    evaluation_end=date.fromisoformat(os.environ["ASHARES_HANDOFF_EVALUATION_END"]),
    out_of_sample_start=date.fromisoformat(oos_text) if oos_text else None,
    calendar_rule_version=os.environ["ASHARES_HANDOFF_CALENDAR_VERSION"],
    market_rule_version=os.environ["ASHARES_HANDOFF_MARKET_VERSION"],
    fee_rule_version=os.environ["ASHARES_HANDOFF_FEE_VERSION"],
    benchmark_code=os.environ["ASHARES_HANDOFF_BENCHMARK"],
)
print(f"DATASET_ID={dataset_id}", flush=True)
print(f"BUILD={build_dataset(dataset_id)}", flush=True)
print(f"VALIDATE={validate_dataset(dataset_id)}", flush=True)
archive = os.environ.get("ASHARES_HANDOFF_ARCHIVE") or None
freeze_dataset(dataset_id, code_archive_path=archive)
print(f"FROZEN_DATASET_ID={dataset_id}", flush=True)
'@

try {
    & $python -u -m data_collect.jobs.backtest_dataset_handoff `
        --baseline-tag $BaselineTag `
        --schema-version $SchemaVersion `
        --expected-snapshots $ExpectedSnapshots `
        --warmup-start $WarmupStart `
        --evaluation-start $EvaluationStart `
        --evaluation-end $EvaluationEnd `
        --dataset-name $DatasetName `
        --dataset-version $DatasetVersion `
        --calendar-rule-version $CalendarRuleVersion `
        --market-rule-version $MarketRuleVersion `
        --fee-rule-version $FeeRuleVersion `
        --benchmark-code $BenchmarkCode `
        --code-archive-path $ArchivePath *>> $LogPath
    if ($LASTEXITCODE -ne 0) {
        throw "Dataset handoff failed with exit code $LASTEXITCODE"
    }
    Write-Log "Dataset handoff completed successfully"
}
finally {
    @(
        "ASHARES_HANDOFF_BASELINE", "ASHARES_HANDOFF_SCHEMA", "ASHARES_HANDOFF_EXPECTED",
        "ASHARES_HANDOFF_DATASET_NAME", "ASHARES_HANDOFF_DATASET_VERSION",
        "ASHARES_HANDOFF_WARMUP_START", "ASHARES_HANDOFF_EVALUATION_START",
        "ASHARES_HANDOFF_EVALUATION_END", "ASHARES_HANDOFF_OOS_START",
        "ASHARES_HANDOFF_CALENDAR_VERSION", "ASHARES_HANDOFF_MARKET_VERSION",
        "ASHARES_HANDOFF_FEE_VERSION", "ASHARES_HANDOFF_BENCHMARK",
        "ASHARES_HANDOFF_ARCHIVE"
    ) | ForEach-Object { Remove-Item "Env:$_" -ErrorAction SilentlyContinue }
}
