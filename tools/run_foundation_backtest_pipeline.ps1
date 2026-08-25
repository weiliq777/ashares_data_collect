param(
    [Parameter(Mandatory = $true)]
    [string]$CodeArchivePath,
    [string]$BaselineTag = "foundation_v2_20260815_rebuilt",
    [int]$SnapshotBatchSize = 5,
    [int]$DatasetBatchDays = 5
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$archive = (Resolve-Path -LiteralPath $CodeArchivePath).Path

if ($SnapshotBatchSize -lt 1 -or $SnapshotBatchSize -gt 20) {
    throw "SnapshotBatchSize must be between 1 and 20"
}
if ($DatasetBatchDays -lt 1 -or $DatasetBatchDays -gt 20) {
    throw "DatasetBatchDays must be between 1 and 20"
}

function Invoke-CheckedPython {
    param([string[]]$Arguments)
    Write-Output "[$(Get-Date -Format o)] python $($Arguments -join ' ')"
    & $python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Python stage failed with exit code $LASTEXITCODE"
    }
}

Set-Location -LiteralPath $projectRoot
Write-Output "[$(Get-Date -Format o)] foundation-v2 historical pipeline started"
Write-Output "archive=$archive snapshot_batch_size=$SnapshotBatchSize dataset_batch_days=$DatasetBatchDays"

Invoke-CheckedPython -Arguments @(
    "-m", "data_collect.jobs.tushare_foundation_pipeline",
    "production-derived",
    "--output-end", "2026-08-06",
    "--batch-stocks", "25"
)

Invoke-CheckedPython -Arguments @(
    "-m", "data_collect.jobs.tushare_release",
    "create",
    "--tag", $BaselineTag,
    "--cutoff", "20260806",
    "--schema-version", "feature-contract-v2.0",
    "--notes", "Clean v2.0 foundation rebuild after PIT/version lineage repair"
)

Invoke-CheckedPython -Arguments @(
    "-m", "data_collect.jobs.tushare_release",
    "validate",
    "--tag", $BaselineTag
)

Invoke-CheckedPython -Arguments @(
    "-m", "data_collect.jobs.research_snapshot_series",
    "--calendar-start", "2021-08-02",
    "--calendar-end", "2026-08-06",
    "--baseline-tag", $BaselineTag,
    "--schema-version", "research-v2.0",
    "--freeze",
    "--code-archive-path", $archive,
    "--max-snapshots", "0",
    "--batch-size", $SnapshotBatchSize.ToString()
)

Invoke-CheckedPython -Arguments @(
    "-m", "data_collect.jobs.backtest_dataset_handoff",
    "--baseline-tag", $BaselineTag,
    "--schema-version", "research-v2.0",
    "--expected-snapshots", "1215",
    "--warmup-start", "2021-08-02",
    "--evaluation-start", "2021-08-02",
    "--evaluation-end", "2026-08-06",
    "--out-of-sample-start", "2025-08-06",
    "--dataset-name", "foundation-v2-history-20260815",
    "--dataset-version", "dataset-foundation-v2-20260815-v1",
    "--batch-days", $DatasetBatchDays.ToString(),
    "--code-archive-path", $archive
)

Write-Output "[$(Get-Date -Format o)] foundation-v2 historical pipeline completed"
