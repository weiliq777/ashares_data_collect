$ErrorActionPreference = 'Stop'

# Wait for the two market Raw gap-repair workers. Once both finish, rebuild
# only derived layers. Raw tables are never deleted, updated, or truncated.
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$logDir = Join-Path $root 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Get-MissingMarketDates {
    $query = @"
WITH cal AS (
  SELECT DISTINCT cal_date AS trade_date
  FROM tushare_trade_cal_raw
  WHERE exchange='SSE' AND (payload->>'is_open')::int=1
    AND cal_date BETWEEN DATE '2021-08-02' AND DATE '2026-08-03'
)
SELECT
  (SELECT count(*) FROM cal c WHERE NOT EXISTS
     (SELECT 1 FROM tushare_daily_raw r WHERE r.trade_date=c.trade_date))
  +
  (SELECT count(*) FROM cal c WHERE NOT EXISTS
     (SELECT 1 FROM tushare_daily_basic_raw r WHERE r.trade_date=c.trade_date));
"@
    $value = (& docker compose -f (Join-Path $root 'docker-compose.yml') exec -T postgres psql -U ashares -d ashares -Atqc $query | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw "coverage query failed" }
    return [int]$value
}

Write-Output "$(Get-Date -Format s) waiting for market gap repair workers"
while ($true) {
    $workers = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq 'python.exe' -and $_.CommandLine -match 'data_collect\.jobs\.tushare_market_gap_repair'
    })
    if ($workers.Count -eq 0) { break }
    Start-Sleep -Seconds 30
}

$missing = Get-MissingMarketDates
Write-Output "$(Get-Date -Format s) gap workers finished unresolved_missing=$missing"
if ($missing -ne 0) {
    throw "market Raw gaps remain; derived rebuild not started"
}

foreach ($table in @('daily', 'valuation')) {
    Write-Output "$(Get-Date -Format s) rebuilding Standard table=$table"
    & $python -u -m data_collect.jobs.tushare_raw_to_standard --table $table
    if ($LASTEXITCODE -ne 0) { throw "Raw to Standard failed table=$table" }
}

Write-Output "$(Get-Date -Format s) rebuilding phase2 Standard and PIT"
& $python -u -m data_collect.jobs.tushare_phase2_standard --task all
if ($LASTEXITCODE -ne 0) { throw 'phase2 Standard/PIT rebuild failed' }

Write-Output "$(Get-Date -Format s) rebuilding phase2 Features"
& $python -u -m data_collect.jobs.tushare_phase2_features --task all
if ($LASTEXITCODE -ne 0) { throw 'phase2 Feature rebuild failed' }

Write-Output "$(Get-Date -Format s) running final quality audit"
& $python -u -c "import json; from data_collect.jobs.tushare_data_quality import run; print(json.dumps(run(), ensure_ascii=False, indent=2))"
if ($LASTEXITCODE -ne 0) { throw 'final quality audit failed' }

Write-Output "$(Get-Date -Format s) market gap repair and derived rebuild finished"
