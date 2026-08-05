param(
    [Parameter(Mandatory = $true)]
    [int]$RepairPid
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$log = Join-Path $root "logs\finish_adj_factor_gap_repair_$stamp.log"

function Write-Log([string]$Message) {
    Add-Content -Encoding UTF8 -Path $log -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
}

New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
Write-Log "waiting for repair pid=$RepairPid"
while (Get-Process -Id $RepairPid -ErrorAction SilentlyContinue) {
    Start-Sleep -Seconds 30
}
Write-Log 'repair process finished; rebuilding adj_factor Standard'

& $python -u -m data_collect.jobs.tushare_raw_to_standard --table adj_factor *>> $log
if ($LASTEXITCODE -ne 0) { throw 'adj_factor Standard rebuild failed' }

Write-Log 'rebuilding price Feature'
& $python -u -m data_collect.jobs.tushare_phase2_features --task price *>> $log
if ($LASTEXITCODE -ne 0) { throw 'price Feature rebuild failed' }

Write-Log 'running final quality audit'
& $python -u -m data_collect.jobs.tushare_data_quality *>> $log
if ($LASTEXITCODE -ne 0) { throw 'quality audit failed' }
Write-Log 'repair finish workflow completed; baseline remains draft until manual review'
