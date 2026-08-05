$ErrorActionPreference = 'Stop'

# Wait for the current serial phase-2 supervisor, then run the read-only audit.
# This script never writes Raw/Standard/PIT/Feature tables.
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$sequenceLog = Join-Path $root 'logs\phase2_resume_and_finish.log'
$auditLog = Join-Path $root 'logs\phase2_quality_after_finish.log'
$stdout = Join-Path $root 'logs\phase2_quality_after_finish.stdout.log'
$stderr = Join-Path $root 'logs\phase2_quality_after_finish.stderr.log'

New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
Add-Content -Encoding UTF8 -Path $auditLog -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') quality watcher started"

while ($true) {
    $lines = if (Test-Path $sequenceLog) { Get-Content $sequenceLog -Tail 20 } else { @() }
    if ($lines -match 'resume and finish supervisor finished') {
        break
    }
    if ($lines -match 'failed; inspect') {
        Add-Content -Encoding UTF8 -Path $auditLog -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') supervisor failure detected; audit not started"
        exit 2
    }
    Start-Sleep -Seconds 30
}

$code = "import json; from data_collect.jobs.tushare_data_quality import run; print(json.dumps(run(), ensure_ascii=False, indent=2))"
Add-Content -Encoding UTF8 -Path $auditLog -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') quality audit started"
$process = Start-Process -FilePath $python -WorkingDirectory $root -ArgumentList @('-u', '-c', $code) `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -PassThru
Start-Sleep -Seconds 8
$markers = Select-String -Path $stdout, $stderr -Pattern 'Traceback|ERROR|Exception|failed' -CaseSensitive:$false -ErrorAction SilentlyContinue
if ($markers) {
    Add-Content -Encoding UTF8 -Path $auditLog -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') quality startup error markers detected"
} else {
    Add-Content -Encoding UTF8 -Path $auditLog -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') quality process started pid=$($process.Id)"
}
