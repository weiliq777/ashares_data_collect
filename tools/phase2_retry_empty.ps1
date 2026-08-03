$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$rawSequenceLog = Join-Path $root 'logs\phase2_raw_sequence.log'
$sequenceLog = Join-Path $root 'logs\phase2_empty_retry_sequence.log'

function Write-SequenceLog([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    Add-Content -Encoding UTF8 -Path $sequenceLog -Value $line
}

Write-SequenceLog 'empty retry supervisor started; waiting for raw sequence'
while (-not (Select-String -Path $rawSequenceLog -Pattern 'sequence supervisor finished' -Quiet -ErrorAction SilentlyContinue)) {
    Start-Sleep -Seconds 20
}

$datasets = @('stock_st', 'suspend_d', 'stk_limit')
foreach ($dataset in $datasets) {
    $stdout = Join-Path $root "logs\phase2_${dataset}_retry_empty.stdout.log"
    $stderr = Join-Path $root "logs\phase2_${dataset}_retry_empty.stderr.log"
    Write-SequenceLog "$dataset retry started"
    $arguments = @(
        '-u', '-m', 'data_collect.jobs.tushare_phase2_raw_init',
        '--dataset', $dataset,
        '--start-date', '20210802',
        '--end-date', '20260803',
        '--pause', '1.0',
        '--retry-empty'
    )
    $process = Start-Process -FilePath $python -WorkingDirectory $root -ArgumentList $arguments `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 8
    $startupErrors = Select-String -Path $stdout, $stderr -Pattern 'Traceback|ERROR|Exception|failed' -ErrorAction SilentlyContinue
    if ($startupErrors) {
        Write-SequenceLog "$dataset retry startup error detected; inspect $stdout and $stderr"
    }
    Wait-Process -Id $process.Id
    if ($process.ExitCode -ne 0) {
        Write-SequenceLog "$dataset retry exited code=$($process.ExitCode)"
        exit $process.ExitCode
    }
    $tailErrors = Select-String -Path $stdout, $stderr -Pattern 'Traceback|ERROR|Exception|failed' -ErrorAction SilentlyContinue
    if ($tailErrors) {
        Write-SequenceLog "$dataset retry completed with error markers; inspect logs"
    } else {
        Write-SequenceLog "$dataset retry exited successfully"
    }
}

Write-SequenceLog 'empty retry supervisor finished'
