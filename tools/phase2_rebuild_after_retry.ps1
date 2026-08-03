$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$retryLog = Join-Path $root 'logs\phase2_empty_retry_sequence.log'
$sequenceLog = Join-Path $root 'logs\phase2_final_rebuild_sequence.log'

function Write-SequenceLog([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    Add-Content -Encoding UTF8 -Path $sequenceLog -Value $line
}

Write-SequenceLog 'final rebuild supervisor started; waiting for empty retry'
while (-not (Select-String -Path $retryLog -Pattern 'empty retry supervisor finished' -Quiet -ErrorAction SilentlyContinue)) {
    Start-Sleep -Seconds 20
}

while (Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -match 'tushare_phase2_standard|tushare_phase2_features'
}) {
    Write-SequenceLog 'waiting for existing derived process to exit'
    Start-Sleep -Seconds 20
}

$tasks = @(
    @{ Name = 'standard'; Module = 'data_collect.jobs.tushare_phase2_standard'; Arguments = @('--task', 'all') },
    @{ Name = 'features'; Module = 'data_collect.jobs.tushare_phase2_features'; Arguments = @('--task', 'all') }
)
foreach ($task in $tasks) {
    $stdout = Join-Path $root "logs\phase2_final_$($task.Name).stdout.log"
    $stderr = Join-Path $root "logs\phase2_final_$($task.Name).stderr.log"
    Write-SequenceLog "$($task.Name) final rebuild started"
    $arguments = @('-u', '-m', $task.Module) + $task.Arguments
    $process = Start-Process -FilePath $python -WorkingDirectory $root -ArgumentList $arguments `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 8
    $startupErrors = Select-String -Path $stdout, $stderr -Pattern 'Traceback|ERROR|Exception|failed' -ErrorAction SilentlyContinue
    if ($startupErrors) {
        Write-SequenceLog "$($task.Name) startup error detected; inspect logs"
    }
    Wait-Process -Id $process.Id
    if ($process.ExitCode -ne 0) {
        Write-SequenceLog "$($task.Name) exited code=$($process.ExitCode)"
        exit $process.ExitCode
    }
    Write-SequenceLog "$($task.Name) final rebuild exited successfully"
}

Write-SequenceLog 'final rebuild supervisor finished'
