$ErrorActionPreference = 'Stop'

# Serial phase-2 supervisor. Raw jobs are checkpointed and hash-idempotent.
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$sequenceLog = Join-Path $root 'logs\phase2_resume_and_finish.log'

function Write-SequenceLog([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $Message"
    Add-Content -Encoding UTF8 -Path $sequenceLog -Value $line
}

function Invoke-CheckedTask([hashtable]$Task) {
    $name = [string]$Task.Name
    $stdout = Join-Path $root "logs\phase2_${name}_resume_${stamp}.stdout.log"
    $stderr = Join-Path $root "logs\phase2_${name}_resume_${stamp}.stderr.log"
    $arguments = @('-u', '-m', [string]$Task.Module) + $Task.Arguments
    Write-SequenceLog "$name started stdout=$stdout stderr=$stderr"
    $process = Start-Process -FilePath $python -WorkingDirectory $root -ArgumentList $arguments `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 8
    $startupErrors = Select-String -Path $stdout, $stderr -Pattern 'Traceback|ERROR|Exception|failed' -CaseSensitive:$false -ErrorAction SilentlyContinue
    if ($startupErrors) {
        Write-SequenceLog "$name startup error markers detected"
    }
    Wait-Process -Id $process.Id -ErrorAction SilentlyContinue
    $process.Refresh()
    $errors = Select-String -Path $stdout, $stderr -Pattern 'Traceback|ERROR|Exception|failed' -CaseSensitive:$false -ErrorAction SilentlyContinue
    $stdoutSize = (Get-Item $stdout -ErrorAction SilentlyContinue).Length
    $stderrSize = (Get-Item $stderr -ErrorAction SilentlyContinue).Length
    $exitCode = $process.ExitCode
    # The Windows uv shim may expose a blank ExitCode. The final database audit
    # is authoritative; here we stop only on explicit errors or no output.
    if ($errors -or (($stdoutSize + $stderrSize) -eq 0)) {
        Write-SequenceLog "$name failed exit=$exitCode output=$($stdoutSize + $stderrSize)"
        throw "$name failed; inspect $stdout and $stderr"
    }
    Write-SequenceLog "$name finished exit=$exitCode output=$($stdoutSize + $stderrSize)"
}

New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
Write-SequenceLog 'resume and finish supervisor started'

$tasks = @(
    @{ Name = 'namechange'; Module = 'data_collect.jobs.tushare_phase2_raw_init'; Arguments = @('--dataset', 'namechange', '--start-date', '20210802', '--end-date', '20260803', '--pause', '0.5') },
    @{ Name = 'dividend'; Module = 'data_collect.jobs.tushare_phase2_raw_init'; Arguments = @('--dataset', 'dividend', '--start-date', '20210802', '--end-date', '20260803', '--pause', '0.5') },
    @{ Name = 'index_daily'; Module = 'data_collect.jobs.tushare_phase2_raw_init'; Arguments = @('--dataset', 'index_daily', '--start-date', '20210802', '--end-date', '20260803', '--pause', '0.5') },
    @{ Name = 'stock_st_retry_empty'; Module = 'data_collect.jobs.tushare_phase2_raw_init'; Arguments = @('--dataset', 'stock_st', '--start-date', '20210802', '--end-date', '20260803', '--pause', '1.0', '--retry-empty') },
    @{ Name = 'suspend_d_retry_empty'; Module = 'data_collect.jobs.tushare_phase2_raw_init'; Arguments = @('--dataset', 'suspend_d', '--start-date', '20210802', '--end-date', '20260803', '--pause', '1.0', '--retry-empty') },
    @{ Name = 'stk_limit_retry_empty'; Module = 'data_collect.jobs.tushare_phase2_raw_init'; Arguments = @('--dataset', 'stk_limit', '--start-date', '20210802', '--end-date', '20260803', '--pause', '1.0', '--retry-empty') },
    @{ Name = 'standard'; Module = 'data_collect.jobs.tushare_phase2_standard'; Arguments = @('--task', 'all') },
    @{ Name = 'features'; Module = 'data_collect.jobs.tushare_phase2_features'; Arguments = @('--task', 'all') }
)

foreach ($task in $tasks) {
    Invoke-CheckedTask $task
}

Write-SequenceLog 'resume and finish supervisor finished'
