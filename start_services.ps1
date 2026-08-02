$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectDir

Write-Host "[1/3] Starting Docker dependencies..."
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Docker Compose start failed" }

Write-Host "[2/3] Checking PostgreSQL and OpenSearch..."
docker compose ps
docker compose exec -T postgres pg_isready -U ashares -d ashares
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL is not ready" }

$os = Invoke-RestMethod http://127.0.0.1:9200
Write-Host "OpenSearch $($os.version.number) is ready"

Write-Host "[3/3] Starting Python schedulers..."
$Python = Join-Path $ProjectDir ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { throw "Virtual environment not found: $Python" }

Start-Process powershell.exe -WorkingDirectory $ProjectDir -ArgumentList @(
    "-NoExit", "-Command", "& '$Python' 'run_news.py' '--mode' 'scheduler'"
)

Start-Process powershell.exe -WorkingDirectory $ProjectDir -ArgumentList @(
    "-NoExit", "-Command", "& '$Python' 'run_job.py' '--mode' 'scheduler'"
)

Write-Host "Services started. Two scheduler windows were opened."
