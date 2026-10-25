[CmdletBinding()]
param(
    [ValidateSet("init", "migrate", "status", "test", "test-real-golden", "test-real-window", "stop", "clean")]
    [string]$Action = "status",
    [ValidatePattern("^[a-z0-9][a-z0-9-]{1,30}$")]
    [string]$InstanceName = "default",
    [ValidateRange(15433, 15999)]
    [int]$HostPort = 15433,
    [switch]$RemoveVolume
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repoRoot "docker-compose.integration-test.yml"
$stateDir = if ($InstanceName -eq "default") {
    Join-Path $repoRoot ".codex\integration-test"
} else {
    Join-Path $repoRoot ".codex\integration-test\$InstanceName"
}
$envFile = Join-Path $stateDir "compose.env"
$identityFile = Join-Path $stateDir "environment-id.txt"
$migrationStateFile = Join-Path $stateDir "migrations.json"
$testConfigFile = Join-Path $stateDir "config.yaml"
$instanceToken = $InstanceName.Replace("-", "_")
$projectName = if ($InstanceName -eq "default") { "ashares-integration-test" } else { "ashares-$InstanceName" }
$volumeName = if ($InstanceName -eq "default") {
    "ashares_integration_test_pgdata"
} else {
    "ashares_${instanceToken}_pgdata"
}
$containerName = if ($InstanceName -eq "default") {
    "ashares-integration-test-postgres"
} else {
    "ashares-$InstanceName-postgres"
}
$script:hostPort = $HostPort
$migrationDir = Join-Path $repoRoot "sql"

function New-SecretToken {
    $bytes = [byte[]]::new(36)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return [Convert]::ToBase64String($bytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

function Initialize-LocalState {
    New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
    if (-not (Test-Path -LiteralPath $envFile)) {
        $adminPassword = New-SecretToken
        $appPassword = New-SecretToken
        @(
            "ASHARES_TEST_ADMIN_PASSWORD=$adminPassword"
            "ASHARES_TEST_APP_PASSWORD=$appPassword"
        ) | Set-Content -LiteralPath $envFile -Encoding ascii
    }
    if (-not (Test-Path -LiteralPath $identityFile)) {
        [guid]::NewGuid().ToString() | Set-Content -LiteralPath $identityFile -Encoding ascii
    }
    $script:localEnv = @{}
    foreach ($line in Get-Content -LiteralPath $envFile) {
        if ($line -match '^([A-Z0-9_]+)=(.*)$') {
            $script:localEnv[$Matches[1]] = $Matches[2]
        }
    }
    foreach ($required in @("ASHARES_TEST_ADMIN_PASSWORD", "ASHARES_TEST_APP_PASSWORD")) {
        if (-not $script:localEnv[$required]) {
            throw "Integration-test credential file is missing $required. Remove only $envFile and rerun init."
        }
    }
    $expectedSettings = @{
        ASHARES_TEST_CONTAINER_NAME = $containerName
        ASHARES_TEST_HOST_PORT = [string]$HostPort
        ASHARES_TEST_VOLUME_NAME = $volumeName
    }
    $environmentLines = @(Get-Content -LiteralPath $envFile)
    foreach ($setting in $expectedSettings.GetEnumerator()) {
        $matchingLines = @($environmentLines | Where-Object { $_ -match "^$($setting.Key)=" })
        if ($matchingLines.Count -gt 0 -and $matchingLines[-1] -ne "$($setting.Key)=$($setting.Value)") {
            throw "Integration-test environment setting $($setting.Key) does not match this instance identity."
        }
        if ($matchingLines.Count -eq 0) {
            $environmentLines += "$($setting.Key)=$($setting.Value)"
        }
        $script:localEnv[$setting.Key] = $setting.Value
    }
    Set-Content -LiteralPath $envFile -Value $environmentLines -Encoding ascii
    $script:environmentId = (Get-Content -LiteralPath $identityFile -Raw).Trim()
    $parsedEnvironmentId = [guid]::Empty
    if (-not [guid]::TryParse($script:environmentId, [ref]$parsedEnvironmentId)) {
        throw "Integration-test environment identity is invalid."
    }
}

function Invoke-Compose {
    param([string[]]$ComposeArgs)
    & docker compose --env-file $envFile -f $composeFile -p $projectName @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($ComposeArgs -join ' ')"
    }
}

function Assert-OwnedContainer {
    $id = (& docker compose --env-file $envFile -f $composeFile -p $projectName ps -q --all postgres 2>$null | Out-String).Trim()
    if (-not $id) {
        throw "Integration-test PostgreSQL container is not present. Run -Action init first."
    }
    $inspectionJson = (& docker inspect $id 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect the integration-test PostgreSQL container."
    }
    try {
        $container = @($inspectionJson | ConvertFrom-Json -ErrorAction Stop)[0]
    }
    catch {
        throw "Could not decode the integration-test PostgreSQL container identity."
    }
    $name = ([string]$container.Name).TrimStart("/")
    $labels = $container.Config.Labels
    $environment = [string]$labels.PSObject.Properties['org.ashares.environment'].Value
    $database = [string]$labels.PSObject.Properties['org.ashares.database'].Value
    if ($name -ne $containerName -or $environment -ne "integration-test" -or $database -ne "ashares_test") {
        throw "Refusing to operate on a container that does not carry the expected integration-test identity."
    }
    return $id
}

function Invoke-TestPsql {
    param(
        [string]$Database = "ashares_test",
        [string]$User = "ashares",
        [string]$Sql,
        [string]$File,
        [switch]$AppPasswordVariable
    )
    $arguments = @("exec", "-e", "PGPASSWORD=$($script:localEnv['ASHARES_TEST_ADMIN_PASSWORD'])", $containerName,
        "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", $User, "-d", $Database)
    if ($AppPasswordVariable) {
        $arguments += @("-v", "app_password=$($script:localEnv['ASHARES_TEST_APP_PASSWORD'])")
    }
    if ($File) {
        $arguments += @("-f", "/migrations/$File")
    }
    elseif ($Sql) {
        $arguments += @("-c", $Sql)
    }
    & docker @arguments
    if ($LASTEXITCODE -ne 0) {
        $subject = if ($File) { " migration '$File'" } else { " SQL command" }
        throw "psql failed for database '$Database'$subject."
    }
}

function Get-MigrationHash {
    param([System.IO.FileInfo]$File)
    return (Get-FileHash -LiteralPath $File.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-TestPsqlOutput {
    param([string]$Sql)
    $output = & docker exec -e "PGPASSWORD=$($script:localEnv['ASHARES_TEST_ADMIN_PASSWORD'])" $containerName `
        psql -X -v ON_ERROR_STOP=1 -U ashares -d ashares_test -At -F "|" -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "Could not read the integration-test migration ledger." }
    return ($output | Out-String).Trim()
}

function Write-LocalMigrationState {
    param([System.Collections.IDictionary]$Applied)
    $files = @(Get-OrderedMigrationFiles)
    $entries = @($files | Where-Object { $Applied.Contains($_.Name) } | ForEach-Object {
        @{ name = $_.Name; sha256 = $Applied[$_.Name] }
    })
    @{ schema_version = 2; applied = $entries } |
        ConvertTo-Json -Depth 4 |
        Set-Content -LiteralPath $migrationStateFile -Encoding utf8
}

function Initialize-MigrationLedger {
    $sql = @"
CREATE SCHEMA IF NOT EXISTS integration_test;
CREATE TABLE IF NOT EXISTS integration_test.schema_migration (
    migration_name TEXT PRIMARY KEY,
    migration_sha256 CHAR(64) NOT NULL,
    applied_order INTEGER NOT NULL UNIQUE,
    record_source TEXT NOT NULL CHECK (record_source IN ('migration', 'local_checkpoint_adoption')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"@
    [void](Invoke-TestPsql -Sql $sql)

    $rows = Get-TestPsqlOutput -Sql `
        "SELECT migration_name || '|' || migration_sha256 FROM integration_test.schema_migration ORDER BY applied_order"
    $databaseState = @{}
    if ($rows) {
        foreach ($line in $rows -split "`r?`n") {
            $parts = $line.Split('|', 2)
            if ($parts.Count -ne 2) { throw "Integration-test migration ledger row is malformed." }
            $databaseState[$parts[0]] = $parts[1].Trim()
        }
    }

    $localState = @{}
    $localSchemaVersion = $null
    if (Test-Path -LiteralPath $migrationStateFile) {
        $decoded = Get-Content -LiteralPath $migrationStateFile -Raw | ConvertFrom-Json -AsHashtable
        $localSchemaVersion = $decoded.schema_version
        foreach ($entry in $decoded.applied) { $localState[$entry.name] = $entry.sha256 }
    }

    if ($databaseState.Count -eq 0 -and $localState.Count -gt 0) {
        $files = @(Get-OrderedMigrationFiles)
        $expectedNames = @($files | ForEach-Object Name)
        if ($localSchemaVersion -notin @(1, 2) -or
            $localState.Count -ne $files.Count -or
            @($localState.Keys | Where-Object { $_ -notin $expectedNames }).Count -gt 0) {
            throw "Local migration checkpoint is incomplete or incompatible with the current migration set; refusing ledger adoption. Clean only the isolated test volume and rerun migrate."
        }
        $knownTables = Get-TestPsqlOutput -Sql `
            "SELECT (to_regclass('ops.tushare_data_release') IS NOT NULL)::int || '|' || (to_regclass('research.stock_snapshot_daily') IS NOT NULL)::int"
        if ($knownTables -ne '1|1') {
            throw "Local migration checkpoint exists but the database has no ledger or expected migrated tables; refusing unsafe adoption. Clean only the isolated test volume and rerun migrate."
        }
        $adoptionRows = @()
        foreach ($file in $files) {
            if (-not $localState.ContainsKey($file.Name)) { continue }
            $currentHash = Get-MigrationHash $file
            if ($localState[$file.Name] -ne $currentHash) {
                throw "Local migration checkpoint differs from source for $($file.Name); refusing ledger adoption."
            }
            $escapedName = $file.Name.Replace("'", "''")
            $adoptionRows += "('$escapedName', '$currentHash', $($adoptionRows.Count + 1), 'local_checkpoint_adoption')"
        }
        if ($adoptionRows.Count -gt 0) {
            [void](Invoke-TestPsql -Sql ("INSERT INTO integration_test.schema_migration " +
                "(migration_name, migration_sha256, applied_order, record_source) VALUES " +
                ($adoptionRows -join ",")))
        }
        $rows = Get-TestPsqlOutput -Sql `
            "SELECT migration_name || '|' || migration_sha256 FROM integration_test.schema_migration ORDER BY applied_order"
        foreach ($line in $rows -split "`r?`n") {
            $parts = $line.Split('|', 2)
            $databaseState[$parts[0]] = $parts[1].Trim()
        }
    }
    elseif ($databaseState.Count -eq 0) {
        $existingProjectSchema = Get-TestPsqlOutput -Sql `
            "SELECT (to_regclass('ops.tushare_data_release') IS NOT NULL)::int"
        if ($existingProjectSchema -eq '1') {
            throw "Migrated database has no integration-test migration ledger or local checkpoint. Refusing to infer migration state; clean only the isolated test volume."
        }
    }

    return $databaseState
}

function Start-TestDatabase {
    Invoke-Compose @("up", "-d", "--wait", "--wait-timeout", "180", "postgres")
    [void](Assert-OwnedContainer)
}

function Ensure-CompatibilityDatabase {
    $existingOutput = & docker exec -e "PGPASSWORD=$($script:localEnv['ASHARES_TEST_ADMIN_PASSWORD'])" $containerName `
        psql -X -U ashares -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='ashares'"
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect the isolated PostgreSQL cluster." }
    $existing = ($existingOutput | Out-String).Trim()
    if ($existing -ne "1") {
        & docker exec -e "PGPASSWORD=$($script:localEnv['ASHARES_TEST_ADMIN_PASSWORD'])" $containerName `
            psql -X -U ashares -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE ashares"
        if ($LASTEXITCODE -ne 0) { throw "Could not create the empty migration-compatibility database inside the test container." }
    }
}

function Ensure-HistoricalMigrationPrerequisites {
    # These empty compatibility objects model legacy deployment artifacts that
    # are not created by the clean-schema migrations but are referenced by
    # historical migrations. Keep them confined to ashares_test.
    $sql = @"
CREATE TABLE IF NOT EXISTS public.etf_minute (
    "代码" VARCHAR(12) NOT NULL,
    "日期" DATE NOT NULL,
    "时间" TIME NOT NULL
) PARTITION BY RANGE ("日期");
CREATE TABLE IF NOT EXISTS public.etf_1min_1day_2024_05
    PARTITION OF public.etf_minute
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS public.tushare_daily_basic_raw_corrupt_20260811 (
    integration_test_placeholder BOOLEAN
);
CREATE TABLE IF NOT EXISTS public.tushare_daily_raw_corrupt_20260811 (
    integration_test_placeholder BOOLEAN
);
"@
    Invoke-TestPsql -Sql $sql
}

function Get-OrderedMigrationFiles {
    $files = @(Get-ChildItem -LiteralPath $migrationDir -Filter "*.sql" | Sort-Object Name)
    # Migration 050 creates a view over std.daily_kline, while the project adds
    # that target in migration 057. Apply the original 057 file before 050 to
    # satisfy the declared object dependency; neither migration is rewritten.
    $dailyKline = @($files | Where-Object Name -eq "057_daily_kline_partitioned_pilot.sql")
    if ($dailyKline.Count -ne 1) {
        throw "Expected exactly one migration defining the std.daily_kline target."
    }
    $beforeView = @($files | Where-Object { $_.Name -lt "050_mart_contract_views.sql" })
    $afterView = @($files | Where-Object {
        $_.Name -ge "050_mart_contract_views.sql" -and
        $_.Name -ne "057_daily_kline_partitioned_pilot.sql"
    })
    return @($beforeView + $dailyKline + $afterView)
}

function Complete-TestDatabaseIdentity {
    $migrationFiles = @(Get-OrderedMigrationFiles)
    $digestInput = ($migrationFiles | ForEach-Object { "$($_.Name):$(Get-MigrationHash $_)" }) -join "`n"
    $sha = [System.Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($digestInput))
    $chainHash = ([Convert]::ToHexString($sha)).ToLowerInvariant()
    $sql = @"
CREATE SCHEMA IF NOT EXISTS integration_test;
CREATE TABLE IF NOT EXISTS integration_test.environment_identity (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    environment_id UUID NOT NULL,
    environment_kind TEXT NOT NULL CHECK (environment_kind = 'TEST_FIXTURE'),
    migration_chain_hash CHAR(64) NOT NULL,
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO integration_test.environment_identity
    (singleton, environment_id, environment_kind, migration_chain_hash)
VALUES (TRUE, '$($script:environmentId)'::uuid, 'TEST_FIXTURE', '$chainHash')
ON CONFLICT (singleton) DO UPDATE SET
    environment_id=EXCLUDED.environment_id,
    environment_kind=EXCLUDED.environment_kind,
    migration_chain_hash=EXCLUDED.migration_chain_hash,
    initialized_at=CURRENT_TIMESTAMP;
REVOKE CONNECT ON DATABASE ashares_test FROM PUBLIC;
GRANT CONNECT ON DATABASE ashares_test TO ashares, ashares_app;
REVOKE CONNECT ON DATABASE ashares FROM PUBLIC, ashares_app;
GRANT USAGE ON SCHEMA integration_test TO ashares_app;
GRANT SELECT ON integration_test.environment_identity TO ashares_app;
GRANT SELECT ON integration_test.schema_migration TO ashares_app;
"@
    Invoke-TestPsql -Sql $sql
    $config = @"
database:
  host: "127.0.0.1"
  port: $($script:hostPort)
  user: "ashares_app"
  password: "$($script:localEnv['ASHARES_TEST_APP_PASSWORD'])"
  database: "ashares_test"
"@
    Set-Content -LiteralPath $testConfigFile -Value $config -Encoding utf8
}

function Invoke-Migrations {
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    $startedAt = [DateTimeOffset]::UtcNow
    $newMigrationCount = 0
    Start-TestDatabase
    Ensure-CompatibilityDatabase
    Ensure-HistoricalMigrationPrerequisites
    $state = Initialize-MigrationLedger
    $files = @(Get-OrderedMigrationFiles)
    foreach ($file in $files) {
        $hash = Get-MigrationHash $file
        if ($state.ContainsKey($file.Name)) {
            if ($state[$file.Name] -ne $hash) {
                throw "Applied migration changed: $($file.Name). Reset only this isolated test database before reapplying."
            }
            continue
        }
        Write-Host "Applying $($file.Name)"
        if ($file.Name -eq "048_data_access_app_role.sql") {
            Invoke-TestPsql -File $file.Name -AppPasswordVariable
        }
        else {
            Invoke-TestPsql -File $file.Name
        }
        $nextOrder = $state.Count + 1
        $escapedName = $file.Name.Replace("'", "''")
        Invoke-TestPsql -Sql @"
INSERT INTO integration_test.schema_migration
    (migration_name, migration_sha256, applied_order, record_source)
VALUES ('$escapedName', '$hash', $nextOrder, 'migration');
"@
        $newMigrationCount += 1
        $state[$file.Name] = $hash
        Write-LocalMigrationState -Applied $state
    }
    Complete-TestDatabaseIdentity
    $timer.Stop()
    @{
        started_at_utc = $startedAt.ToString("o")
        completed_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        elapsed_seconds = [math]::Round($timer.Elapsed.TotalSeconds, 3)
        total_migration_files = $files.Count
        newly_applied_migration_files = $newMigrationCount
        migration_order_exception = "057_daily_kline_partitioned_pilot.sql before 050_mart_contract_views.sql"
    } | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath (Join-Path $stateDir "migration-run.json") -Encoding utf8
    Write-Host "Migration run elapsed seconds: $([math]::Round($timer.Elapsed.TotalSeconds, 3)); newly applied files: $newMigrationCount."
}

function Write-TestConfigEnvironment {
    $env:DATA_COLLECT_CONFIG = $testConfigFile
    $env:ASHARES_ENV = "integration_test"
    $env:ASHARES_TEST_ENVIRONMENT_ID = $script:environmentId
    $env:ASHARES_TEST_DATABASE_PORT = [string]$script:hostPort
}

function Ensure-AlphaResearchRuleFixture {
    # Market and fee rules are intentionally not writable by the application
    # account. Seed one clearly identified rule set as the isolated database
    # owner so the Dataset integration test exercises the real read contract
    # without broadening application permissions.
    $sql = @"
BEGIN;
INSERT INTO foundation.market_rule_effective
    (rule_version,market_rule_version,effective_from,effective_to,market,
     min_buy_lot,allow_sell_odd_lot,t_plus_one,price_tick,cash_settlement_days,
     limit_pct,approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('alpha-research-v21-synthetic-v3','cn-a-share-v2',DATE '2024-09-12',NULL,
     '主板',100,TRUE,TRUE,0.01,1,0.10,'APPROVED','TEST_FIXTURE',
     TIMESTAMPTZ '2024-09-11 17:00:00+08',repeat('a',64))
ON CONFLICT DO NOTHING;

INSERT INTO foundation.tax_rule_effective
    (tax_rule_version,effective_from,effective_to,market,stamp_duty_sell_rate,
     transfer_fee_rate,approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('research-fee-assumption-v3',DATE '2024-09-12',NULL,'主板',0.001,0.00001,
     'APPROVED','TEST_FIXTURE',TIMESTAMPTZ '2024-09-11 17:00:00+08',repeat('a',64))
ON CONFLICT DO NOTHING;

INSERT INTO backtest.app_execution_params
    (execution_params_version,fee_rule_version,market,effective_from,effective_to,
     commission_rate,min_commission,slippage_bps,gap_stress_pct,
     max_volume_participation_pct,liquidity_policy,execution_model,parameters,
     approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('research-fee-assumption-v3:2024-09-12','research-fee-assumption-v3',
     '主板',DATE '2024-09-12',NULL,0.0003,5.0,5.0,0.0,1.0,
     'reject_full_order','bar_open_close',
     '{"fixture_kind":"TEST_FIXTURE","provenance":"Synthetic contract-only integration fixture; not production policy evidence"}'::jsonb,
     'APPROVED','TEST_FIXTURE',TIMESTAMPTZ '2024-09-11 17:00:00+08',repeat('a',64))
ON CONFLICT DO NOTHING;
COMMIT;
"@
    [void](Invoke-TestPsql -Sql $sql)
}

function Ensure-RealGoldenSampleFixture {
    # The bounded real-market artifact has its own immutable identity and
    # lifecycle rules. It is never merged into the synthetic TEST_FIXTURE rows.
    $sql = @"
CREATE TABLE IF NOT EXISTS integration_test.real_sample_import (
    sample_version TEXT PRIMARY KEY,
    environment_id UUID NOT NULL,
    fixture_marker TEXT NOT NULL CHECK (fixture_marker='REAL_SAMPLE_TEST_ONLY'),
    loader_contract_version TEXT NOT NULL,
    bundle_sha256 CHAR(64) NOT NULL,
    manifest_sha256 CHAR(64) NOT NULL,
    bundle_bytes BIGINT NOT NULL CHECK (bundle_bytes > 0),
    bundle_payload BYTEA NOT NULL,
    manifest JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
GRANT SELECT, INSERT, DELETE ON integration_test.real_sample_import TO ashares_app;

INSERT INTO foundation.market_rule_effective
    (rule_version,market_rule_version,effective_from,effective_to,market,
     min_buy_lot,allow_sell_odd_lot,t_plus_one,price_tick,cash_settlement_days,
     limit_pct,approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('real-golden-v1','cn-a-share-v2',DATE '2023-09-22',NULL,'主板',
     100,TRUE,TRUE,0.01,1,0.10,'APPROVED','REAL_SAMPLE_TEST_ONLY',
     TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64)),
    ('real-golden-v1','cn-a-share-v2',DATE '2023-09-22',NULL,'创业板',
     100,TRUE,TRUE,0.01,1,0.20,'APPROVED','REAL_SAMPLE_TEST_ONLY',
     TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64)),
    ('real-golden-v1','cn-a-share-v2',DATE '2023-09-22',NULL,'科创板',
     200,TRUE,TRUE,0.01,1,0.20,'APPROVED','REAL_SAMPLE_TEST_ONLY',
     TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64))
ON CONFLICT DO NOTHING;

INSERT INTO foundation.tax_rule_effective
    (tax_rule_version,effective_from,effective_to,market,stamp_duty_sell_rate,
     transfer_fee_rate,approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('real-golden-fees-v1',DATE '2023-09-22',NULL,'主板',0.001,0.00001,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64)),
    ('real-golden-fees-v1',DATE '2023-09-22',NULL,'创业板',0.001,0.00001,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64)),
    ('real-golden-fees-v1',DATE '2023-09-22',NULL,'科创板',0.001,0.00001,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64))
ON CONFLICT DO NOTHING;

INSERT INTO backtest.app_execution_params
    (execution_params_version,fee_rule_version,market,effective_from,effective_to,
     commission_rate,min_commission,slippage_bps,gap_stress_pct,
     max_volume_participation_pct,liquidity_policy,execution_model,parameters,
     approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('real-golden-fees-v1:2023-09-22','real-golden-fees-v1','主板',
     DATE '2023-09-22',NULL,0.0003,5.0,5.0,0.0,1.0,
     'reject_full_order','bar_open_close',
     '{"fixture_kind":"REAL_SAMPLE_TEST_ONLY","provenance":"bounded real sample integration evidence only"}'::jsonb,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64)),
    ('real-golden-fees-v1:2023-09-22','real-golden-fees-v1','创业板',
     DATE '2023-09-22',NULL,0.0003,5.0,5.0,0.0,1.0,
     'reject_full_order','bar_open_close',
     '{"fixture_kind":"REAL_SAMPLE_TEST_ONLY","provenance":"bounded real sample integration evidence only"}'::jsonb,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64)),
    ('real-golden-fees-v1:2023-09-22','real-golden-fees-v1','科创板',
     DATE '2023-09-22',NULL,0.0003,5.0,5.0,0.0,1.0,
     'reject_full_order','bar_open_close',
     '{"fixture_kind":"REAL_SAMPLE_TEST_ONLY","provenance":"bounded real sample integration evidence only"}'::jsonb,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-21 17:00:00+08',repeat('b',64))
ON CONFLICT DO NOTHING;

INSERT INTO foundation.market_rule_effective
    (rule_version,market_rule_version,effective_from,effective_to,market,
     min_buy_lot,allow_sell_odd_lot,t_plus_one,price_tick,cash_settlement_days,
     limit_pct,approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('real-financial-window-v1','real-financial-window-market-v1',DATE '2023-09-12',NULL,'主板',
     100,TRUE,TRUE,0.01,1,0.10,'APPROVED','REAL_SAMPLE_TEST_ONLY',
     TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64)),
    ('real-financial-window-v1','real-financial-window-market-v1',DATE '2023-09-12',NULL,'创业板',
     100,TRUE,TRUE,0.01,1,0.20,'APPROVED','REAL_SAMPLE_TEST_ONLY',
     TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64)),
    ('real-financial-window-v1','real-financial-window-market-v1',DATE '2023-09-12',NULL,'科创板',
     200,TRUE,TRUE,0.01,1,0.20,'APPROVED','REAL_SAMPLE_TEST_ONLY',
     TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64))
ON CONFLICT DO NOTHING;

INSERT INTO foundation.tax_rule_effective
    (tax_rule_version,effective_from,effective_to,market,stamp_duty_sell_rate,
     transfer_fee_rate,approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('real-financial-window-fees-v1',DATE '2023-09-12',NULL,'主板',0.001,0.00001,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64)),
    ('real-financial-window-fees-v1',DATE '2023-09-12',NULL,'创业板',0.001,0.00001,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64)),
    ('real-financial-window-fees-v1',DATE '2023-09-12',NULL,'科创板',0.001,0.00001,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64))
ON CONFLICT DO NOTHING;

INSERT INTO backtest.app_execution_params
    (execution_params_version,fee_rule_version,market,effective_from,effective_to,
     commission_rate,min_commission,slippage_bps,gap_stress_pct,
     max_volume_participation_pct,liquidity_policy,execution_model,parameters,
     approval_status,approved_by,approved_at,evidence_hash)
VALUES
    ('real-financial-window-fees-v1:2023-09-12','real-financial-window-fees-v1','主板',
     DATE '2023-09-12',NULL,0.0003,5.0,5.0,0.0,1.0,'reject_full_order','bar_open_close',
     '{"fixture_kind":"REAL_SAMPLE_TEST_ONLY","provenance":"bounded real sample integration evidence only"}'::jsonb,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64)),
    ('real-financial-window-fees-v1:2023-09-12','real-financial-window-fees-v1','创业板',
     DATE '2023-09-12',NULL,0.0003,5.0,5.0,0.0,1.0,'reject_full_order','bar_open_close',
     '{"fixture_kind":"REAL_SAMPLE_TEST_ONLY","provenance":"bounded real sample integration evidence only"}'::jsonb,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64)),
    ('real-financial-window-fees-v1:2023-09-12','real-financial-window-fees-v1','科创板',
     DATE '2023-09-12',NULL,0.0003,5.0,5.0,0.0,1.0,'reject_full_order','bar_open_close',
     '{"fixture_kind":"REAL_SAMPLE_TEST_ONLY","provenance":"bounded real sample integration evidence only"}'::jsonb,
     'APPROVED','REAL_SAMPLE_TEST_ONLY',TIMESTAMPTZ '2023-09-11 17:00:00+08',repeat('c',64))
ON CONFLICT DO NOTHING;
"@
    [void](Invoke-TestPsql -Sql $sql)
}

function Invoke-Status {
    $container = (& docker compose --env-file $envFile -f $composeFile -p $projectName ps --format json postgres 2>$null | Out-String).Trim()
    if ($container) { Write-Host $container } else { Write-Host "Container: not started" }
    if (Test-Path -LiteralPath $testConfigFile) { Write-Host "Runtime config: prepared (credentials are not displayed)" }
    Write-Host "Docker volume: $volumeName"
    $containerId = (& docker compose --env-file $envFile -f $composeFile -p $projectName ps -q --all postgres 2>$null | Out-String).Trim()
    if ($containerId) {
        $metrics = & docker exec -e "PGPASSWORD=$($script:localEnv['ASHARES_TEST_ADMIN_PASSWORD'])" $containerName `
            psql -X -U ashares -d ashares_test -At -F "|" -c `
            "SELECT current_database(),pg_database_size(current_database()),(SELECT COUNT(*) FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema')),(SELECT COUNT(*) FROM pg_trigger WHERE NOT tgisinternal),COALESCE((SELECT COUNT(*) FROM integration_test.schema_migration),0)"
        if ($LASTEXITCODE -eq 0 -and $metrics) {
            $parts = ($metrics | Out-String).Trim().Split('|')
            Write-Host "Database metrics: name=$($parts[0]), bytes=$($parts[1]), user_tables=$($parts[2]), triggers=$($parts[3]), migration_ledger=$($parts[4])"
        }
    }
    if (Test-Path -LiteralPath (Join-Path $stateDir "migration-run.json")) {
        $migrationRun = Get-Content -LiteralPath (Join-Path $stateDir "migration-run.json") -Raw | ConvertFrom-Json
        Write-Host "Last migration run: $($migrationRun.elapsed_seconds)s; newly applied=$($migrationRun.newly_applied_migration_files)"
    }
}

Initialize-LocalState
switch ($Action) {
    "init" {
        Start-TestDatabase
        Ensure-CompatibilityDatabase
        Write-Host "Isolated PostgreSQL is ready; database=ashares_test, container=$containerName, host port=$HostPort."
        Write-Host "No project migrations have been applied yet. Run -Action migrate."
    }
    "migrate" { Invoke-Migrations; Invoke-Status }
    "status" { Invoke-Status }
    "test" {
        Invoke-Migrations
        Write-TestConfigEnvironment
        Ensure-AlphaResearchRuleFixture
        uv run pytest -o addopts= -m integration tests/test_integration_test_database.py
        if ($LASTEXITCODE -ne 0) { throw "Integration-test database smoke tests failed." }
        uv run pytest -o addopts= -m integration tests/test_integration_alpha_research_dataset.py::test_synthetic_v21_dataset_lifecycle_and_alpha_reader
        if ($LASTEXITCODE -ne 0) { throw "Integration-test Alpha Dataset checks failed." }
    }
    "test-real-golden" {
        Invoke-Migrations
        Write-TestConfigEnvironment
        Ensure-RealGoldenSampleFixture
        uv run pytest -o addopts= tests/test_alpha_research_golden_sample.py::test_real_financial_warning_source_component_fails_closed_at_factor_gate
        if ($LASTEXITCODE -ne 0) { throw "Real financial source-quality contract check failed." }
        uv run pytest -o addopts= -m integration tests/test_integration_alpha_research_dataset.py::test_real_golden_sample_import_is_repeatable_and_fails_closed
        if ($LASTEXITCODE -ne 0) { throw "Real golden sample integration checks failed." }
    }
    "test-real-window" {
        Invoke-Migrations
        Write-TestConfigEnvironment
        Ensure-RealGoldenSampleFixture
        $env:ASHARES_REAL_GOLDEN_SAMPLE_VERSION = "real-financial-window-v1"
        $env:ASHARES_REAL_GOLDEN_E2E_CONTRACT_VERSION = "real-financial-window-e2e-v7"
        uv run pytest -o addopts= -m integration tests/test_integration_test_database.py
        if ($LASTEXITCODE -ne 0) { throw "Isolated database contract checks failed." }
        uv run pytest -o addopts= -m integration tests/test_integration_alpha_research_dataset.py::test_real_golden_sample_import_is_repeatable_and_fails_closed
        if ($LASTEXITCODE -ne 0) { throw "Clean real financial window E2E checks failed." }
    }
    "stop" {
        [void](Assert-OwnedContainer)
        Invoke-Compose @("stop", "postgres")
        Write-Host "Integration-test container stopped; named volume preserved."
    }
    "clean" {
        [void](Assert-OwnedContainer)
        Invoke-Compose @("down", "--remove-orphans")
        if ($RemoveVolume) {
            $volume = (& docker volume inspect --format '{{ index .Labels "org.ashares.environment" }}|{{ index .Labels "org.ashares.database" }}' $volumeName 2>$null | Out-String).Trim()
            if ($volume -ne "integration-test|ashares_test") {
                throw "Refusing to remove a volume without the expected integration-test identity labels."
            }
            & docker volume rm $volumeName
            if ($LASTEXITCODE -ne 0) { throw "Could not remove the verified integration-test volume." }
            Remove-Item -LiteralPath $migrationStateFile -Force -ErrorAction SilentlyContinue
            Remove-Item -LiteralPath (Join-Path $stateDir "migration-run.json") -Force -ErrorAction SilentlyContinue
            Remove-Item -LiteralPath $testConfigFile -Force -ErrorAction SilentlyContinue
            Write-Host "Only the verified integration-test volume was removed. Local credentials are retained for a fresh init."
        }
        else {
            Write-Host "Integration-test container removed; named volume preserved. Pass -RemoveVolume to delete the verified test volume."
        }
    }
}
