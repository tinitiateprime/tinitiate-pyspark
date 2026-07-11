Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:DatabaseRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$script:RepoRoot = (Resolve-Path (Join-Path $script:DatabaseRoot "..")).Path
$script:Generator = Join-Path $script:DatabaseRoot "scripts\generate_database_sources.py"
$script:Loader = "pyspark-database/scripts/load_files_to_postgres.py"
$script:CdcLoader = "pyspark-database/scripts/apply_sales_transaction_cdc.py"

function Write-Phase {
    param(
        [Parameter(Mandatory)] [int] $Number,
        [Parameter(Mandatory)] [string] $Title
    )
    Write-Host ""
    Write-Host "PHASE $Number - $Title" -ForegroundColor Cyan
    Write-Host ("=" * (10 + $Title.Length)) -ForegroundColor DarkCyan
}

function Assert-TutorialPrerequisites {
    param([string] $PostgresContainer = "postgres")

    $null = Get-Command python -ErrorAction Stop
    $null = Get-Command docker -ErrorAction Stop
    $running = docker inspect -f "{{.State.Running}}" $PostgresContainer 2>$null
    if ($LASTEXITCODE -ne 0 -or $running -ne "true") {
        throw "Container '$PostgresContainer' is not running. Start it with the tutorial Docker Compose command."
    }
}

function Reset-ScenarioOutput {
    param([Parameter(Mandatory)] [string] $RelativePath)

    $absolutePath = Join-Path $script:RepoRoot $RelativePath
    if (Test-Path -LiteralPath $absolutePath) {
        $resolved = (Resolve-Path -LiteralPath $absolutePath).Path
        $dataRoot = (Join-Path $script:RepoRoot "data")
        if (-not $resolved.StartsWith($dataRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove scenario output outside the repository data directory: $resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

function Invoke-SourceGenerator {
    param(
        [Parameter(Mandatory)] [string] $OutputRoot,
        [Parameter(Mandatory)] [string[]] $Arguments
    )

    $absoluteOutput = Join-Path $script:RepoRoot $OutputRoot
    & python $script:Generator --output-dir $absoluteOutput @Arguments --overwrite
    if ($LASTEXITCODE -ne 0) {
        throw "Source generation failed with exit code $LASTEXITCODE."
    }
}

function Show-ScenarioManifest {
    param(
        [Parameter(Mandatory)] [string] $OutputRoot,
        [Parameter(Mandatory)] [string] $ManifestName
    )

    $manifest = Join-Path $script:RepoRoot "$OutputRoot\_manifests\$ManifestName.json"
    if (-not (Test-Path -LiteralPath $manifest)) {
        throw "Manifest not found: $manifest"
    }
    Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json | ConvertTo-Json -Depth 5
}

function Invoke-PySparkLoad {
    param(
        [Parameter(Mandatory)] [string] $SourcePath,
        [Parameter(Mandatory)] [ValidateSet("csv", "json", "parquet")] [string] $SourceFormat,
        [Parameter(Mandatory)] [string] $TargetTable,
        [Parameter(Mandatory)] [string] $Scenario,
        [Parameter(Mandatory)] [int] $ExpectedFiles,
        [int] $WritePartitions = 4,
        [int] $BatchSize = 10000,
        [ValidateSet("append", "overwrite")] [string] $WriteMode = "overwrite",
        [string] $JdbcUrl = "jdbc:postgresql://postgres:5432/tinitiateai",
        [string] $DbUser = "ti_dbuser",
        [string] $DbPassword = "tiuser!23456",
        [string] $MinioEndpoint = "http://minio:9000",
        [string] $MinioAccessKey = "minio",
        [string] $MinioSecretKey = "minio123",
        [string] $DockerNetwork = "pyspark-database_default",
        [string] $SparkImage = "jupyter/pyspark-notebook:latest"
    )

    $containerSource = $SourcePath.Replace("\", "/")
    $containerName = "pyspark-db-$($TargetTable.Replace('_', '-'))-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
    docker run --rm --name $containerName `
        --network $DockerNetwork `
        -e "POSTGRES_JDBC_URL=$JdbcUrl" `
        -e "POSTGRES_USER=$DbUser" `
        -e "POSTGRES_PASSWORD=$DbPassword" `
        -e "MINIO_ENDPOINT=$MinioEndpoint" `
        -e "MINIO_ACCESS_KEY=$MinioAccessKey" `
        -e "MINIO_SECRET_KEY=$MinioSecretKey" `
        -v "${script:RepoRoot}:/home/jovyan/work/repo" `
        -w /home/jovyan/work/repo `
        $SparkImage `
        spark-submit `
        --conf spark.jars.ivy=/home/jovyan/work/repo/data/ivy-cache `
        --conf spark.ui.showConsoleProgress=false `
        --packages org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4 `
        $script:Loader `
        --source-path $containerSource `
        --source-format $SourceFormat `
        --target-table $TargetTable `
        --scenario $Scenario `
        --expected-files $ExpectedFiles `
        --write-mode $WriteMode `
        --write-partitions $WritePartitions `
        --batch-size $BatchSize
    if ($LASTEXITCODE -ne 0) {
        throw "Spark load failed for $TargetTable with exit code $LASTEXITCODE."
    }
}

function Invoke-PySparkCdc {
    param(
        [Parameter(Mandatory)] [string] $SourcePath,
        [Parameter(Mandatory)] [ValidateSet("csv", "json", "parquet")] [string] $SourceFormat,
        [int] $WritePartitions = 4,
        [int] $BatchSize = 10000,
        [string] $JdbcUrl = "jdbc:postgresql://postgres:5432/tinitiateai",
        [string] $DbUser = "ti_dbuser",
        [string] $DbPassword = "tiuser!23456",
        [string] $MinioEndpoint = "http://minio:9000",
        [string] $MinioAccessKey = "minio",
        [string] $MinioSecretKey = "minio123",
        [string] $DockerNetwork = "pyspark-database_default",
        [string] $SparkImage = "jupyter/pyspark-notebook:latest"
    )

    $containerSource = $SourcePath.Replace("\", "/")
    $containerName = "pyspark-db-cdc-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
    docker run --rm --name $containerName `
        --network $DockerNetwork `
        -e "POSTGRES_JDBC_URL=$JdbcUrl" `
        -e "POSTGRES_USER=$DbUser" `
        -e "POSTGRES_PASSWORD=$DbPassword" `
        -e "MINIO_ENDPOINT=$MinioEndpoint" `
        -e "MINIO_ACCESS_KEY=$MinioAccessKey" `
        -e "MINIO_SECRET_KEY=$MinioSecretKey" `
        -v "${script:RepoRoot}:/home/jovyan/work/repo" `
        -w /home/jovyan/work/repo `
        $SparkImage `
        spark-submit `
        --conf spark.jars.ivy=/home/jovyan/work/repo/data/ivy-cache `
        --conf spark.ui.showConsoleProgress=false `
        --packages org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4 `
        $script:CdcLoader `
        --source-path $containerSource `
        --source-format $SourceFormat `
        --write-partitions $WritePartitions `
        --batch-size $BatchSize
    if ($LASTEXITCODE -ne 0) {
        throw "Spark CDC load failed with exit code $LASTEXITCODE."
    }
}

function Invoke-TutorialQuery {
    param(
        [Parameter(Mandatory)] [string] $Sql,
        [string] $PostgresContainer = "postgres",
        [string] $Database = "tinitiateai",
        [string] $DbUser = "ti_dbuser",
        [string] $DbPassword = "tiuser!23456"
    )

    docker exec -e "PGPASSWORD=$DbPassword" $PostgresContainer psql -U $DbUser -d $Database -v ON_ERROR_STOP=1 -P pager=off -c $Sql
    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL validation query failed with exit code $LASTEXITCODE."
    }
}

function Show-LoadSummary {
    param(
        [Parameter(Mandatory)] [string] $TargetTable,
        [Parameter(Mandatory)] [string] $Scenario
    )

    Invoke-TutorialQuery -Sql "SELECT COUNT(*) AS target_rows FROM training.$TargetTable;"
    Invoke-TutorialQuery -Sql "SELECT scenario, target_table, source_files, accepted_rows, rejected_rows, read_seconds, write_seconds FROM training.load_audit WHERE scenario = '$Scenario' ORDER BY load_id DESC LIMIT 1;"
}

function Write-CleanupInstructions {
    param([Parameter(Mandatory)] [string] $OutputRoot)

    Write-Host "Generated data remains at: $OutputRoot" -ForegroundColor Yellow
    Write-Host "Remove it when finished with:" -ForegroundColor Yellow
    Write-Host "  Remove-Item -LiteralPath '$OutputRoot' -Recurse -Force" -ForegroundColor DarkYellow
}
