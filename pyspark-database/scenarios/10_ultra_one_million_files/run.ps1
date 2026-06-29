param(
    [int] $FileCount = 10000,
    [int] $RowsPerFile = 1,
    [ValidateSet("csv", "json", "parquet")] [string] $Format = "json",
    [switch] $ConfirmMillionFiles
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/10_ultra_one_million_files"
$scenario = "scenario-10-ultra-$FileCount-$Format"

if ($FileCount -ge 1000000 -and -not $ConfirmMillionFiles) {
    throw "One million files requires -ConfirmMillionFiles. Review disk, inode, runtime, and cleanup capacity first."
}

Write-Phase 1 "Check prerequisites, disk capacity, and safety confirmation"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate the sales-transaction stress dataset"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "ultra",
    "--ultra-format", $Format,
    "--ultra-file-count", $FileCount,
    "--ultra-rows-per-file", $RowsPerFile,
    "--allow-ultra"
)

Write-Phase 3 "Inspect the ultra-volume manifest"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "ultra"

Write-Phase 4 "Load files into training.sales_transaction"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/10_ultra_sales_transaction/sales_transaction" `
    -SourceFormat $Format -TargetTable sales_transaction -Scenario $scenario `
    -ExpectedFiles $FileCount -WritePartitions 4 -BatchSize 10000

Write-Phase 5 "Validate target rows and isolate listing cost from JDBC cost"
Show-LoadSummary -TargetTable sales_transaction -Scenario $scenario

Write-Phase 6 "Review compaction and cleanup"
Write-Host "Expected rows: $($FileCount * $RowsPerFile)"
Write-Host "Production lesson: compact source files before the database load; more JDBC writers do not fix file listing."
Write-CleanupInstructions -OutputRoot $outputRoot
