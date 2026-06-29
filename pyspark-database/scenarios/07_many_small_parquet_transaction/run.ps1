param(
    [int] $FileCount = 20,
    [int] $RowsPerFile = 10
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/07_many_small_parquet_transaction"
$scenario = "scenario-07-parquet-small-transaction"

Write-Phase 1 "Check prerequisites"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate small sales-transaction Parquet files"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "parquet-small-transaction",
    "--small-file-count", $FileCount,
    "--small-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect the dataset manifest"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "parquet-small-transaction"

Write-Phase 4 "Load Parquet files into training.sales_transaction"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/07_parquet_small_sales_transaction/sales_transaction" `
    -SourceFormat parquet -TargetTable sales_transaction -Scenario $scenario `
    -ExpectedFiles $FileCount -WritePartitions 4

Write-Phase 5 "Validate rows and audit metrics"
Show-LoadSummary -TargetTable sales_transaction -Scenario $scenario

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows: $($FileCount * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
