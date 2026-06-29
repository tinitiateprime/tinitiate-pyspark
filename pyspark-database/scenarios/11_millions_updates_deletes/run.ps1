param(
    [int] $BaseFileCount = 2,
    [int] $BaseRowsPerFile = 100,
    [int] $UpdateCount = 100,
    [int] $DeleteCount = 25,
    [int] $ChangesPerFile = 100,
    [ValidateSet("csv", "json", "parquet")] [string] $ChangeFormat = "parquet",
    [switch] $ConfirmMillions
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/11_millions_updates_deletes"
$scenario = "scenario-11-cdc"
$totalChanges = $UpdateCount + $DeleteCount

if ($totalChanges -ge 1000000 -and -not $ConfirmMillions) {
    throw "One million or more changes requires -ConfirmMillions. Start with the defaults and verify capacity first."
}
if (($BaseFileCount * $BaseRowsPerFile) -lt $totalChanges) {
    throw "The base dataset must contain at least as many rows as updates plus deletes."
}

Write-Phase 1 "Check prerequisites and CDC capacity"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate the base table and change files"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "ultra", "cdc",
    "--ultra-format", "parquet",
    "--ultra-file-count", $BaseFileCount,
    "--ultra-rows-per-file", $BaseRowsPerFile,
    "--cdc-updates", $UpdateCount,
    "--cdc-deletes", $DeleteCount,
    "--cdc-rows-per-file", $ChangesPerFile,
    "--allow-ultra"
)

Write-Phase 3 "Inspect base and CDC manifests"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "ultra"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "cdc"

Write-Phase 4 "Load the base sales_transaction table"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/10_ultra_sales_transaction/sales_transaction" `
    -SourceFormat parquet -TargetTable sales_transaction -Scenario "$scenario-base" `
    -ExpectedFiles $BaseFileCount -WritePartitions 4
Invoke-TutorialQuery -Sql "SELECT COUNT(*) AS rows_before_cdc FROM training.sales_transaction;"

Write-Phase 5 "Stage and atomically apply updates and deletes"
Invoke-PySparkCdc `
    -SourcePath "$outputRoot/11_cdc_sales_transaction/$ChangeFormat/sales_transaction_changes" `
    -SourceFormat $ChangeFormat -WritePartitions 4

Write-Phase 6 "Reconcile updated, deleted, unmatched, and remaining rows"
Invoke-TutorialQuery -Sql "SELECT COUNT(*) AS rows_after_cdc FROM training.sales_transaction;"
Invoke-TutorialQuery -Sql "SELECT COUNT(*) AS staging_rows_after_commit FROM training.sales_transaction_changes_staging;"
Invoke-TutorialQuery -Sql "SELECT scenario, accepted_rows, rejected_rows, read_seconds, write_seconds FROM training.load_audit WHERE scenario = '$scenario-base' ORDER BY load_id DESC LIMIT 1;"

Write-Phase 7 "Review transaction design and cleanup"
Write-Host "Generated changes: $UpdateCount updates + $DeleteCount deletes = $totalChanges"
Write-Host "Production lesson: deduplicate by key, stage in batches, and apply set-based SQL in one transaction."
Write-CleanupInstructions -OutputRoot $outputRoot
