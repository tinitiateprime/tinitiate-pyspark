param(
    [int] $FilesPerTable = 20,
    [int] $RowsPerFile = 10
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/02_many_small_json_multiple_tables"
$scenario = "scenario-02-json-small-multi"

Write-Phase 1 "Check prerequisites"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate customer, sales, product, and location JSON datasets"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "json-small-multi",
    "--small-file-count", $FilesPerTable,
    "--small-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect the multi-table manifest"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "json-small-multi"

Write-Phase 4 "Load dimensions and fact table in dependency order"
foreach ($table in @("location", "product", "customer", "sales")) {
    $partitions = if ($table -eq "sales") { 4 } else { 2 }
    Invoke-PySparkLoad `
        -SourcePath "$outputRoot/02_json_small_multi/$table" `
        -SourceFormat json -TargetTable $table -Scenario $scenario `
        -ExpectedFiles $FilesPerTable -WritePartitions $partitions
}

Write-Phase 5 "Validate all PostgreSQL tables"
foreach ($table in @("location", "product", "customer", "sales")) {
    Invoke-TutorialQuery -Sql "SELECT '$table' AS table_name, COUNT(*) AS target_rows FROM training.$table;"
}
Invoke-TutorialQuery -Sql "SELECT target_table, accepted_rows, rejected_rows, read_seconds, write_seconds FROM training.load_audit WHERE scenario = '$scenario' ORDER BY load_id;"

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows per table: $($FilesPerTable * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
