param(
    [int] $FilesPerTable = 20,
    [int] $RowsPerFile = 10
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/05_many_small_csv_multiple_tables"
$scenario = "scenario-05-csv-small-multi"

Write-Phase 1 "Check prerequisites"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate employee, department, project, and assignment CSV datasets"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "csv-small-multi",
    "--small-file-count", $FilesPerTable,
    "--small-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect the multi-table manifest"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "csv-small-multi"

Write-Phase 4 "Load tables in dependency order"
foreach ($table in @("dept", "projects", "emp", "emp_projects")) {
    $partitions = if ($table -in @("emp", "emp_projects")) { 4 } else { 2 }
    Invoke-PySparkLoad `
        -SourcePath "$outputRoot/05_csv_small_multi/$table" `
        -SourceFormat csv -TargetTable $table -Scenario $scenario `
        -ExpectedFiles $FilesPerTable -WritePartitions $partitions
}

Write-Phase 5 "Validate all PostgreSQL tables"
foreach ($table in @("dept", "projects", "emp", "emp_projects")) {
    Invoke-TutorialQuery -Sql "SELECT '$table' AS table_name, COUNT(*) AS target_rows FROM training.$table;"
}
Invoke-TutorialQuery -Sql "SELECT target_table, accepted_rows, rejected_rows, read_seconds, write_seconds FROM training.load_audit WHERE scenario = '$scenario' ORDER BY load_id;"

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows per table: $($FilesPerTable * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
