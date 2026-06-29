param(
    [int] $FileCount = 20,
    [int] $RowsPerFile = 10
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/04_many_small_csv_emp"
$scenario = "scenario-04-csv-small-emp"

Write-Phase 1 "Check prerequisites"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate the employee CSV dataset"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "csv-small-emp",
    "--small-file-count", $FileCount,
    "--small-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect the dataset manifest"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "csv-small-emp"

Write-Phase 4 "Load all CSV files into training.emp"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/04_csv_small_emp/emp" `
    -SourceFormat csv -TargetTable emp -Scenario $scenario `
    -ExpectedFiles $FileCount -WritePartitions 4

Write-Phase 5 "Validate rows, rejects, and audit metrics"
Show-LoadSummary -TargetTable emp -Scenario $scenario

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows: $($FileCount * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
