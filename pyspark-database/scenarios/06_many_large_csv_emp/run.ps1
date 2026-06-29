param(
    [int] $FileCount = 2,
    [int] $RowsPerFile = 100000,
    [int] $WritePartitions = 8
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/06_many_large_csv_emp"
$scenario = "scenario-06-csv-large-emp"

Write-Phase 1 "Check prerequisites and capacity"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate large employee CSV files"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "csv-large-emp",
    "--large-file-count", $FileCount,
    "--large-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect expected files and rows"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "csv-large-emp"

Write-Phase 4 "Load large CSV files into training.emp"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/06_csv_large_emp/emp" `
    -SourceFormat csv -TargetTable emp -Scenario $scenario `
    -ExpectedFiles $FileCount -WritePartitions $WritePartitions -BatchSize 20000

Write-Phase 5 "Validate counts and read-versus-write timing"
Show-LoadSummary -TargetTable emp -Scenario $scenario

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows: $($FileCount * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
