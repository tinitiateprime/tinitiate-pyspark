param(
    [int] $FileCount = 2,
    [int] $RowsPerFile = 100000,
    [int] $WritePartitions = 8
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/03_many_large_json_sales"
$scenario = "scenario-03-json-large-sales"

Write-Phase 1 "Check prerequisites and capacity"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate large sales JSON files"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "json-large-sales",
    "--large-file-count", $FileCount,
    "--large-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect expected files and rows"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "json-large-sales"

Write-Phase 4 "Load large JSON files into training.sales"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/03_json_large_sales/sales" `
    -SourceFormat json -TargetTable sales -Scenario $scenario `
    -ExpectedFiles $FileCount -WritePartitions $WritePartitions -BatchSize 20000

Write-Phase 5 "Validate counts and read-versus-write timing"
Show-LoadSummary -TargetTable sales -Scenario $scenario

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows: $($FileCount * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
