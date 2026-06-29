param(
    [int] $FileCount = 20,
    [int] $RowsPerFile = 10
)

. (Join-Path $PSScriptRoot "..\..\scripts\scenario_common.ps1")
$outputRoot = "data/database_scenarios/01_many_small_json_customer"
$scenario = "scenario-01-json-small-customer"

Write-Phase 1 "Check prerequisites"
Assert-TutorialPrerequisites

Write-Phase 2 "Generate the customer JSON dataset"
Invoke-SourceGenerator -OutputRoot $outputRoot -Arguments @(
    "--scenario", "json-small-customer",
    "--small-file-count", $FileCount,
    "--small-rows-per-file", $RowsPerFile
)

Write-Phase 3 "Inspect the dataset manifest"
Show-ScenarioManifest -OutputRoot $outputRoot -ManifestName "json-small-customer"

Write-Phase 4 "Load all JSON files into training.customer"
Invoke-PySparkLoad `
    -SourcePath "$outputRoot/01_json_small_customer/customer" `
    -SourceFormat json -TargetTable customer -Scenario $scenario `
    -ExpectedFiles $FileCount -WritePartitions 4

Write-Phase 5 "Validate PostgreSQL rows and audit metrics"
Show-LoadSummary -TargetTable customer -Scenario $scenario

Write-Phase 6 "Review and cleanup"
Write-Host "Expected rows: $($FileCount * $RowsPerFile)"
Write-CleanupInstructions -OutputRoot $outputRoot
