# This script will ensure that the following files are tracked by Git but changes to these files will not be included in pull requests
# Add any files to the list that fall under this category
# If you have permission issues running this script, run the command `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`
# RUN THIS SCRIPT FROM THIS DIRECTORY WITH `./set-assume-unchanged.ps1`

$currentDir = Get-Location
$scriptDir = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent

if ($currentDir.Path -ne $scriptDir) {
    Write-Host "Please run this script from its own directory: $scriptDir"
    exit
}

$files = @(
    "../RDF_Map.prop", 
    "../social.properties", 
    "../db/security/security_OWL.OWL", 
    "../db/scheduler/scheduler_OWL.OWL", 
    "../db/themes/themes_OWL.OWL", 
    "../db/UserTrackingDatabase/UserTrackingDatabase_OWL.OWL", 
    "../db/ModelInferenceLogsDatabase/ModelInferenceLogsDatabase_OWL.OWL",
    "../db/LocalMasterDatabase/MasterDatabase_OWL.OWL",
    "../db/form_builder_engine/form_builder_engine_OWL.OWL"
)

foreach ($file in $files) {
    Write-Host "Setting assume-unchanged for $file"
    git update-index --assume-unchanged $file
}

