# This script will ensure that the following files are tracked by Git but changes to these files will not be included in pull requests
# Add any files to the list that fall under this category
# If you have permission issues running this script, run the command `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`
# Run this script from Semoss root with command `./set-assume-unchanged.ps1`

$files = @("RDF_Map.prop", "social.properties", "db\security\security_OWL.OWL")

foreach ($file in $files) {
    Write-Host "Setting assume-unchanged for $file"
    git update-index --assume-unchanged $file
}
