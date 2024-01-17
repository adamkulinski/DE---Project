$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')


#Write-Host "Stopping Airflow and Flask services..."
Write-Host "Stopping Airflow services..."

# Execute AirflowStop.ps1 in the background
Start-Job -ScriptBlock {
    & "$env:project_location\powershell_scripts\AirflowStop.ps1"
}

# Can't stop Flask because it's not a werkzeug server
# Has to be done manually

## Execute FlaskStop.ps1 in the background
#Start-Job -ScriptBlock {
#    & "$env:project_location\powershell_scripts\FlaskStop.ps1"
#}
