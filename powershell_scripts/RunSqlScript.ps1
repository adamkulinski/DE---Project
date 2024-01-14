param (
    [string]$sql_script_name
)

# Getting system variable
$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')

# Creating variables for SQLite CLI
$scriptPath = "$project_location\sqlite_jobs\$sql_script_name"
$dbPath = "$project_location\db\Piggybank.db"

# Check if the SQL script exists
if (-not (Test-Path -Path $scriptPath)) {
    Write-Error "SQL script file not found at path: $scriptPath"
    exit 1
}
else {
    # Execute the script
    sqlite3.exe $dbPath ".read '$scriptPath'"
}
