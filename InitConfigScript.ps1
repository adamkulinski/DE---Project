# Set a global variable
[Environment]::SetEnvironmentVariable('PROJECT_LOCATION', 'C:\Studia\Test_project\DE---Project', 'User')

$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')

Set-Location $project_location

# Output global variable
Write-Host "Global variable project_location set to: $project_location"

# Define the SQLite directory path
$sqliteDir = 'C:\sqlite'

# Check if the sqlite directory already exists
if (Test-Path $sqliteDir)
{
    Write-Host "Directory $sqliteDir already exists."

    # Check if sqlite directory is already in the Path
    $envPath = [System.Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
    if (-not $envPath.Contains($sqliteDir))
    {
        # Add C:\sqlite to the 'Path' system variable
        Write-Host "Adding $sqliteDir to system Path."
        $newPath = $envPath + ';' + $sqliteDir
        [System.Environment]::SetEnvironmentVariable('Path', $newPath, [System.EnvironmentVariableTarget]::Machine)
    }
    else
    {
        Write-Host "$sqliteDir is already in the system Path."
    }
}
else
{
    # 1. Download zip file
    $zipUrl = 'https://www.sqlite.org/2024/sqlite-tools-win-x64-3450000.zip'
    $zipFile = "$env:TEMP\sqlite-tools-win-x64-3450000.zip"
    Invoke-WebRequest -Uri $zipUrl -OutFile $zipFile

    # 2. Create the folder and unpack contents
    New-Item -ItemType Directory -Path $sqliteDir -Force
    Expand-Archive -LiteralPath $zipFile -DestinationPath $sqliteDir

    # 3. Add C:\sqlite to the 'Path' system variable
    $envPath = [System.Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
    $newPath = $envPath + ';' + $sqliteDir
    [System.Environment]::SetEnvironmentVariable('Path', $newPath, [System.EnvironmentVariableTarget]::Machine)
    Write-Host "$sqliteDir added to system Path."
}

Write-Host "Starting script at ${project_location}\powershell_scripts\AirflowInit.ps1"
Write-Host "Starting script at ${project_location}\powershell_scripts\FlaskInit.ps1"


# Execute AirflowInit.ps1 in the background
Start-Job -ScriptBlock {
    & "$env:project_location\powershell_scripts\AirflowInit.ps1"
}

# Execute FlaskInit.ps1 in the background
Start-Job -ScriptBlock {
    & "$env:project_location\powershell_scripts\FlaskInit.ps1"
}

