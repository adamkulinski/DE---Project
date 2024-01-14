# Get the project path
$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')
$csvPath = "$project_location\data\raw"
$dbPath = "$project_location\db\Piggybank.db"

# Define the CSV files and their corresponding table names
$csvTableMap = @{
    "client.csv" = "client_stage";
    "household.csv" = "household_stage";
    "income.csv" = "income_stage";
    "loan.csv" = "loan_stage"
}

foreach ($entry in $csvTableMap.GetEnumerator()) {
    $csvFile = $entry.Key
    $tableName = $entry.Value

    # Create a temporary file for SQLite commands
    $tempFile = [System.IO.Path]::GetTempFileName()
    $commands = ".mode csv`n.import --skip 1 '$csvPath\$csvFile' $tableName"
    [System.IO.File]::WriteAllText($tempFile, $commands)

    # Run the SQLite commands from the temporary file
    sqlite3.exe $dbPath ".read $tempFile"

    # Clean up the temporary file
    Remove-Item $tempFile
}
