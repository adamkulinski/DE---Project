# Set a global variable
[Environment]::SetEnvironmentVariable('PROJECT_LOCATION', 'D:\ACCENTURE\DE - Project', 'User')

# Output global variable
Write-Host "Global variable project_location set to: $project_location"

$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')

Set-Location $project_location

# Build the Docker image
docker build -t piggybank .

# Run the Docker image

docker-compose up --build