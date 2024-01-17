# FlaskStop.ps1
# Send a request to the Flask shutdown endpoint

$uri = "http://localhost:5000/shutdown"

try {
    # Making a GET request to the shutdown endpoint
    Invoke-WebRequest -Uri $uri -Method Get
    Write-Host "Shutdown request sent to Flask server."
}
catch {
    Write-Host "Failed to send shutdown request: $_"
}
