param(
    [Parameter(Mandatory=$true)]
    [ValidateRange(1,10)]
    [int]$NodeCount
)

# Regenerate config to match the requested node count.
.\scripts\generate-config.ps1 -NodeCount $NodeCount

# Clean stale WAL data for a fresh demo run. Comment this out if you want
# nodes to recover their previous state instead of starting clean.
if (Test-Path data) {
    Remove-Item -Recurse -Force data
}

for ($i = 1; $i -le $NodeCount; $i++) {
    $raftAddr = "localhost:$(9000 + $i)"
    $httpAddr = ":$(8080 + $i)"
    $cmd = ".\raftkv.exe --id $i --raft-addr $raftAddr --http-addr $httpAddr"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
    Start-Sleep -Milliseconds 200  # stagger startup slightly
}

Write-Host "Launched $NodeCount nodes in separate windows."
Write-Host "Dashboard seed address: http://localhost:8081"