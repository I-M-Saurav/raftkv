param(
    [Parameter(Mandatory=$true)]
    [ValidateRange(1,10)]
    [int]$NodeCount
)

$configPath = "config/cluster.yaml"
$lines = @("nodes:")

for ($i = 1; $i -le $NodeCount; $i++) {
    $raftPort = 9000 + $i
    $httpPort = 8080 + $i
    $lines += "  - id: $i"
    $lines += "    address: `"localhost:$raftPort`""
    $lines += "    http_address: `"localhost:$httpPort`""
}

$lines -join "`n" | Out-File -FilePath $configPath -Encoding utf8
Write-Host "Generated $configPath with $NodeCount nodes (ports 900X / 808X)"