$hadoopHome = Join-Path $env:USERPROFILE ".hadoop"
$binDir = Join-Path $hadoopHome "bin"
$winutilsPath = Join-Path $binDir "winutils.exe"

if (Test-Path $winutilsPath) {
    Write-Host "winutils.exe already exists at: $winutilsPath" -ForegroundColor Green
    exit 0
}

New-Item -ItemType Directory -Path $binDir -Force | Out-Null

Write-Host "Downloading winutils.exe..." -ForegroundColor Cyan

$urls = @(
    "https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe",
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.0/bin/winutils.exe",
    "https://github.com/kontext-tech/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe"
)

$success = $false
foreach ($url in $urls) {
    try {
        Write-Host "Trying: $url" -ForegroundColor Yellow
        Invoke-WebRequest -Uri $url -OutFile $winutilsPath -UseBasicParsing -ErrorAction Stop
        Write-Host "Successfully downloaded winutils.exe!" -ForegroundColor Green
        $success = $true
        break
    } catch {
        Write-Host "Failed: $_" -ForegroundColor Red
        continue
    }
}

if (-not $success) {
    Write-Host "Could not download winutils.exe from any source." -ForegroundColor Red
    Write-Host "Please download manually from: https://github.com/steveloughran/winutils" -ForegroundColor Yellow
    exit 1
}

Write-Host "winutils.exe saved to: $winutilsPath" -ForegroundColor Green

