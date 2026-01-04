Write-Host "Setting up Java (OpenJDK 17) for PySpark..." -ForegroundColor Cyan

$javaInstallDir = Join-Path $env:LOCALAPPDATA "Java"
$jdkDir = Join-Path $javaInstallDir "jdk-17"

if (Test-Path (Join-Path $jdkDir "bin\java.exe")) {
    Write-Host "Java already installed at: $jdkDir" -ForegroundColor Green
    $env:JAVA_HOME = $jdkDir
    [System.Environment]::SetEnvironmentVariable("JAVA_HOME", $jdkDir, "User")
    Write-Host "JAVA_HOME set to: $jdkDir" -ForegroundColor Green
    & (Join-Path $jdkDir "bin\java.exe") -version
    exit 0
}

if (-not (Test-Path $javaInstallDir)) {
    New-Item -ItemType Directory -Path $javaInstallDir -Force | Out-Null
}

Write-Host "Downloading Java (OpenJDK 17) ZIP package..." -ForegroundColor Cyan

$apiUrl = "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse"
$zipPath = Join-Path $env:TEMP "OpenJDK17.zip"

try {
    $response = Invoke-WebRequest -Uri "$apiUrl?project=jdk" -Method Head -MaximumRedirection 0 -ErrorAction SilentlyContinue
    if ($response.StatusCode -eq 302 -or $response.StatusCode -eq 301) {
        $downloadUrl = $response.Headers.Location
        Write-Host "Found download URL: $downloadUrl" -ForegroundColor Green
    } else {
        throw "Could not get redirect URL"
    }
} catch {
    Write-Host "Using fallback download method..." -ForegroundColor Yellow
    $downloadUrl = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_windows_hotspot_17.0.13_11.zip"
}

Write-Host "Downloading from: $downloadUrl" -ForegroundColor Cyan
Write-Host "This may take a few minutes..." -ForegroundColor Yellow

try {
    $ProgressPreference = 'Continue'
    Invoke-WebRequest -Uri $downloadUrl -OutFile $zipPath -UseBasicParsing
    Write-Host "Download completed!" -ForegroundColor Green
    
    Write-Host "Extracting Java..." -ForegroundColor Cyan
    $extractPath = Join-Path $env:TEMP "OpenJDK17_extract"
    if (Test-Path $extractPath) {
        Remove-Item -Path $extractPath -Recurse -Force
    }
    Expand-Archive -Path $zipPath -DestinationPath $extractPath -Force
    
    $jdkFolders = Get-ChildItem -Path $extractPath -Directory | Where-Object { $_.Name -like "jdk*" }
    if ($jdkFolders.Count -eq 0) {
        $allFolders = Get-ChildItem -Path $extractPath -Directory -Recurse | Where-Object { 
            Test-Path (Join-Path $_.FullName "bin\java.exe")
        }
        if ($allFolders.Count -gt 0) {
            $jdkSource = $allFolders[0].FullName
        } else {
            throw "Could not find JDK directory in extracted files"
        }
    } else {
        $jdkSource = $jdkFolders[0].FullName
    }
    
    if (Test-Path $jdkDir) {
        Remove-Item -Path $jdkDir -Recurse -Force
    }
    Move-Item -Path $jdkSource -Destination $jdkDir
    Write-Host "Java extracted to: $jdkDir" -ForegroundColor Green
    
    Remove-Item -Path $zipPath -Force
    Remove-Item -Path $extractPath -Recurse -Force
    
    $env:JAVA_HOME = $jdkDir
    [System.Environment]::SetEnvironmentVariable("JAVA_HOME", $jdkDir, "User")
    Write-Host ""
    Write-Host "JAVA_HOME set to: $jdkDir" -ForegroundColor Green
    Write-Host ""
    
    Write-Host "Verifying Java installation..." -ForegroundColor Cyan
    & (Join-Path $jdkDir "bin\java.exe") -version
    
    Write-Host ""
    Write-Host "Java setup completed successfully!" -ForegroundColor Green
    Write-Host "Note: You may need to restart your terminal for JAVA_HOME to take effect in new sessions." -ForegroundColor Yellow
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Java manually:" -ForegroundColor Yellow
    Write-Host "  1. Download from: https://adoptium.net/temurin/releases/?version=17" -ForegroundColor White
    Write-Host "  2. Select: Windows x64, JDK, .zip package" -ForegroundColor White
    Write-Host "  3. Extract and set JAVA_HOME to the extracted folder" -ForegroundColor White
    exit 1
}

