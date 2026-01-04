Write-Host "Downloading Java (OpenJDK 17) installer..." -ForegroundColor Cyan

$javaUrl = "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse"
$installerPath = "$env:TEMP\OpenJDK17.msi"

try {
    $response = Invoke-WebRequest -Uri "$javaUrl?project=jdk" -Method Head -MaximumRedirection 0 -ErrorAction SilentlyContinue
    if ($response.StatusCode -eq 302 -or $response.StatusCode -eq 301) {
        $actualUrl = $response.Headers.Location
        Write-Host "Found download URL: $actualUrl" -ForegroundColor Green
    } else {
        $actualUrl = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_windows_hotspot_17.0.13_11.msi"
        Write-Host "Using fallback URL" -ForegroundColor Yellow
    }
} catch {
    $actualUrl = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_windows_hotspot_17.0.13_11.msi"
    Write-Host "Using fallback URL due to API issue" -ForegroundColor Yellow
}

Write-Host "Downloading from: $actualUrl" -ForegroundColor Cyan
Write-Host "Saving to: $installerPath" -ForegroundColor Cyan

try {
    Invoke-WebRequest -Uri $actualUrl -OutFile $installerPath -UseBasicParsing
    Write-Host "Download completed!" -ForegroundColor Green
    
    Write-Host ""
    Write-Host "To install Java, you need to run this script as Administrator." -ForegroundColor Yellow
    Write-Host "Right-click PowerShell and select 'Run as Administrator', then run:" -ForegroundColor Yellow
    Write-Host "  Start-Process msiexec.exe -ArgumentList '/i `"$installerPath`" /quiet /norestart' -Wait -Verb RunAs" -ForegroundColor White
    Write-Host ""
    Write-Host "Or manually run the installer:" -ForegroundColor Yellow
    Write-Host "  Start-Process `"$installerPath`"" -ForegroundColor White
    Write-Host ""
    
    $possibleJavaPaths = @(
        "C:\Program Files\Eclipse Adoptium",
        "C:\Program Files\Java",
        "C:\Program Files (x86)\Java"
    )
    
    Write-Host "After installation, Java will likely be in one of these locations:" -ForegroundColor Cyan
    foreach ($path in $possibleJavaPaths) {
        Write-Host "  - $path" -ForegroundColor White
    }
    
} catch {
    Write-Host "Error downloading Java: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please download Java manually from:" -ForegroundColor Yellow
    Write-Host "  https://adoptium.net/temurin/releases/?version=17" -ForegroundColor White
    Write-Host "  Select: Windows x64, JDK, .msi installer" -ForegroundColor White
    exit 1
}

