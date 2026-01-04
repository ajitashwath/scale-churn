Write-Host "Checking Java installation..." -ForegroundColor Cyan

$javaInPath = Get-Command java -ErrorAction SilentlyContinue
if ($javaInPath) {
    Write-Host "Java found in PATH" -ForegroundColor Green
    java -version
} else {
    Write-Host "Java not found in PATH" -ForegroundColor Red
}

$javaHome = $env:JAVA_HOME
if ($javaHome) {
    Write-Host ""
    Write-Host "JAVA_HOME is set to: $javaHome" -ForegroundColor Green
    
    $javaExe = Join-Path $javaHome "bin\java.exe"
    if (Test-Path $javaExe) {
        Write-Host "Java executable found at: $javaExe" -ForegroundColor Green
    } else {
        Write-Host "Java executable NOT found at: $javaExe" -ForegroundColor Red
        Write-Host "JAVA_HOME may be set incorrectly" -ForegroundColor Yellow
    }
} else {
    Write-Host ""
    Write-Host "JAVA_HOME is not set" -ForegroundColor Red
    Write-Host ""
    Write-Host "To set JAVA_HOME, run:" -ForegroundColor Yellow
    Write-Host '  [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jdk-XX", "User")' -ForegroundColor White
    Write-Host "  (Replace jdk-XX with your actual JDK version)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Searching for Java in common locations..." -ForegroundColor Cyan
$commonPaths = @(
    "C:\Program Files\Java",
    "C:\Program Files (x86)\Java",
    "$env:LOCALAPPDATA\Programs\Java"
)

$foundJava = $false
foreach ($path in $commonPaths) {
    if (Test-Path $path) {
        $jdkDirs = Get-ChildItem -Path $path -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "jdk*" -or $_.Name -like "java*" }
        if ($jdkDirs) {
            Write-Host "Found Java installations in: $path" -ForegroundColor Green
            foreach ($dir in $jdkDirs) {
                $javaExe = Join-Path $dir.FullName "bin\java.exe"
                if (Test-Path $javaExe) {
                    Write-Host "  - $($dir.Name) at $($dir.FullName)" -ForegroundColor White
                    if (-not $foundJava) {
                        Write-Host ""
                        Write-Host "To set JAVA_HOME for this installation, run:" -ForegroundColor Yellow
                        $javaHomeCmd = '$env:JAVA_HOME = "' + $dir.FullName + '"'
                        Write-Host "  $javaHomeCmd" -ForegroundColor White
                        $setEnvCmd = '[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "' + $dir.FullName + '", "User")'
                        Write-Host "  $setEnvCmd" -ForegroundColor White
                        $foundJava = $true
                    }
                }
            }
        }
    }
}

if (-not $foundJava) {
    Write-Host "No Java installations found in common locations" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Please install Java from:" -ForegroundColor Cyan
    Write-Host "  - https://adoptium.net/ (Eclipse Temurin - Recommended)" -ForegroundColor White
    Write-Host "  - https://www.oracle.com/java/technologies/downloads/" -ForegroundColor White
}

Write-Host ""
Write-Host "Note: After setting JAVA_HOME, restart your terminal/PowerShell for changes to take effect." -ForegroundColor Yellow
