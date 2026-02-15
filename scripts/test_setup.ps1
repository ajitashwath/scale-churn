# Spark Scale Churn Pipeline Test
# This script validates the environment setup

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Spark Scale Churn - Setup Validation" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$errors = @()

# Test 1: Check Python
Write-Host "[1/5] Checking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  ✓ $pythonVersion" -ForegroundColor Green
} catch {
    $errors += "Python not found in PATH"
    Write-Host "  ✗ Python not found" -ForegroundColor Red
}

# Test 2: Check Java
Write-Host "`n[2/5] Checking Java installation..." -ForegroundColor Yellow
$javaHome = $env:JAVA_HOME
if ($javaHome -and (Test-Path "$javaHome\bin\java.exe")) {
    $javaVersion = & "$javaHome\bin\java.exe" -version 2>&1 | Select-Object -First 1
    Write-Host "  ✓ JAVA_HOME: $javaHome" -ForegroundColor Green
    Write-Host "  ✓ $javaVersion" -ForegroundColor Green
} else {
    # Try to find Java
    $javaCmd = Get-Command java -ErrorAction SilentlyContinue
    if ($javaCmd) {
        $javaVersion = java -version 2>&1 | Select-Object -First 1
        Write-Host "  ⚠ Java found but JAVA_HOME not set" -ForegroundColor Yellow
        Write-Host "  ✓ $javaVersion" -ForegroundColor Green
    } else {
        $errors += "Java not found. Install JDK 8+ and set JAVA_HOME"
        Write-Host "  ✗ Java not found" -ForegroundColor Red
        Write-Host "  → Run: .\scripts\setup_java.ps1" -ForegroundColor Yellow
    }
}

# Test 3: Check PySpark
Write-Host "`n[3/5] Checking PySpark installation..." -ForegroundColor Yellow
try {
    $pysparkCheck = python -c "import pyspark; print(f'PySpark {pyspark.__version__}')" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ $pysparkCheck" -ForegroundColor Green
    } else {
        $errors += "PySpark not installed"
        Write-Host "  ✗ PySpark not installed" -ForegroundColor Red
        Write-Host "  → Run: pip install -r requirements.txt" -ForegroundColor Yellow
    }
} catch {
    $errors += "PySpark check failed"
    Write-Host "  ✗ PySpark check failed" -ForegroundColor Red
}

# Test 4: Check data files
Write-Host "`n[4/5] Checking data files..." -ForegroundColor Yellow
$dataFile = "data\WA_Fn-UseC_-Telco-Customer-Churn.csv"
if (Test-Path $dataFile) {
    $fileSize = (Get-Item $dataFile).Length
    $fileSizeMB = [math]::Round($fileSize / 1MB, 2)
    Write-Host "  ✓ Data file found: $dataFile ($fileSizeMB MB)" -ForegroundColor Green
} else {
    $errors += "Data file not found: $dataFile"
    Write-Host "  ✗ Data file not found: $dataFile" -ForegroundColor Red
}

# Test 5: Test Spark session creation
Write-Host "`n[5/5] Testing Spark session creation..." -ForegroundColor Yellow
$sparkTest = @"
import sys
sys.path.insert(0, 'src')
from utils import setup_java_home, setup_hadoop_windows
setup_java_home()
setup_hadoop_windows()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').master('local[1]').getOrCreate()
print(f'Spark {spark.version}')
spark.stop()
"@

try {
    $sparkResult = python -c $sparkTest 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ Spark session created successfully" -ForegroundColor Green
        Write-Host "  ✓ $sparkResult" -ForegroundColor Green
    } else {
        $errors += "Spark session creation failed"
        Write-Host "  ✗ Spark session creation failed" -ForegroundColor Red
        Write-Host "  Error: $sparkResult" -ForegroundColor Red
    }
} catch {
    $errors += "Spark test failed"
    Write-Host "  ✗ Spark test failed: $_" -ForegroundColor Red
}

# Summary
Write-Host "`n========================================" -ForegroundColor Cyan
if ($errors.Count -eq 0) {
    Write-Host "✓ All checks passed! Environment is ready." -ForegroundColor Green
    Write-Host "`nYou can now run:" -ForegroundColor Cyan
    Write-Host "  .\scripts\run_pipeline.ps1" -ForegroundColor White
    exit 0
} else {
    Write-Host "✗ Setup validation failed with $($errors.Count) error(s):" -ForegroundColor Red
    foreach ($error in $errors) {
        Write-Host "  • $error" -ForegroundColor Red
    }
    Write-Host "`nPlease fix the errors above before proceeding." -ForegroundColor Yellow
    exit 1
}
