# Spark Scale Churn - Complete Pipeline Runner
# Executes ETL → Feature Engineering → Model Training

param(
    [switch]$SkipValidation = $false
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Spark Scale Churn - Pipeline Runner" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Step 0: Validate setup (unless skipped)
if (-not $SkipValidation) {
    Write-Host "[0/3] Validating environment setup...`n" -ForegroundColor Yellow
    & .\scripts\test_setup.ps1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`n✗ Setup validation failed. Fix errors before running pipeline." -ForegroundColor Red
        exit 1
    }
    Write-Host ""
}

# Step 1: Run ETL
Write-Host "[1/3] Running ETL Pipeline..." -ForegroundColor Yellow
Write-Host "Reading: data\WA_Fn-UseC_-Telco-Customer-Churn.csv" -ForegroundColor Gray
Write-Host "Writing: data\processed\telco_cleaned\`n" -ForegroundColor Gray

$etlStart = Get-Date
try {
    python src\etl.py --input data\WA_Fn-UseC_-Telco-Customer-Churn.csv --output data\processed
    if ($LASTEXITCODE -ne 0) {
        throw "ETL failed with exit code $LASTEXITCODE"
    }
    $etlDuration = ((Get-Date) - $etlStart).TotalSeconds
    Write-Host "  ✓ ETL completed in $([math]::Round($etlDuration, 2))s`n" -ForegroundColor Green
} catch {
    Write-Host "  ✗ ETL failed: $_" -ForegroundColor Red
    exit 1
}

# Verify ETL output
if (Test-Path "data\processed\telco_cleaned") {
    $parquetFiles = Get-ChildItem "data\processed\telco_cleaned" -Recurse -Filter "*.parquet"
    Write-Host "  → Created $($parquetFiles.Count) Parquet file(s)" -ForegroundColor Gray
} else {
    Write-Host "  ✗ Expected output directory not found" -ForegroundColor Red
    exit 1
}

# Step 2: Run Feature Engineering
Write-Host "`n[2/3] Running Feature Engineering & Model Training..." -ForegroundColor Yellow
Write-Host "Reading: data\processed\telco_cleaned\" -ForegroundColor Gray
Write-Host "Writing: data\models\`n" -ForegroundColor Gray

$feStart = Get-Date
try {
    python src\feature_eng.py
    if ($LASTEXITCODE -ne 0) {
        throw "Feature engineering failed with exit code $LASTEXITCODE"
    }
    $feDuration = ((Get-Date) - $feStart).TotalSeconds
    Write-Host "  ✓ Feature engineering completed in $([math]::Round($feDuration, 2))s`n" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Feature engineering failed: $_" -ForegroundColor Red
    exit 1
}

# Verify model output
if (Test-Path "data\models") {
    $modelDirs = Get-ChildItem "data\models" -Directory
    Write-Host "  → Created model(s): $($modelDirs.Name -join ', ')" -ForegroundColor Gray
} else {
    Write-Host "  ⚠ Models directory not found (may not have been created)" -ForegroundColor Yellow
}

# Step 3: Summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "✓ Pipeline completed successfully!" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Cyan

$totalDuration = $etlDuration + $feDuration
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  • ETL: $([math]::Round($etlDuration, 2))s" -ForegroundColor White
Write-Host "  • Feature Engineering: $([math]::Round($feDuration, 2))s" -ForegroundColor White
Write-Host "  • Total: $([math]::Round($totalDuration, 2))s" -ForegroundColor White

Write-Host "`nOutputs:" -ForegroundColor Cyan
Write-Host "  • Cleaned data: data\processed\telco_cleaned\" -ForegroundColor White
Write-Host "  • Models: data\models\" -ForegroundColor White

Write-Host "`nNext Steps:" -ForegroundColor Cyan
Write-Host "  • Review logs above for AUC score and metrics" -ForegroundColor White
Write-Host "  • Use models for batch predictions or deployment" -ForegroundColor White
Write-Host "  • Run with Docker: docker-compose up" -ForegroundColor White
