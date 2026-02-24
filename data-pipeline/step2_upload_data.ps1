# =============================================================
# STEP 2 — Upload Sample (Dirty) Data to the Raw S3 Bucket
# Uploads:  sample_orders_dirty.csv  ->  s3://<raw-bucket>/raw/orders/
# =============================================================

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " STEP 2: Uploading Sample Data" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n[1/3] Reading pipeline config..." -ForegroundColor Yellow
if (-not (Test-Path "pipeline_config.json")) {
    Write-Host "ERROR: pipeline_config.json not found. Run step1_create_buckets.ps1 first." -ForegroundColor Red
    exit 1
}
$Config     = Get-Content "pipeline_config.json" | ConvertFrom-Json
$RawBucket  = $Config.RawBucket
Write-Host "       Raw bucket: s3://$RawBucket" -ForegroundColor Green

Write-Host "`n[2/3] Checking sample data file..." -ForegroundColor Yellow
$DataFile = "sample_orders_dirty.csv"
if (-not (Test-Path $DataFile)) {
    Write-Host "ERROR: $DataFile not found in current directory." -ForegroundColor Red
    exit 1
}

# Show the user what quality issues are baked in
Write-Host "`n       DATA QUALITY ISSUES intentionally embedded in the file:" -ForegroundColor Magenta
Write-Host "       --- DATA QUALITY ISSUES ---" -ForegroundColor Magenta
Write-Host "        1. DUPLICATE ROWS      - order_id 1001, 1003, 1016" -ForegroundColor White
Write-Host "        2. MISSING VALUES      - customer_name, email, product" -ForegroundColor White
Write-Host "        3. MIXED DATE FORMATS  - 2024-01-15 / 15/01/2024 / Jan 18 2024 / 2024/01/23" -ForegroundColor White
Write-Host "        4. INCONSISTENT CASE   - 'usa' vs 'USA', 'COMPLETED' vs 'completed'" -ForegroundColor White
Write-Host "        5. INVALID QUANTITIES  - negative (-1) and zero (0)" -ForegroundColor White
Write-Host "        6. UNREALISTIC VALUES  - quantity = 999999" -ForegroundColor White
Write-Host "        7. LITERAL NULL        - product = NULL (string)" -ForegroundColor White
Write-Host "        8. MISSING UNIT_PRICE  - row 1020 has blank price" -ForegroundColor White
Write-Host "        9. WHITESPACE IN NAME  - '  Peter White  '" -ForegroundColor White
Write-Host "       10. INVALID EMAIL       - 'jack@' missing domain" -ForegroundColor White
Write-Host "       ------------------------------" -ForegroundColor Magenta

Write-Host "`n[3/3] Uploading to s3://$RawBucket/raw/orders/ ..." -ForegroundColor Yellow
aws s3 cp $DataFile "s3://$RawBucket/raw/orders/$DataFile"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n       Upload SUCCESS" -ForegroundColor Green
    Write-Host "       S3 path: s3://$RawBucket/raw/orders/$DataFile" -ForegroundColor Green
} else {
    Write-Host "ERROR: Upload failed." -ForegroundColor Red
    exit 1
}

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " STEP 2 COMPLETE" -ForegroundColor Green
Write-Host " Next: run  .\step3_create_glue_role.ps1" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan
