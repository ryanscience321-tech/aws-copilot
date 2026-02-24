# =============================================================
# STEP 4 — Create Glue Database + Crawler, then run the Crawler
#
# WHAT THIS DOES:
#   1. Creates a Glue Data Catalog database called "data_pipeline_db"
#   2. Creates a Glue Crawler that points at s3://<raw-bucket>/raw/orders/
#   3. Runs the crawler  (it infers schema and populates the catalog table)
#   4. Polls until the crawler finishes, then prints the discovered schema
#
# After this step you can browse the table in:
#   AWS Console -> Glue -> Data Catalog -> Databases -> data_pipeline_db
# =============================================================

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " STEP 4: Glue Crawler — Discover Schema" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n[1/6] Reading pipeline config..." -ForegroundColor Yellow
if (-not (Test-Path "pipeline_config.json")) {
    Write-Host "ERROR: pipeline_config.json not found. Run previous steps first." -ForegroundColor Red
    exit 1
}
$Config      = Get-Content "pipeline_config.json" | ConvertFrom-Json
$Region      = $Config.Region
$RawBucket   = $Config.RawBucket
$RoleArn     = $Config.GlueRoleArn
$DbName      = "data_pipeline_db"
$CrawlerName = "orders-raw-crawler"

Write-Host "       Region     : $Region"      -ForegroundColor Green
Write-Host "       Raw bucket : s3://$RawBucket/raw/orders/" -ForegroundColor Green
Write-Host "       Database   : $DbName"      -ForegroundColor Green
Write-Host "       Crawler    : $CrawlerName" -ForegroundColor Green

# --- 1. Create Glue Database ---
Write-Host "`n[2/6] Creating Glue Data Catalog database '$DbName'..." -ForegroundColor Yellow
aws glue create-database `
    --database-input "{`"Name`":`"$DbName`",`"Description`":`"Raw and clean order data`"}" `
    --region $Region 2>&1 | Out-Null
Write-Host "       Database ready." -ForegroundColor Green

# --- 2. Create Crawler ---
Write-Host "`n[3/6] Creating Glue Crawler '$CrawlerName'..." -ForegroundColor Yellow
$ExistingCrawler = aws glue get-crawler --name $CrawlerName --query "Crawler.Name" --output text --region $Region 2>$null
if ($ExistingCrawler -eq $CrawlerName) {
    Write-Host "       Crawler already exists – skipping creation." -ForegroundColor Yellow
} else {
    aws glue create-crawler `
        --name $CrawlerName `
        --role $RoleArn `
        --database-name $DbName `
        --targets "{`"S3Targets`":[{`"Path`":`"s3://$RawBucket/raw/orders/`"}]}" `
        --table-prefix "raw_" `
        --schema-change-policy "{`"UpdateBehavior`":`"UPDATE_IN_DATABASE`",`"DeleteBehavior`":`"LOG`"}" `
        --region $Region | Out-Null
    Write-Host "       Crawler created." -ForegroundColor Green
}

# --- 3. Start Crawler ---
Write-Host "`n[4/6] Starting crawler (this will take ~1-2 minutes)..." -ForegroundColor Yellow
aws glue start-crawler --name $CrawlerName --region $Region 2>&1 | Out-Null
Write-Host "       Crawler started." -ForegroundColor Green

# --- 4. Poll until READY ---
Write-Host "`n[5/6] Waiting for crawler to finish..." -ForegroundColor Yellow
$Spinner = @('|','/','-','\')
$i = 0
do {
    Start-Sleep -Seconds 10
    $State = aws glue get-crawler --name $CrawlerName --query "Crawler.State" --output text --region $Region
    Write-Host "       [$($Spinner[$i % 4])] Crawler state: $State" -ForegroundColor DarkCyan
    $i++
} while ($State -ne "READY")
Write-Host "       Crawler finished!" -ForegroundColor Green

# --- 5. Show discovered tables ---
Write-Host "`n[6/6] Tables discovered in catalog:" -ForegroundColor Yellow
$Tables = aws glue get-tables --database-name $DbName --region $Region --query "TableList[].Name" --output text
Write-Host "       $Tables" -ForegroundColor Green

# Save for next step
$Config | Add-Member -NotePropertyName "GlueDatabase"   -NotePropertyValue $DbName      -Force
$Config | Add-Member -NotePropertyName "GlueCrawler"    -NotePropertyValue $CrawlerName -Force
$Config | ConvertTo-Json | Out-File -FilePath "pipeline_config.json" -Encoding utf8

Write-Host "`n  TIP: Open the AWS Console -> Glue -> Data Catalog -> Tables" -ForegroundColor Magenta
Write-Host "       to browse the discovered schema with all the dirty data visible." -ForegroundColor Magenta

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " STEP 4 COMPLETE" -ForegroundColor Green
Write-Host " Next: run  .\step5_create_etl_job.ps1" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan
