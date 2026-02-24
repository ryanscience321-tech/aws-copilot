# =============================================================
# STEP 6 (Optional) — Register clean data in Glue Catalog
#                     and run a sample query via Amazon Athena
#
# WHAT THIS DOES:
#   1. Runs the Glue crawler again against the clean bucket so the
#      catalog gets a "clean_orders" table
#   2. Creates an Athena query result bucket
#   3. Executes two sample SQL queries via Athena
# =============================================================

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " STEP 6: Query Clean Data with Athena" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n[1/5] Reading pipeline config..." -ForegroundColor Yellow
if (-not (Test-Path "pipeline_config.json")) {
    Write-Host "ERROR: pipeline_config.json not found. Run previous steps first." -ForegroundColor Red
    exit 1
}
$Config        = Get-Content "pipeline_config.json" | ConvertFrom-Json
$Region        = $Config.Region
$CleanBucket   = $Config.CleanBucket
$ScriptBucket  = $Config.ScriptBucket
$RoleArn       = $Config.GlueRoleArn
$DbName        = $Config.GlueDatabase
$CrawlerName   = "orders-clean-crawler"
$AthenaBucket  = "$ScriptBucket"   # reuse scripts bucket for Athena results

Write-Host "       Clean bucket : s3://$CleanBucket/clean/orders/" -ForegroundColor Green
Write-Host "       Database     : $DbName" -ForegroundColor Green

# --- 1. Create a crawler for the clean data ---
Write-Host "`n[2/5] Creating clean-data crawler '$CrawlerName'..." -ForegroundColor Yellow
$Existing = aws glue get-crawler --name $CrawlerName --query "Crawler.Name" --output text --region $Region 2>$null
if ($Existing -eq $CrawlerName) {
    Write-Host "       Crawler already exists." -ForegroundColor Yellow
} else {
    aws glue create-crawler `
        --name $CrawlerName `
        --role $RoleArn `
        --database-name $DbName `
        --targets "{`"S3Targets`":[{`"Path`":`"s3://$CleanBucket/clean/orders/`"}]}" `
        --table-prefix "clean_" `
        --region $Region | Out-Null
    Write-Host "       Crawler created." -ForegroundColor Green
}

aws glue start-crawler --name $CrawlerName --region $Region 2>&1 | Out-Null
Write-Host "       Crawler started, waiting..." -ForegroundColor Yellow
do {
    Start-Sleep -Seconds 10
    $State = aws glue get-crawler --name $CrawlerName --query "Crawler.State" --output text --region $Region
    Write-Host "       State: $State" -ForegroundColor DarkCyan
} while ($State -ne "READY")
Write-Host "       Crawler done." -ForegroundColor Green

# --- 2. Set Athena workgroup output location ---
Write-Host "`n[3/5] Configuring Athena output location..." -ForegroundColor Yellow
$AthenaOutput = "s3://$AthenaBucket/athena-results/"
aws athena update-work-group `
    --work-group primary `
    --configuration-updates "ResultConfigurationUpdates={OutputLocation=$AthenaOutput}" `
    --region $Region 2>&1 | Out-Null
Write-Host "       Athena results -> $AthenaOutput" -ForegroundColor Green

# --- Helper: run an Athena query and wait for results ---
function Invoke-AthenaQuery {
    param([string]$Sql, [string]$Label)
    Write-Host "`n       Running: $Label" -ForegroundColor Yellow
    $ExecId = aws athena start-query-execution `
        --query-string $Sql `
        --query-execution-context "Database=$DbName" `
        --result-configuration "OutputLocation=$AthenaOutput" `
        --region $Region `
        --query "QueryExecutionId" `
        --output text
    do {
        Start-Sleep -Seconds 3
        $State = aws athena get-query-execution --query-execution-id $ExecId `
            --query "QueryExecution.Status.State" --output text --region $Region
    } while ($State -notin @("SUCCEEDED","FAILED","CANCELLED"))

    if ($State -eq "SUCCEEDED") {
        aws athena get-query-results --query-execution-id $ExecId `
            --region $Region --query "ResultSet.Rows[*].Data[*].VarCharValue" --output table
    } else {
        Write-Host "       Query $State" -ForegroundColor Red
    }
}

# --- 3. Sample queries ---
Write-Host "`n[4/5] Running sample Athena queries against clean data..." -ForegroundColor Yellow

Invoke-AthenaQuery `
    -Sql "SELECT COUNT(*) AS total_clean_orders FROM clean_orders LIMIT 1;" `
    -Label "Total clean order count"

Invoke-AthenaQuery `
    -Sql "SELECT status, COUNT(*) AS orders, ROUND(SUM(order_total),2) AS revenue FROM clean_orders GROUP BY status ORDER BY revenue DESC;" `
    -Label "Revenue by order status"

Invoke-AthenaQuery `
    -Sql "SELECT country, COUNT(*) AS orders FROM clean_orders GROUP BY country ORDER BY orders DESC LIMIT 10;" `
    -Label "Top 10 countries by order count"

Write-Host "`n[5/5] Done." -ForegroundColor Green
Write-Host "       You can run any SQL in the AWS Console -> Athena -> Query Editor" -ForegroundColor Magenta
Write-Host "       Select database: $DbName  and table: clean_orders" -ForegroundColor Magenta

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " STEP 6 COMPLETE" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
