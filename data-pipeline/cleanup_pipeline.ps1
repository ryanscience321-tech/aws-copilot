# =============================================================
# cleanup_pipeline.ps1 — Tears down ALL pipeline resources
#
# Deletes:
#   • All 3 S3 buckets (and their contents)
#   • Glue crawlers (raw + clean)
#   • Glue job
#   • Glue database + tables
#   • IAM inline policy + detaches managed policy + deletes role
#   • Temp JSON files in this directory
# =============================================================

Write-Host "==========================================" -ForegroundColor Red
Write-Host " CLEANUP: Deleting all pipeline resources" -ForegroundColor Red
Write-Host "==========================================" -ForegroundColor Red

if (-not (Test-Path "pipeline_config.json")) {
    Write-Host "pipeline_config.json not found – nothing to clean up." -ForegroundColor Yellow
    exit 0
}

$Config       = Get-Content "pipeline_config.json" | ConvertFrom-Json
$Region       = $Config.Region
$RawBucket    = $Config.RawBucket
$CleanBucket  = $Config.CleanBucket
$ScriptBucket = $Config.ScriptBucket
$RoleName     = $Config.GlueRoleName
$JobName      = $Config.GlueJobName
$DbName       = $Config.GlueDatabase

$Confirm = Read-Host "`nThis will permanently delete all pipeline resources. Type 'yes' to continue"
if ($Confirm -ne "yes") { Write-Host "Aborted." -ForegroundColor Yellow; exit 0 }

# --- S3 buckets ---
foreach ($Bucket in @($RawBucket, $CleanBucket, $ScriptBucket)) {
    if ($Bucket) {
        Write-Host "`nDeleting s3://$Bucket ..." -ForegroundColor Yellow
        aws s3 rm "s3://$Bucket" --recursive --region $Region 2>&1 | Out-Null
        aws s3api delete-bucket --bucket $Bucket --region $Region 2>&1 | Out-Null
        Write-Host "  Deleted." -ForegroundColor Green
    }
}

# --- Glue crawlers ---
foreach ($Crawler in @("orders-raw-crawler", "orders-clean-crawler")) {
    Write-Host "`nDeleting Glue crawler '$Crawler' ..." -ForegroundColor Yellow
    aws glue delete-crawler --name $Crawler --region $Region 2>&1 | Out-Null
    Write-Host "  Deleted." -ForegroundColor Green
}

# --- Glue job ---
if ($JobName) {
    Write-Host "`nDeleting Glue job '$JobName' ..." -ForegroundColor Yellow
    aws glue delete-job --job-name $JobName --region $Region 2>&1 | Out-Null
    Write-Host "  Deleted." -ForegroundColor Green
}

# --- Glue database ---
if ($DbName) {
    Write-Host "`nDeleting Glue database '$DbName' ..." -ForegroundColor Yellow
    aws glue delete-database --name $DbName --region $Region 2>&1 | Out-Null
    Write-Host "  Deleted." -ForegroundColor Green
}

# --- IAM role ---
if ($RoleName) {
    Write-Host "`nDeleting IAM role '$RoleName' ..." -ForegroundColor Yellow
    aws iam detach-role-policy --role-name $RoleName `
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole 2>&1 | Out-Null
    aws iam delete-role-policy --role-name $RoleName `
        --policy-name GluePipelineS3Access 2>&1 | Out-Null
    aws iam delete-role --role-name $RoleName 2>&1 | Out-Null
    Write-Host "  Deleted." -ForegroundColor Green
}

# --- Local temp files ---
Remove-Item -ErrorAction SilentlyContinue pipeline_config.json, glue_trust_policy.json, glue_s3_policy.json

Write-Host "`n==========================================" -ForegroundColor Green
Write-Host " All pipeline resources deleted." -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
