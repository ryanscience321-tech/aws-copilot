# =============================================================
# STEP 1 — Create S3 Buckets
# Creates:
#   • data-pipeline-raw-<suffix>   (input  – dirty data lands here)
#   • data-pipeline-clean-<suffix> (output – Glue writes cleansed Parquet here)
#   • data-pipeline-scripts-<suffix> (Glue job script storage)
# =============================================================

param(
    [string]$Region = "us-east-1"
)

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " STEP 1: Creating S3 Buckets" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Derive a unique suffix from your AWS account ID so bucket names are globally unique
Write-Host "`n[1/4] Fetching AWS Account ID..." -ForegroundColor Yellow
$AccountId = (aws sts get-caller-identity --query Account --output text)
if (-not $AccountId) {
    Write-Host "ERROR: Could not get AWS Account ID. Make sure AWS CLI is configured." -ForegroundColor Red
    exit 1
}
Write-Host "       Account ID: $AccountId" -ForegroundColor Green

$Suffix    = $AccountId.Substring($AccountId.Length - 6)   # last 6 digits
$RawBucket     = "data-pipeline-raw-$Suffix"
$CleanBucket   = "data-pipeline-clean-$Suffix"
$ScriptBucket  = "data-pipeline-scripts-$Suffix"

Write-Host "`n[2/4] Bucket names that will be created:" -ForegroundColor Yellow
Write-Host "       RAW    : s3://$RawBucket"    -ForegroundColor White
Write-Host "       CLEAN  : s3://$CleanBucket"  -ForegroundColor White
Write-Host "       SCRIPTS: s3://$ScriptBucket" -ForegroundColor White

# Helper: create a bucket (skips if it already exists)
function New-S3Bucket {
    param([string]$Name, [string]$Rgn)

    Write-Host "`n       Creating s3://$Name ..." -ForegroundColor Yellow
    if ($Rgn -eq "us-east-1") {
        aws s3api create-bucket --bucket $Name --region $Rgn 2>&1 | Out-Null
    } else {
        aws s3api create-bucket --bucket $Name --region $Rgn `
            --create-bucket-configuration LocationConstraint=$Rgn 2>&1 | Out-Null
    }

    # Block all public access (security best-practice)
    aws s3api put-public-access-block `
        --bucket $Name `
        --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" | Out-Null

    Write-Host "       s3://$Name  -> CREATED" -ForegroundColor Green
}

Write-Host "`n[3/4] Creating buckets..." -ForegroundColor Yellow
New-S3Bucket -Name $RawBucket    -Rgn $Region
New-S3Bucket -Name $CleanBucket  -Rgn $Region
New-S3Bucket -Name $ScriptBucket -Rgn $Region

Write-Host "`n[4/4] Saving bucket names for later steps..." -ForegroundColor Yellow
$Config = @{
    Region        = $Region
    AccountId     = $AccountId
    RawBucket     = $RawBucket
    CleanBucket   = $CleanBucket
    ScriptBucket  = $ScriptBucket
}
$Config | ConvertTo-Json | Out-File -FilePath "pipeline_config.json" -Encoding utf8
Write-Host "       Saved -> pipeline_config.json" -ForegroundColor Green

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " STEP 1 COMPLETE" -ForegroundColor Green
Write-Host " Next: run  .\step2_upload_data.ps1" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan
