# =============================================================
# STEP 3 — Create the IAM Role that AWS Glue will assume
#
# WHY: Glue needs permission to:
#   • Read  from the raw      S3 bucket
#   • Write to  the clean     S3 bucket
#   • Read  from the scripts  S3 bucket
#   • Write logs to CloudWatch
#   • Access the Glue Data Catalog
# =============================================================

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " STEP 3: Creating IAM Role for Glue" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n[1/5] Reading pipeline config..." -ForegroundColor Yellow
if (-not (Test-Path "pipeline_config.json")) {
    Write-Host "ERROR: pipeline_config.json not found. Run step1 first." -ForegroundColor Red
    exit 1
}
$Config      = Get-Content "pipeline_config.json" | ConvertFrom-Json
$RawBucket   = $Config.RawBucket
$CleanBucket = $Config.CleanBucket
$ScriptBucket= $Config.ScriptBucket
$RoleName    = "GlueDataPipelineRole"

Write-Host "       Role name: $RoleName" -ForegroundColor Green

# --- Trust policy: allow Glue service to assume this role ---
Write-Host "`n[2/5] Building trust policy..." -ForegroundColor Yellow
$TrustPolicy = @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "glue.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
'@
$TrustPolicy | Out-File -FilePath "glue_trust_policy.json" -Encoding ascii
Write-Host "       Trust policy written to glue_trust_policy.json" -ForegroundColor Green

# --- Inline S3 policy scoped to our three buckets ---
Write-Host "`n[3/5] Building S3 access policy..." -ForegroundColor Yellow
$S3Policy = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::$RawBucket",
        "arn:aws:s3:::$RawBucket/*",
        "arn:aws:s3:::$CleanBucket",
        "arn:aws:s3:::$CleanBucket/*",
        "arn:aws:s3:::$ScriptBucket",
        "arn:aws:s3:::$ScriptBucket/*"
      ]
    }
  ]
}
"@
$S3Policy | Out-File -FilePath "glue_s3_policy.json" -Encoding ascii
Write-Host "       S3 policy written to glue_s3_policy.json" -ForegroundColor Green

# --- Create the IAM role (idempotent – ignores EntityAlreadyExists) ---
Write-Host "`n[4/5] Creating IAM role..." -ForegroundColor Yellow
$ExistingRole = aws iam get-role --role-name $RoleName --query "Role.RoleName" --output text 2>$null
if ($ExistingRole -eq $RoleName) {
    Write-Host "       Role already exists – skipping creation." -ForegroundColor Yellow
} else {
    aws iam create-role `
        --role-name $RoleName `
        --assume-role-policy-document file://glue_trust_policy.json `
        --description "Role used by AWS Glue data pipeline" | Out-Null
    Write-Host "       Role created." -ForegroundColor Green
}

# Attach AWS managed Glue service policy (CloudWatch logs + basic Glue perms)
aws iam attach-role-policy `
    --role-name $RoleName `
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole | Out-Null
Write-Host "       Attached AWSGlueServiceRole managed policy." -ForegroundColor Green

# Attach our scoped S3 inline policy
aws iam put-role-policy `
    --role-name $RoleName `
    --policy-name GluePipelineS3Access `
    --policy-document file://glue_s3_policy.json | Out-Null
Write-Host "       Attached scoped S3 inline policy." -ForegroundColor Green

# Save role ARN for later steps
Write-Host "`n[5/5] Saving role ARN..." -ForegroundColor Yellow
$RoleArn = aws iam get-role --role-name $RoleName --query "Role.Arn" --output text
$Config | Add-Member -NotePropertyName "GlueRoleArn"  -NotePropertyValue $RoleArn  -Force
$Config | Add-Member -NotePropertyName "GlueRoleName" -NotePropertyValue $RoleName -Force
$Config | ConvertTo-Json | Out-File -FilePath "pipeline_config.json" -Encoding utf8
Write-Host "       Role ARN: $RoleArn" -ForegroundColor Green

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " STEP 3 COMPLETE" -ForegroundColor Green
Write-Host " Next: run  .\step4_create_crawler.ps1" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan
