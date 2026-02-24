# step7_auto_trigger.ps1
# Auto-trigger Glue job when a CSV is uploaded to s3://data-pipeline-raw-179367/raw/orders/
# Pattern: S3 Event Notification -> Lambda -> Glue StartJobRun

$Region     = "us-east-1"
$AccountId  = "830087179367"
$RawBucket  = "data-pipeline-raw-179367"
$GlueJob    = "orders-data-cleanse-job"
$FuncName   = "TriggerGlueJob"
$RoleName   = "LambdaGlueTriggerRole"
$Tmp        = $env:TEMP

Write-Host "`n=== AUTO-TRIGGER SETUP ===" -ForegroundColor Cyan
Write-Host "Bucket  : $RawBucket"
Write-Host "Glue Job: $GlueJob"
Write-Host "Pattern : S3 -> Lambda -> Glue"

# ?? Step 1: Create IAM role for Lambda ???????????????????????????????????????
Write-Host "`n[1/5] Creating IAM role '$RoleName'..." -ForegroundColor Yellow

@'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
'@ | Set-Content -Path "$Tmp\lambda_trust.json" -Encoding ascii

$ExistingRole = aws iam get-role --role-name $RoleName --query "Role.Arn" --output text 2>$null
if ($ExistingRole -like "arn:*") {
    Write-Host "       Role already exists" -ForegroundColor Gray
    $RoleArn = $ExistingRole.Trim()
} else {
    $RoleArn = (aws iam create-role `
        --role-name $RoleName `
        --assume-role-policy-document "file://$Tmp\lambda_trust.json" `
        --output json | ConvertFrom-Json).Role.Arn
    Write-Host "       Created: $RoleArn" -ForegroundColor Green
}

# Basic Lambda execution + Glue StartJobRun
aws iam attach-role-policy `
    --role-name $RoleName `
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole | Out-Null

@"
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "glue:StartJobRun",
    "Resource": "arn:aws:glue:${Region}:${AccountId}:job/${GlueJob}"
  }]
}
"@ | Set-Content -Path "$Tmp\lambda_glue_policy.json" -Encoding ascii

aws iam put-role-policy `
    --role-name $RoleName `
    --policy-name "StartGlueJob" `
    --policy-document "file://$Tmp\lambda_glue_policy.json"

Write-Host "       Policies attached" -ForegroundColor Green

# ?? Step 2: Create Lambda function zip ???????????????????????????????????????
Write-Host "`n[2/5] Creating Lambda function '$FuncName'..." -ForegroundColor Yellow

# Write the Python handler
@"
import boto3, json, os

GLUE_JOB = os.environ.get('GLUE_JOB_NAME', '$GlueJob')

def lambda_handler(event, context):
    glue = boto3.client('glue')
    for record in event.get('Records', []):
        key = record['s3']['object']['key']
        print(f'Triggering Glue job for: {key}')
        resp = glue.start_job_run(
            JobName=GLUE_JOB,
            Arguments={'--TRIGGERED_BY': key}
        )
        print(f'Job run ID: {resp[\"JobRunId\"]}')
    return {'statusCode': 200, 'body': 'Glue job triggered'}
"@ | Set-Content -Path "$Tmp\lambda_handler.py" -Encoding ascii

# Zip it
Compress-Archive -Path "$Tmp\lambda_handler.py" -DestinationPath "$Tmp\lambda.zip" -Force

# Wait for IAM propagation
Write-Host "       Waiting 15s for IAM role propagation..." -ForegroundColor Gray
Start-Sleep -Seconds 15

# Create or update the function
$ExistingFunc = aws lambda get-function --function-name $FuncName --query "Configuration.FunctionArn" --output text --region $Region 2>$null
if ($ExistingFunc -like "arn:*") {
    aws lambda update-function-code `
        --function-name $FuncName `
        --zip-file "fileb://$Tmp\lambda.zip" `
        --region $Region | Out-Null
    Write-Host "       Function updated" -ForegroundColor Green
    $FuncArn = $ExistingFunc.Trim()
} else {
    $FuncArn = (aws lambda create-function `
        --function-name $FuncName `
        --runtime python3.12 `
        --role $RoleArn `
        --handler lambda_handler.lambda_handler `
        --zip-file "fileb://$Tmp\lambda.zip" `
        --environment "Variables={GLUE_JOB_NAME=$GlueJob}" `
        --timeout 30 `
        --region $Region `
        --output json | ConvertFrom-Json).FunctionArn
    Write-Host "       Function created: $FuncArn" -ForegroundColor Green
}

# ?? Step 3: Allow S3 to invoke the Lambda ????????????????????????????????????
Write-Host "`n[3/5] Granting S3 permission to invoke Lambda..." -ForegroundColor Yellow

aws lambda remove-permission `
    --function-name $FuncName `
    --statement-id "S3TriggerPermission" `
    --region $Region 2>$null | Out-Null

aws lambda add-permission `
    --function-name $FuncName `
    --statement-id "S3TriggerPermission" `
    --action "lambda:InvokeFunction" `
    --principal "s3.amazonaws.com" `
    --source-arn "arn:aws:s3:::${RawBucket}" `
    --source-account $AccountId `
    --region $Region | Out-Null

Write-Host "       Permission granted" -ForegroundColor Green

# ?? Step 4: Configure S3 event notification ??????????????????????????????????
Write-Host "`n[4/5] Configuring S3 event notification on '$RawBucket'..." -ForegroundColor Yellow

@"
{
  "LambdaFunctionConfigurations": [{
    "Id": "TriggerGlueOnUpload",
    "LambdaFunctionArn": "$FuncArn",
    "Events": ["s3:ObjectCreated:*"],
    "Filter": {
      "Key": {
        "FilterRules": [
          {"Name": "prefix", "Value": "raw/orders/"},
          {"Name": "suffix", "Value": ".csv"}
        ]
      }
    }
  }]
}
"@ | Set-Content -Path "$Tmp\s3_notif.json" -Encoding ascii

aws s3api put-bucket-notification-configuration `
    --bucket $RawBucket `
    --notification-configuration "file://$Tmp\s3_notif.json" `
    --region $Region

if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: S3 notification setup failed" -ForegroundColor Red; exit 1 }
Write-Host "       S3 notification configured" -ForegroundColor Green

# ?? Step 5: Test by uploading the sample file ?????????????????????????????????
Write-Host "`n[5/5] Setup complete!" -ForegroundColor Green

Write-Host "`n=== SETUP COMPLETE ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Any .csv uploaded to:" -ForegroundColor White
Write-Host "   s3://$RawBucket/raw/orders/" -ForegroundColor Yellow
Write-Host ""
Write-Host "will automatically trigger Lambda '$FuncName'" -ForegroundColor White
Write-Host "which starts Glue job '$GlueJob'" -ForegroundColor White
Write-Host ""
Write-Host "To test:" -ForegroundColor White
Write-Host "   aws s3 cp sample_orders_dirty.csv s3://$RawBucket/raw/orders/test_upload.csv --region $Region" -ForegroundColor Gray
Write-Host "   # Wait ~30s then check:"
Write-Host "   aws glue get-job-runs --job-name $GlueJob --query 'JobRuns[0].{Status:JobRunState,Started:StartedOn}' --output table --region $Region" -ForegroundColor Gray
