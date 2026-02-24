# =============================================================
# STEP 5 - Create the Glue ETL Job, Run It, and Monitor Progress
#
# WHAT THIS DOES:
#   1. Uploads  glue_transform.py  to the scripts S3 bucket
#   2. Creates  a Glue Spark job that references that script
#   3. Starts   the job with --INPUT_PATH and --OUTPUT_PATH args
#   4. Polls    until the job run is SUCCEEDED or FAILED
#   5. Shows    the output files written to the clean bucket
#
# After this step your cleansed Parquet files are in:
#   s3://<clean-bucket>/clean/orders/
# =============================================================

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " STEP 5: Glue ETL Job - Cleanse the Data" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n[1/7] Reading pipeline config..." -ForegroundColor Yellow
if (-not (Test-Path "pipeline_config.json")) {
    Write-Host "ERROR: pipeline_config.json not found. Run previous steps first." -ForegroundColor Red
    exit 1
}
$Config       = Get-Content "pipeline_config.json" | ConvertFrom-Json
$Region       = $Config.Region
$RawBucket    = $Config.RawBucket
$CleanBucket  = $Config.CleanBucket
$ScriptBucket = $Config.ScriptBucket
$RoleArn      = $Config.GlueRoleArn
$JobName      = "orders-data-cleanse-job"
$ScriptKey    = "scripts/glue_transform.py"

$InputPath  = "s3://$RawBucket/raw/orders/"
$OutputPath = "s3://$CleanBucket/clean/orders/"

Write-Host "       Job name   : $JobName"    -ForegroundColor Green
Write-Host "       Input      : $InputPath"  -ForegroundColor Green
Write-Host "       Output     : $OutputPath" -ForegroundColor Green

# --- 1. Upload the PySpark script to S3 ---
Write-Host "`n[2/7] Uploading Glue script to s3://$ScriptBucket/$ScriptKey ..." -ForegroundColor Yellow
aws s3 cp "glue_transform.py" "s3://$ScriptBucket/$ScriptKey"
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Script upload failed." -ForegroundColor Red; exit 1 }
Write-Host "       Script uploaded." -ForegroundColor Green

# --- 2. Create (or update) the Glue job ---
Write-Host "`n[3/7] Creating Glue job '$JobName'..." -ForegroundColor Yellow
$ExistingJob = aws glue get-job --job-name $JobName --query "Job.Name" --output text --region $Region 2>$null

if ($ExistingJob -eq $JobName) {
    Write-Host "       Job already exists - updating script location..." -ForegroundColor Yellow
    aws glue update-job `
        --job-name $JobName `
        --job-update "ScriptLocation=s3://$ScriptBucket/$ScriptKey" `
        --region $Region | Out-Null
} else {
    # Glue version 4.0 = PySpark on Python 3.10 with auto-scaling DPUs
    $CmdJson = @{Name = "glueetl"; ScriptLocation = "s3://$ScriptBucket/$ScriptKey"; PythonVersion = "3"} | ConvertTo-Json -Compress
    $CmdJson | Set-Content -Path "$env:TEMP\glue_cmd.json" -Encoding ascii

    $DefArgs = @{"--job-language" = "python"; "--TempDir" = "s3://$ScriptBucket/tmp/"; "--enable-metrics" = ""; "--enable-continuous-cloudwatch-log" = "true"} | ConvertTo-Json -Compress
    $DefArgs | Set-Content -Path "$env:TEMP\glue_defargs.json" -Encoding ascii

    aws glue create-job `
        --name $JobName `
        --role $RoleArn `
        --command "file://$env:TEMP\glue_cmd.json" `
        --glue-version "4.0" `
        --worker-type "G.1X" `
        --number-of-workers 2 `
        --timeout 30 `
        --default-arguments "file://$env:TEMP\glue_defargs.json" `
        --region $Region | Out-Null
    Write-Host "       Job created." -ForegroundColor Green
}

# --- 3. Start the job run with our dynamic arguments ---
Write-Host "`n[4/7] Starting job run..." -ForegroundColor Yellow
$RunArgsObj = @{"--INPUT_PATH" = $InputPath; "--OUTPUT_PATH" = $OutputPath} | ConvertTo-Json -Compress
$RunArgsObj | Set-Content -Path "$env:TEMP\glue_runargs.json" -Encoding ascii
$RunId   = aws glue start-job-run `
    --job-name $JobName `
    --arguments "file://$env:TEMP\glue_runargs.json" `
    --region $Region `
    --query "JobRunId" `
    --output text

if (-not $RunId) { Write-Host "ERROR: Could not start job run." -ForegroundColor Red; exit 1 }
Write-Host "       Job Run ID: $RunId" -ForegroundColor Green

# --- 4. Poll until terminal state ---
Write-Host "`n[5/7] Monitoring job run (Glue Spark jobs typically take 3-5 minutes)..." -ForegroundColor Yellow
Write-Host "       TIP: You can also watch this in AWS Console -> Glue -> Jobs -> Run details" -ForegroundColor Magenta

$Spinner = @('|','/','-','\')
$i = 0
do {
    Start-Sleep -Seconds 15
    $RunState = aws glue get-job-run `
        --job-name $JobName `
        --run-id $RunId `
        --query "JobRun.JobRunState" `
        --output text `
        --region $Region
    Write-Host "       [$($Spinner[$i % 4])] Job state: $RunState" -ForegroundColor DarkCyan
    $i++
} while ($RunState -notin @("SUCCEEDED","FAILED","ERROR","TIMEOUT","STOPPED"))

if ($RunState -eq "SUCCEEDED") {
    Write-Host "`n       JOB SUCCEEDED" -ForegroundColor Green
} else {
    Write-Host "`n       JOB $RunState - check CloudWatch logs for details." -ForegroundColor Red
    $ErrorMsg = aws glue get-job-run `
        --job-name $JobName `
        --run-id $RunId `
        --query "JobRun.ErrorMessage" `
        --output text `
        --region $Region
    Write-Host "       Error: $ErrorMsg" -ForegroundColor Red
    exit 1
}

# --- 5. List output files ---
Write-Host "`n[6/7] Clean output files:" -ForegroundColor Yellow
aws s3 ls "s3://$CleanBucket/clean/orders/" --recursive --human-readable

# --- 6. Save job info ---
Write-Host "`n[7/7] Saving job info to pipeline_config.json..." -ForegroundColor Yellow
$Config | Add-Member -NotePropertyName "GlueJobName" -NotePropertyValue $JobName  -Force
$Config | Add-Member -NotePropertyName "LastRunId"   -NotePropertyValue $RunId    -Force
$Config | Add-Member -NotePropertyName "OutputPath"  -NotePropertyValue $OutputPath -Force
$Config | ConvertTo-Json | Out-File -FilePath "pipeline_config.json" -Encoding utf8

Write-Host "`n  Your clean Parquet data is ready at:" -ForegroundColor Magenta
Write-Host "  $OutputPath" -ForegroundColor White

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " STEP 5 COMPLETE  - Pipeline Finished!" -ForegroundColor Green
Write-Host "" -ForegroundColor White
Write-Host " To query the clean data with Athena:" -ForegroundColor White
Write-Host "   Run .\step6_query_with_athena.ps1" -ForegroundColor White
Write-Host " To tear everything down:" -ForegroundColor White
Write-Host "   Run .\cleanup_pipeline.ps1" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan
