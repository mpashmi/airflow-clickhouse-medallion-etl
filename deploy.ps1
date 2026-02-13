<#
.SYNOPSIS
    Safe deploy script for Airflow DAGs to S3.
    Uploads files only — NEVER deletes from the shared S3 bucket.

.DESCRIPTION
    This script syncs local DAGs to your S3 DAG bucket
    with safety checks:
      - Blocks --delete flag (shared bucket protection)
      - Excludes __pycache__ and .pyc files
      - Shows a diff of what will be uploaded before confirming
      - Logs every deploy with timestamp

    Set the S3_DAG_BUCKET environment variable before running:
      $env:S3_DAG_BUCKET = "s3://your-bucket/airflow-dags/"

.EXAMPLE
    .\deploy.ps1
    .\deploy.ps1 -DryRun
#>

param(
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

# ── Configuration (from environment variables) ────────────────────────────────
$S3_BUCKET   = $env:S3_DAG_BUCKET
$LOCAL_DAGS  = "dags/"
$LOG_FILE    = "deploy.log"

if (-not $S3_BUCKET) {
    Write-Host "ERROR: S3_DAG_BUCKET environment variable is not set." -ForegroundColor Red
    Write-Host "Set it first:  `$env:S3_DAG_BUCKET = 's3://your-bucket/airflow-dags/'" -ForegroundColor Yellow
    exit 1
}

# ── Safety: block any --delete usage ──────────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  SAFE DEPLOY — upload only, never delete   " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Source:  $LOCAL_DAGS" -ForegroundColor Gray
Write-Host "Target:  $S3_BUCKET" -ForegroundColor Gray
Write-Host ""

# ── Step 1: Show what will be uploaded (dry run) ─────────────────────────────
Write-Host "Checking what will be uploaded..." -ForegroundColor Yellow
Write-Host ""

$dryRunOutput = aws s3 sync $LOCAL_DAGS $S3_BUCKET `
    --exclude "*.pyc" `
    --exclude "__pycache__/*" `
    --exclude "*.pyo" `
    --exclude ".git/*" `
    --dryrun 2>&1

if (-not $dryRunOutput) {
    Write-Host "Nothing to upload — S3 is already up to date." -ForegroundColor Green
    exit 0
}

Write-Host "Files to upload:" -ForegroundColor Yellow
$dryRunOutput | ForEach-Object { Write-Host "  $_" -ForegroundColor White }
Write-Host ""

# ── Step 2: Safety check — confirm no deletes ────────────────────────────────
$deleteLines = $dryRunOutput | Where-Object { $_ -match "delete:" }
if ($deleteLines) {
    Write-Host "ERROR: Delete operations detected! This should never happen." -ForegroundColor Red
    Write-Host "This script does NOT support deleting files from the shared S3 bucket." -ForegroundColor Red
    exit 1
}

# ── Step 3: Confirm or dry-run ────────────────────────────────────────────────
if ($DryRun) {
    Write-Host "[DRY RUN] No files were uploaded." -ForegroundColor Yellow
    exit 0
}

$confirm = Read-Host "Proceed with upload? (y/n)"
if ($confirm -ne "y") {
    Write-Host "Cancelled." -ForegroundColor Yellow
    exit 0
}

# ── Step 4: Upload ────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Uploading..." -ForegroundColor Green

aws s3 sync $LOCAL_DAGS $S3_BUCKET `
    --exclude "*.pyc" `
    --exclude "__pycache__/*" `
    --exclude "*.pyo" `
    --exclude ".git/*"

if ($LASTEXITCODE -eq 0) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "$timestamp | SUCCESS | Uploaded to $S3_BUCKET"
    Add-Content -Path $LOG_FILE -Value $logEntry

    Write-Host ""
    Write-Host "Deploy successful!" -ForegroundColor Green
    Write-Host "Logged to $LOG_FILE" -ForegroundColor Gray
} else {
    Write-Host "Deploy FAILED — check AWS credentials and connectivity." -ForegroundColor Red
    exit 1
}
