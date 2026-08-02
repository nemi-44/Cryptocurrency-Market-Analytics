param(
  [string]$StackName = "crypto-analytics",
  [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"

$outputs = aws cloudformation describe-stacks `
  --stack-name $StackName `
  --region $Region `
  --query "Stacks[0].Outputs" `
  --output json | ConvertFrom-Json

$apiEndpoint = ($outputs | Where-Object { $_.OutputKey -eq "ApiLatestUrl" }).OutputValue
$bucketName = ($outputs | Where-Object { $_.OutputKey -eq "DashboardBucketName" }).OutputValue

if (-not $apiEndpoint -or -not $bucketName) {
  throw "Could not find ApiLatestUrl or DashboardBucketName in stack outputs."
}

$buildDir = ".runtime\frontend-build"
New-Item -ItemType Directory -Force -Path $buildDir | Out-Null

$html = Get-Content -Raw "frontend\index.html"
$html = $html.Replace("API_URL_PLACEHOLDER", $apiEndpoint)
Set-Content -Path "$buildDir\index.html" -Value $html -Encoding UTF8

aws s3 cp "$buildDir\index.html" "s3://$bucketName/index.html" `
  --region $Region `
  --content-type "text/html" `
  --cache-control "no-store"

Write-Host "Uploaded static dashboard to s3://$bucketName/index.html"
Write-Host "Open the DashboardWebsiteUrl CloudFormation output."
