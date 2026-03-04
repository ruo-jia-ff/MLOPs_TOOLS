param(
    [string]$Remote = "origin",
    [string]$Branch = "main"
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$LocalPath = (Get-Location).Path
Write-Host "Working in: $LocalPath" -ForegroundColor Cyan

if (-not (Test-Path ".git")) {
    Write-Error "No .git folder found in '$LocalPath'. Aborting."
    exit 1
}

# -- 1. Get the remote URL
$remoteUrl = git remote get-url $Remote
Write-Host "Remote URL: $remoteUrl" -ForegroundColor Cyan

# -- 2. Clone remote into a temp folder
$tempDir = Join-Path $env:TEMP ("remote_clone_" + [System.IO.Path]::GetRandomFileName().Replace(".", ""))
Write-Host "Cloning remote into: $tempDir" -ForegroundColor Cyan
git clone --branch $Branch $remoteUrl $tempDir
if ($LASTEXITCODE -ne 0) {
    Write-Error "Clone failed. Aborting."
    exit 1
}

Set-Location $tempDir

# -- 3. Stage ALL remote file deletions (except .github)
Write-Host "Staging deletion of all remote files (keeping .github)..." -ForegroundColor Cyan
$trackedFiles = git ls-files
foreach ($file in $trackedFiles) {
    # Skip anything inside .github
    if ($file -notmatch "^\.github/") {
        git rm -f $file | Out-Null
    }
}

# Commit the deletions first
$status = git status --porcelain
if ($status) {
    git commit -m "Delete all remote files except .github"
    Write-Host "Deletion committed." -ForegroundColor Green
}

# -- 4. Copy local files into the clone (skip .git only)
Write-Host "Copying local files..." -ForegroundColor Cyan
Get-ChildItem -Path $LocalPath -Force | Where-Object {
    $_.Name -ne ".git"
} | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination (Join-Path $tempDir $_.Name) -Recurse -Force
}

# -- 5. Stage and commit the new local files
git add --all
$status = git status --porcelain
if ($status) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    git commit -m "Upload of local repo to remote [$timestamp]"
    Write-Host "Upload committed." -ForegroundColor Green
} else {
    Write-Host "Nothing new to commit." -ForegroundColor Yellow
}

# -- 6. Push both commits
git push $Remote $Branch --force
Write-Host "Push complete. If Dockerfile present, a task will be generated." -ForegroundColor Green

# -- 7. Clean up
Set-Location $LocalPath
Remove-Item $tempDir -Recurse -Force

Write-Host ""
Write-Host "Done! Remote '$Remote/$Branch' has been wiped and replaced with your local files." -ForegroundColor Green
Write-Host ""
