# =========================
# TCGCSV Archive Downloader + Extractor (Only New Downloads)
# =========================

# --- Settings ---
$start       = Get-Date "2024-02-08"
$end         = Get-Date
$archivesDir = "F:\Pokemon historical data archives"
$extractRoot = "F:\Pokemon historical data extracted"
$sevenZip    = "C:\Program Files\7-Zip\7z.exe"

# --- Prep folders ---
New-Item -ItemType Directory -Force -Path $archivesDir | Out-Null
New-Item -ItemType Directory -Force -Path $extractRoot | Out-Null

# --- Validate 7-Zip exists ---
if (-not (Test-Path $sevenZip)) {
    throw "7-Zip not found at: $sevenZip`nUpdate `$sevenZip to the correct path."
}

# Track only the archives we downloaded during THIS run
$newDownloads = New-Object System.Collections.Generic.List[string]

# =========================
# 1) Download archives day-by-day (record only new ones)
# =========================
for ($d = $start; $d -le $end; $d = $d.AddDays(1)) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    $url     = "https://tcgcsv.com/archive/tcgplayer/prices-$dateStr.ppmd.7z"
    $outFile = Join-Path $archivesDir "prices-$dateStr.ppmd.7z"

    if (Test-Path $outFile) {
        Write-Host "SKIP DOWNLOAD: $dateStr (already have archive)"
        continue
    }

    Write-Host "DOWNLOADING: $dateStr"
    & curl.exe -L --fail $url -o $outFile

    if ($LASTEXITCODE -ne 0) {
        if (Test-Path $outFile) { Remove-Item $outFile -Force }
        Write-Host "SKIP DOWNLOAD: $dateStr (missing or failed)"
        continue
    }

    $size = (Get-Item $outFile).Length
    Write-Host "OK DOWNLOAD: $dateStr ($size bytes)"
    $newDownloads.Add($outFile) | Out-Null
}

Write-Host ""
Write-Host "DOWNLOAD PHASE DONE"
Write-Host ""

# If nothing new was downloaded, don't waste time scanning/extracting everything
if ($newDownloads.Count -eq 0) {
    Write-Host "No new downloads this run. Nothing to extract."
    Write-Host "ALL DONE"
    return
}

# =========================
# 2) Extract ONLY the new downloads
# =========================
foreach ($outFile in $newDownloads) {
    $item = Get-Item $outFile -ErrorAction SilentlyContinue
    if (-not $item) { continue }

    $dest = Join-Path $extractRoot $item.BaseName

    # If extraction folder already exists AND contains files, skip
    if (Test-Path $dest) {
        $hasFiles = Get-ChildItem $dest -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($hasFiles) {
            Write-Host "SKIP EXTRACT (already extracted): $($item.Name)"
            continue
        }
    }

    Write-Host "EXTRACTING: $($item.Name)"
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    & $sevenZip x $item.FullName "-o$dest" -y | Out-Null
    Write-Host "DONE EXTRACT: $($item.Name)"
}

Write-Host ""
Write-Host "ALL DONE"