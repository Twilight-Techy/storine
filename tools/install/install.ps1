# Installs the Storine binaries (storage-server, storage-cli, storine-sql, storine-console)
# from a GitHub release on Windows 10 / Server 2016 or newer (x86_64; Windows on ARM runs them
# under emulation).
#
#   irm https://github.com/Twilight-Techy/storine/releases/latest/download/install.ps1 | iex
#
# Environment:
#   STORINE_VERSION      version to install, e.g. 0.2.0 (default: the latest release)
#   STORINE_INSTALL_DIR  where the binaries go (default: %LOCALAPPDATA%\Programs\Storine\bin),
#                        added to the user's PATH
#
# The archive is checked against the release's SHA256SUMS before anything is installed.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
# Windows PowerShell 5.1 does not offer TLS 1.2 by default.
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

$repo = 'Twilight-Techy/storine'
$base = "https://github.com/$repo/releases"

function Say($m) { Write-Host "storine: $m" }

$version = $env:STORINE_VERSION
if (-not $version) {
    $latest = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$repo/releases/latest" -Headers @{ 'User-Agent' = 'storine-installer' }
    $version = $latest.tag_name
}
$version = $version.TrimStart('v')
if (-not $version) { throw 'storine: could not find the latest version: set STORINE_VERSION' }

$dir = $env:STORINE_INSTALL_DIR
if (-not $dir) { $dir = Join-Path $env:LOCALAPPDATA 'Programs\Storine\bin' }

$name = "storine-$version-x86_64-windows"
$tmp = Join-Path ([IO.Path]::GetTempPath()) ("storine-" + [Guid]::NewGuid())
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
    Say "downloading Storine $version for x86_64 windows"
    $zip = Join-Path $tmp "$name.zip"
    $sums = Join-Path $tmp 'SHA256SUMS'
    Invoke-WebRequest -UseBasicParsing -Uri "$base/download/v$version/$name.zip" -OutFile $zip
    Invoke-WebRequest -UseBasicParsing -Uri "$base/download/v$version/SHA256SUMS" -OutFile $sums

    $want = $null
    foreach ($line in Get-Content $sums) {
        $parts = $line -split '\s+', 2
        if ($parts.Count -eq 2 -and $parts[1].TrimStart('*') -eq "$name.zip") { $want = $parts[0].ToLower() }
    }
    if (-not $want) { throw "storine: $name.zip is not listed in SHA256SUMS" }
    $got = (Get-FileHash -Algorithm SHA256 $zip).Hash.ToLower()
    if ($got -ne $want) { throw "storine: checksum mismatch for $name.zip (expected $want, got $got)" }

    Expand-Archive -Path $zip -DestinationPath $tmp -Force
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    $bins = Get-ChildItem (Join-Path $tmp "$name\bin") -Filter *.exe
    foreach ($b in $bins) { Copy-Item $b.FullName (Join-Path $dir $b.Name) -Force }
    Say ("installed " + (($bins | ForEach-Object Name) -join ' ') + " into $dir")

    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $entries = @($userPath -split ';' | Where-Object { $_ })
    if ($entries -notcontains $dir) {
        [Environment]::SetEnvironmentVariable('Path', (($entries + $dir) -join ';'), 'User')
        Say "added $dir to your user PATH: open a new terminal to use it"
    }
    if (($env:Path -split ';') -notcontains $dir) { $env:Path = "$env:Path;$dir" }
    Say 'start a server with: storage-server .\data 127.0.0.1:7878'
}
finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
