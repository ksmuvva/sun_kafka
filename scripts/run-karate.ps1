<#
.SYNOPSIS
  Checks the prerequisites for the Karate_kafka proof of concept, runs the Karate tests and reports the result.

.DESCRIPTION
  1. Checks Java (JDK) 21, Maven 3.9+ and Git, and that Maven Central and packages.confluent.io can be reached.
  2. With -InstallMissing, offers to install what is missing (Java and Git with winget, Maven as a portable copy).
  3. Runs the Karate tests and says plainly whether they passed.
  4. Opens the HTML report.

  Exit code: 0 = tests passed, 1 = tests failed, 2 = a prerequisite is missing.

  Needs Windows PowerShell 5.1 or PowerShell 7. EXPERIMENTAL: written but not yet tested on Windows.

.PARAMETER Feature
  Run only one feature file, e.g. kafka-consumer, orders-e2e, or your own (my-first).

.PARAMETER Everything
  Run every test in the repository (Karate, Cucumber, JUnit) instead of only Karate.

.PARAMETER InstallMissing
  Offer to install missing tools. Asks before each install.

.PARAMETER Yes
  With -InstallMissing: do not ask, answer yes.

.PARAMETER CheckOnly
  Only check the prerequisites, do not run the tests.

.PARAMETER NoReport
  Do not open the HTML report afterwards.

.PARAMETER ShowLog
  Show the full Maven output (very long) instead of the short summary.

.EXAMPLE
  .\scripts\run-karate.ps1

.EXAMPLE
  .\scripts\run-karate.ps1 -Feature kafka-consumer

.EXAMPLE
  .\scripts\run-karate.ps1 -CheckOnly -InstallMissing
#>
[CmdletBinding()]
param(
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$Feature,
    [switch]$Everything,
    [switch]$InstallMissing,
    [switch]$Yes,
    [switch]$CheckOnly,
    [switch]$NoReport,
    [switch]$ShowLog
)

$ErrorActionPreference = 'Stop'

# Older Windows PowerShell does not use TLS 1.2 by default, which Maven Central and Apache require.
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
} catch { }

$RepoRoot = Split-Path -Parent $PSScriptRoot
$OnWindows = ($env:OS -eq 'Windows_NT')
$MavenVersion = '3.9.11'

# ---------- small output helpers ----------
function Write-Step([string]$Text) { Write-Host ''; Write-Host "== $Text" -ForegroundColor Cyan }
function Write-Ok([string]$Text)   { Write-Host "  [ OK ] $Text" -ForegroundColor Green }
function Write-Bad([string]$Text)  { Write-Host "  [FAIL] $Text" -ForegroundColor Red }
function Write-Note([string]$Text) { Write-Host "  [ !! ] $Text" -ForegroundColor Yellow }

# ---------- tools folder (portable Maven lives here, no admin rights needed) ----------
function Get-ToolsDir {
    $base = $env:LOCALAPPDATA
    if (-not $base) { $base = [IO.Path]::GetTempPath() }
    return (Join-Path $base 'Karate_kafka\tools')
}

# After an install the PATH of this window is stale. Re-read it, keeping anything we added ourselves.
function Update-SessionPath {
    if (-not $OnWindows) { return }
    $machine = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    $user = [Environment]::GetEnvironmentVariable('Path', 'User')
    $env:Path = "$machine;$user"
    Add-PortableTools
}

function Add-PortableTools {
    $dir = Get-ToolsDir
    if (-not (Test-Path $dir)) { return }
    $bin = Get-ChildItem -Path $dir -Directory -Filter 'apache-maven-*' -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending | Select-Object -First 1
    if ($bin) {
        $mavenBin = Join-Path $bin.FullName 'bin'
        if ($env:Path -notlike "*$mavenBin*") { $env:Path = "$mavenBin;$env:Path" }
    }
}

# ---------- run a tool and capture everything it prints (java -version prints to stderr) ----------
function Get-ToolOutput([string]$Name, [string[]]$ToolArgs) {
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $cmd) { return $null }
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $lines = & $cmd.Source @ToolArgs 2>&1 | ForEach-Object { "$_" }
        return ($lines -join "`n")
    } catch {
        return $null
    } finally {
        $ErrorActionPreference = $saved
    }
}

# ---------- the checks ----------
function Test-Java {
    $out = Get-ToolOutput 'java' @('-version')
    if (-not $out) { return [pscustomobject]@{ Ok = $false; Text = 'Java was not found on PATH' } }
    if ($out -match 'version "(\d+)(?:\.(\d+))?') {
        $major = [int]$Matches[1]
        if ($major -eq 1 -and $Matches[2]) { $major = [int]$Matches[2] }   # old style: 1.8 means Java 8
        if ($major -lt 21) { return [pscustomobject]@{ Ok = $false; Text = "Java $major found, but Java 21 or newer is needed" } }
        if ($major -gt 21) { return [pscustomobject]@{ Ok = $true; Text = "Java $major found (21 is the tested version)" } }
        return [pscustomobject]@{ Ok = $true; Text = "Java $major" }
    }
    return [pscustomobject]@{ Ok = $false; Text = 'Could not read the Java version' }
}

function Test-Maven {
    $out = Get-ToolOutput 'mvn' @('-version')
    if (-not $out) { return [pscustomobject]@{ Ok = $false; Text = 'Maven was not found on PATH' } }
    if ($out -match 'Apache Maven (\d+)\.(\d+)\.(\d+)') {
        $major = [int]$Matches[1]; $minor = [int]$Matches[2]
        $label = "Maven $($Matches[1]).$($Matches[2]).$($Matches[3])"
        if ($major -gt 3 -or ($major -eq 3 -and $minor -ge 9)) { return [pscustomobject]@{ Ok = $true; Text = $label } }
        return [pscustomobject]@{ Ok = $false; Text = "$label found, but Maven 3.9 or newer is needed" }
    }
    $first = ($out -split "`n")[0]
    return [pscustomobject]@{ Ok = $false; Text = "Maven did not start properly: $first" }
}

function Test-Git {
    $out = Get-ToolOutput 'git' @('--version')
    if ($out -and $out -match 'git version (\S+)') { return [pscustomobject]@{ Ok = $true; Text = "Git $($Matches[1])" } }
    return [pscustomobject]@{ Ok = $false; Text = 'Git was not found on PATH' }
}

function Test-Url([string]$Url) {
    try {
        $r = Invoke-WebRequest -Uri $Url -Method Head -UseBasicParsing -TimeoutSec 20
        return ($r.StatusCode -eq 200)
    } catch {
        return $false
    }
}

# ---------- installers (only used with -InstallMissing, each one asks first) ----------
function Confirm-Install([string]$Question) {
    if ($Yes) { return $true }
    $answer = Read-Host "  $Question (y/N)"
    return ($answer -match '^[Yy]')
}

function Install-WithWinget([string]$Id, [string]$Label, [string]$ManualHelp) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Note "winget is not available, so I cannot install $Label. $ManualHelp"
        return $false
    }
    if (-not (Confirm-Install "Install $Label with winget now?")) { return $false }
    & winget install --id $Id -e --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Note "winget could not install $Label (exit code $LASTEXITCODE). $ManualHelp"
        return $false
    }
    Update-SessionPath
    return $true
}

function Install-PortableMaven {
    $dir = Get-ToolsDir
    if (-not (Confirm-Install "Download Apache Maven $MavenVersion (about 9 MB) to $dir ? No admin rights needed.")) { return $false }
    $url = "https://archive.apache.org/dist/maven/maven-3/$MavenVersion/binaries/apache-maven-$MavenVersion-bin.zip"
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $zip = Join-Path $dir "apache-maven-$MavenVersion-bin.zip"
    try {
        Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
        $content = (Invoke-WebRequest -Uri "$url.sha512" -UseBasicParsing).Content
        if ($content -is [byte[]]) { $content = [Text.Encoding]::ASCII.GetString($content) }
        $expected = (($content.ToString().Trim()) -split '\s+')[0].ToLower()
        $actual = (Get-FileHash -Algorithm SHA512 -Path $zip).Hash.ToLower()
        if ($expected -ne $actual) { throw 'The downloaded Maven file does not match its published checksum.' }
        Expand-Archive -Path $zip -DestinationPath $dir -Force
    } catch {
        Write-Note "Could not install Maven: $($_.Exception.Message)"
        return $false
    } finally {
        if (Test-Path $zip) { Remove-Item $zip -Force -ErrorAction SilentlyContinue }
    }
    Add-PortableTools
    return $true
}

# ======================= main =======================
Write-Host 'Karate_kafka: Kafka producer/consumer integration tests (Karate proof of concept)'
Add-PortableTools

Write-Step '1. Checking prerequisites'

$java = Test-Java
if (-not $java.Ok -and $InstallMissing) {
    if (Install-WithWinget 'Microsoft.OpenJDK.21' 'Java JDK 21' 'Install JDK 21 yourself: https://learn.microsoft.com/java/openjdk/download') { $java = Test-Java }
}
if ($java.Ok) { Write-Ok $java.Text } else { Write-Bad $java.Text }

$maven = Test-Maven
if (-not $maven.Ok -and $InstallMissing) {
    if (Install-PortableMaven) { $maven = Test-Maven }
}
if ($maven.Ok) { Write-Ok $maven.Text } else { Write-Bad $maven.Text }

$git = Test-Git
if (-not $git.Ok -and $InstallMissing) {
    if (Install-WithWinget 'Git.Git' 'Git' 'Install Git yourself: https://git-scm.com/download/win') { $git = Test-Git }
}
if ($git.Ok) { Write-Ok $git.Text } else { Write-Note "$($git.Text). Only needed to clone the code or undo edits with git; the tests do not need it." }

$mavenCentral = Test-Url 'https://repo.maven.apache.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.pom'
$confluent = Test-Url 'https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.6.1/kafka-avro-serializer-7.6.1.pom'
if ($mavenCentral) { Write-Ok 'Maven Central is reachable' } else { Write-Note 'Maven Central (repo.maven.apache.org) is not reachable from here' }
if ($confluent) { Write-Ok 'packages.confluent.io is reachable' } else { Write-Note 'packages.confluent.io is not reachable from here' }
if (-not ($mavenCentral -and $confluent)) {
    Write-Note 'Dependencies are downloaded on the first run. If this is not your first run, Maven may already have them; otherwise ask your network team to allow both sites (or set a proxy in %USERPROFILE%\.m2\settings.xml).'
}

if (-not ($java.Ok -and $maven.Ok)) {
    Write-Host ''
    Write-Bad 'A required tool is missing, so the tests cannot run.'
    if (-not $InstallMissing) { Write-Host '  Run again with -InstallMissing to be offered an install, or install the tools listed above yourself.' }
    exit 2
}
if ($CheckOnly) {
    Write-Host ''
    Write-Ok 'All required tools are present.'
    exit 0
}

# ---------- run the tests ----------
if ($Everything -and $Feature) { Write-Bad 'Use either -Everything or -Feature, not both.'; exit 2 }
if (-not (Test-Path (Join-Path $RepoRoot 'pom.xml'))) {
    Write-Bad "pom.xml not found in $RepoRoot. Run this script from inside the Karate_kafka folder (scripts\run-karate.ps1)."
    exit 2
}

Write-Step '2. Running the tests (the first run downloads dependencies and can take several minutes)'
$mvnArgs = @('-B', 'test')
if (-not $Everything) { $mvnArgs += '-Dtest=KarateTest' }
if ($Feature) { $mvnArgs += "-Dkarate.options=classpath:karate/$Feature.feature" }
Write-Host "  mvn $($mvnArgs -join ' ')"

$mvnPath = (Get-Command mvn).Source
$logFile = Join-Path (Join-Path $RepoRoot 'target') 'karate-run.log'
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $logFile) | Out-Null
$keep = '^(scenarios:|features:|elapsed:)|Tests run:|BUILD (SUCCESS|FAILURE)|match failed|status code was'
$lineCount = 0
$saved = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
Push-Location $RepoRoot
try {
    & $mvnPath @mvnArgs 2>&1 |
        ForEach-Object { "$_" } |
        Tee-Object -FilePath $logFile |
        ForEach-Object {
            $lineCount++
            if ($ShowLog -or $_ -match $keep) { Write-Host $_ }
            elseif ($lineCount % 100 -eq 0) { Write-Host '.' -NoNewline }
        }
    $mavenExit = $LASTEXITCODE
} finally {
    Pop-Location
    $ErrorActionPreference = $saved
}
Write-Host ''

# ---------- read the result ----------
Write-Step '3. Result'
$log = Get-Content -Path $logFile -Raw
$passed = $false
if ($Everything) {
    $m = [regex]::Matches($log, 'Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)\s*\r?\n')
    if ($m.Count -gt 0) {
        $last = $m[$m.Count - 1]
        Write-Host "  Tests run: $($last.Groups[1].Value), failures: $($last.Groups[2].Value), errors: $($last.Groups[3].Value), skipped: $($last.Groups[4].Value)  (tests that need Docker skip themselves)"
    }
} else {
    $m = [regex]::Matches($log, 'scenarios:\s+(\d+)\s+\|\s+passed:\s+(\d+)\s+\|\s+failed:\s+(\d+)\s*\r?\n')
    if ($m.Count -gt 0) {
        $last = $m[$m.Count - 1]
        Write-Host "  Karate scenarios: $($last.Groups[1].Value), passed: $($last.Groups[2].Value), failed: $($last.Groups[3].Value)"
    }
}
$passed = ($mavenExit -eq 0)

if ($passed) {
    Write-Ok 'PASS: all selected tests passed.'
} else {
    Write-Bad 'FAIL: some tests failed, or the build could not run.'
    $why = Select-String -Path $logFile -Pattern 'match failed|status code was|COMPILATION ERROR|Could not resolve|Could not transfer' -Context 0, 2 | Select-Object -First 3
    foreach ($hit in $why) { Write-Host "    $($hit.Line.Trim())" }
    Write-Host "  Full output: $logFile"
}

$report = Join-Path $RepoRoot 'target\karate-reports\karate-summary.html'
if (Test-Path $report) {
    Write-Host "  Report: $report"
    if ($OnWindows -and -not $NoReport) { Start-Process $report }
}
if ($passed) { exit 0 } else { exit 1 }
