param($installPath, $toolsPath, $package, $project) 

$ppmLibDir = Join-Path $installPath "lib\\net45"
$targetsFile = Join-Path $installPath "build\\Microsoft.Research.Peloponnese.targets"
Write-Host "Package installed in " + $ppmLibDir + ".  Checking targets file " + $targetsFile 

(Get-Content $targetsFile) | Foreach-Object { $_ -replace 'PELOPONNESE_PACKAGE_DIRECTORY', $installPath } | Set-Content $targetsFile
