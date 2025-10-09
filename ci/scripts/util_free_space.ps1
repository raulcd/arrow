# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Show disk space before cleanup
Write-Host "::group::Disk space before cleanup"
Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
Write-Host "::endgroup::"

# Remove large applications via Windows Features/Programs
Write-Host "::group::Removing Windows applications"
try {
    # Remove browsers
    Write-Host "Removing Firefox..."
    Get-Package "*Firefox*" -ErrorAction SilentlyContinue | Uninstall-Package -Force -ErrorAction SilentlyContinue
    Write-Host "Disk space after removing Firefox"
    Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
    Write-Host "Removing Chrome..."
    Get-Package "*Chrome*" -ErrorAction SilentlyContinue | Uninstall-Package -Force -ErrorAction SilentlyContinue
    Write-Host "Disk space after removing Chrome"
    Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
    Write-Host "Removing Edge..."
    Get-Package "*Edge*" -ErrorAction SilentlyContinue | Uninstall-Package -Force -ErrorAction SilentlyContinue
    Write-Host "Disk space after removing Edge"
    Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
    # Remove other large applications
    Write-Host "Removing SQL Server..."
    Get-Package "*SQL Server*" -ErrorAction SilentlyContinue | Uninstall-Package -Force -ErrorAction SilentlyContinue
    Write-Host "Disk space after removing SQL Server"
    Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
    Write-Host "Removing Android SDK..."
    Get-Package "*Android SDK*" -ErrorAction SilentlyContinue | Uninstall-Package -Force -ErrorAction SilentlyContinue
    Write-Host "Disk space after removing Android SDK"
    Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
    Write-Host "Removing Visual Studio Installer..."
    Get-Package "*Visual Studio Installer*" -ErrorAction SilentlyContinue | Uninstall-Package -Force -ErrorAction SilentlyContinue
    Write-Host "Disk space after removing Visual Studio Installer"
    Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
} catch {
    Write-Host "Error removing packages: $_"
}
Write-Host "::endgroup::"

# Remove large directories
Write-Host "::group::Removing large directories"

# Android SDK (~15GB)
if (Test-Path "C:\Android") {
    Write-Host "Removing Android SDK..."
    Remove-Item -Path "C:\Android" -Recurse -Force -ErrorAction SilentlyContinue
}

# Visual Studio installations (~5-10GB)
if (Test-Path "C:\Program Files (x86)\Microsoft Visual Studio") {
    Write-Host "Removing old Visual Studio installations..."
    Remove-Item -Path "C:\Program Files (x86)\Microsoft Visual Studio\2017" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "C:\Program Files (x86)\Microsoft Visual Studio\2019" -Recurse -Force -ErrorAction SilentlyContinue
}

# .NET installations (~2-3GB)
if (Test-Path "C:\Program Files\dotnet\sdk") {
    Write-Host "Removing old .NET SDKs..."
    Get-ChildItem "C:\Program Files\dotnet\sdk" | Where-Object { $_.Name -lt "6.0" } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

# PowerShell modules (~1GB)
if (Test-Path "C:\Program Files\WindowsPowerShell\Modules") {
    Write-Host "Removing PowerShell modules..."
    Remove-Item -Path "C:\Program Files\WindowsPowerShell\Modules\Az*" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "C:\Program Files\WindowsPowerShell\Modules\SqlServer*" -Recurse -Force -ErrorAction SilentlyContinue
}

# Node.js and npm cache (~1-2GB)
if (Test-Path "C:\npm") {
    Write-Host "Removing npm cache..."
    Remove-Item -Path "C:\npm" -Recurse -Force -ErrorAction SilentlyContinue
}
if (Test-Path "C:\Users\runneradmin\AppData\Roaming\npm-cache") {
    Remove-Item -Path "C:\Users\runneradmin\AppData\Roaming\npm-cache" -Recurse -Force -ErrorAction SilentlyContinue
}

# Python installations (~500MB-1GB)
if (Test-Path "C:\hostedtoolcache\windows\Python") {
    Write-Host "Removing old Python versions..."
    Get-ChildItem "C:\hostedtoolcache\windows\Python" | Where-Object { $_.Name -lt "3.9" } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

# Go installations (~1GB)
if (Test-Path "C:\hostedtoolcache\windows\go") {
    Write-Host "Removing Go installations..."
    Remove-Item -Path "C:\hostedtoolcache\windows\go" -Recurse -Force -ErrorAction SilentlyContinue
}

# Java installations (~2GB)
if (Test-Path "C:\hostedtoolcache\windows\Java_Temurin-Hotspot_jdk") {
    Write-Host "Removing old Java versions..."
    Get-ChildItem "C:\hostedtoolcache\windows\Java_Temurin-Hotspot_jdk" | Where-Object { $_.Name -lt "11" } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

# Windows SDK (~2-3GB)
if (Test-Path "C:\Program Files (x86)\Windows Kits\10\bin") {
    Write-Host "Removing old Windows SDK versions..."
    Get-ChildItem "C:\Program Files (x86)\Windows Kits\10\bin" | Where-Object { $_.Name -lt "10.0.19041.0" } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Host "::endgroup::"

Write-Host "::group::Chocolatey packages"
& choco uninstall "R.Project" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "julia" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "imagemagick" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "php" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "apache-httpd" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "nginx" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "maven" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "Minikube" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "pulumi" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "packer" -y -x --remove-dependencies 2>$null | Out-Null
& choco uninstall "kubernetes-helm" -y -x --remove-dependencies 2>$null | Out-Null

try {
    if (Get-Command choco -ErrorAction SilentlyContinue) {
        Write-Host "Chocolatey packages installed:"
        $chocoPackages = & choco list --local-only --limit-output
        $chocoPackages | ForEach-Object {
            $parts = $_ -split '\|'
            if ($parts.Length -ge 2) {
                Write-Host "  $($parts[0]) v$($parts[1])"
            }
        }
        Write-Host "Total Chocolatey packages: $($chocoPackages)"
        Write-Host "Total Chocolatey packages: $($chocoPackages.Count)"

        # Check size of Chocolatey directory
        $chocoSize = Get-DirectorySize "C:\ProgramData\chocolatey"
        Write-Host "Chocolatey directory size: $chocoSize"
    } else {
        Write-Host "Chocolatey not installed"
    }
} catch {
    Write-Host "Error checking Chocolatey: $_"
}
Write-Host "::endgroup::"

# Clean Windows temporary files
Write-Host "::group::Cleaning Windows temporary files"
Remove-Item -Path "C:\Windows\Temp\*" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "C:\Users\runneradmin\AppData\Local\Temp\*" -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "::endgroup::"

# Show disk space after cleanup
Write-Host "::group::Disk space after cleanup"
Get-WmiObject -Class Win32_LogicalDisk | Select-Object DeviceID, @{Name="Size(GB)";Expression={[math]::Round($_.Size/1GB,2)}}, @{Name="FreeSpace(GB)";Expression={[math]::Round($_.FreeSpace/1GB,2)}}
Write-Host "::endgroup::"