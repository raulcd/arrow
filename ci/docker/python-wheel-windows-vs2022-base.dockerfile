# escape=`

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


# NOTE: To build this Dockerfile, you probably need to do the following two
# things:
#
# 1. Increase your container image size to a higher value.
#
#  e.g.,
#
#    Set a custom 'storage-opts' value in your Windows Docker config and restart
#    Docker:
#
#        "storage-opts": [
#             "size=50GB"
#        ]
#
#    See
#
#       https://learn.microsoft.com/en-us/virtualization/windowscontainers/manage-containers/container-storage#example
#
#    for details on this step and
#
#       https://learn.microsoft.com/en-us/visualstudio/install/build-tools-container?view=vs-2022#troubleshoot-build-tools-containers
#
#    for more information.
#
# 2. Increase the memory limit for the build container to at least 4GB.
#
#  e.g.,
#
#     docker build -t sometag -m 4GB --file `
#       .\ci\docker\python-wheel-windows-vs2022-base.dockerfile .

# NOTE: You must update PYTHON_WHEEL_WINDOWS_IMAGE_REVISION in .env
# when you update this file.

FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Ensure we in a command shell and not Powershell
SHELL ["cmd", "/S", "/C"]

# Install MSVC BuildTools
#
# The set of components below (lines starting with --add) is the most minimal
# set we could find that would still compile Arrow C++.
RUN `
  curl -SL --output vs_buildtools.exe https://aka.ms/vs/17/release/vs_buildtools.exe `
  && (start /w vs_buildtools.exe --quiet --wait --norestart --nocache `
  --installPath "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\BuildTools" `
  --add Microsoft.VisualStudio.Component.VC.CoreBuildTools `
  --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
  --add Microsoft.VisualStudio.Component.Windows11SDK.26100 `
  --add Microsoft.VisualStudio.Component.VC.CMake.Project `
  --add Microsoft.VisualStudio.Component.Vcpkg `
  || IF "%ERRORLEVEL%"=="3010" EXIT 0) `
  && del /q vs_buildtools.exe
