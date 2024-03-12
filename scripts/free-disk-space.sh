#!/usr/bin/env bash
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



# Copy from https://github.com/apache/flink/blob/release-1.18.1/tools/azure-pipelines/free_disk_space.sh
echo "=============================================================================="
echo "Freeing up disk space on CI system"
echo "=============================================================================="

echo "Listing 100 largest packages"
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -n | tail -n 100
df -h
echo "Removing large packages"
sudo apt-get remove -y '^ghc-8.*'
sudo apt-get remove -y '^dotnet-.*'
sudo apt-get remove -y '^llvm-.*'
sudo apt-get remove -y 'php.*'
sudo apt-get remove -y azure-cli google-cloud-sdk hhvm google-chrome-stable firefox powershell mono-devel
sudo apt-get autoremove -y
sudo apt-get clean
df -h

echo "Removing large directories"
rm -rf /usr/local/lib/android
rm -rf /usr/share/dotnet
rm -rf /usr/local/graalvm/
rm -rf /usr/local/.ghcup/
rm -rf /usr/local/share/powershell
rm -rf /usr/local/share/chromium
df -h
