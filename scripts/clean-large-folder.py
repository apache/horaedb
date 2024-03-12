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

import shutil
import sys
from pathlib import Path


def clean_if_necessary(folder, threshold_size):
    root_dir = Path(folder)
    size = sum(f.stat().st_size for f in root_dir.glob(
        '**/*') if f.is_file())

    print(f"The size of {folder} is {size}, threshold is {threshold_size}")
    if size > threshold_size:
        print(f"Folder size exceeds the threshold, remove it")
        shutil.rmtree(folder)


GB = 1024 * 1024 * 1024

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 clean_large_folder.py check_folder threshold_size(GB)")
    check_folder = sys.argv[1]
    threshold_size = int(sys.argv[2]) * GB
    clean_if_necessary(check_folder, threshold_size)
