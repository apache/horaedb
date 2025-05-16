#!/usr/bin/env python3
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

# coding: utf-8

import requests
import sys
import time

api_root = 'http://localhost:5440'


def get_heap_profile(seconds=20):
    # Get raw heap profile data from server
    start_time = time.time()
    response = requests.get(f"{api_root}/debug/pprof/heap/{seconds}")
    end_time = time.time()
    print(f"Pprof request took {end_time - start_time:.2f} seconds")

    if response.status_code != 200:
        print(f"Failed to get heap profile: {response.status_code}")
        sys.exit(1)
    return response.content


def save_profile(profile_data):
    # Save raw profile data to file
    with open("heap.pb.gz", "wb") as f:
        f.write(profile_data)


def main():
    print("Testing heap pprof endpoint...")
    raw_pprof = get_heap_profile()
    save_profile(raw_pprof)
    print("Raw profile data saved to: heap.pb.gz")


if __name__ == "__main__":
    main()
