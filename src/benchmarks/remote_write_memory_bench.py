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

import subprocess
import json
import sys
import os
from typing import Dict, Any
import argparse

try:
    from tabulate import tabulate
except ImportError:
    print("Error: tabulate library not found.")
    print("Please install it with: pip3 install tabulate")
    sys.exit(1)


class MemoryBenchmark:
    def __init__(self, scale, mode, use_unsafe=False):
        self.project_path = "."
        self.data_path = "../remote_write/tests/workloads/1709380533560664458.data"
        self.scale = scale
        self.mode = mode
        self.use_unsafe = use_unsafe
        self.parsers = [
            "pooled",
            "prost",
            "rust-protobuf",
            "quick-protobuf",
        ]

    def build_binary(self) -> bool:
        features_msg = " with unsafe-split" if self.use_unsafe else ""
        print(f"Building binary{features_msg}...")
        try:
            bin_name = "parser_mem"
            build_cmd = ["cargo", "build", "--release", "--bin", bin_name]
            if self.use_unsafe:
                build_cmd.extend(["--features", "unsafe-split"])
            result = subprocess.run(
                build_cmd,
                cwd=self.project_path,
                check=False,
            )
            if result.returncode != 0:
                print("Failed to build binary")
                return False
            return True
        except Exception as e:
            print(f"Failed to build binary: {e}")
            return False

    def run_parser(self, parser: str, mode: str, scale: int, bin_name: str) -> Dict[str, Any]:
        binary_path = f"../../target/release/{bin_name}"

        cmd = [binary_path, mode, str(scale), parser]

        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_path,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            if result.returncode != 0:
                print(f"Error running {parser}: {result.stderr}")
                return {}

            return json.loads(result.stdout.strip())

        except subprocess.TimeoutExpired:
            print(f"Timeout running {parser} {mode} {scale}")
            return {}
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON from {parser}: {e}")
            print(f"Raw output: {result.stdout}")
            return {}
        except FileNotFoundError:
            print(f"Binary not found: {binary_path}")
            print(f"Please run: cargo build --release --bins")
            return {}
        except Exception as e:
            print(f"Exception running {parser}: {e}")
            return {}

    def run_benchmarks(self) -> Dict[str, Dict[str, Any]]:
        results = {}
        successful_count = 0
        total_count = len(self.parsers)

        print(f"\nRunning benchmarks for {total_count} parsers...")

        for i, parser in enumerate(self.parsers, 1):
            print(f"\n[{i}/{total_count}] Testing {parser}...")
            print(f"Running {self.mode} mode with scale {self.scale}...")
            result = self.run_parser(
                parser, self.mode, self.scale, "parser_mem")

            if result:
                result["parser"] = parser
                results[parser] = result
                successful_count += 1
                print(f"Success")
            else:
                print(f"Failed")

        print(
            f"\nCompleted: {successful_count}/{total_count} parsers succeeded")

        if successful_count == total_count:
            print("All parsers succeeded - generating report...")
            return results
        else:
            print("Some parsers failed - skipping report generation")
            return {}

    def analyze_results(self, results: Dict[str, Dict[str, Any]]):
        if not results:
            print("\nNo results to analyze - all parsers failed or were skipped")
            return

        print(f"\n{'='*80}")
        print("MEMORY BENCHMARK RESULTS")
        print(f"{'='*80}")
        print(f"Mode: {self.mode.upper()}, Scale: {self.scale}")
        print()

        headers = [
            "Parser",
            "ThreadAlloc",
            "ThreadDealloc",
            "Allocated",
            "Active",
            "Metadata",
            "Mapped",
            "Resident",
            "Retained",
        ]

        table_data = []
        for parser in self.parsers:
            if parser in results:
                result = results[parser]
                memory = result.get("memory", {})
                row = [
                    parser,
                    f"{memory.get('thread_allocated_diff', 0):,}",
                    f"{memory.get('thread_deallocated_diff', 0):,}",
                    f"{memory.get('allocated', 0):,}",
                    f"{memory.get('active', 0):,}",
                    f"{memory.get('metadata', 0):,}",
                    f"{memory.get('mapped', 0):,}",
                    f"{memory.get('resident', 0):,}",
                    f"{memory.get('retained', 0):,}",
                ]
                table_data.append(row)

        print("SUMMARY TABLE (All values in bytes)")
        print(
            tabulate(
                table_data,
                headers=headers,
                tablefmt="grid",
                stralign="right",
                numalign="right",
            )
        )


def main():
    parser = argparse.ArgumentParser(
        description="Memory benchmark for protobuf parsers"
    )
    parser.add_argument(
        "--unsafe", action="store_true", help="Enable unsafe-split feature"
    )
    parser.add_argument(
        "--mode",
        choices=["sequential", "concurrent"],
        default="sequential",
        help="Test mode to run (default: sequential)",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=10,
        help="Scale value for benchmark (default: 10)",
    )

    args = parser.parse_args()

    if args.scale <= 0:
        print(f"Invalid scale value '{args.scale}', scale must be positive")
        sys.exit(1)

    data_path = "../remote_write/tests/workloads/1709380533560664458.data"
    if not os.path.exists(data_path):
        print(f"Test data file not found at {data_path}")
        print("Please ensure test data exists before running benchmarks")
        sys.exit(1)

    benchmark = MemoryBenchmark(
        scale=args.scale, mode=args.mode, use_unsafe=args.unsafe
    )

    if not benchmark.build_binary():
        sys.exit(1)

    print(f"\nRunning memory benchmarks...")
    print(f"Mode: {args.mode}")
    print(f"Scale: {args.scale}")
    print(f"Unsafe optimization: {'enabled' if args.unsafe else 'disabled'}")

    results = benchmark.run_benchmarks()
    benchmark.analyze_results(results)


if __name__ == "__main__":
    main()
