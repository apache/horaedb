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

import argparse
import subprocess
import sys

try:
    from tabulate import tabulate
except ImportError:
    print("Error: tabulate library not found.")
    print("Please install it with: pip3 install tabulate")
    sys.exit(1)

PARSERS = ["pooled", "prost", "rust-protobuf", "quick-protobuf"]
BINARY_PATH = "../../target/release/parser_mem"


def build_binary(use_unsafe: bool):
    print(f"Building binary{' with unsafe-split' if use_unsafe else ''}...")
    cmd = ["cargo", "build", "--release", "--bin", "parser_mem"]
    if use_unsafe:
        cmd.extend(["--features", "unsafe-split"])
    result = subprocess.run(cmd, cwd=".")
    return result.returncode == 0


def parse_total_alloc(stdout: str):
    for line in stdout.splitlines():
        if line.startswith("Total allocated bytes (cumulative):"):
            try:
                raw = line.split(":", 1)[1].strip().split(" ")[0]
                return int(raw)
            except Exception:
                return None
    return None


def run_parser(parser: str, mode: str, scale: int):
    cmd = [BINARY_PATH, mode, str(scale), parser]
    try:
        res = subprocess.run(
            cmd, cwd=".", capture_output=True, text=True, timeout=300)
    except Exception as e:
        print(f"[error] run {parser}: {e}", file=sys.stderr)
        return None
    if res.returncode != 0:
        print(f"[error] run {parser} failed: {res.stderr}", file=sys.stderr)
        return None
    total = parse_total_alloc(res.stdout)
    if total is None:
        print(
            f"[error] parse total bytes failed for {parser}. Output:\n{res.stdout}",
            file=sys.stderr,
        )
    return total


def print_table(results: dict, mode: str, scale: int):
    print(f"\n{'='*60}")
    print("MEMORY BENCHMARK RESULTS")
    print(f"{'='*60}")
    print(f"Mode: {mode}, Scale: {scale}")
    headers = ["Parser", "TotalAllocatedBytes"]
    rows = [[p, f"{results[p]:,}"] for p in PARSERS if p in results]
    print(
        tabulate(
            rows,
            headers=headers,
            tablefmt="grid",
            stralign="right",
            numalign="right",
        )
    )


def main():
    ap = argparse.ArgumentParser(
        description="Memory benchmark for protobuf parsers")
    ap.add_argument("--unsafe", action="store_true",
                    help="Enable unsafe-split feature")
    ap.add_argument(
        "--mode", choices=["sequential", "concurrent"], default="sequential", help="Run mode")
    ap.add_argument("--scale", type=int, default=10, help="Benchmark scale")
    args = ap.parse_args()
    if args.scale <= 0:
        print("scale must be positive", file=sys.stderr)
        return 1
    if not build_binary(args.unsafe):
        print("build failed", file=sys.stderr)
        return 1
    results = {}
    failed = False
    for parser in PARSERS:
        total = run_parser(parser, args.mode, args.scale)
        if total is None:
            failed = True
        else:
            results[parser] = total
    if results:
        print_table(results, args.mode, args.scale)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
