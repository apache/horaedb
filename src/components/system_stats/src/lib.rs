// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Helps to collect and report statistics about the system.

use std::{sync::Mutex, time::Duration};

pub use sysinfo::LoadAvg;
use sysinfo::{Cpu, CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

/// The stats about the system.
#[derive(Debug)]
pub struct SystemStats {
    /// The usage's range is [0, 1.0]
    pub cpu_usage: f32,
    /// The memory is counted in byte.
    pub used_memory: u64,
    /// The memory is counted in byte.
    pub total_memory: u64,
    pub load_avg: LoadAvg,
}

/// Collect the stats of the system for reporting.
///
/// One background thread will be spawned to run stats collection.
pub struct SystemStatsCollector {
    total_memory: u64,
    system: Mutex<System>,
}

pub type ErrorMessage = String;

impl SystemStatsCollector {
    /// Create an new collector for the system stats.
    pub fn try_new() -> Result<Self, ErrorMessage> {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return Err("Unsupported system to collect system metrics".to_string());
        }

        let system = System::new_with_specifics(Self::make_mem_refresh_kind());
        Ok(Self {
            total_memory: system.total_memory(),
            system: Mutex::new(system),
        })
    }

    /// Collect the system stats for `observe_dur`.
    ///
    /// The [`sysinfo::MINIMUM_CPU_UPDATE_INTERVAL`] will be used if
    /// `observe_dur` is smaller.
    pub async fn collect_and_report(&self, observe_dur: Duration) -> SystemStats {
        {
            let mut system = self.system.lock().unwrap();
            system.refresh_specifics(Self::make_cpu_refresh_kind());
        }

        let wait_dur = sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.max(observe_dur);
        tokio::time::sleep(wait_dur).await;

        let mut system = self.system.lock().unwrap();
        system.refresh_specifics(Self::make_cpu_and_mem_refresh_kind());

        SystemStats {
            cpu_usage: self.compute_cpu_usage(system.cpus()),
            used_memory: system.used_memory(),
            total_memory: self.total_memory,
            load_avg: System::load_average(),
        }
    }

    // Refresh and compute the latest cpu usage.
    fn compute_cpu_usage(&self, cpus: &[Cpu]) -> f32 {
        let mut num_cpus = 0;
        let mut total_cpu_usage = 0.0;
        let valid_cpus = cpus.iter().filter(|v| !v.cpu_usage().is_nan());
        for cpu in valid_cpus {
            total_cpu_usage += cpu.cpu_usage();
            num_cpus += 1;
        }

        if num_cpus != 0 {
            total_cpu_usage / (num_cpus as f32) / 100.0
        } else {
            0f32
        }
    }

    #[inline]
    fn make_mem_refresh_kind() -> RefreshKind {
        let mem_refresh_kind = MemoryRefreshKind::new().with_ram();
        RefreshKind::new().with_memory(mem_refresh_kind)
    }

    #[inline]
    fn make_cpu_refresh_kind() -> RefreshKind {
        let cpu_refresh_kind = CpuRefreshKind::new().with_cpu_usage();
        RefreshKind::new().with_cpu(cpu_refresh_kind)
    }

    #[inline]
    fn make_cpu_and_mem_refresh_kind() -> RefreshKind {
        let cpu_refresh_kind = CpuRefreshKind::new().with_cpu_usage();
        let mem_refresh_kind = MemoryRefreshKind::new().with_ram();
        RefreshKind::new()
            .with_cpu(cpu_refresh_kind)
            .with_memory(mem_refresh_kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_system_stats(stats: &SystemStats) {
        assert!(stats.total_memory > 0);
        assert!(stats.used_memory > 0);
        assert!(stats.used_memory < stats.total_memory);
        assert!(stats.cpu_usage >= 0.0);
        assert!(stats.load_avg.one >= 0.0);
        assert!(stats.load_avg.five >= 0.0);
        assert!(stats.load_avg.fifteen >= 0.0);
    }

    #[tokio::test]
    async fn test_normal_case() {
        let collector = SystemStatsCollector::try_new().unwrap();
        let stats = collector
            .collect_and_report(Duration::from_millis(500))
            .await;
        check_system_stats(&stats);

        let mut all_cpu_usages = Vec::with_capacity(5);
        for _ in 0..5 {
            let new_stats = collector
                .collect_and_report(Duration::from_millis(200))
                .await;
            check_system_stats(&new_stats);
            all_cpu_usages.push(new_stats.cpu_usage);
        }

        // Ensure the stats will be updated for every collection.
        assert!(all_cpu_usages.into_iter().any(|v| v != stats.cpu_usage));
    }
}
