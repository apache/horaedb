// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Signal handler
//!
//! Only works on unix like environments

pub use self::details::wait_for_signal;

#[cfg(unix)]
mod details {
    use log::info;
    use signal_hook::{consts::TERM_SIGNALS, iterator::Signals};

    pub fn wait_for_signal() {
        let mut sigs = Signals::new(TERM_SIGNALS).expect("Failed to register signal handlers");

        for signal in &mut sigs {
            if TERM_SIGNALS.contains(&signal) {
                info!("Received signal {}, stopping server...", signal);
                break;
            }
        }
    }
}

#[cfg(not(unix))]
mod details {
    pub fn wait_for_signal() {}
}
