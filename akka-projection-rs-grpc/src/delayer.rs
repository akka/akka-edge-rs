// Copied from Streambed, to avoid a dependency given that this is the
// only requirement. Original source: https://github.com/streambed/streambed-rs/blob/main/streambed/src/delayer.rs
//
// A utility for delaying with exponential backoff. Once retries have
// been attained then we start again. There are a hard set of values
// here as it is private within Streambed.

use std::time::Duration;

use exponential_backoff::Backoff;
use tokio::time;

const MIN_DELAY: Duration = Duration::from_millis(500);
const MAX_DELAY: Duration = Duration::from_secs(10);

pub struct Delayer {
    backoff: Backoff,
    retry_attempt: u32,
}

impl Delayer {
    pub async fn delay(&mut self) {
        let delay = if let Some(d) = self.backoff.next(self.retry_attempt) {
            d
        } else {
            MAX_DELAY
        };
        time::sleep(delay).await;
        self.retry_attempt = self.retry_attempt.wrapping_add(1);
    }
}

impl Default for Delayer {
    fn default() -> Self {
        let backoff = Backoff::new(8, MIN_DELAY, MAX_DELAY);
        Self {
            backoff,
            retry_attempt: 0,
        }
    }
}
