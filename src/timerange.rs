/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::fmt;

use crate::stage::Stage;

pub struct TimeRange {
    stage: Stage,
    time_start: i64,
    time_end: i64
}

impl TimeRange {
    pub fn new(stage: &Stage, time_start: i64, time_end: i64) -> Self {
        TimeRange {
            stage: stage.clone(),
            time_start: time_start,
            time_end: time_end,
        }
    }

    pub fn ranges(&self) -> Vec<(i64, i64, i64)> {
        let first_offset = self.stage.time_offset_ms(self.time_start);
        let last_offset = self.stage.time_offset_ms(self.time_end);

        let mut offset = first_offset.0;
        let mut offset_start = first_offset.1;

        let mut out = vec![];

        while offset != last_offset.0 {
            out.push((offset, offset_start, self.stage.table_row_size_ms()));

            offset_start = 0;
            offset += self.stage.table_row_size_ms();
        }

        out.push((offset, offset_start, last_offset.1));

        out
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({} -> {})", self.stage, self.time_start, self.time_end)
    }
}
