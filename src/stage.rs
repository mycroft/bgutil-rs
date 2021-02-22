/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::fmt;
use std::convert::TryFrom;
use std::string::String;

use regex::Regex;

#[derive(Copy,Clone,Debug)]
pub struct Stage {
    points: u32,
    precision: u32,
    factor: char,
}

impl TryFrom<&str> for Stage {
    type Error = &'static str;

    fn try_from(stage: &str) -> Result<Self, Self::Error> {
        let re = Regex::new(r"^(\d+)\*(\d+)(.)");

        if let Err(_) = re {
            return Err("regex initialisation failed");
        }

        let captures = match re.unwrap().captures(&stage) {
            None => return Err("invalid regex capture"),
            Some(c) => c,
        };

        let points = captures.get(1).unwrap().as_str().parse::<u32>().unwrap();

        let factor = captures.get(3).unwrap().as_str();
        if factor.len() != 1 {
            return Err("invalid factor length")
        }

        let factor = factor.chars().nth(0).unwrap();

        match factor {
            's' | 'm' | 'h' | 'd' | 'w' | 'y' => {},
            _ => {
                return Err("invalid precision unit")
            }
        };

        let precision = captures.get(2).unwrap().as_str().parse::<u32>().unwrap();

        Ok(Stage {
            points: points,
            precision: precision,
            factor: factor,
        })
    }
}

impl Stage {
    pub fn precision_as_seconds(self: &Self) -> i64 {
        let factor = match self.factor {
            's' => 1,
            'm' => 60,
            'h' => 60 * 60,
            'd' => 60 * 60 * 24,
            'w' => 60 * 60 * 24 * 7,
            'y' => 60 * 60 * 24 * 365,
            _ => unreachable!()
        };

        factor * self.precision as i64
    }

    pub fn to_string(self: &Self) -> String {
        format!("{}*{}{}", self.points, self.precision, self.factor)
    }

    pub fn time_offset_ms(self: &Self, ts: i64) -> (i64, i64) {
        let table_row_size_ms = self.table_row_size_ms();
        let time_offset_ms = ts * 1000 % table_row_size_ms;
        let time_start_ms = ts * 1000 - time_offset_ms;

        (
            time_start_ms,
            time_offset_ms / (self.precision_as_seconds() * 1000)
        )
    }

    pub fn table_name(self: &Self) -> String {
        // XXX aggregations?
        format!("datapoints_{}p_{}{}_0", self.points, self.precision, self.factor)
    }

    pub fn table_row_size_ms(self: &Self) -> i64 {
        let hour = 3600;
        let _max_partition_size = 25000;
        let _expected_points_per_read = 2000;
        let _min_partition_size_ms = 6 * hour;

        std::cmp::min(
            self.precision_as_seconds() * 1000 * _max_partition_size,
            std::cmp::max(
                self.precision_as_seconds() * 1000 * _expected_points_per_read,
                _min_partition_size_ms,
            )
        )
    }
}

impl PartialEq for Stage {
    fn eq(&self, other: &Self) -> bool {
        self.points == other.points
            && self.precision == other.precision
            && self.factor == other.factor
    }
}

impl Eq for Stage {}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
