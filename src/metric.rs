/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use crate::Stage;

use std::collections::HashMap;
use std::fmt;
use std::convert::TryFrom;

use cassandra_cpp::Row;
use chrono::Utc;

#[derive(Debug)]
pub struct Metric {
    id: String,
    name: String,
    config: HashMap<String, String>,
    created_on: u64,
    updated_on: u64
}

impl Metric {
    pub fn id(self: &Self) -> &String {
        &self.id
    }

    pub fn name(self: &Self) -> &String {
        &self.name
    }

    pub fn updated_on(self: &Self) -> u64 {
        self.updated_on
    }

    pub fn config(self: &Self, name: String) -> Result<String, String> {
        let res = self.config.get(&name);
        if let Some(v) = res {
            Ok(v.to_string())
        } else {
            Err("Invalid key".to_string())
        }
    }

    pub fn stages(self: &Self) -> Result<Vec<Stage>, String> {
        let mut out = vec![];
        let stages = self.config("retention".to_string());

        if let Err(err) = stages {
            return Err(err);
        }

        for stage in stages.unwrap().split(":") {
            match Stage::try_from(stage) {
                Ok(stage) => out.push(stage),
                Err(err) => {
                    return Err(err.to_string())
                }
            };
        }

        Ok(out)
    }
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?}",
            self.name,
            self.config,
        )
    }
}

impl From<String> for Metric {
    fn from(name: String) -> Self {
        Metric {
            id: String::from(""),
            name: name,
            config: HashMap::new(),
            created_on: 0,
            updated_on: 0
        }
    }
}

impl From<Row> for Metric {
    fn from(row: Row) -> Self {
        let mut config : HashMap<String, String> = HashMap::new();
        match row.get_column_by_name("config".to_string()).unwrap().get_map() {
            Ok(v) => {
                v.map(|(k, v)| config.insert(k.to_string(), v.to_string()))
                 .count();
            },
            Err(_) => {},
        };

        let created_on = row.get_column_by_name("created_on".to_string());
        let created_on_timestamp = if let Ok(creation_time) = created_on {
            match creation_time.get_uuid() {
                Err(_) => 0,
                Ok(v) => v.timestamp(),
            }
        } else {
            Utc::now().timestamp() as u64
        };

        let updated_on = row.get_column_by_name("updated_on".to_string());
        let updated_on_timestamp = if let Ok(updated_time) = updated_on {
            match updated_time.get_uuid() {
                Err(_) => 0,
                Ok(v) => v.timestamp(),
            }
        } else {
            Utc::now().timestamp() as u64
        };

        let uuid = match row.get_column_by_name("id".to_string()).unwrap().get_uuid() {
            Ok(v) => v.to_string(),
            Err(_) => String::from(""),
        };

        Self {
            id: uuid,
            name: row.get_column_by_name("name".to_string()).unwrap().to_string(),
            config: config,
            created_on: created_on_timestamp,
            updated_on: updated_on_timestamp
        }
    }
}
