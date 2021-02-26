/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */

use cassandra_cpp::Session as CassSession;
use cassandra_cpp::Error as CassError;
use cassandra_cpp::Consistency;

use crate::cassandra::*;

pub struct Session {
    metadata: CassSession,
    points: CassSession,
    dry_run: bool,
}

impl Session {
    pub fn new(metadata_contact: &str, points_contact: &str) -> Result<Self, CassError> {
        let metadata = connect(metadata_contact)?;
        let points = connect(points_contact)?;

        let session = Self {
            metadata: metadata,
            points: points,
            dry_run: false,
        };

        Ok(session)
    }

    pub fn set_dry_run(&mut self, dry_run: bool) {
        self.dry_run = dry_run
    }

    pub fn metadata_session(&self) -> &CassSession {
        &self.metadata
    }

    pub fn points_session(&self) -> &CassSession {
        &self.points
    }

    // XXX to make configurable
    pub fn read_consistency(&self) -> Consistency {
        Consistency::LOCAL_QUORUM
    }

    // XXX to make configurable
    pub fn write_consistency(&self) -> Consistency {
        Consistency::LOCAL_QUORUM
    }

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }
}
