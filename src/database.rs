use crate::config::Config;
use crate::page;
use std::sync::RwLock;

#[derive(Debug)]
pub enum DatabaseError {
    Closed,
    InvalidPageId,
    InvalidPageSize,
    Io(std::io::Error),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::Closed => write!(f, "database is closed"),
            DatabaseError::InvalidPageId => write!(f, "invalid page ID"),
            DatabaseError::InvalidPageSize => write!(f, "invalid page size"),
            DatabaseError::Io(e) => write!(f, "io error: {}", e),
        }
    }
}

impl std::error::Error for DatabaseError {}

impl From<std::io::Error> for DatabaseError {
    fn from(e: std::io::Error) -> Self {
        DatabaseError::Io(e)
    }
}

pub struct Database {
    pager: RwLock<page::Pager>,
    closed: RwLock<bool>,
}

impl Database {
    pub fn new_with_config(path: &str, config: Option<&Config>) -> Result<Self, DatabaseError> {
        let page_size = config
            .and_then(|c| c.storage.as_ref())
            .and_then(|s| s.page_size)
            .unwrap_or(page::PAGE_SIZE);
        let pager = page::Pager::new(path, page_size).map_err(DatabaseError::Io)?;
        Ok(Database {
            pager: RwLock::new(pager),
            closed: RwLock::new(false),
        })
    }

    pub fn close(&self) -> Result<(), DatabaseError> {
        let mut closed = self.closed.write().unwrap();
        if *closed {
            return Err(DatabaseError::Closed);
        }
        *closed = true;
        Ok(())
    }

    pub fn get_page(&self, page_id: usize) -> Result<page::Page, DatabaseError> {
        if *self.closed.read().unwrap() {
            return Err(DatabaseError::Closed);
        }
        let pager = self.pager.read().unwrap();
        pager.get_page(page_id).map_err(DatabaseError::Io)
    }

    pub fn write_page(&self, page_id: usize, data: &[u8]) -> Result<(), DatabaseError> {
        if *self.closed.read().unwrap() {
            return Err(DatabaseError::Closed);
        }
        let mut pager = self.pager.write().unwrap();
        pager.write_page(page_id, data).map_err(DatabaseError::Io)
    }

    pub fn page_count(&self) -> usize {
        self.pager.read().unwrap().page_count()
    }

    pub fn page_size(&self) -> usize {
        self.pager.read().unwrap().page_size()
    }
}
