use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 8 * 1024; // 8KB

/// A single page of data in the database.
#[derive(Debug, Clone)]
pub struct Page {
    pub data: Vec<u8>,
}

/// Pager manages reading and writing pages to disk.
pub struct Pager {
    file: Mutex<File>,
    pub page_size: usize,
    pub page_count: usize,
}

impl Pager {
    pub fn new(path: &str, page_size: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let metadata = file.metadata()?;
        let page_count = (metadata.len() as usize) / page_size;
        Ok(Pager {
            file: Mutex::new(file),
            page_size,
            page_count,
        })
    }

    pub fn get_page(&self, page_num: usize) -> std::io::Result<Page> {
        let mut file = self.file.lock().unwrap();
        let mut data = vec![0u8; self.page_size];
        let offset = (page_num * self.page_size) as u64;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut data)?;
        Ok(Page { data })
    }

    pub fn write_page(&mut self, page_num: usize, data: &[u8]) -> std::io::Result<()> {
        if data.len() != self.page_size {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid page size"));
        }
        let mut file = self.file.lock().unwrap();
        let offset = (page_num * self.page_size) as u64;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        if page_num >= self.page_count {
            self.page_count = page_num + 1;
        }
        Ok(())
    }

    pub fn page_count(&self) -> usize {
        self.page_count
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }
} 
