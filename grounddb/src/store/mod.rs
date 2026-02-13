// Store and Collection - the main API surface
// Will be implemented by rust-core-dev

/// The main entry point for GroundDB
pub struct Store;

impl Store {
    pub fn open(_path: &str) -> crate::Result<Self> {
        todo!("Store::open")
    }
}
