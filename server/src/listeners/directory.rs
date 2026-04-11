use crate::{prelude::*, types::node_data::EventSource};
use std::{fs::File, io::Read, path::PathBuf};

// We want all of these functions to be synchronous just for ease of use since they are fast (for now)
// Asynchronous stuff can be done in the listen function (waiting for next file event)
pub(crate) trait DirectoryListener {
    // are we tracking a file right now
    fn is_reading(&self, event_source: EventSource) -> bool;
    // get file that we are tracking
    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File>;
    // when file is created what do we do?
    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()>;
    // how do we want to process data that we just processed?
    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()>;

    fn on_file_modification(&mut self, event_source: EventSource) -> Result<()> {
        let mut buf = String::new();
        let file = self.file_mut(event_source).as_mut().ok_or("No file being tracked")?;
        file.read_to_string(&mut buf)?;
        self.process_data(buf, event_source)?;
        Ok(())
    }
}
