use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// An event from the file watcher, ready for the Store to process.
#[derive(Debug, Clone)]
pub struct WatcherEvent {
    pub path: PathBuf,
    pub kind: ChangeKind,
}

/// A filesystem watcher that monitors collection directories for changes.
/// Debounced events are sent through an mpsc channel for the Store to process.
pub struct FileWatcher {
    _watcher: RecommendedWatcher,
    /// Handle to the background thread processing events
    _thread: std::thread::JoinHandle<()>,
    /// Receiver for debounced file change events
    pub event_rx: mpsc::Receiver<WatcherEvent>,
}

impl FileWatcher {
    /// Start watching the given directories for file changes.
    /// Debounced events (100ms) are available via `event_rx`.
    ///
    /// `root` is the data directory root.
    /// `dirs` are the collection base directories to watch (relative to root).
    pub fn start(
        root: &Path,
        dirs: &[PathBuf],
    ) -> Result<Self, notify::Error> {
        let (notify_tx, notify_rx) = mpsc::channel::<notify::Result<Event>>();
        let (event_tx, event_rx) = mpsc::channel::<WatcherEvent>();

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                let _ = notify_tx.send(res);
            },
            Config::default(),
        )?;

        // Watch each collection directory recursively
        for dir in dirs {
            let abs_dir = root.join(dir);
            if abs_dir.exists() {
                watcher.watch(&abs_dir, RecursiveMode::Recursive)?;
            }
        }

        // Background thread to process events with debouncing
        let thread = std::thread::spawn(move || {
            let debounce = Duration::from_millis(100);
            let mut pending: Vec<(PathBuf, ChangeKind)> = Vec::new();
            let mut last_event = Instant::now();

            loop {
                match notify_rx.recv_timeout(debounce) {
                    Ok(Ok(event)) => {
                        let kind = match event.kind {
                            EventKind::Create(_) => Some(ChangeKind::Created),
                            EventKind::Modify(_) => Some(ChangeKind::Modified),
                            EventKind::Remove(_) => Some(ChangeKind::Deleted),
                            _ => None,
                        };

                        if let Some(kind) = kind {
                            for path in event.paths {
                                // Only care about files with our supported extensions
                                if is_document_file(&path) {
                                    pending.push((path, kind));
                                }
                            }
                        }
                        last_event = Instant::now();
                    }
                    Ok(Err(e)) => {
                        log::warn!("File watcher error: {e}");
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Debounce: if enough time has passed since the last event, flush
                        if !pending.is_empty() && last_event.elapsed() >= debounce {
                            // Deduplicate paths (keep last change kind)
                            let mut seen = std::collections::HashMap::new();
                            for (path, kind) in pending.drain(..) {
                                seen.insert(path, kind);
                            }
                            for (path, kind) in seen {
                                if event_tx.send(WatcherEvent { path, kind }).is_err() {
                                    return; // Receiver dropped
                                }
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Watcher was dropped, exit the thread
                        break;
                    }
                }
            }
        });

        Ok(FileWatcher {
            _watcher: watcher,
            _thread: thread,
            event_rx,
        })
    }
}

/// The kind of file change detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    Created,
    Modified,
    Deleted,
}

/// Check if a path looks like a GroundDB document file.
fn is_document_file(path: &Path) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some("md") | Some("json") | Some("jsonl") => true,
        _ => false,
    }
}
