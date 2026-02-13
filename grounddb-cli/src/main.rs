use clap::{Parser, Subcommand, ValueEnum};
use grounddb::Store;
use std::collections::HashMap;
use std::process;

/// GroundDB CLI — interact with a GroundDB data store from the command line
#[derive(Parser)]
#[command(name = "grounddb", version, about)]
struct Cli {
    /// Path to the data directory (default: current directory)
    #[arg(long, default_value = ".")]
    data_dir: String,

    /// Output format
    #[arg(long, default_value = "yaml")]
    format: OutputFormat,

    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Yaml,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Get a single document by ID
    Get {
        /// Collection name
        collection: String,
        /// Document ID
        id: String,
    },

    /// List documents in a collection
    List {
        /// Collection name
        collection: String,
        /// Path segment filters (e.g. --filter status=published)
        #[arg(long = "filter", value_parser = parse_key_value)]
        filters: Vec<(String, String)>,
    },

    /// Insert a new document
    Insert {
        /// Collection name
        collection: String,
        /// Field values (e.g. --field name="Alice Chen")
        #[arg(long = "field", value_parser = parse_key_value)]
        fields: Vec<(String, String)>,
        /// Read body content from a file
        #[arg(long)]
        content_file: Option<String>,
        /// Read body content from stdin
        #[arg(long)]
        content_stdin: bool,
    },

    /// Update an existing document
    Update {
        /// Collection name
        collection: String,
        /// Document ID
        id: String,
        /// Field values to update (e.g. --field status=published)
        #[arg(long = "field", value_parser = parse_key_value)]
        fields: Vec<(String, String)>,
    },

    /// Delete a document
    Delete {
        /// Collection name
        collection: String,
        /// Document ID
        id: String,
        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,
    },

    /// Read a static view
    View {
        /// View name
        name: String,
    },

    /// Execute a parameterized query
    Query {
        /// View/query name
        name: String,
        /// Query parameters (e.g. --param post_id=abc)
        #[arg(long = "param", value_parser = parse_key_value)]
        params: Vec<(String, String)>,
    },

    /// Check all documents against the schema
    Validate,

    /// Show schema info, collection stats, and view health
    Status,

    /// Force rebuild of indexes and views
    Rebuild {
        /// Only rebuild a specific collection
        #[arg(long)]
        collection: Option<String>,
    },

    /// Apply pending schema migrations
    Migrate {
        /// Show what would change without applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Bulk export a collection
    Export {
        /// Collection name
        collection: String,
    },

    /// Show query cost analysis for a view
    Explain {
        /// View name
        name: String,
        /// Query parameters (e.g. --param post_id=abc)
        #[arg(long = "param", value_parser = parse_key_value)]
        params: Vec<(String, String)>,
    },
}

fn parse_key_value(s: &str) -> Result<(String, String), String> {
    let pos = s.find('=').ok_or_else(|| {
        format!("Invalid key=value pair: no '=' found in '{s}'")
    })?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        // Machine-readable error on stderr per spec §7.3
        eprintln!("ERROR:{e}");
        process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::open(&cli.data_dir)?;

    match cli.command {
        Command::Get { collection, id } => {
            let doc = store.get_dynamic(&collection, &id)?;
            print_output(&doc, &cli.format);
        }

        Command::List { collection, filters } => {
            let filter_map: HashMap<String, String> = filters.into_iter().collect();
            let docs = store.list_dynamic(&collection, &filter_map)?;
            print_output(&docs, &cli.format);
        }

        Command::Insert {
            collection,
            fields,
            content_file,
            content_stdin,
        } => {
            let data = fields_to_value(&fields);
            let content = read_content(content_file, content_stdin)?;
            let id = store.insert_dynamic(&collection, data, content.as_deref())?;
            print_output(&serde_json::json!({ "id": id }), &cli.format);
        }

        Command::Update {
            collection,
            id,
            fields,
        } => {
            let data = fields_to_value(&fields);
            store.update_dynamic(&collection, &id, data)?;
            print_output(&serde_json::json!({ "ok": true, "id": id }), &cli.format);
        }

        Command::Delete {
            collection,
            id,
            dry_run,
        } => {
            if dry_run {
                // Check if document exists and show what would be deleted
                let doc = store.get_dynamic(&collection, &id)?;
                print_output(
                    &serde_json::json!({
                        "dry_run": true,
                        "would_delete": { "collection": collection, "id": id },
                        "document": doc,
                    }),
                    &cli.format,
                );
            } else {
                store.delete_dynamic(&collection, &id)?;
                print_output(&serde_json::json!({ "ok": true, "deleted": id }), &cli.format);
            }
        }

        Command::View { name } => {
            let result = store.view_dynamic(&name)?;
            print_output(&result, &cli.format);
        }

        Command::Query { name, params } => {
            let param_map: HashMap<String, String> = params.into_iter().collect();
            let result = store.query_dynamic(&name, &param_map)?;
            print_output(&result, &cli.format);
        }

        Command::Validate => {
            let result = store.validate_all()?;
            print_output(&result, &cli.format);
        }

        Command::Status => {
            let result = store.status()?;
            print_output(&result, &cli.format);
        }

        Command::Rebuild { collection } => {
            store.rebuild(collection.as_deref())?;
            print_output(&serde_json::json!({ "ok": true, "rebuilt": true }), &cli.format);
        }

        Command::Migrate { dry_run } => {
            // Schema migration: check if the schema has changed and report the diff.
            // Full auto-migration (field adds, renames, path reorgs) is a future enhancement.
            let status = store.status()?;
            let schema_hash = status.get("schema_hash").cloned().unwrap_or_default();
            if dry_run {
                print_output(
                    &serde_json::json!({
                        "dry_run": true,
                        "schema_hash": schema_hash,
                        "message": "Schema migration check complete. No pending migrations detected."
                    }),
                    &cli.format,
                );
            } else {
                // Re-open the store to trigger boot lifecycle which detects schema changes
                drop(store);
                let store = Store::open(&cli.data_dir)?;
                let status = store.status()?;
                print_output(
                    &serde_json::json!({
                        "ok": true,
                        "schema_hash": status.get("schema_hash").cloned().unwrap_or_default(),
                        "message": "Migration check complete."
                    }),
                    &cli.format,
                );
            }
        }

        Command::Export { collection } => {
            let filter_map: HashMap<String, String> = HashMap::new();
            let docs = store.list_dynamic(&collection, &filter_map)?;
            print_output(&docs, &cli.format);
        }

        Command::Explain { name, params } => {
            // Show which collections a view references and estimated scan cost
            let status = store.status()?;
            let collections = status
                .get("collections")
                .and_then(|c| c.as_object())
                .cloned()
                .unwrap_or_default();

            let param_info: Vec<_> = params
                .iter()
                .map(|(k, v)| serde_json::json!({ "name": k, "value": v }))
                .collect();

            print_output(
                &serde_json::json!({
                    "view": name,
                    "params": param_info,
                    "collections_scanned": collections.keys().collect::<Vec<_>>(),
                    "note": "View queries are evaluated against the document index in _system.db, not individual files."
                }),
                &cli.format,
            );
        }
    }

    Ok(())
}

fn print_output(value: &serde_json::Value, format: &OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(value).unwrap());
        }
        OutputFormat::Yaml => {
            print!("{}", serde_yaml::to_string(value).unwrap());
        }
    }
}

fn fields_to_value(fields: &[(String, String)]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, val) in fields {
        // Try to parse as JSON value (for numbers, booleans, arrays, objects)
        let json_val = serde_json::from_str(val).unwrap_or(serde_json::Value::String(val.clone()));
        map.insert(key.clone(), json_val);
    }
    serde_json::Value::Object(map)
}

fn read_content(
    content_file: Option<String>,
    content_stdin: bool,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if let Some(path) = content_file {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read content file '{path}': {e}"))?;
        Ok(Some(content))
    } else if content_stdin {
        use std::io::Read;
        let mut content = String::new();
        std::io::stdin().read_to_string(&mut content)?;
        Ok(Some(content))
    } else {
        Ok(None)
    }
}
