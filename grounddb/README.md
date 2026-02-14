# grounddb

[![Crates.io](https://img.shields.io/crates/v/grounddb.svg)](https://crates.io/crates/grounddb)
[![docs.rs](https://img.shields.io/docsrs/grounddb)](https://docs.rs/grounddb)

Core library for [GroundDB](https://github.com/justmaier/groundDb) — a schema-driven data layer that uses Markdown files as the source of truth.

This crate provides schema parsing, document storage, path templates, validation, SQL views, and the `Store` API. Pair it with [`grounddb-codegen`](https://crates.io/crates/grounddb-codegen) for compile-time typed Rust structs.

## Usage

```rust
use grounddb::Store;

let store = Store::open("data")?;
let docs = store.collection("users").list()?;
```

For full documentation, examples, and the schema format, see the [repository README](https://github.com/justmaier/groundDb).
