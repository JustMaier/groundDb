# grounddb-codegen

[![Crates.io](https://img.shields.io/crates/v/grounddb-codegen.svg)](https://crates.io/crates/grounddb-codegen)
[![docs.rs](https://img.shields.io/docsrs/grounddb-codegen)](https://docs.rs/grounddb-codegen)

Compile-time code generation for [GroundDB](https://github.com/justmaier/groundDb).

Reads a `schema.yaml` and generates a complete Rust source file with:

- **Typed structs** for each collection (e.g., `User`, `Post`)
- **Enums** for fields with `enum` constraints (e.g., `UserRole`, `PostStatus`)
- **Partial-update structs** where all fields are `Option` (e.g., `UserPartial`)
- **View row structs** derived from SQL SELECT columns (e.g., `PostFeedRow`)
- **Query parameter structs** for parameterized views (e.g., `PostCommentsParams`)
- **`StoreExt` trait** with typed collection accessors and view methods

## Setup

Add to your `Cargo.toml`:

```toml
[build-dependencies]
grounddb-codegen = "1"
```

Create a `build.rs`:

```rust
fn main() {
    grounddb_codegen::generate_from_schema(
        "data/schema.yaml",
        "src/generated.rs",
    ).expect("codegen failed");
}
```

Include the generated module:

```rust
mod generated;
use generated::*;
```

## What Gets Generated

Given a schema with a `users` collection:

```yaml
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }
```

The codegen produces:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub email: String,
    #[serde(default)]
    pub role: UserRole,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserRole { Admin, Member, Guest }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserPartial {
    pub name: Option<String>,
    pub email: Option<String>,
    pub role: Option<UserRole>,
}
```

For full documentation, see the [repository README](https://github.com/justmaier/groundDb).
