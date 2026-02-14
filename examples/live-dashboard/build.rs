fn main() {
    grounddb_codegen::generate_from_schema(
        "data/schema.yaml",
        &format!("{}/generated.rs", std::env::var("OUT_DIR").unwrap()),
    )
    .expect("Code generation failed");
}
