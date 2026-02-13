// Path template engine - parsing, slugification, interpolation
// Will be implemented by rust-core-dev

pub struct PathTemplate {
    pub raw: String,
    pub segments: Vec<PathSegment>,
}

pub enum PathSegment {
    Literal(String),
    Field { name: String, format: Option<String> },
    NestedField { parent: String, child: String },
}

impl PathTemplate {
    pub fn parse(_template: &str) -> crate::Result<Self> {
        todo!("Path template parsing")
    }

    pub fn render(&self, _fields: &serde_yaml::Value) -> crate::Result<String> {
        todo!("Path template rendering")
    }
}

/// Slugify a string for use in file paths
pub fn slugify(input: &str) -> String {
    slug::slugify(input)
}
