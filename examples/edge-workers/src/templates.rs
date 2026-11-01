//! Sources a worker can start from: the files in `templates/`, in the binaries.

pub struct Template {
    pub name: &'static str,
    pub description: &'static str,
    pub source: &'static str,
    /// The object bindings its source needs: the name in `env`, and the class it exports.
    pub objects: &'static [(&'static str, &'static str)],
}

pub const TEMPLATES: [Template; 6] = [
    Template {
        name: "hello",
        description: "One answer for every request",
        source: include_str!("../templates/hello.js"),
        objects: &[],
    },
    Template {
        name: "json-api",
        description: "A JSON API with routes by method and path",
        source: include_str!("../templates/json-api.js"),
        objects: &[],
    },
    Template {
        name: "redirect",
        description: "Redirects by path, and to the TARGET variable",
        source: include_str!("../templates/redirect.js"),
        objects: &[],
    },
    Template {
        name: "proxy",
        description: "Passes each request to the UPSTREAM variable",
        source: include_str!("../templates/proxy.js"),
        objects: &[],
    },
    Template {
        name: "feature-flag",
        description: "A feature turned on by the NEW_CHECKOUT variable, for all or for a percent",
        source: include_str!("../templates/feature-flag.js"),
        objects: &[],
    },
    Template {
        name: "counter",
        description: "A Durable Object: a counter per name, kept across runners (bind COUNTER=Counter)",
        source: include_str!("../templates/counter.js"),
        objects: &[("COUNTER", "Counter")],
    },
];

pub fn template(name: &str) -> Option<&'static Template> {
    TEMPLATES.iter().find(|template| template.name == name)
}
