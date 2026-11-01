//! A web node embeds the built page, `ui/dist`. Without it the build stops here and says what to run.

fn main() {
    // include_dir! reads ui/dist at compile time without telling cargo, so a rebuilt page would not reach the binary.
    println!("cargo:rerun-if-changed=ui/dist");
    let dir = std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR for a build script");
    let index = std::path::Path::new(&dir).join("ui/dist/index.html");
    if !index.is_file() {
        eprintln!(
            "error: {} is missing.\nA web node serves the page from it: run `npm ci && npm run build` in {dir}/ui, then build again.",
            index.display()
        );
        std::process::exit(1);
    }
}
