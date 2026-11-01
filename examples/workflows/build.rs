//! An api node embeds the built console, `ui/dist`. Without it the build stops here and says what to run.

fn main() {
    // include_dir! reads ui/dist at compile time without telling cargo, so a rebuilt console would not reach the binary.
    println!("cargo:rerun-if-changed=ui/dist");
    let dir = std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR for a build script");
    let index = std::path::Path::new(&dir).join("ui/dist/index.html");
    if !index.is_file() {
        eprintln!(
            "error: {} is missing.\nAn api node serves the console from it: run `npm install && npm run build` in {dir}/ui, then build again.",
            index.display()
        );
        std::process::exit(1);
    }
}
