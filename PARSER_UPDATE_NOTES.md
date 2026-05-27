# Parser update notes

Applied update:

- `src/parse/sky.rs` now handles sky-condition layers in either broadcast order:
  - altitude before coverage, e.g. `2800 scattered 5500 broken`
  - coverage before altitude, e.g. `scattered 2800 broken 5500`
  - mixed forms, e.g. `2800 scattered ceiling 4900 broken 5500 overcast`

- It preserves strict altimeter behavior from the provided project:
  - four-digit altimeters such as `2995` or `3005` parse to `A2995`/`A3005`
  - three-digit candidates such as `295` are rejected and remain `N/A`
  - no padding, insertion, or guessing of missing altimeter digits is performed

Known validation note:

- I could not run `cargo test` in this environment because `cargo` is not installed in the container. Run `cargo test` and `cargo build --release` on your Rust build host after unpacking.
