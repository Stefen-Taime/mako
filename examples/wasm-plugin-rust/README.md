# Mako WASM Plugin — Rust Example

Rust implementation of the Mako plugin ABI. Adds `_enriched: true` and `_plugin_lang: "rust"` to every event.

## Prerequisites

```bash
rustup target add wasm32-wasip1
```

## Build

```bash
cargo build --target wasm32-wasip1 --release
cp target/wasm32-wasip1/release/mako_plugin_rust.wasm plugin.wasm
```

## Usage in pipeline.yaml

```yaml
transforms:
  - name: rust_enrich
    type: plugin
    config:
      path: ./examples/wasm-plugin-rust/plugin.wasm
```

## ABI

The WASM module exports three functions expected by the Mako runtime:

| Export | Signature | Description |
|---|---|---|
| `alloc` | `(size: u32) -> u32` | Allocate `size` bytes, return pointer |
| `dealloc` | `(ptr: u32, size: u32)` | Free memory (no-op here) |
| `transform` | `(ptr: u32, len: u32) -> u64` | Transform JSON event, return `(result_ptr << 32) \| result_len` or `0` to drop |
