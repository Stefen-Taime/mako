# Transforms

Write YAML, not code. Each transform is a pure function chained in sequence.

## Data Governance

```yaml
transforms:
  # SHA-256 hash with salt (PII compliance)
  - name: pii_hash
    type: hash_fields
    fields: [email, phone, ssn, credit_card_number]

  # Partial masking: john@email.com -> j***@email.com
  - name: mask_display
    type: mask_fields
    fields: [email_display, card_display]

  # Remove internal/debug fields
  - name: cleanup
    type: drop_fields
    fields: [internal_trace_id, debug_payload]
```

## Filtering & Enrichment

```yaml
transforms:
  # SQL WHERE-style filter (supports AND, OR, IS NULL, IS NOT NULL)
  - name: prod_only
    type: filter
    condition: "environment = production AND status != test"

  # Rename fields for warehouse convention
  - name: standardize
    type: rename_fields
    mapping:
      amt: amount
      ts: event_timestamp
      cust_id: customer_id

  # Deduplication by key
  - name: dedupe
    type: deduplicate
    fields: [event_id]
```

## WASM Plugins

Extend Mako with custom transforms written in any language that compiles to WebAssembly. Powered by [wazero](https://github.com/tetratelabs/wazero) (pure Go, zero CGO).

```yaml
transforms:
  - name: custom_enrich
    type: plugin
    config:
      path: ./plugins/enrich.wasm
      function: transform     # optional, default "transform"
```

Your WASM module must export three functions:

| Export | Signature | Description |
|--------|-----------|-------------|
| `alloc` | `(size: u32) -> ptr: u32` | Allocate memory for input |
| `dealloc` | `(ptr: u32, size: u32)` | Free memory (can be no-op) |
| `transform` | `(ptr: u32, len: u32) -> u64` | Process event, return `(result_ptr << 32 \| result_len)`. Return `0` to drop. |

Events are passed as JSON. WASM plugins can be chained with built-in transforms in any order.

### Supported languages

| Language | Status | Notes |
|---|---|---|
| **Rust** | Recommended | `cdylib` crate, no `_start` — works out of the box |
| **TinyGo** | Supported | Benign `_start`, works when TinyGo version is compatible |
| **Go standard** | Not supported | `_start` exits the module after `main()` returns — architectural limitation |

> **Note:** Mako skips `_start` when loading plugins (`WithStartFunctions()` in wazero). Plugins are libraries, not programs. Go standard modules require `_start` for runtime initialization, which then exits the module — making them incompatible.

### Rust example (recommended)

```bash
cd examples/wasm-plugin-rust
rustup target add wasm32-wasip1
cargo build --target wasm32-wasip1 --release
cp target/wasm32-wasip1/release/mako_plugin_rust.wasm plugin.wasm
```

```yaml
transforms:
  - name: rust_enrich
    type: plugin
    config:
      path: ./examples/wasm-plugin-rust/plugin.wasm
```

See `examples/wasm-plugin-rust/` for the full source (adds `_enriched: true` and `_plugin_lang: "rust"` to every event).

### TinyGo example

```bash
cd examples/wasm-plugin
tinygo build -o plugin.wasm -target=wasi -no-debug main.go
```

See `examples/wasm-plugin/` for the full source (adds `_enriched: true` and `_processed_at` timestamp).
