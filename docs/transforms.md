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

Events are passed as JSON. See `examples/wasm-plugin/` for a complete Go/TinyGo example.

Build a plugin with TinyGo:

```bash
cd examples/wasm-plugin
tinygo build -o plugin.wasm -target=wasi -no-debug main.go
```

WASM plugins can be chained with built-in transforms in any order.
