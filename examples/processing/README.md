# Processing E2E Example

End-to-end tests for Flo's stream processing engine, exercising the full
lifecycle through the Go SDK.

## Pipelines

| File | What it tests |
|------|--------------|
| `passthrough.yaml` | Stream → stream, no operators |
| `filter-aggregate.yaml` | `filter` + `keyby` + `aggregate` (sum) |
| `map-projection.yaml` | `map` field projection with constants |
| `flatmap-explode.yaml` | `flatmap` array explosion |
| `classify-routing.yaml` | `classify` + tag-based multi-sink routing |
| `kv-enrichment.yaml` | `kv_lookup` in enrich mode |

## Running

### Against a local Flo server

```bash
# Terminal 1 — start Flo
flo server start --port 4453

# Terminal 2 — run tests
cd sdks/go
FLO_ENDPOINT=localhost:4453 go test -v -count=1 ./examples/processing/
```

### With testcontainers (requires Docker)

Build the Flo image first:

```bash
docker build -t flo:latest ./flo
```

Then run tests — the container starts automatically:

```bash
cd sdks/go
go test -v -count=1 ./examples/processing/
```

Override the image with `FLO_IMAGE`:

```bash
FLO_IMAGE=ghcr.io/floruntime/flo:nightly go test -v -count=1 ./examples/processing/
```

## Test Coverage

| Test | Covers |
|------|--------|
| `TestPassthroughPipeline` | Submit, stream source/sink, record flow |
| `TestFilterAggregatePipeline` | Filter expressions, keyby, aggregate sum |
| `TestMapProjectionPipeline` | JSONPath extraction, constant injection |
| `TestFlatMapPipeline` | Array explosion, element key extraction |
| `TestClassifyRoutingPipeline` | Tag-based classify, multi-sink AND matching |
| `TestKVEnrichmentPipeline` | KV seeding, kv_lookup enrich mode |
| `TestJobLifecycle` | Status, List, Savepoint, Stop, Restore, Rescale, Cancel |
| `TestDeclarativeSync` | SyncBytes, SyncDir |
| `TestMultiplePipelines` | Concurrent pipeline submission and listing |
