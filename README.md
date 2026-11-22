# api-factory-runtime

The runtime library of the services that **api-factory** generates: everything a generated service needs at run time,
nothing it needs at generation time. Generated code imports only this module (and its own generated packages); the
factory itself — the generator, the core algebra, the verification — is a separate, private project.

| Package | What it does |
|---|---|
| `server` | The HTTP/Connect server: health (`/healthz`, `X-App-Version`), metrics, authentication (`AUTH_MODE`), graceful shutdown, the database pool |
| `apierr` | Maps database and validation errors to Connect error codes |
| `paging` | Keyset page tokens: signed, bound to the request's filters |
| `shard` | Routes a request to one of several databases by a shard key (e.g. per store) |
| `clock` | The request's start time (`now()` in queries); in tests (`FACTORY_TEST_CLOCK=1`) the header `X-Factory-Now` fixes it |
| `conv` | Conversions between database and protobuf types (decimal, date, timestamp) |
| `metrics` | Request counts and latency histograms in the Prometheus text format (no dependency) |
| `anomaly` | Counts values read from the database that the domain layer does not know (`data_anomaly_total`) |
| `testkit` | Helpers for the generated services' tests |

Code comments are in Chinese (the factory's working language); identifiers and this README are in English.

Versioned with SemVer tags; a generated service pins the version it was generated against.

License: Apache-2.0.
