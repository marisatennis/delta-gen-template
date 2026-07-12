# Library Tests

Pytest suite for `fabric_libs/` and the vendored `deltagen/`.

## Running

From the template root:

```bash
pytest datalake/libs/tests/ -v
```

The `conftest.py` puts `datalake/libs/` on `sys.path` so tests can import
`fabric_libs` and `deltagen` without installing them.

## What belongs here

- **Smoke tests** — small unit tests that exercise plugin registration, helper
  functions, and pure-Python utilities. No real Spark session.
- **Mock-based tests** — tests that mock `SparkSession` / `DataFrame` to verify
  orchestration logic, dimension/DQ plugin behaviour, etc.
- **Schema/config validators** — tests that load real YAML configs from
  `datalake/inputs/` and assert they parse cleanly with `YamlConfigProvider`.

## What does NOT belong here

- End-to-end tests that need a real lakehouse — those live in the platform
  notebooks themselves (run them in a dev workspace).
- Tests for the Delta-Gen engine internals — those live in the
  [delta-gen](https://github.com/marisatennis/delta-gen) repo's own test suite.
