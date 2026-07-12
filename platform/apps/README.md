# Apps Layer

Power Apps and Power Automate flows that read from / write to the platform.

## Intended contents

Per-app subdirectory containing:
- `<app-name>.Solution/` — Power Apps solution exports
- `<app-name>.PowerAutomate/` — flow definitions
- `README.md` — what the app does, what tables it reads/writes, deployment notes

## Conventions

- Apps should write user-edited data (e.g., business overrides, manual mappings) to a `control.*` or `mapping.*` Delta table in the silver lakehouse, NOT directly to gold.
- Use a service principal with scoped access — not a personal account — for production deployments.

> Note: Only populate this directory if a project actually uses Power Apps / Automate.
> It is fine to leave empty.
