# pydantic-durability

A replay conformance grid for [Pydantic AI Harness](https://pydantic.dev/docs/ai/harness/)
capabilities under durable execution: every capability, on Temporal, DBOS and Prefect, with its
hooks and then every tool it contributes.

> **A SAFE verdict counts only because the grid can be shown to say BROKEN.**

Two control capabilities write to a file after each model request, one in the hook body and one
through `@durable_operation`. Every engine column reads the first as BROKEN and the second as
SAFE before any other cell is reported.

- **Results and findings:** [FINDINGS.md](FINDINGS.md), full table in [results/GRID.md](results/GRID.md)
- **Recovery, demonstrated:** [suite/dbos_recovery.py](suite/dbos_recovery.py) runs a DBOS workflow,
  forks it after its last recorded step, and counts the writes that run again

## Running it

The suite runs against a local checkout of `pydantic/pydantic-ai` in `upstream/` with its dev
environment installed (`make install`, Temporal, DBOS and Prefect extras). Temporal starts a local
dev server; DBOS uses SQLite; Prefect uses `prefect_test_harness`. Nothing leaves the machine: the
audit refuses non-local connections, and integrations get fake tokens.

```bash
upstream/.venv/bin/python suite/run_grid.py            # Temporal, hooks
upstream/.venv/bin/python suite/run_grid.py --tools    # Temporal, every tool
upstream/.venv/bin/python suite/run_engines.py dbos [--tools]
upstream/.venv/bin/python suite/run_engines.py prefect [--tools]
python3 suite/report.py                                 # results/GRID.md
upstream/.venv/bin/python suite/dbos_recovery.py
```
