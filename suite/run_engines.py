"""Run every grid case on DBOS or Prefect and write `results/grid.<engine>.json`.

    upstream/.venv/bin/python suite/run_engines.py dbos|prefect [CaseName ...]

Neither engine sandboxes orchestration code, so the check is the audit alone: a socket,
subprocess or file-write event inside a DBOS workflow outside a step, or inside a Prefect
flow outside a task, runs again when the engine recovers the run. The durable output must
also equal the same agent's output without durability.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import run_grid  # noqa: E402  (sets the offline tokens before any capability is built)
import grid  # noqa: E402
import grid_selection  # noqa: E402

RESULTS = Path(__file__).parent.parent / 'results'


async def run_case(engine: str, name: str, orchestrate: Any) -> dict[str, object]:
    grid_selection.CURRENT = name
    b = grid.built(name, rebuild=True)
    if b.agent is None:
        v, reason = run_grid.construct_verdict(str(b.error))
        return {'case': name, 'module': b.case.module, 'mode': engine, 'verdict': v, 'reason': reason, 'events': []}
    grid.AUDIT.events.clear()
    grid.AUDIT.blocked.clear()
    grid.AUDIT.active = True
    started = time.monotonic()
    error = output = None
    try:
        reference, reference_error = await run_grid.plain_output(b)
        try:
            output = await orchestrate(name)
        except Exception as exc:
            error = grid.format_exception(exc)
    finally:
        grid.AUDIT.active = False
    events = [asdict(e) for e in grid.AUDIT.events]
    v, reason = run_grid.verdict(error, events, output, reference, grid.AUDIT.blocked)
    if v == 'BROKEN' and reference_error is not None and not events:
        v, reason = 'NOT_RUN', f'plain run also fails: {reference_error}'
    return {
        'case': name,
        'module': b.case.module,
        'mode': engine,
        'verdict': v,
        'reason': reason,
        'output': output,
        'reference': reference,
        'error': error,
        'events': events[:20],
        'event_count': len(events),
        'blocked_hosts': sorted(set(grid.AUDIT.blocked)),
        'workspace': grid_selection.WORKSPACE,
        'tools_called': list(run_grid.LAST_TOOLS),
        'seconds': round(time.monotonic() - started, 1),
    }


async def sweep(engine: str, names: list[str], orchestrate: Any) -> list[dict[str, object]]:
    rows = []
    for name in names:
        grid_selection.WORKSPACE = False
        row = await run_case(engine, name, orchestrate)
        failure = str(row.get('error') or row.get('reason') or '')
        if run_grid.needs_workspace(failure):
            grid_selection.WORKSPACE = True
            row = await run_case(engine, name, orchestrate)
        rows.append(row)
        print(f"{name:26} {engine:12} {row['verdict']:8} {str(row['reason'])[:110]}", flush=True)
    return rows


def main(engine: str, selected: list[str]) -> None:
    grid_selection.ENGINE = engine
    for sub in ('capability_creation', 'runtime_authoring', 'skills', 'workspace'):
        (grid.SCRATCH / sub).mkdir(parents=True, exist_ok=True)
    names = selected or [c.name for c in grid.CASES]
    if engine == 'dbos':
        from dbos import DBOS

        db = grid.SCRATCH / 'dbos.sqlite'
        db.unlink(missing_ok=True)
        DBOS(config={'name': 'grid', 'system_database_url': f'sqlite:///{db}', 'run_admin_server': False})
        DBOS.launch()

        @DBOS.workflow()
        async def orchestrate(case: str) -> str:  # type: ignore[misc]
            return (await grid.built(case).agent.run('go')).output  # type: ignore[union-attr]

        try:
            rows = asyncio.run(sweep(engine, names, orchestrate))
        finally:
            DBOS.destroy()
    elif engine == 'prefect':
        from prefect import flow
        from prefect.testing.utilities import prefect_test_harness

        @flow(name='grid', persist_result=False)
        async def orchestrate(case: str) -> str:
            return (await grid.built(case).agent.run('go')).output  # type: ignore[union-attr]

        with prefect_test_harness(server_startup_timeout=120):
            rows = asyncio.run(sweep(engine, names, orchestrate))
    else:
        raise SystemExit(f'unknown engine {engine!r}')
    suffix = ('.tools' if grid_selection.TOOLS else '') + ('' if not selected else '.partial')
    (RESULTS / f'grid.{engine}{suffix}.json').write_text(json.dumps(rows, indent=2, default=str))


if __name__ == '__main__':
    args = sys.argv[1:]
    if '--tools' in args:
        args.remove('--tools')
        grid_selection.TOOLS = True
    main(args[0], args[1:])
