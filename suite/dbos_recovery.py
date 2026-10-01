"""Demonstrate, not infer: a DBOS recovery re-runs a harness tool's side effects.

The grid's audit says `CapabilityCreation` writes files from DBOS workflow code. This script
shows the consequence. It runs one workflow in which the model calls `author_capability`, then
forks that workflow from its last recorded step, which is what DBOS recovery does: the workflow
body runs again and every recorded step returns its stored result. The `ProtectedWrite` control
runs the same way and must not write again, since its write is a step.

    upstream/.venv/bin/python suite/dbos_recovery.py
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import run_grid  # noqa: E402,F401  (sets the offline tokens before any capability is built)
import grid  # noqa: E402
import grid_selection  # noqa: E402

from dbos import DBOS, SetWorkflowID  # noqa: E402

AUTHORED = grid.SCRATCH / 'capability_creation'
CASES_UNDER_TEST = sys.argv[1:] or [
    'CapabilityCreation', 'RuntimeAuthoring', 'LocalStack', 'ExaSearch', 'ExaAgent', 'YouSearch', 'YouResearch',
    'ExaSearch[dynamic]',
]


def ledger(kind: str) -> int:
    """Lines the controls actually wrote, wherever they ran: workflow code or a step."""
    import controls

    if not controls.LEDGER.exists():
        return 0
    return controls.LEDGER.read_text().splitlines().count(kind)


def writes_since(marker: int) -> list[str]:
    """Every side effect recorded in workflow code since `marker`: writes, processes, name lookups."""
    return [f'{e.event} {e.detail[:60]} @ {e.where}' for e in grid.AUDIT.events[marker:]]


async def first_run_then_recover(case: str, workflow_fn: Any) -> dict[str, Any]:
    grid_selection.CURRENT = case
    if grid.built(case, rebuild=True).agent is None:
        raise RuntimeError(f'{case} could not be built: {grid.built(case).error}')
    workflow_id = f'recovery-{case}-{uuid.uuid4()}'
    grid.AUDIT.events.clear()
    kind = {'Control[unprotected]': 'unprotected', 'Control[durable_operation]': 'protected'}.get(case)
    before = ledger(kind) if kind else 0
    grid.AUDIT.active = True
    try:
        with SetWorkflowID(workflow_id):
            first = await workflow_fn(case)
        first_writes = writes_since(0)
        after_first = ledger(kind) if kind else 0
        steps = await DBOS.list_workflow_steps_async(workflow_id)
        marker = len(grid.AUDIT.events)
        # `start_step` is the first step to re-execute. Starting after the last recorded one is
        # recovery of a run whose steps all committed: every step replays from the record and
        # only workflow code runs again.
        handle = await DBOS.fork_workflow_async(workflow_id, max(step['function_id'] for step in steps) + 1)
        second = await handle.get_result()
        recovery_writes = writes_since(marker)
        after_recovery = ledger(kind) if kind else 0
    finally:
        grid.AUDIT.active = False
    return {
        'case': case,
        'outputs': [first, second],
        'recorded_steps': len(steps),
        'ledger_lines_first_run': after_first - before,
        'ledger_lines_on_recovery': after_recovery - after_first,
        'writes_first_run': len(first_writes),
        'writes_on_recovery': len(recovery_writes),
        'first_run_effects': first_writes[:12],
        'recovery_effects': recovery_writes[:12],
    }


def main() -> None:
    grid_selection.ENGINE = 'dbos'
    grid_selection.TOOLS = True
    grid_selection.WORKSPACE = True
    shutil.rmtree(grid.SCRATCH / 'capability_creation', ignore_errors=True)
    for sub in ('capability_creation', 'workspace'):
        (grid.SCRATCH / sub).mkdir(parents=True, exist_ok=True)
    db = grid.SCRATCH / 'dbos-recovery.sqlite'
    db.unlink(missing_ok=True)
    DBOS(config={'name': 'grid-recovery', 'system_database_url': f'sqlite:///{db}', 'run_admin_server': False})
    DBOS.launch()

    @DBOS.workflow()
    async def orchestrate(case: str) -> str:
        return (await grid.built(case).agent.run('go')).output  # type: ignore[union-attr]

    async def run_all() -> list[dict[str, Any]]:
        rows = [
            await first_run_then_recover('Control[unprotected]', orchestrate),
            await first_run_then_recover('Control[durable_operation]', orchestrate),
        ]
        for case in CASES_UNDER_TEST:
            rows.append(await first_run_then_recover(case, orchestrate))
        return rows

    try:
        rows = asyncio.run(run_all())
    finally:
        DBOS.destroy()
    out = Path(__file__).parent.parent / 'results' / 'dbos_recovery.json'
    out.write_text(json.dumps(rows, indent=2))
    for row in rows:
        print(
            f"{row['case']:28} steps={row['recorded_steps']:<3} writes first={row['writes_first_run']:<3}"
            f" on recovery={row['writes_on_recovery']:<3}"
            f" ledger first={row['ledger_lines_first_run']} on recovery={row['ledger_lines_on_recovery']}"
        )


if __name__ == '__main__':
    main()
