"""Run every grid case on Temporal and write `results/grid.json` and `results/GRID.md`.

    upstream/.venv/bin/python suite/run_grid.py [CaseName ...]

Two sandbox modes per case: `default` is what a user gets from the plugins, and
`passthrough` adds `pydantic_ai_harness` to the passthrough modules, the documented
workaround when the default sandbox refuses a capability. A capability can be SAFE in one
and not the other, and that difference is itself the finding.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
import uuid
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Offline credentials. Integrations that refuse to construct without a token get a fake one,
# so their hooks can run; the audit hook refuses every non-local connection, so the fakes
# never reach a real service. Tool calls that need the service then fail in their activity.
OFFLINE_TOKENS = [
    'DAY_AI_ACCESS_TOKEN', 'GITHUB_TOKEN', 'GRAIN_ACCESS_TOKEN', 'LINEAR_ACCESS_TOKEN', 'LOGFIRE_API_KEY',
    'NOTION_ACCESS_TOKEN', 'ORDINAL_ACCESS_TOKEN', 'POSTHOG_PERSONAL_API_KEY', 'PYLON_ACCESS_TOKEN',
    'SLACK_USER_TOKEN', 'EXA_API_KEY', 'STACKONE_API_KEY', 'YDC_API_KEY',
]
for _var in OFFLINE_TOKENS:
    os.environ.setdefault(_var, 'grid-offline-not-a-real-token')

import grid  # noqa: E402
import grid_selection  # noqa: E402
from temporalio.client import Client  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions  # noqa: E402

from pydantic_ai.durable_exec.temporal import AgentPlugin, PydanticAIPlugin  # noqa: E402

PORT = 7311
RESULTS = Path(__file__).parent.parent / 'results'
CREDENTIAL = re.compile(
    r'api[_ ]?key|token|credential|unauthori[sz]ed|401|403|not authenticated|environment variable|'
    r'connect(ion)? (refused|error)|name or service not known|nodename nor servname|ssh|docker|'
    r'not installed|install it with|No such file or directory: .(bwrap|ssh|docker|localstack)',
    re.I,
)

DEFAULT_RUNNER = SandboxedWorkflowRunner(
    restrictions=SandboxRestrictions.default.with_passthrough_modules('grid_selection')
)
MODES = {
    'default': None,
    'passthrough': SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules(
            'pydantic_ai_harness', 'grid_selection', *filter(None, os.environ.get('GRID_PASSTHROUGH', '').split(','))
        )
    ),
}


REFUSAL = re.compile(
    r'cannot be used inside a durable|does not support durable execution|cannot be added at runtime with|'
    r'only runs when the run.s workspace is this machine',
    re.I,
)


def verdict(
    error: str | None,
    events: list[dict[str, str]],
    output: str | None,
    reference: str | None,
    blocked: list[str] | None = None,
) -> tuple[str, str]:
    if events:
        first = events[0]
        return 'BROKEN', f"workflow-side {first['event']} at {first['where']}"
    if error is None:
        if output == reference:
            return 'SAFE', ''
        return 'BROKEN', f'durable output {output!r} != plain output {reference!r}'
    if 'RestrictedWorkflowAccessError' in error or 'is restricted' in error:
        return 'SANDBOX', error
    if REFUSAL.search(error):
        return 'REFUSED', error
    if blocked:
        return 'NOT_RUN', f'offline: needs {", ".join(sorted(set(blocked)))}: {error}'
    if 'Nondeterminism' in error or 'nondetermin' in error.lower():
        return 'BROKEN', f'non-determinism: {error}'
    if CREDENTIAL.search(error) or 'grid blocks external network' in error:
        return 'NOT_RUN', f'needs external service or credentials: {error}'
    return 'FAILED', error


# Tools the reference run called, so a SAFE row says what it actually exercised.
LAST_TOOLS: list[str] = []


def construct_verdict(error: str) -> tuple[str, str]:
    """A capability that cannot even be assembled with the engine's durability capability."""
    if "need to have a unique `id`" in error:
        return 'NO_ID', error
    if REFUSAL.search(error):
        return 'REFUSED', error
    return 'NOT_RUN', error


def needs_workspace(failure: str) -> bool:
    return 'needs a workspace' in failure or 'none is attached' in failure or 'LocalWorkspace' in failure


async def plain_output(built: grid.Built) -> tuple[str | None, str | None]:
    """The same agent and workload without durability: what the durable run must reproduce."""
    assert built.plain is not None
    from pydantic_ai.messages import ToolCallPart

    LAST_TOOLS.clear()
    try:
        result = await built.plain.run('go')
    except Exception as exc:
        return None, grid.format_exception(exc)
    LAST_TOOLS.extend(p.tool_name for m in result.all_messages() for p in m.parts if isinstance(p, ToolCallPart))
    return result.output, None


async def run_case(client: Client, name: str, mode: str) -> dict[str, object]:
    grid_selection.CURRENT = name
    built = grid.built(name, rebuild=True)
    if built.agent is None:
        v, reason = construct_verdict(str(built.error))
        return {'case': name, 'module': built.case.module, 'mode': mode, 'verdict': v, 'reason': reason, 'events': []}
    grid.AUDIT.active = True
    try:
        reference, reference_error = await plain_output(built)
    finally:
        grid.AUDIT.active = False
    grid.AUDIT.events.clear()
    grid.AUDIT.blocked.clear()
    error: str | None = None
    output: str | None = None
    started = time.monotonic()
    kwargs = {'workflow_runner': MODES[mode] or DEFAULT_RUNNER}
    try:
        async with Worker(
            client,
            task_queue=f'grid-{name}-{mode}',
            workflows=[grid.GridWorkflow],
            plugins=[AgentPlugin(built.agent)],
            max_cached_workflows=0,
            workflow_failure_exception_types=[Exception],
            **kwargs,
        ):
            grid.AUDIT.active = True
            output = await client.execute_workflow(
                grid.GridWorkflow.run,
                name,
                id=f'grid-{name}-{mode}-{uuid.uuid4()}',
                task_queue=f'grid-{name}-{mode}',
                execution_timeout=timedelta(seconds=60),
            )
    except Exception as exc:
        error = grid.format_exception(exc)
    finally:
        grid.AUDIT.active = False
    events = [asdict(e) for e in grid.AUDIT.events]
    v, reason = verdict(error, events, output, reference, grid.AUDIT.blocked)
    if v == 'BROKEN' and reference_error is not None and not events:
        # The plain run failed too: not a durability defect, a workload the case cannot take.
        v, reason = 'NOT_RUN', f'plain run also fails: {reference_error}'
    return {
        'case': name,
        'module': built.case.module,
        'mode': mode,
        'verdict': v,
        'reason': reason,
        'output': output,
        'reference': reference,
        'reference_error': reference_error,
        'workspace': grid_selection.WORKSPACE,
        'tools_called': list(LAST_TOOLS),
        'blocked_hosts': sorted(set(grid.AUDIT.blocked)),
        'error': error,
        'events': events[:20],
        'event_count': len(events),
        'seconds': round(time.monotonic() - started, 1),
    }


def write_markdown(rows: list[dict[str, object]]) -> None:
    by_case: dict[str, dict[str, dict[str, object]]] = {}
    for row in rows:
        by_case.setdefault(str(row['case']), {})[str(row['mode'])] = row
    lines = [
        '| Capability | default sandbox | passthrough | first finding |',
        '|---|---|---|---|',
    ]
    for case, modes in by_case.items():
        d, p = modes.get('default', {}), modes.get('passthrough', {})
        finding = next((str(m['reason']) for m in (d, p) if m.get('verdict') != 'SAFE' and m.get('reason')), '')
        lines.append(f"| {case} | {d.get('verdict', '-')} | {p.get('verdict', '-')} | {finding[:180].replace('|', '/')} |")
    (RESULTS / 'GRID.md').write_text('\n'.join(lines) + '\n')


async def main(selected: list[str]) -> None:
    RESULTS.mkdir(exist_ok=True)
    for sub in ('capability_creation', 'runtime_authoring', 'skills', 'workspace'):
        (grid.SCRATCH / sub).mkdir(parents=True, exist_ok=True)
    names = selected or [c.name for c in grid.CASES]
    async with await WorkflowEnvironment.start_local(  # pyright: ignore[reportUnknownMemberType]
        port=PORT, dev_server_extra_args=['--dynamic-config-value', 'frontend.enableServerVersionCheck=false']
    ):
        client = await Client.connect(f'localhost:{PORT}', plugins=[PydanticAIPlugin()])
        rows = []
        for name in names:
            for mode in MODES:
                grid_selection.WORKSPACE = False
                row = await run_case(client, name, mode)
                failure = str(row.get('error') or row.get('reason') or '')
                if needs_workspace(failure):
                    grid_selection.WORKSPACE = True
                    row = await run_case(client, name, mode)
                rows.append(row)
                print(f"{name:26} {mode:12} {row['verdict']:8} {str(row['reason'])[:110]}", flush=True)
    tag = '.tools' if grid_selection.TOOLS else ''
    path = RESULTS / (f'grid{tag}.json' if not selected else f'grid{tag}.partial.json')
    path.write_text(json.dumps(rows, indent=2, default=str))
    if not selected and not grid_selection.TOOLS:
        write_markdown(rows)


if __name__ == '__main__':
    args = sys.argv[1:]
    if '--tools' in args:
        args.remove('--tools')
        grid_selection.TOOLS = True
    asyncio.run(main(args))
