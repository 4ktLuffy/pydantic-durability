"""Combine the six sweeps into `results/GRID.md`: one row per case, one column per engine.

Each cell is the worse of the case's two workloads (hooks only, then every tool), so a
capability is SAFE only if both its hooks and its tools are. Run after the sweeps:

    upstream/.venv/bin/python suite/run_grid.py && upstream/.venv/bin/python suite/run_grid.py --tools
    upstream/.venv/bin/python suite/run_engines.py dbos [--tools]   (and prefect)
    python3 suite/report.py
"""

from __future__ import annotations

import json
from pathlib import Path

RESULTS = Path(__file__).parent.parent / 'results'
SOURCES = {
    'Temporal': ('grid.json', 'grid.tools.json', 'default'),
    'Prefect': ('grid.prefect.json', 'grid.prefect.tools.json', 'prefect'),
    'DBOS': ('grid.dbos.json', 'grid.dbos.tools.json', 'dbos'),
}
# Worst first: the cell shows the most serious result either workload produced.
SEVERITY = ['BROKEN', 'FAILED', 'NO_ID', 'SANDBOX', 'REFUSED', 'NOT_RUN', 'SAFE']


def load(name: str, mode: str) -> dict[str, dict[str, object]]:
    return {str(r['case']): r for r in json.loads((RESULTS / name).read_text()) if r['mode'] == mode}


def main() -> None:
    cells: dict[str, dict[str, dict[str, object]]] = {}
    order: list[str] = []
    for engine, (hooks, tools, mode) in SOURCES.items():
        for name in (hooks, tools):
            for case, row in load(name, mode).items():
                if case not in order:
                    order.append(case)
                current = cells.setdefault(case, {}).get(engine)
                if current is None or SEVERITY.index(str(row['verdict'])) < SEVERITY.index(str(current['verdict'])):
                    cells[case][engine] = row
    lines = ['| Case | Temporal | Prefect | DBOS | Evidence for the worst cell |', '|---|---|---|---|---|']
    tally = {engine: {} for engine in SOURCES}
    for case in order:
        row_cells = cells[case]
        worst = min(row_cells.values(), key=lambda r: SEVERITY.index(str(r['verdict'])))
        events = worst.get('events') or []
        evidence = f"{events[0]['event']} at `{events[0]['where']}`" if events else str(worst.get('reason') or '')
        evidence = evidence.replace('|', '/').replace('\n', ' ')[:150]
        verdicts = []
        for engine in SOURCES:
            v = str(row_cells.get(engine, {}).get('verdict', '-'))
            tally[engine][v] = tally[engine].get(v, 0) + 1
            verdicts.append(f'**{v}**' if v in ('BROKEN', 'FAILED', 'NO_ID') else v)
        lines.append(f"| {case} | {' | '.join(verdicts)} | {'' if worst['verdict'] == 'SAFE' else evidence} |")
    summary = ['| Engine | ' + ' | '.join(SEVERITY) + ' |', '|---' * (len(SEVERITY) + 1) + '|']
    for engine, counts in tally.items():
        summary.append(f'| {engine} | ' + ' | '.join(str(counts.get(v, 0)) for v in SEVERITY) + ' |')
    (RESULTS / 'GRID.md').write_text('\n'.join(summary) + '\n\n' + '\n'.join(lines) + '\n')
    print('\n'.join(summary))


if __name__ == '__main__':
    main()
