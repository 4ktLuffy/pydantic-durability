"""What changed between two grid runs: the nightly report.

    python3 suite/diff_grids.py results/grid-dac0464 results

Compares every (case, engine, workload) verdict in two result directories and prints only the
cells that changed, with the new evidence, so a regression on a new commit is one line.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

FILES = {
    ('Temporal', 'hooks'): ('grid.json', 'default'),
    ('Temporal', 'tools'): ('grid.tools.json', 'default'),
    ('Prefect', 'hooks'): ('grid.prefect.json', 'prefect'),
    ('Prefect', 'tools'): ('grid.prefect.tools.json', 'prefect'),
    ('DBOS', 'hooks'): ('grid.dbos.json', 'dbos'),
    ('DBOS', 'tools'): ('grid.dbos.tools.json', 'dbos'),
}


def load(directory: Path) -> dict[tuple[str, str, str], dict[str, object]]:
    cells: dict[tuple[str, str, str], dict[str, object]] = {}
    for (engine, workload), (name, mode) in FILES.items():
        path = directory / name
        if not path.exists():
            continue
        for row in json.loads(path.read_text()):
            if row['mode'] == mode:
                cells[(str(row['case']), engine, workload)] = row
    return cells


def evidence(row: dict[str, object]) -> str:
    events = row.get('events') or []
    if events:
        first = events[0]  # type: ignore[index]
        return f"{first['event']} at {first['where']}"  # type: ignore[index]
    return str(row.get('reason') or '')[:120]


def main(old_dir: str, new_dir: str) -> int:
    old, new = load(Path(old_dir)), load(Path(new_dir))
    changed = []
    for key in sorted(set(old) | set(new)):
        before = str(old.get(key, {}).get('verdict', 'absent'))
        after = str(new.get(key, {}).get('verdict', 'absent'))
        if before != after:
            changed.append((key, before, after, evidence(new.get(key, {})) if key in new else ''))
    print(f'{len(old)} cells before, {len(new)} after, {len(changed)} changed')
    for (case, engine, workload), before, after, why in changed:
        print(f'  {case:26} {engine:8} {workload:5} {before:8} -> {after:8} {why}')
    return 1 if changed else 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
