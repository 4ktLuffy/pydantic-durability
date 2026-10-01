"""Which grid case the current worker runs.

Passed through the workflow sandbox, so the runner's assignment is visible to the sandbox's
re-import of `grid`, which then builds only that case at import time.
"""

CURRENT: str | None = None

# Attach a `LocalWorkspace`: set by the runner for capabilities that refuse to run without one.
WORKSPACE: bool = False

# The durable engine the selected case is assembled for: 'temporal', 'dbos' or 'prefect'.
ENGINE: str = 'temporal'

# Call every tool the capability contributes, once each, after `ping`.
TOOLS: bool = False
