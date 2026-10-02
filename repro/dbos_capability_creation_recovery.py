"""Minimal repro for pydantic/pydantic-ai#9554: a DBOS recovery re-runs CapabilityCreation's file write.

    python dbos_capability_creation_recovery.py            # the bug: the write repeats
    python dbos_capability_creation_recovery.py --dynamic  # control: wrapped in DynamicCapability, it doesn't

No network, no API keys: a FunctionModel calls `author_capability` once, then answers. The script
counts calls to `CapabilityStore.write` during the first run and during a recovery
(`DBOS.fork_workflow_async` after the last recorded step, so every step replays from its record
and only workflow code runs again).
"""

import asyncio
import sys
import tempfile
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from pydantic_ai import Agent
from pydantic_ai.capabilities import DynamicCapability, LocalWorkspace
from pydantic_ai.durable_exec.dbos import DBOSDurability
from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai_harness.capability_creation import CapabilityCreation
from pydantic_ai_harness.capability_creation._store import CapabilityStore

CODE = 'from pydantic_ai.capabilities import AbstractCapability\n\nclass Hello(AbstractCapability):\n    pass\n'
WRITES = {'count': 0}
original_write = CapabilityStore.write


def counting_write(self, name, code):  # type: ignore[no-untyped-def]
    WRITES['count'] += 1
    return original_write(self, name, code)


CapabilityStore.write = counting_write  # type: ignore[method-assign]


def model(messages: list[ModelRequest | ModelResponse], info: AgentInfo) -> ModelResponse:
    done = any(isinstance(p, ToolReturnPart) for m in messages if isinstance(m, ModelRequest) for p in m.parts)
    if done:
        return ModelResponse(parts=[TextPart('done')])
    return ModelResponse(parts=[ToolCallPart('author_capability', {'name': 'hello', 'code': CODE})])


root = Path(tempfile.mkdtemp())
creation = CapabilityCreation(root / 'authored')
if '--dynamic' in sys.argv:
    creation = DynamicCapability(lambda ctx: CapabilityCreation(root / 'authored'), id='capability_creation')
agent = Agent(
    FunctionModel(model),
    name='repro_9554',
    capabilities=[LocalWorkspace(root), creation, DBOSDurability()],
)


@DBOS.workflow()
async def run() -> str:
    return (await agent.run('author a capability')).output


async def main() -> None:
    with SetWorkflowID('repro-9554'):
        print('first run:', await run(), '| store writes:', WRITES['count'])
    steps = await DBOS.list_workflow_steps_async('repro-9554')
    before = WRITES['count']
    handle = await DBOS.fork_workflow_async('repro-9554', max(s['function_id'] for s in steps) + 1)
    print('recovery:', await handle.get_result(), '| store writes during recovery:', WRITES['count'] - before)


DBOS(config={'name': 'repro_9554', 'system_database_url': f'sqlite:///{root / "dbos.sqlite"}', 'run_admin_server': False})
DBOS.launch()
try:
    asyncio.run(main())
finally:
    DBOS.destroy()
