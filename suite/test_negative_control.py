"""The control that makes every other cell of the grid admissible.

A capability whose hook writes to external state once per model request must leave
exactly one write per request after a durable run. With the workflow cache disabled,
Temporal replays the workflow from history on every task, so a hook that runs in
workflow code writes again on each replay.

The instrument is admissible only if it reports the unprotected probe as BROKEN and
the same probe behind `@durable_operation` as SAFE. If either half does not hold, no
SAFE verdict elsewhere in the suite means anything.
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

import pytest
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions
from temporalio.workflow import ActivityConfig

from pydantic_ai import Agent
from pydantic_ai.capabilities import AbstractCapability, durable_operation
from pydantic_ai.durable_exec.temporal import AgentPlugin, PydanticAIPlugin, TemporalDurability
from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models import ModelRequestContext
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import RunContext

pytestmark = pytest.mark.temporal

PORT = 7301
TASK_QUEUE = 'durability-control'
ACTIVITY_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=1),
)
# The probes stand in for an external store, so their module must not be re-imported
# inside the sandbox: a sandboxed copy would count into a different dict.
RESTRICTIONS = SandboxRestrictions.default.with_passthrough_modules('coverage', 'annotated_types', __name__)

# The "external store". Keyed by probe id so the two probes never share a count.
WRITES: Counter[str] = Counter()


@dataclass
class UnprotectedProbe(AbstractCapability[None]):
    """Writes in the hook body, which Temporal runs as workflow code."""

    id: str | None = 'unprotected_probe'

    async def after_model_request(
        self, ctx: RunContext[None], *, request_context: ModelRequestContext, response: ModelResponse
    ) -> ModelResponse:
        WRITES[self.id or ''] += 1
        return response


@dataclass
class ProtectedProbe(AbstractCapability[None]):
    """Same write, dispatched through the capability's durable operation."""

    id: str | None = 'protected_probe'

    @durable_operation('write')
    async def _write(self) -> None:
        WRITES[self.id or ''] += 1

    async def after_model_request(
        self, ctx: RunContext[None], *, request_context: ModelRequestContext, response: ModelResponse
    ) -> ModelResponse:
        await self._write()
        return response


def _two_step_model(messages: list[ModelRequest | ModelResponse], info: AgentInfo) -> ModelResponse:
    """One tool call, then a final answer: two model requests, so two expected writes.

    More than one workflow task is what gives replay a chance to re-run earlier hooks.
    """
    if len(messages) == 1:
        return ModelResponse(parts=[ToolCallPart('ping', {})])
    return ModelResponse(parts=[TextPart('done')])


def _agent(name: str, probe: AbstractCapability[None]) -> Agent[None, str]:
    agent = Agent(
        FunctionModel(_two_step_model),
        name=name,
        capabilities=[probe, TemporalDurability[None](activity_config=ACTIVITY_CONFIG)],
    )

    @agent.tool_plain
    def ping() -> str:
        return 'pong'

    return agent


unprotected_agent = _agent('control_unprotected', UnprotectedProbe())
protected_agent = _agent('control_protected', ProtectedProbe())


@workflow.defn
class UnprotectedWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> str:
        return (await unprotected_agent.run(prompt)).output


@workflow.defn
class ProtectedWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> str:
        return (await protected_agent.run(prompt)).output


@pytest.fixture(scope='module')
async def client() -> AsyncIterator[Client]:
    async with await WorkflowEnvironment.start_local(  # pyright: ignore[reportUnknownMemberType]
        port=PORT,
        dev_server_extra_args=['--dynamic-config-value', 'frontend.enableServerVersionCheck=false'],
    ):
        yield await Client.connect(f'localhost:{PORT}', plugins=[PydanticAIPlugin()])


async def _writes_after_run(client: Client, workflow_cls: type, agent: Agent[None, str], probe_id: str) -> int:
    WRITES.clear()
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[workflow_cls],
        plugins=[AgentPlugin(agent)],
        workflow_runner=SandboxedWorkflowRunner(restrictions=RESTRICTIONS),
        max_cached_workflows=0,  # every workflow task replays history from the start
    ):
        output = await client.execute_workflow(
            workflow_cls.run,  # pyright: ignore[reportAttributeAccessIssue]
            'go',
            id=f'control-{probe_id}-{uuid.uuid4()}',
            task_queue=TASK_QUEUE,
            execution_timeout=timedelta(seconds=60),
        )
    assert output == 'done'
    return WRITES[probe_id]


async def test_unprotected_hook_is_reported_broken(client: Client) -> None:
    writes = await _writes_after_run(client, UnprotectedWorkflow, unprotected_agent, 'unprotected_probe')
    print(f"unprotected: {writes} writes for 2 model requests")
    assert writes > 2, f'expected replay to repeat the write (BROKEN); saw {writes} for 2 model requests'


async def test_durable_operation_hook_is_reported_safe(client: Client) -> None:
    writes = await _writes_after_run(client, ProtectedWorkflow, protected_agent, 'protected_probe')
    print(f"protected: {writes} writes for 2 model requests")
    assert writes == 2, f'expected exactly one write per model request (SAFE); saw {writes}'
