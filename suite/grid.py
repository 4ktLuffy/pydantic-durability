"""Replay conformance grid: every Pydantic AI Harness capability, run as a Temporal workflow.

Each case is an agent built at module level, as Temporal requires: the workflow sandbox
re-imports this module, so the capability is constructed inside the sandbox exactly as a
user's module-level agent would be. One generic workflow runs whichever case it is named.

The worker runs with `max_cached_workflows=0`, so every workflow task replays history from
the start, and with `workflow_failure_exception_types=[Exception]`, so a sandbox violation or
a non-determinism error fails the workflow instead of retrying the task until the timeout.

Side effects in workflow code are caught with a `sys.addaudithook` that records socket,
subprocess and file-write events raised while `temporalio.workflow.in_workflow()` is true.
Activities run outside the workflow context, so tool calls and `@durable_operation` methods
do not count. Any such event is a defect: workflow code runs again on every replay.
"""

from __future__ import annotations

import os
import sys
import traceback
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from pydantic_ai import Agent
    from pydantic_ai.capabilities import AbstractCapability, LocalWorkspace
    from pydantic_ai.durable_exec.temporal import TemporalDurability
    from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
    from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
    from temporalio.common import RetryPolicy
    from temporalio.workflow import ActivityConfig

    import grid_selection

warnings.simplefilter('ignore')

from datetime import timedelta  # noqa: E402

ACTIVITY_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=1),
)
# Created by the runner before any worker starts; cases only name it.
SCRATCH = Path(__file__).parent.parent / 'results' / 'scratch'  # no resolve(): the sandbox restricts it


# --- workload --------------------------------------------------------------------------


def workload(messages: list[ModelRequest | ModelResponse], info: AgentInfo) -> ModelResponse:
    """Call `ping` once, then answer. Two model requests and one tool call in every run.

    With `grid_selection.TOOLS`, also call each of the capability's own tools once, with the
    smallest arguments its schema accepts, before answering.
    """
    if grid_selection.TOOLS:
        called = {
            part.tool_name
            for message in messages
            if isinstance(message, ModelResponse)
            for part in message.parts
            if isinstance(part, ToolCallPart)
        }
        for tool in info.function_tools:
            if tool.name not in called:
                return ModelResponse(parts=[ToolCallPart(tool.name, _minimal_args(tool.parameters_json_schema))])
        return ModelResponse(parts=[TextPart('done')])
    pinged = any(
        isinstance(part, ToolReturnPart) and part.tool_name == 'ping'
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
    )
    if not pinged and any(t.name == 'ping' for t in info.function_tools):
        return ModelResponse(parts=[ToolCallPart('ping', {})])
    return ModelResponse(parts=[TextPart('done')])


async def stream_workload(messages: list[ModelRequest | ModelResponse], info: AgentInfo):  # type: ignore[no-untyped-def]
    """The same workload for capabilities that request streamed responses."""
    part = workload(messages, info).parts[0]
    if isinstance(part, ToolCallPart):
        calls: DeltaToolCalls = {0: DeltaToolCall(name=part.tool_name, json_args=part.args_as_json_str())}
        yield calls
    else:
        assert isinstance(part, TextPart)
        yield part.content


def judge_model(messages: list[ModelRequest | ModelResponse], info: AgentInfo) -> ModelResponse:
    """A stand-in model for capabilities that make their own model calls (judges, advisors)."""
    if info.output_tools:
        tool = info.output_tools[0]
        return ModelResponse(parts=[ToolCallPart(tool.name, _minimal_args(tool.parameters_json_schema))])
    return ModelResponse(parts=[TextPart('ok')])


def _minimal_args(schema: dict[str, Any]) -> dict[str, Any]:
    args: dict[str, Any] = {}
    for name in schema.get('required', []):
        prop = schema.get('properties', {}).get(name, {})
        kind = prop.get('type')
        args[name] = {'boolean': True, 'integer': 0, 'number': 0.0, 'array': [], 'object': {}}.get(kind, 'ok')
        if 'enum' in prop:
            args[name] = prop['enum'][0]
    return args


# --- cases -----------------------------------------------------------------------------


@dataclass
class Case:
    name: str
    module: str
    build: Callable[[], AbstractCapability[None]]
    note: str = ''


@dataclass
class Built:
    case: Case
    agent: Agent[None, str] | None = None
    plain: Agent[None, str] | None = None
    error: str | None = None


def _h(module: str, attr: str) -> Any:
    import importlib

    return getattr(importlib.import_module(f'pydantic_ai_harness.{module}'), attr)


def _control(name: str) -> AbstractCapability[None]:
    import controls

    return getattr(controls, name)()


def _fn() -> FunctionModel:
    return FunctionModel(judge_model)


def _dir(name: str) -> Path:
    return SCRATCH / name


def _cases() -> list[Case]:
    c = Case
    base = [
        # Controls first: the grid is admissible only if these read BROKEN and SAFE.
        c('Control[unprotected]', 'controls', lambda: _control('UnprotectedWrite')),
        c('Control[durable_operation]', 'controls', lambda: _control('ProtectedWrite')),
        c('Advisor', 'advisor', lambda: _h('advisor', 'Advisor')(model=_fn())),
        c('AskUser', 'ask_user', lambda: _h('ask_user', 'AskUser')(answerer=None)),
        c('BackgroundTools', 'background_tools', lambda: _h('background_tools', 'BackgroundTools')()),
        c(
            'BubblewrapSandbox',
            'bubblewrap_sandbox',
            lambda: _h('bubblewrap_sandbox', 'BubblewrapSandbox')(_h('shell', 'Shell')()),
        ),
        c('WarnOnCacheBusts', 'cache_stability', lambda: _h('cache_stability', 'WarnOnCacheBusts')()),
        c('CacheStabilityMonitor', 'cache_stability', lambda: _h('cache_stability', 'CacheStabilityMonitor')()),
        c(
            'CapabilityCreation',
            'capability_creation',
            lambda: _h('capability_creation', 'CapabilityCreation')(_dir('capability_creation')),
        ),
        c('CodeMode', 'code_mode', lambda: _h('code_mode', 'CodeMode')()),
        c('Coder', 'coder', lambda: _h('coder', 'Coder')()),
        c('RepoContext', 'repo_context', lambda: _h('repo_context', 'RepoContext')()),
        c(
            'ConversationSearch',
            'conversation_search',
            lambda: _h('conversation_search', 'ConversationSearch')(
                source=_h('conversation_search', 'SnapshotHistorySource')(_h('step_persistence', 'InMemoryStepStore')())
            ),
        ),
        c('DayAI', 'day_ai', lambda: _h('day_ai', 'DayAI')()),
        c('PyaiDocs', 'docs', lambda: _h('docs', 'PyaiDocs')()),
        c(
            'DynamicWorkflow',
            'dynamic_workflow',
            lambda: _h('dynamic_workflow', 'DynamicWorkflow')(agents=[Agent(_fn(), name='grid_sub')]),
        ),
        c('E2BSandbox', 'e2b_sandbox', lambda: _h('e2b_sandbox', 'E2BSandbox')()),
        c('ExaSearch', 'exa', lambda: _h('exa', 'ExaSearch')()),
        c('ExaAgent', 'exa', lambda: _h('exa', 'ExaAgent')()),
        c('FileSystem', 'filesystem', lambda: _h('filesystem', 'FileSystem')()),
        c('GitHub', 'github', lambda: _h('github', 'GitHub')()),
        c('Grain', 'grain', lambda: _h('grain', 'Grain')()),
        c('ToolGuardrail', 'guardrails', lambda: _h('guardrails', 'ToolGuardrail')()),
        # The shipped detectors, not a stub: these are what users put in front of an agent.
        c(
            'InputGuardrail',
            'guardrails',
            lambda: _h('guardrails', 'InputGuardrail')(_h('guardrails', 'detectors').personal_data()),
        ),
        c(
            'OutputGuardrail',
            'guardrails',
            lambda: _h('guardrails', 'OutputGuardrail')(_h('guardrails', 'detectors').secret_data()),
        ),
        c('Linear', 'linear', lambda: _h('linear', 'Linear')()),
        c('LocalStack', 'localstack', lambda: _h('localstack', 'LocalStack')()),
        c('ManagedPrompt', 'logfire', lambda: _h('logfire', 'ManagedPrompt')('grid-prompt', default='Be brief.')),
        c('LogfireMCP', 'logfire_mcp', lambda: _h('logfire_mcp', 'LogfireMCP')()),
        c('Macroscope', 'macroscope', lambda: _h('macroscope', 'Macroscope')()),
        c('ModalSandbox', 'modal_sandbox', lambda: _h('modal_sandbox', 'ModalSandbox')()),
        c('Notion', 'notion', lambda: _h('notion', 'Notion')()),
        c('Ordinal', 'ordinal', lambda: _h('ordinal', 'Ordinal')()),
        c('OverflowingToolOutput', 'overflowing_tool_output', lambda: _h('overflowing_tool_output', 'OverflowingToolOutput')()),
        c('PlaywrightBrowser', 'playwright', lambda: _h('playwright', 'PlaywrightBrowser')()),
        c('PostHog', 'posthog', lambda: _h('posthog', 'PostHog')()),
        c(
            'PromptInjectionDefender',
            'prompt_injection_defender',
            lambda: _h('prompt_injection_defender', 'PromptInjectionDefender')(),
        ),
        c('PydanticAIDocs', 'pydantic_ai_docs', lambda: _h('pydantic_ai_docs', 'PydanticAIDocs')()),
        c('Pylon', 'pylon', lambda: _h('pylon', 'Pylon')()),
        c('RepairToolArguments', 'repair_tool_arguments', lambda: _h('repair_tool_arguments', 'RepairToolArguments')()),
        c('Researcher', 'researcher', lambda: _h('researcher', 'Researcher')()),
        c(
            'RuntimeAuthoring',
            'runtime_authoring',
            lambda: _h('runtime_authoring', 'RuntimeAuthoring')(_dir('runtime_authoring')),
        ),
        c('Shell', 'shell', lambda: _h('shell', 'Shell')()),
        c('Skills', 'skills', lambda: _h('skills', 'Skills')((str(_dir('skills')),))),
        c('Slack', 'slack', lambda: _h('slack', 'Slack')()),
        c('SpritesSandbox', 'sprites_sandbox', lambda: _h('sprites_sandbox', 'SpritesSandbox')()),
        c('SSHWorkspace', 'ssh_workspace', lambda: _h('ssh_workspace', 'SSHWorkspace')('grid@127.0.0.1')),
        c('StackOne', 'stackone', lambda: _h('stackone', 'StackOne')('grid-account')),
        c('SubAgents', 'subagents', lambda: _h('subagents', 'SubAgents')()),
        # The configuration docs/harness/durable-execution.md prescribes for Temporal.
        c(
            'SubAgents[documented]',
            'subagents',
            lambda: _h('subagents', 'SubAgents')(include_self=True, agent_folders=None),
        ),
        c('TrajectoryJudge', 'trajectory_judge', lambda: _h('trajectory_judge', 'TrajectoryJudge')(model=_fn())),
        c('YouSearch', 'youdotcom', lambda: _h('youdotcom', 'YouSearch')()),
        c('YouResearch', 'youdotcom', lambda: _h('youdotcom', 'YouResearch')()),
    ]
    # Capabilities whose toolset has no default `id` refuse `TemporalDurability` at
    # construction. Each is run again with an explicit `id`, to test what happens past that.
    with_id = {
        'AskUser': lambda: _h('ask_user', 'AskUser')(answerer=None, id='grid_ask_user'),
        'CapabilityCreation': lambda: _h('capability_creation', 'CapabilityCreation')(
            _dir('capability_creation'), id='grid_capability_creation'
        ),
        'PyaiDocs': lambda: _h('docs', 'PyaiDocs')(id='grid_pyai_docs'),
        'LocalStack': lambda: _h('localstack', 'LocalStack')(id='grid_localstack'),
        'Macroscope': lambda: _h('macroscope', 'Macroscope')(id='grid_macroscope'),
        'PydanticAIDocs': lambda: _h('pydantic_ai_docs', 'PydanticAIDocs')(id='grid_pydantic_ai_docs'),
        'RuntimeAuthoring': lambda: _h('runtime_authoring', 'RuntimeAuthoring')(
            _dir('runtime_authoring'), id='grid_runtime_authoring'
        ),
    }
    return base + [c(f'{name}[id]', next(x.module for x in base if x.name == name), build) for name, build in with_id.items()]


def _agent_name(case: Case) -> str:
    return 'grid_' + ''.join(ch if ch.isalnum() else '_' for ch in case.name.lower())


def _assemble(case: Case, durable: bool) -> Agent[None, str]:
    capabilities: list[AbstractCapability[None]] = [case.build()]
    if grid_selection.WORKSPACE:
        capabilities.append(LocalWorkspace(SCRATCH / 'workspace'))
    if durable:
        capabilities.append(_durability())
    model = FunctionModel(workload, stream_function=stream_workload)
    agent = Agent(model, name=_agent_name(case), capabilities=capabilities, retries=3)

    @agent.tool_plain
    def ping() -> str:
        return 'pong'

    return agent


def _durability() -> AbstractCapability[None]:
    if grid_selection.ENGINE == 'dbos':
        from pydantic_ai.durable_exec.dbos import DBOSDurability

        return DBOSDurability[None]()
    if grid_selection.ENGINE == 'prefect':
        from pydantic_ai.durable_exec.prefect import PrefectDurability

        return PrefectDurability[None]()
    return TemporalDurability[None](activity_config=ACTIVITY_CONFIG)


def _build(case: Case) -> Built:
    """The durable agent under test, and the same agent without durability as its reference."""
    try:
        agent = _assemble(case, durable=True)
        plain = _assemble(case, durable=False)
    except Exception as error:  # construction is a result, not a crash of the grid
        return Built(case, error=f'construct: {type(error).__name__}: {error}'[:300])
    return Built(case, agent=agent, plain=plain)


CASES = _cases()
_BUILT: dict[str, Built] = {}


def built(name: str, rebuild: bool = False) -> Built:
    """Build one case on first use, so a constructor's side effects belong to its own run."""
    if rebuild or name not in _BUILT:
        _BUILT[name] = _build(next(case for case in CASES if case.name == name))
    return _BUILT[name]


# Agents with `TemporalDurability` must exist before the workflow runs, so the selected case
# is built at import: once by the runner, and again by each sandbox re-import of this module.
if grid_selection.CURRENT is not None:
    built(grid_selection.CURRENT)


@workflow.defn
class GridWorkflow:
    @workflow.run
    async def run(self, case: str) -> str:
        result = built(case)
        if result.agent is None:
            raise RuntimeError(f'inside the sandbox: {result.error}')
        return (await result.agent.run('go')).output


# --- audit -----------------------------------------------------------------------------


@dataclass
class WorkflowSideEffect:
    event: str
    detail: str
    where: str


@dataclass
class Audit:
    events: list[WorkflowSideEffect] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    active: bool = False


AUDIT = Audit()
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC
_ALWAYS = {
    'socket.connect',
    'socket.getaddrinfo',
    'subprocess.Popen',
    'os.system',
    'os.posix_spawn',
    'os.exec',
    'os.remove',
    'os.rename',
    'os.mkdir',
    'os.rmdir',
    'shutil.rmtree',
    'shutil.copyfile',
    'http.client.connect',
    'urllib.Request',
    'sqlite3.connect',
}


def _is_write_open(args: tuple[Any, ...]) -> bool:
    if len(args) < 3:
        return False
    mode, flags = args[1], args[2]
    if isinstance(mode, str) and any(ch in mode for ch in 'wax+'):
        return True
    return isinstance(flags, int) and bool(flags & _WRITE_FLAGS)


_ENGINE_PATHS = ('/temporalio/', '/dbos/', '/prefect/', '/sqlalchemy/', '/opentelemetry/', '/logfire/')


def _attribute(depth: int = 2) -> str | None:
    """Who caused the event: the innermost frame that is capability code, or None for engine code.

    Walking outward from the event, the first frame that belongs to a capability (harness or
    the grid's controls) owns it. If an engine's own frame comes first, the event is that
    engine's bookkeeping, such as Prefect creating its storage directory inside a flow, and is
    not a capability defect.
    """
    frame = sys._getframe(depth)  # 0 is this function, 1 the audit hook, 2 the code that raised the event
    while frame is not None:
        name = frame.f_code.co_filename
        if 'pydantic_ai_harness' in name or name.endswith('controls.py'):
            short = name.split('pydantic_ai_harness/', 1)[-1].split('suite/', 1)[-1]
            return f'{short}:{frame.f_lineno} in {frame.f_code.co_name}'
        if any(part in name for part in _ENGINE_PATHS):
            return None
        frame = frame.f_back
    return None


_LOCAL_HOSTS = {'127.0.0.1', '::1', 'localhost', '0.0.0.0'}


def _external_host(event: str, args: tuple[Any, ...]) -> str | None:
    """The non-local host a socket event targets, if any. The grid never leaves this machine."""
    host: object = None
    if event == 'socket.getaddrinfo' and args:
        host = args[0]
    elif event == 'socket.connect' and len(args) > 1 and isinstance(args[1], tuple) and args[1]:
        host = args[1][0]
    if isinstance(host, bytes):
        host = host.decode(errors='replace')
    if isinstance(host, str) and host not in _LOCAL_HOSTS and not host.startswith('/'):
        return host
    return None


def _in_orchestration() -> bool:
    """True in code the engine re-executes on recovery: workflow or flow, not activity, step or task."""
    engine = grid_selection.ENGINE
    try:
        if engine == 'temporal':
            return workflow.in_workflow()
        if engine == 'dbos':
            from dbos._context import get_local_dbos_context

            ctx = get_local_dbos_context()
            return bool(ctx and ctx.is_within_workflow() and not ctx.is_step() and not ctx.is_transaction())
        if engine == 'prefect':
            from prefect.context import FlowRunContext, TaskRunContext

            return FlowRunContext.get() is not None and TaskRunContext.get() is None
    except Exception:
        return False
    return False


def _audit_hook(event: str, args: tuple[Any, ...]) -> None:
    if not AUDIT.active:
        return
    external = _external_host(event, args) if event.startswith('socket.') else None
    if external is not None:
        AUDIT.blocked.append(external)
        # A blocked attempt from orchestration code is still the defect: record it first.
        if _in_orchestration() and (where := _attribute()) is not None:
            AUDIT.events.append(WorkflowSideEffect(event, repr(args)[:160], where))
        raise ConnectionRefusedError(f'grid blocks external network: {external}')
    if event not in _ALWAYS and not (event == 'open' and _is_write_open(args)):
        return
    if not _in_orchestration():
        return
    where = _attribute()
    if where is None:
        return
    AUDIT.events.append(WorkflowSideEffect(event, repr(args)[:160], where))


def _install_resolver_probe() -> None:
    """See name lookups in the task that asks for them.

    asyncio resolves names on an executor thread, which does not carry the engine's context, so
    the audit hook sees `socket.getaddrinfo` as outside the workflow even when a workflow-side
    tool asked for it. Checking at `loop.getaddrinfo`, still in the caller's task, closes that gap.
    """
    import asyncio.base_events

    original = asyncio.base_events.BaseEventLoop.getaddrinfo

    async def getaddrinfo(self: Any, host: Any, *args: Any, **kwargs: Any) -> Any:
        if AUDIT.active and _in_orchestration():
            name = host.decode(errors='replace') if isinstance(host, bytes) else host
            if isinstance(name, str) and name not in _LOCAL_HOSTS and (where := _attribute(1)) is not None:
                AUDIT.events.append(WorkflowSideEffect('loop.getaddrinfo', repr(name), where))
        return await original(self, host, *args, **kwargs)

    asyncio.base_events.BaseEventLoop.getaddrinfo = getaddrinfo  # type: ignore[method-assign]


# The sandbox re-imports this module per workflow; only the real import installs the hooks.
if not workflow.unsafe.in_sandbox():
    sys.addaudithook(_audit_hook)
    _install_resolver_probe()


def format_exception(error: BaseException) -> str:
    """The innermost cause, which is where Temporal puts the workflow's own exception."""
    chain: list[str] = []
    current: BaseException | None = error
    while current is not None:
        chain.append(f'{type(current).__name__}: {current}'.splitlines()[0][:240])
        current = current.__cause__ or current.__context__
    return ' <- '.join(chain[-2:]) if chain else traceback.format_exc()[-240:]
