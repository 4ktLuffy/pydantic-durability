| Engine | BROKEN | FAILED | NO_ID | SANDBOX | REFUSED | NOT_RUN | SAFE |
|---|---|---|---|---|---|---|---|
| Temporal | 0 | 0 | 19 | 1 | 3 | 15 | 22 |
| Prefect | 1 | 0 | 19 | 0 | 3 | 15 | 22 |
| DBOS | 11 | 0 | 0 | 0 | 3 | 17 | 29 |

| Case | Temporal | Prefect | DBOS | Evidence for the worst cell |
|---|---|---|---|---|
| Control[unprotected] | SANDBOX | **BROKEN** | **BROKEN** | open at `controls.py:29 in after_model_request` |
| Control[durable_operation] | SAFE | SAFE | SAFE |  |
| Advisor | SAFE | SAFE | SAFE |  |
| AskUser | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| BackgroundTools | SAFE | SAFE | SAFE |  |
| BubblewrapSandbox | SAFE | SAFE | SAFE |  |
| WarnOnCacheBusts | SAFE | SAFE | SAFE |  |
| CacheStabilityMonitor | SAFE | SAFE | SAFE |  |
| CapabilityCreation | **NO_ID** | **NO_ID** | **BROKEN** | os.mkdir at `pydantic_ai_harness/capability_creation/_store.py:100 in write` |
| CodeMode | SAFE | SAFE | SAFE |  |
| Coder | SAFE | SAFE | SAFE |  |
| RepoContext | SAFE | SAFE | SAFE |  |
| ConversationSearch | SAFE | SAFE | SAFE |  |
| DayAI | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs day.ai: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: day.ai <- ApplicationError: ConnectErro |
| PyaiDocs | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| DynamicWorkflow | SAFE | SAFE | SAFE |  |
| E2BSandbox | NOT_RUN | NOT_RUN | NOT_RUN | needs external service or credentials: WorkflowFailureError: Workflow execution failed <- ApplicationError: WorkspaceUnavailableError: No E2B API key  |
| ExaSearch | **NO_ID** | **NO_ID** | **BROKEN** | loop.getaddrinfo at `pydantic_ai_harness/exa/_toolset.py:191 in web_search` |
| ExaAgent | **NO_ID** | **NO_ID** | **BROKEN** | loop.getaddrinfo at `pydantic_ai_harness/exa/_agent.py:176 in exa_agent` |
| FileSystem | SAFE | SAFE | SAFE |  |
| GitHub | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs api.githubcopilot.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: api.githubcopilot.com <- |
| Grain | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs api.grain.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: api.grain.com <- ApplicationErro |
| ToolGuardrail | SAFE | SAFE | SAFE |  |
| InputGuardrail | SAFE | SAFE | SAFE |  |
| OutputGuardrail | SAFE | SAFE | SAFE |  |
| Linear | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs mcp.linear.app: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: mcp.linear.app <- ApplicationEr |
| LocalStack | **NO_ID** | **NO_ID** | **BROKEN** | open at `pydantic_ai_harness/localstack/_toolset.py:297 in _run` |
| ManagedPrompt | SAFE | SAFE | SAFE |  |
| LogfireMCP | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs logfire-us.pydantic.dev: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: logfire-us.pydantic.de |
| Macroscope | **NO_ID** | **NO_ID** | NOT_RUN | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| ModalSandbox | NOT_RUN | NOT_RUN | NOT_RUN | needs external service or credentials: WorkflowFailureError: Workflow execution failed <- ApplicationError: WorkspaceUnavailableError: No Modal creden |
| Notion | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs mcp.notion.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: mcp.notion.com <- ApplicationEr |
| Ordinal | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs app.tryordinal.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: app.tryordinal.com <- Appli |
| OverflowingToolOutput | SAFE | SAFE | SAFE |  |
| PlaywrightBrowser | REFUSED | REFUSED | REFUSED | construct: UserError: PlaywrightBrowser does not support durable execution (e.g. TemporalDurability): a live Chromium browser cannot survive activity  |
| PostHog | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs mcp.posthog.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: mcp.posthog.com <- Application |
| PromptInjectionDefender | SAFE | SAFE | SAFE |  |
| PydanticAIDocs | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| Pylon | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs mcp.usepylon.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: mcp.usepylon.com <- Applicati |
| RepairToolArguments | SAFE | SAFE | SAFE |  |
| Researcher | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| RuntimeAuthoring | **NO_ID** | **NO_ID** | **BROKEN** | os.mkdir at `pydantic_ai_harness/capability_creation/_store.py:100 in write` |
| Shell | SAFE | SAFE | SAFE |  |
| Skills | SAFE | SAFE | SAFE |  |
| Slack | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs mcp.slack.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: mcp.slack.com <- ApplicationErro |
| SpritesSandbox | NOT_RUN | NOT_RUN | NOT_RUN | needs external service or credentials: WorkflowFailureError: Workflow execution failed <- ApplicationError: WorkspaceUnavailableError: No Sprites cred |
| SSHWorkspace | NOT_RUN | NOT_RUN | NOT_RUN | needs external service or credentials: WorkflowFailureError: Workflow execution failed <- ApplicationError: WorkspaceUnavailableError: SSH workspace g |
| StackOne | NOT_RUN | NOT_RUN | NOT_RUN | offline: needs api.stackone.com: ApplicationError: RuntimeError: Client failed to connect: grid blocks external network: api.stackone.com <- Applicati |
| SubAgents | REFUSED | REFUSED | REFUSED | WorkflowFailureError: Workflow execution failed <- ApplicationError: UserError: DynamicToolset cannot be added at runtime with Temporal, because tools |
| SubAgents[documented] | SAFE | SAFE | SAFE |  |
| TrajectoryJudge | REFUSED | REFUSED | REFUSED | WorkflowFailureError: Workflow execution failed <- ApplicationError: UserError: `TrajectoryJudge` cannot be used inside a durable workflow or flow: th |
| YouSearch | **NO_ID** | **NO_ID** | **BROKEN** | loop.getaddrinfo at `pydantic_ai_harness/youdotcom/_toolset.py:329 in web_search` |
| YouResearch | **NO_ID** | **NO_ID** | **BROKEN** | loop.getaddrinfo at `pydantic_ai_harness/youdotcom/_research.py:139 in answer` |
| AskUser[id] | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| CapabilityCreation[id] | **NO_ID** | **NO_ID** | **BROKEN** | os.mkdir at `pydantic_ai_harness/capability_creation/_store.py:100 in write` |
| PyaiDocs[id] | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| LocalStack[id] | **NO_ID** | **NO_ID** | **BROKEN** | open at `pydantic_ai_harness/localstack/_toolset.py:297 in _run` |
| Macroscope[id] | **NO_ID** | **NO_ID** | NOT_RUN | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| PydanticAIDocs[id] | **NO_ID** | **NO_ID** | SAFE | construct: UserError: Toolsets that are 'leaves' (i.e. those that implement their own tool listing and calling) need to have a unique `id` in order to |
| RuntimeAuthoring[id] | **NO_ID** | **NO_ID** | **BROKEN** | os.mkdir at `pydantic_ai_harness/capability_creation/_store.py:100 in write` |
