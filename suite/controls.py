"""The grid's controls: one known-broken capability and its known-safe twin.

Both append a line to a file after every model request. The unprotected one does it in the
hook body, which every engine runs as orchestration code; the protected one does it in a
`@durable_operation`, which every engine runs as an activity, step or task. Every engine
column must report the first BROKEN and the second SAFE before any other cell counts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pydantic_ai.capabilities import AbstractCapability, durable_operation
from pydantic_ai.messages import ModelResponse
from pydantic_ai.models import ModelRequestContext
from pydantic_ai.tools import RunContext

LEDGER = Path(__file__).parent.parent / 'results' / 'scratch' / 'control-ledger.txt'


@dataclass
class UnprotectedWrite(AbstractCapability[None]):
    id: str | None = 'control_unprotected_write'

    async def after_model_request(
        self, ctx: RunContext[None], *, request_context: ModelRequestContext, response: ModelResponse
    ) -> ModelResponse:
        with open(LEDGER, 'a') as ledger:
            ledger.write('unprotected\n')
        return response


@dataclass
class ProtectedWrite(AbstractCapability[None]):
    id: str | None = 'control_protected_write'

    @durable_operation('write')
    async def _write(self) -> None:
        with open(LEDGER, 'a') as ledger:
            ledger.write('protected\n')

    async def after_model_request(
        self, ctx: RunContext[None], *, request_context: ModelRequestContext, response: ModelResponse
    ) -> ModelResponse:
        await self._write()
        return response
