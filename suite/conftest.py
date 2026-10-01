import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Temporal's client runs on asyncio only."""
    return 'asyncio'
