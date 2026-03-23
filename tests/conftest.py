"""Shared pytest fixtures."""

from __future__ import annotations

import pytest_asyncio
from temporalio.testing import WorkflowEnvironment


@pytest_asyncio.fixture
async def workflow_env() -> WorkflowEnvironment:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        yield env
    finally:
        await env.shutdown()
