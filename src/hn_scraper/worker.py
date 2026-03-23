"""Temporal worker process — connects to the server and polls for tasks."""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from hn_scraper.activities import ALL_ACTIVITIES
from hn_scraper.workflow import HackerNewsScrapeWorkflow

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def _connect_with_retry(addr: str, max_retries: int = 20, delay: float = 3.0) -> Client:
    for attempt in range(1, max_retries + 1):
        try:
            return await Client.connect(addr)
        except RuntimeError:
            if attempt == max_retries:
                raise
            log.warning("Temporal not ready (%d/%d), retrying in %ss", attempt, max_retries, delay)
            await asyncio.sleep(delay)
    raise RuntimeError("unreachable")


async def _run() -> None:
    addr = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    tq = os.environ.get("TEMPORAL_TASK_QUEUE", "hn-scraper")
    log.info("Connecting to Temporal at %s queue=%s", addr, tq)
    client = await _connect_with_retry(addr)
    worker = Worker(
        client, task_queue=tq, workflows=[HackerNewsScrapeWorkflow],
        activities=ALL_ACTIVITIES, max_concurrent_activities=10,
    )
    log.info("Worker started")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(_run())
