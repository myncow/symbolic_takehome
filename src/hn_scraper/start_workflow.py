"""Start a single HackerNewsScrapeWorkflow and wait for its result."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

from temporalio.client import Client

from hn_scraper.models import HnScrapeInput
from hn_scraper.workflow import HackerNewsScrapeWorkflow

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def _run() -> None:
    addr = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    tq = os.environ.get("TEMPORAL_TASK_QUEUE", "hn-scraper")
    max_pages = int(os.environ.get("HN_MAX_PAGES", "3"))
    comments = os.environ.get("SCRAPE_COMMENTS", "true").lower() in ("1", "true", "yes")

    inp = HnScrapeInput(
        start_url=os.environ.get("HN_START_URL", "https://news.ycombinator.com/"),
        max_pages=max_pages,
        scrape_comments=comments,
    )
    log.info("Starting workflow on %s queue=%s max_pages=%d comments=%s", addr, tq, max_pages, comments)
    client = await Client.connect(addr)
    result = await client.execute_workflow(
        HackerNewsScrapeWorkflow.run,
        inp,
        id=f"hn-scrape-{uuid.uuid4()}",
        task_queue=tq,
    )
    log.info("Workflow completed: %s", result)


if __name__ == "__main__":
    asyncio.run(_run())
