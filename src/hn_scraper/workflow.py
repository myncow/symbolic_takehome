"""Temporal workflow: scrape HN pages, persist stories, comments, and OG metadata."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from hn_scraper.models import (
    HnScrapeInput,
    HnScrapeSummary,
    PersistCommentsInput,
    PersistPageMetaInput,
    PersistStoriesInput,
    coerce_scrape_comments_result,
    coerce_scrape_page_result,
)

with workflow.unsafe.imports_passed_through():
    from hn_scraper.activities import (
        persist_comments_activity,
        persist_page_meta_activity,
        persist_stories_activity,
        scrape_comments_activity,
        scrape_page_activity,
        scrape_page_meta_activity,
    )

_NO_RETRY = RetryPolicy(maximum_attempts=1)


@workflow.defn(name="HackerNewsScrapeWorkflow")
class HackerNewsScrapeWorkflow:
    @workflow.run
    async def run(self, inp: HnScrapeInput) -> HnScrapeSummary:
        run_id = workflow.info().run_id
        scraped_at = workflow.now()
        current_url = inp.start_url
        total_stories = 0
        total_comments = 0
        total_metas = 0
        pages = 0

        for page_number in range(1, inp.max_pages + 1):
            scrape_raw = await workflow.execute_activity(
                scrape_page_activity,
                args=[current_url, page_number],
                start_to_close_timeout=timedelta(minutes=3),
                retry_policy=_NO_RETRY,
            )
            scrape = coerce_scrape_page_result(scrape_raw) if isinstance(scrape_raw, dict) else scrape_raw

            total_stories += await workflow.execute_activity(
                persist_stories_activity,
                PersistStoriesInput(
                    workflow_run_id=run_id,
                    stories=scrape.stories,
                    scraped_at=scraped_at,
                ),
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=_NO_RETRY,
            )

            if inp.scrape_comments:
                comment_handles = []
                comment_story_ids = []
                for story in scrape.stories:
                    if not story.comments_url:
                        continue
                    h = workflow.start_activity(
                        scrape_comments_activity,
                        args=[story.comments_url, story.item_id],
                        start_to_close_timeout=timedelta(minutes=3),
                        retry_policy=_NO_RETRY,
                    )
                    comment_handles.append(h)
                    comment_story_ids.append(story.item_id)

                if comment_handles:
                    comment_results = await asyncio.gather(*comment_handles)
                    for raw_cr in comment_results:
                        cr = coerce_scrape_comments_result(raw_cr) if isinstance(raw_cr, dict) else raw_cr
                        if cr.comments:
                            total_comments += await workflow.execute_activity(
                                persist_comments_activity,
                                PersistCommentsInput(
                                    workflow_run_id=run_id,
                                    story_item_id=cr.story_item_id,
                                    comments=cr.comments,
                                    scraped_at=scraped_at,
                                ),
                                start_to_close_timeout=timedelta(minutes=1),
                                retry_policy=_NO_RETRY,
                            )

            handles = []
            item_ids = []
            for story in scrape.stories:
                if not story.url or story.url.startswith("https://news.ycombinator.com"):
                    continue
                handle = workflow.start_activity(
                    scrape_page_meta_activity,
                    args=[story.url],
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=_NO_RETRY,
                )
                handles.append(handle)
                item_ids.append(story.item_id)

            if handles:
                results = await asyncio.gather(*handles)
                metas = list(zip(item_ids, results))
                total_metas += await workflow.execute_activity(
                    persist_page_meta_activity,
                    PersistPageMetaInput(workflow_run_id=run_id, metas=metas),
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=_NO_RETRY,
                )

            pages += 1

            if not scrape.next_page_url:
                break
            current_url = scrape.next_page_url

        return HnScrapeSummary(
            pages_scraped=pages,
            stories_persisted=total_stories,
            comments_persisted=total_comments,
            page_metas_persisted=total_metas,
            last_url=current_url,
        )
