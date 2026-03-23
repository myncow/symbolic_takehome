"""Temporal activities: browser scraping (HN pages + OG metadata) and Postgres persistence."""

from __future__ import annotations

import asyncio
import os

from temporalio import activity
from temporalio.exceptions import ApplicationError

from hn_scraper.models import (
    PageMeta,
    PersistCommentsInput,
    PersistPageMetaInput,
    PersistStoriesInput,
    ScrapeCommentsResult,
    ScrapePageResult,
    coerce_persist_comments_input,
    coerce_persist_page_meta_input,
    coerce_persist_stories_input,
)
from hn_scraper.parse import parse_hn_comments, parse_hn_listing, parse_page_meta

_TIMEOUT_MS = 60_000

_UPSERT_STORY_SQL = """
INSERT INTO hn_stories (
    workflow_run_id, item_id, title, url, comments_url, site,
    rank_on_page, points, author, page_number, scraped_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workflow_run_id, item_id) DO NOTHING
"""

_UPSERT_COMMENT_SQL = """
INSERT INTO hn_comments (
    workflow_run_id, story_item_id, comment_id, parent_id,
    author, depth, body, commented_at, scraped_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workflow_run_id, comment_id) DO NOTHING
"""

_UPDATE_META_SQL = """
UPDATE hn_stories
SET og_title = %s, og_description = %s, og_image = %s, og_site_name = %s
WHERE workflow_run_id = %s AND item_id = %s
"""


# -- Browser activities -------------------------------------------------------

async def _fetch_html(url: str) -> tuple[str, int | None]:
    # - Playwright over raw HTTP: linked sites may render via client-side JS
    # - domcontentloaded: OG tags live in <head>, no need for full render
    # - Stateless: each call launches + tears down its own browser
    # - Lazy import: keeps module importable inside Temporal's sandbox
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            return await page.content(), resp.status if resp else None
        finally:
            await browser.close()


@activity.defn(name="ScrapePageActivity")
async def scrape_page_activity(url: str, page_number: int) -> ScrapePageResult:
    html, status = await _fetch_html(url)
    if status and status >= 400:
        raise ApplicationError(f"HTTP {status} loading {url}", non_retryable=False)
    stories, next_url = parse_hn_listing(html, page_number)
    if not stories and "titleline" not in html and "hnuser" not in html:
        raise ApplicationError("Unexpected HN page shape (no stories parsed)", non_retryable=True)
    return ScrapePageResult(stories=stories, next_page_url=next_url)


@activity.defn(name="ScrapeCommentsActivity")
async def scrape_comments_activity(comments_url: str, story_item_id: str) -> ScrapeCommentsResult:
    html, status = await _fetch_html(comments_url)
    if status and status >= 400:
        raise ApplicationError(f"HTTP {status} loading {comments_url}", non_retryable=False)
    return ScrapeCommentsResult(story_item_id=story_item_id, comments=parse_hn_comments(html))


@activity.defn(name="ScrapePageMetaActivity")
async def scrape_page_meta_activity(url: str) -> PageMeta:
    # Best-effort: HTTP errors return empty meta instead of failing the workflow
    html, status = await _fetch_html(url)
    if status and status >= 400:
        return PageMeta(og_title=None, og_description=None, og_image=None, og_site_name=None)
    return parse_page_meta(html)


# -- Persistence activities ---------------------------------------------------

def _db_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://temporal:temporal@localhost:5432/temporal")


def _persist_stories_sync(inp: PersistStoriesInput) -> int:
    import psycopg

    inserted = 0
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            for s in inp.stories:
                cur.execute(_UPSERT_STORY_SQL, (
                    inp.workflow_run_id, s.item_id, s.title, s.url,
                    s.comments_url or None, s.site, s.rank, s.points,
                    s.author, s.page_number, inp.scraped_at,
                ))
                inserted += cur.rowcount
        conn.commit()
    return inserted


def _persist_comments_sync(inp: PersistCommentsInput) -> int:
    import psycopg

    inserted = 0
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            for c in inp.comments:
                cur.execute(_UPSERT_COMMENT_SQL, (
                    inp.workflow_run_id, inp.story_item_id, c.comment_id,
                    c.parent_id, c.author, c.depth, c.body,
                    c.commented_at, inp.scraped_at,
                ))
                inserted += cur.rowcount
        conn.commit()
    return inserted


def _persist_page_meta_sync(inp: PersistPageMetaInput) -> int:
    import psycopg

    updated = 0
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            for item_id, meta in inp.metas:
                cur.execute(_UPDATE_META_SQL, (
                    meta.og_title, meta.og_description,
                    meta.og_image, meta.og_site_name,
                    inp.workflow_run_id, item_id,
                ))
                updated += cur.rowcount
        conn.commit()
    return updated


# psycopg is blocking — run in a thread to keep the event loop free

@activity.defn(name="PersistStoriesActivity")
async def persist_stories_activity(inp: PersistStoriesInput) -> int:
    return await asyncio.to_thread(_persist_stories_sync, coerce_persist_stories_input(inp))


@activity.defn(name="PersistCommentsActivity")
async def persist_comments_activity(inp: PersistCommentsInput) -> int:
    return await asyncio.to_thread(_persist_comments_sync, coerce_persist_comments_input(inp))


@activity.defn(name="PersistPageMetaActivity")
async def persist_page_meta_activity(inp: PersistPageMetaInput) -> int:
    return await asyncio.to_thread(_persist_page_meta_sync, coerce_persist_page_meta_input(inp))


ALL_ACTIVITIES = [
    scrape_page_activity,
    scrape_comments_activity,
    scrape_page_meta_activity,
    persist_stories_activity,
    persist_comments_activity,
    persist_page_meta_activity,
]
