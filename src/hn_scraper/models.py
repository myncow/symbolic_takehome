"""Dataclasses for workflow/activity payloads and Temporal sandbox coercion helpers.

Temporal's workflow sandbox may deserialize activity return values as plain dicts
instead of dataclass instances. The coerce_* functions convert them back.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


# -- Records ------------------------------------------------------------------

@dataclass
class StoryRecord:
    item_id: str
    title: str
    url: str
    comments_url: str
    site: str | None
    rank: int
    points: int | None
    author: str | None
    page_number: int


@dataclass
class CommentRecord:
    comment_id: str
    parent_id: str | None
    author: str | None
    depth: int
    body: str
    commented_at: str | None


@dataclass
class PageMeta:
    og_title: str | None
    og_description: str | None
    og_image: str | None
    og_site_name: str | None


# -- Workflow I/O -------------------------------------------------------------

@dataclass
class HnScrapeInput:
    start_url: str
    max_pages: int
    scrape_comments: bool = True


@dataclass
class ScrapePageResult:
    stories: list[StoryRecord]
    next_page_url: str | None


@dataclass
class ScrapeCommentsResult:
    story_item_id: str
    comments: list[CommentRecord]


@dataclass
class PersistStoriesInput:
    workflow_run_id: str
    stories: list[StoryRecord]
    scraped_at: datetime


@dataclass
class PersistCommentsInput:
    workflow_run_id: str
    story_item_id: str
    comments: list[CommentRecord]
    scraped_at: datetime


@dataclass
class PersistPageMetaInput:
    workflow_run_id: str
    metas: list[tuple[str, PageMeta]]  # [(item_id, PageMeta), ...]


@dataclass
class HnScrapeSummary:
    pages_scraped: int
    stories_persisted: int
    comments_persisted: int
    page_metas_persisted: int
    last_url: str


# -- Temporal sandbox coercion ------------------------------------------------

def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"Expected datetime, got {type(value)}")


def _coerce_story(s: Any) -> StoryRecord:
    return StoryRecord(**s) if isinstance(s, dict) else s


def _coerce_comment(c: Any) -> CommentRecord:
    return CommentRecord(**c) if isinstance(c, dict) else c


def _coerce_page_meta(m: Any) -> PageMeta:
    return PageMeta(**m) if isinstance(m, dict) else m


def coerce_scrape_page_result(raw: Any) -> ScrapePageResult:
    if isinstance(raw, dict):
        return ScrapePageResult(
            stories=[_coerce_story(s) for s in (raw.get("stories") or [])],
            next_page_url=raw.get("next_page_url"),
        )
    return raw


def coerce_scrape_comments_result(raw: Any) -> ScrapeCommentsResult:
    if isinstance(raw, dict):
        return ScrapeCommentsResult(
            story_item_id=raw["story_item_id"],
            comments=[_coerce_comment(c) for c in (raw.get("comments") or [])],
        )
    return raw


def coerce_persist_stories_input(raw: Any) -> PersistStoriesInput:
    if isinstance(raw, dict):
        return PersistStoriesInput(
            workflow_run_id=raw["workflow_run_id"],
            stories=[_coerce_story(s) for s in (raw.get("stories") or [])],
            scraped_at=_parse_dt(raw["scraped_at"]),
        )
    return raw


def coerce_persist_comments_input(raw: Any) -> PersistCommentsInput:
    if isinstance(raw, dict):
        return PersistCommentsInput(
            workflow_run_id=raw["workflow_run_id"],
            story_item_id=raw["story_item_id"],
            comments=[_coerce_comment(c) for c in (raw.get("comments") or [])],
            scraped_at=_parse_dt(raw["scraped_at"]),
        )
    return raw


def coerce_persist_page_meta_input(raw: Any) -> PersistPageMetaInput:
    if isinstance(raw, dict):
        metas = []
        for pair in raw.get("metas") or []:
            if isinstance(pair, (list, tuple)):
                metas.append((pair[0], _coerce_page_meta(pair[1])))
            else:
                metas.append(pair)
        return PersistPageMetaInput(
            workflow_run_id=raw["workflow_run_id"],
            metas=metas,
        )
    return raw
