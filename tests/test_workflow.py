"""Integration tests: workflow orchestration with mocked activities."""

from __future__ import annotations

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from hn_scraper.models import (
    CommentRecord,
    HnScrapeInput,
    PageMeta,
    ScrapeCommentsResult,
    ScrapePageResult,
    StoryRecord,
)
from hn_scraper.workflow import HackerNewsScrapeWorkflow


def _story(i: int, page: int = 1) -> StoryRecord:
    return StoryRecord(
        item_id=str(i), title=f"Story {i}", url=f"https://example.com/{i}",
        comments_url=f"https://news.ycombinator.com/item?id={i}",
        site="example.com", rank=i, points=i * 10, author="u", page_number=page,
    )


def _comment(cid: str, depth: int = 0, parent: str | None = None) -> CommentRecord:
    return CommentRecord(
        comment_id=cid, parent_id=parent, author="commenter",
        depth=depth, body=f"Comment {cid}", commented_at="1 hour ago",
    )


def _count_field(inp: object, field: str) -> int:
    items = inp.get(field) if isinstance(inp, dict) else getattr(inp, field)
    return len(items or [])


@activity.defn(name="ScrapeCommentsActivity")
async def _noop_scrape_comments(comments_url: str, story_item_id: str) -> ScrapeCommentsResult:
    return ScrapeCommentsResult(story_item_id=story_item_id, comments=[])


@activity.defn(name="PersistCommentsActivity")
async def _noop_persist_comments(inp) -> int:  # noqa: ANN001
    return 0


@activity.defn(name="ScrapePageMetaActivity")
async def _noop_scrape_meta(url: str) -> PageMeta:
    return PageMeta(og_title=None, og_description=None, og_image=None, og_site_name=None)


@activity.defn(name="PersistPageMetaActivity")
async def _noop_persist_meta(inp) -> int:  # noqa: ANN001
    return 0


def _base_activities() -> list:
    return [_noop_scrape_comments, _noop_persist_comments, _noop_scrape_meta, _noop_persist_meta]


async def _run_workflow(env: WorkflowEnvironment, activities: list, inp: HnScrapeInput, wf_id: str):
    tq = f"test-{wf_id}"
    async with Worker(env.client, task_queue=tq, workflows=[HackerNewsScrapeWorkflow], activities=activities):
        return await env.client.execute_workflow(
            HackerNewsScrapeWorkflow.run, inp, id=wf_id, task_queue=tq,
        )


# -- Story scraping tests -----------------------------------------------------

@pytest.mark.asyncio
async def test_scrapes_two_pages_then_stops(workflow_env: WorkflowEnvironment) -> None:
    calls: list[int] = []

    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        calls.append(page_number)
        if page_number == 1:
            return ScrapePageResult(stories=[_story(1)], next_page_url="https://news.ycombinator.com/?p=2")
        return ScrapePageResult(stories=[_story(2, 2)], next_page_url=None)

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "stories")

    result = await _run_workflow(
        workflow_env, [m_scrape, m_persist] + _base_activities(),
        HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=5, scrape_comments=False),
        "int-two-pages",
    )
    assert result.pages_scraped == 2
    assert result.stories_persisted == 2
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_zero_pages(workflow_env: WorkflowEnvironment) -> None:
    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        raise AssertionError("should not be called")

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist(inp) -> int:  # noqa: ANN001
        raise AssertionError("should not be called")

    result = await _run_workflow(
        workflow_env, [m_scrape, m_persist] + _base_activities(),
        HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=0, scrape_comments=False),
        "int-zero",
    )
    assert result.pages_scraped == 0
    assert result.stories_persisted == 0
    assert result.comments_persisted == 0
    assert result.page_metas_persisted == 0


@pytest.mark.asyncio
async def test_scrape_non_retryable_fails_workflow(workflow_env: WorkflowEnvironment) -> None:
    from temporalio.client import WorkflowFailureError
    from temporalio.exceptions import ApplicationError

    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        raise ApplicationError("bad page shape", non_retryable=True)

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist(inp) -> int:  # noqa: ANN001
        return 0

    with pytest.raises(WorkflowFailureError) as exc_info:
        await _run_workflow(
            workflow_env, [m_scrape, m_persist] + _base_activities(),
            HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=1, scrape_comments=False),
            "int-non-retryable",
        )
    root = exc_info.value.cause.cause
    assert "bad page shape" in str(root)


# -- Comment scraping tests ---------------------------------------------------

@pytest.mark.asyncio
async def test_scrapes_comments_for_each_story(workflow_env: WorkflowEnvironment) -> None:
    comment_calls: list[str] = []

    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        return ScrapePageResult(stories=[_story(1), _story(2)], next_page_url=None)

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist_stories(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "stories")

    @activity.defn(name="ScrapeCommentsActivity")
    async def m_scrape_comments(comments_url: str, story_item_id: str) -> ScrapeCommentsResult:
        comment_calls.append(story_item_id)
        return ScrapeCommentsResult(
            story_item_id=story_item_id,
            comments=[_comment(f"c{story_item_id}a"), _comment(f"c{story_item_id}b", depth=1, parent=f"c{story_item_id}a")],
        )

    @activity.defn(name="PersistCommentsActivity")
    async def m_persist_comments(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "comments")

    result = await _run_workflow(
        workflow_env,
        [m_scrape, m_persist_stories, m_scrape_comments, m_persist_comments, _noop_scrape_meta, _noop_persist_meta],
        HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=1, scrape_comments=True),
        "int-with-comments",
    )
    assert result.pages_scraped == 1
    assert result.stories_persisted == 2
    assert result.comments_persisted == 4
    assert set(comment_calls) == {"1", "2"}


@pytest.mark.asyncio
async def test_comments_disabled_skips_comment_activities(workflow_env: WorkflowEnvironment) -> None:
    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        return ScrapePageResult(stories=[_story(1)], next_page_url=None)

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "stories")

    @activity.defn(name="ScrapeCommentsActivity")
    async def m_scrape_comments(comments_url: str, story_item_id: str) -> ScrapeCommentsResult:
        raise AssertionError("should not be called when comments disabled")

    @activity.defn(name="PersistCommentsActivity")
    async def m_persist_comments(inp) -> int:  # noqa: ANN001
        raise AssertionError("should not be called when comments disabled")

    result = await _run_workflow(
        workflow_env,
        [m_scrape, m_persist, m_scrape_comments, m_persist_comments, _noop_scrape_meta, _noop_persist_meta],
        HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=1, scrape_comments=False),
        "int-comments-off",
    )
    assert result.pages_scraped == 1
    assert result.stories_persisted == 1
    assert result.comments_persisted == 0


# -- OG metadata tests --------------------------------------------------------

@pytest.mark.asyncio
async def test_og_metadata_scraped_for_external_urls(workflow_env: WorkflowEnvironment) -> None:
    meta_calls: list[str] = []

    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        return ScrapePageResult(stories=[_story(1), _story(2)], next_page_url=None)

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "stories")

    @activity.defn(name="ScrapePageMetaActivity")
    async def m_scrape_meta(url: str) -> PageMeta:
        meta_calls.append(url)
        return PageMeta(og_title="T", og_description="D", og_image=None, og_site_name="S")

    @activity.defn(name="PersistPageMetaActivity")
    async def m_persist_meta(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "metas")

    result = await _run_workflow(
        workflow_env,
        [m_scrape, m_persist, _noop_scrape_comments, _noop_persist_comments, m_scrape_meta, m_persist_meta],
        HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=1, scrape_comments=False),
        "int-og-meta",
    )
    assert result.pages_scraped == 1
    assert result.stories_persisted == 2
    assert result.page_metas_persisted == 2
    assert set(meta_calls) == {"https://example.com/1", "https://example.com/2"}


@pytest.mark.asyncio
async def test_og_skips_hn_internal_urls(workflow_env: WorkflowEnvironment) -> None:
    """Stories pointing to HN itself (Ask HN, Show HN) should not trigger OG scraping."""
    meta_calls: list[str] = []

    hn_story = StoryRecord(
        item_id="99", title="Ask HN", url="https://news.ycombinator.com/item?id=99",
        comments_url="https://news.ycombinator.com/item?id=99",
        site=None, rank=1, points=5, author="u", page_number=1,
    )

    @activity.defn(name="ScrapePageActivity")
    async def m_scrape(url: str, page_number: int) -> ScrapePageResult:
        return ScrapePageResult(stories=[hn_story], next_page_url=None)

    @activity.defn(name="PersistStoriesActivity")
    async def m_persist(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "stories")

    @activity.defn(name="ScrapePageMetaActivity")
    async def m_scrape_meta(url: str) -> PageMeta:
        meta_calls.append(url)
        return PageMeta(og_title=None, og_description=None, og_image=None, og_site_name=None)

    @activity.defn(name="PersistPageMetaActivity")
    async def m_persist_meta(inp) -> int:  # noqa: ANN001
        return _count_field(inp, "metas")

    result = await _run_workflow(
        workflow_env,
        [m_scrape, m_persist, _noop_scrape_comments, _noop_persist_comments, m_scrape_meta, m_persist_meta],
        HnScrapeInput(start_url="https://news.ycombinator.com/", max_pages=1, scrape_comments=False),
        "int-og-skip-hn",
    )
    assert result.page_metas_persisted == 0
    assert meta_calls == []
