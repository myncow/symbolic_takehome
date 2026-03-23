"""Unit tests for HN HTML parsing."""

from __future__ import annotations

from pathlib import Path

from hn_scraper.parse import parse_hn_listing

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "hn_sample.html"


def test_parse_hn_listing_sample() -> None:
    html = _FIXTURE.read_text(encoding="utf-8")
    stories, next_url = parse_hn_listing(html, page_number=1)
    assert len(stories) == 2
    assert stories[0].item_id == "100"
    assert stories[0].title == "First story"
    assert stories[0].url == "https://example.com/a"
    assert stories[0].points == 42
    assert stories[0].author == "alice"
    assert "item?id=100" in stories[0].comments_url
    assert stories[1].item_id == "200"
    assert stories[1].url.startswith("https://news.ycombinator.com/item?id=200")
    assert next_url.endswith("?p=2") or "p=2" in (next_url or "")
