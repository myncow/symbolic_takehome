"""Parse Hacker News HTML pages (listings and comment threads) and OpenGraph metadata."""

from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from hn_scraper.models import CommentRecord, PageMeta, StoryRecord

_BASE = "https://news.ycombinator.com/"


def _abs_url(href: str | None) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(_BASE, href)


def _parse_points(text: str | None) -> int | None:
    if not text:
        return None
    m = re.search(r"(\d+)\s+point", text)
    if m:
        return int(m.group(1))
    return None


def parse_hn_listing(html: str, page_number: int) -> tuple[list[StoryRecord], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    stories: list[StoryRecord] = []

    for tr in soup.select("tr.athing"):
        item_id = (tr.get("id") or "").strip()
        titleline = tr.select_one("span.titleline")
        if not titleline:
            continue
        links = titleline.select("a")
        if not links:
            continue
        link = links[0]
        title = link.get_text(strip=True)
        href = link.get("href") or ""
        story_url = _abs_url(href)

        site: str | None = None
        if len(links) >= 2:
            site = links[1].get_text(strip=True).strip("()")

        rank_el = tr.select_one("span.rank")
        rank = 0
        if rank_el:
            raw = rank_el.get_text(strip=True).rstrip(".")
            if raw.isdigit():
                rank = int(raw)

        subtext = None
        sub_tr = tr.find_next_sibling("tr")
        while sub_tr:
            subtext = sub_tr.select_one("td.subtext")
            if subtext:
                break
            sub_tr = sub_tr.find_next_sibling("tr")
        points: int | None = None
        author: str | None = None
        comments_url = ""

        if subtext:
            score_el = subtext.select_one("span.score")
            points = _parse_points(score_el.get_text() if score_el else None)
            user_el = subtext.select_one("a.hnuser")
            if user_el:
                author = user_el.get_text(strip=True)
            for a in subtext.select("a[href*='item?id=']"):
                t = a.get_text(strip=True).lower()
                h = a.get("href") or ""
                if "comment" in t or t == "discuss":
                    comments_url = _abs_url(h)
                    break
            if not comments_url:
                for a in subtext.select('a[href^="item?id="]'):
                    comments_url = _abs_url(a.get("href"))
                    break

        if not item_id:
            continue

        if not comments_url and item_id.isdigit():
            comments_url = urljoin(_BASE, f"item?id={item_id}")

        stories.append(
            StoryRecord(
                item_id=item_id,
                title=title,
                url=story_url,
                comments_url=comments_url,
                site=site,
                rank=rank,
                points=points,
                author=author,
                page_number=page_number,
            )
        )

    more = soup.select_one("a.morelink")
    next_url: str | None = None
    if more and more.get("href"):
        next_url = _abs_url(more.get("href"))

    return stories, next_url


def parse_hn_comments(html: str) -> list[CommentRecord]:
    soup = BeautifulSoup(html, "html.parser")
    comments: list[CommentRecord] = []
    depth_stack: list[str] = []

    for tr in soup.select("tr.athing.comtr"):
        cid = (tr.get("id") or "").strip()
        if not cid:
            continue

        indent_img = tr.select_one("td.ind img")
        depth = int(indent_img.get("width", 0)) // 40 if indent_img else 0

        user_el = tr.select_one("a.hnuser")
        author = user_el.get_text(strip=True) if user_el else None

        age_el = tr.select_one("span.age a")
        commented_at = age_el.get_text(strip=True) if age_el else None

        text_el = tr.select_one("div.commtext")
        body = text_el.get_text(separator="\n", strip=True) if text_el else ""

        while len(depth_stack) > depth:
            depth_stack.pop()
        parent_id = depth_stack[-1] if depth_stack else None
        depth_stack.append(cid)

        comments.append(
            CommentRecord(
                comment_id=cid,
                parent_id=parent_id,
                author=author,
                depth=depth,
                body=body,
                commented_at=commented_at,
            )
        )

    return comments


def parse_page_meta(html: str) -> PageMeta:
    """Extract OpenGraph metadata from an arbitrary HTML page."""
    soup = BeautifulSoup(html, "html.parser")

    def _og(prop: str) -> str | None:
        tag = soup.find("meta", property=f"og:{prop}")
        if tag and tag.get("content"):
            return tag["content"].strip()
        return None

    return PageMeta(
        og_title=_og("title"),
        og_description=_og("description"),
        og_image=_og("image"),
        og_site_name=_og("site_name"),
    )
