Hacker News scraper built on Temporal, Playwright, and Postgres. Scrapes stories, comments, and OpenGraph metadata from linked articles.

## Setup

```bash
docker compose up -d
```

```bash
docker compose logs worker  # look for "Worker started"
```

## Run a scrape

```bash
./scrape.sh
```

Configure with env vars:

```bash
HN_MAX_PAGES=1 SCRAPE_COMMENTS=false ./scrape.sh
HN_MAX_PAGES=10 HN_START_URL=https://news.ycombinator.com/newest ./scrape.sh
```

| Flag | Default | Description |
|---|---|---|
| `HN_MAX_PAGES` | `3` | Listing pages to scrape |
| `SCRAPE_COMMENTS` | `true` | Scrape comment threads |
| `HN_START_URL` | `https://news.ycombinator.com/` | Starting URL |

## Ad-hoc queries

Temporal UI: http://localhost:8233

```bash
export PGDSN="postgresql://temporal:temporal@localhost:5432/temporal"

# Stories from latest run
psql "$PGDSN" -c "SELECT rank_on_page AS rank, title, points, site FROM hn_stories WHERE workflow_run_id = (SELECT workflow_run_id FROM hn_stories ORDER BY scraped_at DESC LIMIT 1) ORDER BY rank_on_page;"

# OG metadata
psql "$PGDSN" -c "SELECT title, og_site_name AS source, left(og_description, 80) AS description FROM hn_stories WHERE og_description IS NOT NULL ORDER BY scraped_at DESC LIMIT 20;"

# Comment counts per story
psql "$PGDSN" -c "SELECT s.title, count(c.id) AS comments FROM hn_stories s JOIN hn_comments c ON c.story_item_id = s.item_id AND c.workflow_run_id = s.workflow_run_id GROUP BY s.title ORDER BY comments DESC LIMIT 15;"
```

## Tests

```bash
uv sync --all-extras
uv run pytest
```

## Architecture

Each scrape runs as a single Temporal workflow (`HackerNewsScrapeWorkflow`) that orchestrates six activities:

| Activity | Does |
|---|---|
| `ScrapePageActivity` | Fetches an HN listing page via Playwright, parses stories + "More" link |
| `ScrapeCommentsActivity` | Fetches a story's comment thread via Playwright |
| `ScrapePageMetaActivity` | Visits each story's external URL and extracts OpenGraph meta tags |
| `PersistStoriesActivity` | Inserts stories to Postgres |
| `PersistCommentsActivity` | Inserts comments to Postgres |
| `PersistPageMetaActivity` | Updates stories with OG metadata (title, description, image, site name) |

