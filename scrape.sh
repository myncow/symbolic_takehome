#!/usr/bin/env bash
set -euo pipefail

docker compose run --rm \
  ${HN_MAX_PAGES:+-e HN_MAX_PAGES="$HN_MAX_PAGES"} \
  ${SCRAPE_COMMENTS:+-e SCRAPE_COMMENTS="$SCRAPE_COMMENTS"} \
  ${HN_START_URL:+-e HN_START_URL="$HN_START_URL"} \
  starter

PGDSN="postgresql://temporal:temporal@localhost:5432/temporal"

echo ""
psql "$PGDSN" -c "
  SELECT rank_on_page AS rank, title, points, site,
         left(og_description, 60) AS og_description
  FROM hn_stories
  WHERE workflow_run_id = (SELECT workflow_run_id FROM hn_stories ORDER BY scraped_at DESC LIMIT 1)
  ORDER BY rank_on_page;
"
