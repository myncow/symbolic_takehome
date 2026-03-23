CREATE TABLE IF NOT EXISTS hn_stories (
    id              BIGSERIAL PRIMARY KEY,
    workflow_run_id TEXT NOT NULL,
    item_id         TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT NOT NULL,
    comments_url    TEXT,
    site            TEXT,
    rank_on_page    INT,
    points          INT,
    author          TEXT,
    page_number     INT NOT NULL,
    scraped_at      TIMESTAMPTZ NOT NULL,
    og_title        TEXT,
    og_description  TEXT,
    og_image        TEXT,
    og_site_name    TEXT,
    UNIQUE (workflow_run_id, item_id)
);

CREATE TABLE IF NOT EXISTS hn_comments (
    id              BIGSERIAL PRIMARY KEY,
    workflow_run_id TEXT NOT NULL,
    story_item_id   TEXT NOT NULL,
    comment_id      TEXT NOT NULL,
    parent_id       TEXT,
    author          TEXT,
    depth           INT NOT NULL DEFAULT 0,
    body            TEXT NOT NULL,
    commented_at    TEXT,
    scraped_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (workflow_run_id, comment_id)
);
