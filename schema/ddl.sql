CREATE SCHEMA IF NOT EXISTS wistia;

CREATE TABLE IF NOT EXISTS wistia.dim_media (
    media_id             VARCHAR(50) PRIMARY KEY,
    play_count           INTEGER,
    play_rate            FLOAT,
    total_watch_time     FLOAT,
    avg_engagement_rate  FLOAT,
    visitor_count        INTEGER,
    load_count           INTEGER,
    ingested_at          VARCHAR(50)
) DISTSTYLE ALL SORTKEY (media_id);

CREATE TABLE IF NOT EXISTS wistia.dim_visitor (
    visitor_id      VARCHAR(200) PRIMARY KEY,
    first_seen_at   VARCHAR(50),
    last_active_at  VARCHAR(50),
    load_count      INTEGER,
    play_count      INTEGER,
    browser         VARCHAR(100),
    browser_version VARCHAR(50),
    platform        VARCHAR(100),
    is_mobile       BOOLEAN,
    visitor_name    VARCHAR(200),
    visitor_email   VARCHAR(200),
    ingested_at     VARCHAR(50)
) DISTSTYLE KEY DISTKEY (visitor_id) SORTKEY (visitor_id);

CREATE TABLE IF NOT EXISTS wistia.fact_engagement (
    media_id       VARCHAR(50),
    visitor_id     VARCHAR(200),
    date           VARCHAR(50),
    load_count     INTEGER,
    play_count     INTEGER,
    last_active_at VARCHAR(50),
    ingested_at    VARCHAR(50)
) DISTSTYLE KEY DISTKEY (visitor_id) SORTKEY (date, media_id);

-- Row count validation
SELECT 'dim_media'       AS table_name, COUNT(*) AS rows FROM wistia.dim_media
UNION ALL
SELECT 'dim_visitor'     AS table_name, COUNT(*) AS rows FROM wistia.dim_visitor
UNION ALL
SELECT 'fact_engagement' AS table_name, COUNT(*) AS rows FROM wistia.fact_engagement;
