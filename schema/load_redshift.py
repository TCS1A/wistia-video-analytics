import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

REDSHIFT_HOST     = os.getenv('REDSHIFT_HOST')
REDSHIFT_DB       = os.getenv('REDSHIFT_DB', 'wistia_db')
REDSHIFT_USER     = os.getenv('REDSHIFT_USER', 'admin')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_PORT     = int(os.getenv('REDSHIFT_PORT', 5439))
S3_PROCESSED      = os.getenv('S3_PROCESSED_BUCKET', 'wistia-analytics-processed-joe')
IAM_ROLE          = os.getenv('IAM_ROLE_ARN', 'arn:aws:iam::617513570720:role/WistiaGlueRole')

DDL = """
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
);
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
);
CREATE TABLE IF NOT EXISTS wistia.fact_engagement (
    media_id       VARCHAR(50),
    visitor_id     VARCHAR(200),
    date           VARCHAR(50),
    load_count     INTEGER,
    play_count     INTEGER,
    last_active_at VARCHAR(50),
    ingested_at    VARCHAR(50)
);
"""

COPY_COMMANDS = [
    ("dim_media",       f"COPY wistia.dim_media FROM 's3://{S3_PROCESSED}/dim_media/' IAM_ROLE '{IAM_ROLE}' FORMAT AS PARQUET;"),
    ("dim_visitor",     f"COPY wistia.dim_visitor FROM 's3://{S3_PROCESSED}/dim_visitor/' IAM_ROLE '{IAM_ROLE}' FORMAT AS PARQUET;"),
    ("fact_engagement", f"COPY wistia.fact_engagement FROM 's3://{S3_PROCESSED}/fact_engagement/' IAM_ROLE '{IAM_ROLE}' FORMAT AS PARQUET;"),
]

VALIDATION_QUERIES = {
    "dim_media rows":       "SELECT COUNT(*) FROM wistia.dim_media;",
    "dim_visitor rows":     "SELECT COUNT(*) FROM wistia.dim_visitor;",
    "fact_engagement rows": "SELECT COUNT(*) FROM wistia.fact_engagement;",
    "top 5 visitors": """
        SELECT visitor_id, browser, platform, play_count
        FROM wistia.dim_visitor
        ORDER BY play_count DESC LIMIT 5;
    """,
}

def get_connection():
    log.info(f'Connecting to Redshift: {REDSHIFT_HOST}')
    return psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD, connect_timeout=30, sslmode='require'
    )

def execute_ddl(conn):
    log.info('Creating schema and tables...')
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log.info('DDL complete.')

def truncate_tables(conn):
    log.info('Truncating tables for full reload...')
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE wistia.fact_engagement;")
        cur.execute("TRUNCATE TABLE wistia.dim_visitor;")
        cur.execute("TRUNCATE TABLE wistia.dim_media;")
    conn.commit()

def run_copy(conn):
    for table, cmd in COPY_COMMANDS:
        log.info(f'Loading {table} from S3...')
        with conn.cursor() as cur:
            cur.execute(cmd)
        conn.commit()
        log.info(f'{table} loaded.')

def run_validation(conn):
    log.info('=== Validation ===')
    with conn.cursor() as cur:
        for name, query in VALIDATION_QUERIES.items():
            cur.execute(query)
            log.info(f'[{name}]: {cur.fetchall()}')

def run():
    log.info('=== Redshift Load Pipeline Starting ===')
    if not REDSHIFT_HOST:
        raise ValueError('REDSHIFT_HOST not set in .env')
    if not REDSHIFT_PASSWORD:
        raise ValueError('REDSHIFT_PASSWORD not set in .env')
    conn = get_connection()
    try:
        execute_ddl(conn)
        truncate_tables(conn)
        run_copy(conn)
        run_validation(conn)
    finally:
        conn.close()
    log.info('=== Redshift Load Pipeline Complete ===')

if __name__ == '__main__':
    run()
