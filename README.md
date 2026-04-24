## Wistia Video Analytics Pipeline

## Overview
An automated data pipeline that ingests Wistia video engagement data, transforms it using PySpark, and loads it into Amazon Redshift for analytics.

## Architecture wistia-video-analytics

Wistia API → Python Ingestor → S3 Raw → AWS Glue (PySpark) → S3 Processed → Redshift
↑
GitHub Actions (CI/CD - daily 6am UTC)

## Tech Stack
- **Ingestion:** Python, Wistia Stats API
- **Storage:** Amazon S3
- **Transformation:** AWS Glue, PySpark
- **Warehouse:** Amazon Redshift Serverless
- **CI/CD:** GitHub Actions

## Data Model
- `wistia.dim_media` — aggregate stats per video (2 records)
- `wistia.dim_visitor` — one row per unique visitor with browser/platform details
- `wistia.fact_engagement` — 222,618 visitor-media interaction records

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/TCS1A/wistia-video-analytics.git
cd wistia-video-analytics
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Fill in your values in .env
```

### 4. Required environment variables
WISTIA_API_TOKEN=
MEDIA_ID_1=
MEDIA_ID_2=
AWS_REGION=
S3_BUCKET_NAME=
REDSHIFT_HOST=
REDSHIFT_DB=
REDSHIFT_USER=
REDSHIFT_PASSWORD=

### 5. Run ingestion locally
```bash
python ingestion/wistia_ingestor.py
```

## CI/CD
GitHub Actions runs the full pipeline daily at 6am UTC:
1. Fetches latest data from Wistia API
2. Uploads raw JSON to S3
3. Triggers AWS Glue PySpark transformation
4. Waits for Glue job to complete
5. Data is available in Redshift

## Project Structure

wistia-video-analytics/
├── .github/workflows/
│   └── pipeline.yml        # CI/CD workflow
├── ingestion/
│   ├── wistia_ingestor.py  # API fetch + S3 upload
│   └── test_connection.py  # API connection test
├── transformation/
│   └── spark_transform.py  # PySpark Glue job
├── schema/
│   └── ddl.sql             # Redshift table definitions
├── requirements.txt
└── README.md

## Trade-offs & Design Decisions
- **Serverless Redshift** over provisioned cluster — lower cost for dev/analytics workloads
- **Parquet format** for processed data — columnar storage optimised for Redshift COPY
- **Parallel ingestion** using ThreadPoolExecutor — reduced runtime from hours to minutes
- **Incremental state tracking** — S3 state files track last run timestamp per media
- **VARCHAR timestamps** in Redshift — avoids Parquet/Redshift timestamp type conflicts