import requests
import boto3
import json
import os
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

API_TOKEN = os.getenv('WISTIA_API_TOKEN')
MEDIA_IDS = [os.getenv('MEDIA_ID_1'), os.getenv('MEDIA_ID_2')]
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
HEADERS   = {'Authorization': f'Bearer {API_TOKEN}'}
BASE_URL  = 'https://api.wistia.com/v1'

s3 = boto3.client('s3')

def fetch_with_retry(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f'Attempt {attempt+1}/{retries} failed: {e}')
            if attempt == retries - 1:
                raise

def fetch_media_stats(media_id):
    url = f'{BASE_URL}/stats/medias/{media_id}.json'
    data = fetch_with_retry(url)
    log.info(f'[{media_id}] Media stats fetched: play_count={data.get("play_count")}')
    return data

def fetch_visitors(media_id):
    """Fetch all visitors for the account with pagination."""
    visitors, page = [], 1
    while True:
        url = f'{BASE_URL}/stats/visitors.json'
        data = fetch_with_retry(url, params={'page': page, 'per_page': 100})
        if not data:
            break
        visitors.extend(data)
        log.info(f'[{media_id}] Page {page}: {len(data)} visitors (total: {len(visitors)})')
        if len(data) < 100:
            break
        page += 1
    return visitors


def get_last_run_timestamp(media_id):
    key = f'state/{media_id}/last_run.json'
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj['Body'].read())['last_run']
    except:
        return None

def save_run_timestamp(media_id):
    key = f'state/{media_id}/last_run.json'
    ts  = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps({'last_run': ts}))
    log.info(f'[{media_id}] Saved run timestamp: {ts}')

def upload_to_s3(data, prefix, media_id):
    ts  = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    key = f'{prefix}/{media_id}/{ts}.json'
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data, default=str))
    log.info(f'Uploaded -> s3://{S3_BUCKET}/{key}')

from concurrent.futures import ThreadPoolExecutor, as_completed

def process_media(media_id):
    if not media_id:
        log.warning('Skipping None media_id')
        return
    log.info(f'--- Processing media: {media_id} ---')
    stats    = fetch_media_stats(media_id)
    visitors = fetch_visitors(media_id)
    upload_to_s3(stats,    'media',    media_id)
    upload_to_s3(visitors, 'visitors', media_id)
    save_run_timestamp(media_id)
    log.info(f'[{media_id}] Done')

def run():
    log.info('=== Wistia Ingestion Pipeline Starting ===')
    if not API_TOKEN:
        raise ValueError('WISTIA_API_TOKEN is not set in .env')
    if not S3_BUCKET:
        raise ValueError('S3_BUCKET_NAME is not set in .env')

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(process_media, mid): mid for mid in MEDIA_IDS if mid}
        for future in as_completed(futures):
            mid = futures[future]
            try:
                future.result()
            except Exception as e:
                log.error(f'[{mid}] Failed: {e}')
                raise

    log.info('=== Ingestion Pipeline Complete ===')
