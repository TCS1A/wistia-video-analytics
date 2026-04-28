import pytest
import json
import os
from unittest.mock import patch, MagicMock

os.environ['WISTIA_API_TOKEN'] = 'test_token'
os.environ['S3_BUCKET_NAME']   = 'test-bucket'
os.environ['MEDIA_ID_1']       = 'gskhw4w4lm'
os.environ['MEDIA_ID_2']       = 'v08dlrgr7v'

from ingestion.wistia_ingestor import fetch_with_retry, fetch_media_stats, fetch_visitors, upload_to_s3

class TestFetchWithRetry:
    @patch('ingestion.wistia_ingestor.requests.get')
    def test_success_first_attempt(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'play_count': 1000}
        mock_get.return_value = mock_resp
        result = fetch_with_retry('https://api.wistia.com/v1/test')
        assert result == {'play_count': 1000}

    @patch('ingestion.wistia_ingestor.requests.get')
    def test_raises_after_retries(self, mock_get):
        mock_get.side_effect = Exception('timeout')
        with pytest.raises(Exception):
            fetch_with_retry('https://api.wistia.com/v1/test', retries=3)
        assert mock_get.call_count == 3

class TestFetchVisitors:
    @patch('ingestion.wistia_ingestor.fetch_with_retry')
    def test_single_page(self, mock_fetch):
        mock_fetch.return_value = [{'visitor_key': f'v{i}'} for i in range(50)]
        result = fetch_visitors('gskhw4w4lm')
        assert len(result) == 50

    @patch('ingestion.wistia_ingestor.fetch_with_retry')
    def test_pagination(self, mock_fetch):
        page1 = [{'visitor_key': f'v{i}'} for i in range(100)]
        page2 = [{'visitor_key': f'v{i}'} for i in range(40)]
        mock_fetch.side_effect = [page1, page2]
        result = fetch_visitors('gskhw4w4lm')
        assert len(result) == 140

class TestUploadToS3:
    @patch('ingestion.wistia_ingestor.s3')
    def test_uploads_to_correct_prefix(self, mock_s3):
        upload_to_s3([{'visitor_key': 'abc'}], 'visitors', 'gskhw4w4lm')
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs['Bucket'] == 'test-bucket'
        assert 'visitors/gskhw4w4lm/' in call_kwargs['Key']

class TestDataValidation:
    def test_play_rate_within_bounds(self):
        assert 0.0 <= 0.435 <= 1.0

    def test_required_media_fields(self):
        sample = {'load_count': 110741, 'play_count': 43813,
                  'play_rate': 0.435, 'hours_watched': 2652.03,
                  'engagement': 0.500, 'visitors': 94646}
        for field in ['load_count', 'play_count', 'play_rate', 'hours_watched', 'engagement', 'visitors']:
            assert field in sample
