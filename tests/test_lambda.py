import pytest
import os
import json
import datetime
from unittest import mock
from moto import mock_aws
import logging # Import logging to capture logs

# Ensure the lambda module can be imported from the parent directory
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Now import the lambda handler
from pipeline_lambda import lambda_handler, FILE_PATH_TED_JSON_TMP # Import constant for checking

BUCKET_NAME = "azao-prospect-radar"

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

@pytest.fixture
def lambda_env_vars():
    """Mocked environment variables for the Lambda function."""
    env_vars = {
        "AZAO_S3_BUCKET_NAME": BUCKET_NAME,
        "AZAO_TED_API_KEY": "fake-ted-api-key",
        "TED_API_ENDPOINT": "https://mock.ted.europa.eu", # Base URL for TED
        "TED_SEARCH_PATH": "/v3/notices/search" # Default search path
    }
    with mock.patch.dict(os.environ, env_vars):
        yield

@pytest.fixture
def mock_s3_bucket(aws_credentials):
    """Create a mock S3 bucket using moto."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
        s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
        yield s3

@pytest.fixture
def mock_requests_get_decp_success():
    """Mock requests.get for successful DECP calls."""
    with mock.patch("requests.get") as mock_get:
        csv_content = "titulaire_id_1;titulaire_id_2;titulaire_id_3\n123456789;;\n;987654321;"
        gzipped_content = gzip.compress(csv_content.encode("utf-8"))
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = gzipped_content
        mock_response.headers = {"Content-Encoding": "gzip"}
        mock_response.raise_for_status = mock.Mock()
        mock_get.return_value = mock_response
        yield mock_get

@pytest.fixture
def mock_requests_post_ted_success():
    """Mock requests.post for successful TED API calls."""
    with mock.patch("requests.post") as mock_post:
        mock_response = mock.Mock()
        mock_response.status_code = 200
        # Simulate one page of results, then less than page_size to stop pagination
        mock_post.side_effect = [
            mock.Mock(status_code=200, json=lambda: {"results": [{"id": f"ted-notice-{i}"} for i in range(100)]}), # Page 1 full
            mock.Mock(status_code=200, json=lambda: {"results": [{"id": "ted-notice-final"}]}) # Page 2 not full
        ]
        yield mock_post

@pytest.fixture
def mock_requests_post_ted_error_400():
    """Mock requests.post for TED API calls to simulate a 400 error."""
    with mock.patch("requests.post") as mock_post:
        mock_response = mock.Mock()
        mock_response.status_code = 400
        mock_response.text = "Simulated TED API 400 Error: Bad Request"
        # raise_for_status would typically be called by the code if not handled, 
        # but our code should handle 4xx/5xx gracefully.
        # So, we don't need mock_response.raise_for_status to raise an error here.
        mock_post.return_value = mock_response
        yield mock_post

# Need to import these after sys.path is modified and before they are used in mocks
import boto3 # Should be picked up by moto
import gzip

def test_lambda_handler_success(
    lambda_env_vars, 
    mock_s3_bucket, 
    mock_requests_get_decp_success, 
    mock_requests_post_ted_success, # Use success mock for TED here
    caplog # Pytest fixture to capture logs
):
    """Test the lambda_handler for a successful execution including TED pagination."""
    caplog.set_level(logging.INFO) # Capture INFO level logs and above
    event = {}
    context = {}

    response = lambda_handler(event, context)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "Pipeline execution finished" in body["message"]
    assert body["leads_found"] == 2 # From the mock DECP data
    assert body["log_file_s3_key"].startswith(f"logs/azao_log_")

    tmp_files = os.listdir("/tmp")
    assert any(f.startswith("azao_leads_") and f.endswith(".csv") for f in tmp_files)
    assert any(f.startswith("azao_log_") and f.endswith(".json") for f in tmp_files)
    assert any(f.startswith("decp_data_30_days") and f.endswith(".csv") for f in tmp_files)
    assert any(f.startswith("ted_results") and f.endswith(".json") for f in tmp_files) # TED file should be created

    s3_client = boto3.client("s3")
    listed_objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="logs/")
    assert "Contents" in listed_objects
    s3_files = [obj["Key"] for obj in listed_objects["Contents"]]
    
    assert len(s3_files) >= 4 # Expecting leads CSV, log JSON, DECP CSV, and TED JSON.
    assert any(f.startswith("logs/azao_leads_") for f in s3_files)
    assert any(f.startswith("logs/azao_log_") for f in s3_files)
    assert any(f.startswith("logs/decp_data_") for f in s3_files)
    assert any(f.startswith("logs/ted_results_") for f in s3_files)

    # Check logs for TED pagination info
    assert "Calling TED API page 1" in caplog.text
    assert "Calling TED API page 2" in caplog.text
    assert "100 notices received from TED for page 1" in caplog.text
    assert "1 notices received from TED for page 2" in caplog.text
    assert "Last page of TED results reached (items < page_size)." in caplog.text

def test_lambda_handler_ted_api_error_400(
    lambda_env_vars, 
    mock_s3_bucket, 
    mock_requests_get_decp_success, 
    mock_requests_post_ted_error_400, # Use 400 error mock for TED
    caplog # Pytest fixture to capture logs
):
    """Test lambda_handler when TED API returns a 400 error."""
    caplog.set_level(logging.INFO) # Capture INFO level logs and above
    event = {}
    context = {}

    response = lambda_handler(event, context)

    # Pipeline should still complete successfully overall
    assert response["statusCode"] == 200 
    body = json.loads(response["body"])
    assert "Pipeline execution finished" in body["message"]
    # Leads found should still be from DECP, as TED failed
    assert body["leads_found"] == 2 # From the mock DECP data
    assert body["log_file_s3_key"].startswith(f"logs/azao_log_")

    # Verify logs show the TED API error
    assert "Erreur API TED (400)" in caplog.text
    assert "Simulated TED API 400 Error: Bad Request" in caplog.text
    assert "Erreur API TED (400) for page 1" in caplog.text # Error on the first page attempt

    # Verify DECP data was processed and uploaded
    tmp_files = os.listdir("/tmp")
    assert any(f.startswith("decp_data_30_days") and f.endswith(".csv") for f in tmp_files)
    assert any(f.startswith("azao_leads_") and f.endswith(".csv") for f in tmp_files) # Leads from DECP
    
    s3_client = boto3.client("s3")
    listed_objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="logs/")
    assert "Contents" in listed_objects
    s3_files = [obj["Key"] for obj in listed_objects["Contents"]]
    assert any(f.startswith("logs/decp_data_") for f in s3_files)
    assert any(f.startswith("logs/azao_leads_") for f in s3_files)
    assert any(f.startswith("logs/azao_log_") for f in s3_files)

    # Verify TED data file was NOT created or uploaded due to the error
    assert not any(f.startswith("ted_results") and f.endswith(".json") for f in tmp_files)
    assert not any(f.startswith("logs/ted_results_") for f in s3_files)
    # Check the log summary in S3 for TED collection status
    log_file_key = body["log_file_s3_key"]
    log_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=log_file_key)
    log_content = json.loads(log_object["Body"].read().decode("utf-8"))
    assert log_content["ted_data_collection"]["file_uploaded_to_s3"] is False
    assert log_content["ted_data_collection"]["results_processed"] == 0

if __name__ == "__main__":
    print("To run tests, use 'pytest -q' in the terminal in the 'azao_starter_ci' directory.")

