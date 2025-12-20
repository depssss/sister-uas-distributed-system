import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture(autouse=True)
def mock_services():
    with patch('redis.Redis') as mock_r, patch('psycopg2.connect') as mock_db:
        mock_r.return_value.ping.return_value = True
        mock_r.return_value.exists.return_value = 0
        yield mock_r

@pytest.fixture
def client():
    from main import app
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client