"""
Configuration file for pytest

This file contains pytest fixtures and configuration settings.
"""

import os
import pytest
import tempfile
import shutil
from unittest.mock import patch

# Add more pytest plugins as needed
pytest_plugins = [
    "pytest-mock",
]

# Define some constants for tests
TEST_ARXIV_ID = '2101.12345'
TEST_PDF_CONTENT = b'%PDF-1.5\nTest PDF content\n%%EOF'

@pytest.fixture(scope="session")
def test_env():
    """Set environment variables for testing."""
    old_env = dict(os.environ)
    os.environ["TESTING"] = "true"
    os.environ["ADMIN_KEY"] = "test_admin_key"
    yield
    os.environ.clear()
    os.environ.update(old_env)

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def test_pdf_file(temp_dir):
    """Create a test PDF file in the temporary directory."""
    file_path = os.path.join(temp_dir, f"{TEST_ARXIV_ID}.pdf")
    with open(file_path, 'wb') as f:
        f.write(TEST_PDF_CONTENT)
    yield file_path
    # Cleanup handled by temp_dir fixture

@pytest.fixture
def disable_network_calls():
    """Disable all network calls during tests."""
    with patch('requests.Session.get') as mock_get, \
         patch('requests.Session.post') as mock_post, \
         patch('requests.get') as mock_direct_get, \
         patch('requests.post') as mock_direct_post:
        
        mock_get.side_effect = Exception("Network calls are disabled in tests")
        mock_post.side_effect = Exception("Network calls are disabled in tests")
        mock_direct_get.side_effect = Exception("Network calls are disabled in tests")
        mock_direct_post.side_effect = Exception("Network calls are disabled in tests")
        
        yield
