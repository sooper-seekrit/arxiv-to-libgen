#!/usr/bin/env python3
"""
Tests for ArXiv to LibGen Backup Tool and Coordination Server; mocks external dependencies.

To run:
    pytest -xvs test_arxiv_backup.py
"""

import os
import time
import json
import tempfile
import threading
import shutil
import pytest
import requests
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock, Mock
from io import BytesIO

# Import the modules to test
# Assuming the main file is named arxiv_backup.py and the server is coordination_server.py
import sys
sys.path.append('.')
try:
    from arxiv_backup import (ArxivClient, LibgenClient, TorNetwork, MemoryMonitor, 
                           CoordinationClient, ArxivToLibgenWorker, TorrentCoordinator)
    from coordination_server import app as coordination_app
except ImportError:
    # If imported as a script, create dummy classes for type hints
    class ArxivClient: pass
    class LibgenClient: pass
    class TorNetwork: pass
    class MemoryMonitor: pass
    class CoordinationClient: pass
    class ArxivToLibgenWorker: pass
    class TorrentCoordinator: pass
    coordination_app = None

# Test constants
TEST_ARXIV_ID = '2101.12345'
TEST_PDF_CONTENT = b'%PDF-1.5\nTest PDF content\n%%EOF'
TEST_XML_RESPONSE = f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/{TEST_ARXIV_ID}</id>
    <title>Test Paper Title</title>
    <summary>This is a test paper abstract</summary>
    <author>
      <name>Test Author</name>
    </author>
    <category term="cs.AI"/>
    <published>2021-01-01T00:00:00Z</published>
    <updated>2021-01-02T00:00:00Z</updated>
  </entry>
</feed>
"""


# ------------------- Fixtures -------------------

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_arxiv_response():
    """Mock response for ArXiv API."""
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.content = TEST_XML_RESPONSE.encode('utf-8')
    return mock_response


@pytest.fixture
def mock_pdf_response():
    """Mock response for PDF download."""
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.iter_content = lambda chunk_size: [TEST_PDF_CONTENT]
    return mock_response


@pytest.fixture
def mock_libgen_response():
    """Mock response for LibGen API."""
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = []  # Empty list means paper doesn't exist
    return mock_response


@pytest.fixture
def mock_arxiv_client(mock_arxiv_response, mock_pdf_response):
    """Mock ArXiv client."""
    with patch('arxiv_backup.ArxivClient', autospec=True) as MockArxivClient:
        client = MockArxivClient.return_value
        client.get_paper_metadata.return_value = {
            'id': TEST_ARXIV_ID,
            'title': 'Test Paper Title',
            'abstract': 'This is a test paper abstract',
            'authors': ['Test Author'],
            'categories': ['cs.AI'],
            'published': '2021-01-01T00:00:00Z',
            'updated': '2021-01-02T00:00:00Z',
            'pdf_url': f'https://arxiv.org/pdf/{TEST_ARXIV_ID}.pdf'
        }
        client.download_paper.return_value = f'/tmp/{TEST_ARXIV_ID}.pdf'
        client.search.return_value = ET.fromstring(TEST_XML_RESPONSE)
        client.session = Mock()
        client.session.get.return_value = mock_arxiv_response
        yield client


@pytest.fixture
def mock_libgen_client(mock_libgen_response):
    """Mock LibGen client."""
    with patch('arxiv_backup.LibgenClient', autospec=True) as MockLibgenClient:
        client = MockLibgenClient.return_value
        client.check_if_exists.return_value = False
        client.calculate_md5.return_value = 'test_md5_hash'
        client.extract_pdf_metadata.return_value = {
            'title': 'Test Paper Title',
            'author': 'Test Author',
            'num_pages': 10
        }
        client.upload_to_libgen.return_value = True
        client.session = Mock()
        client.session.get.return_value = mock_libgen_response
        client.session.post.return_value = Mock(status_code=200)
        yield client


@pytest.fixture
def mock_tor_handler():
    """Mock Tor network handler."""
    with patch('arxiv_backup.TorNetwork', autospec=True) as MockTorNetwork:
        handler = MockTorNetwork.return_value
        handler.setup_tor_proxy.return_value = requests.Session()
        handler.new_identity.return_value = True
        yield handler


@pytest.fixture
def mock_memory_monitor():
    """Mock memory monitor."""
    with patch('arxiv_backup.MemoryMonitor', autospec=True) as MockMemoryMonitor:
        monitor = MockMemoryMonitor.return_value
        monitor.current_usage = 0.5  # 50% usage
        monitor.wait_for_resources.return_value = None
        yield monitor


@pytest.fixture
def mock_coordination_client():
    """Mock coordination client."""
    with patch('arxiv_backup.CoordinationClient', autospec=True) as MockCoordinationClient:
        client = MockCoordinationClient.return_value
        client.register_paper.return_value = True
        client.complete_paper.return_value = True
        yield client


@pytest.fixture
def coordination_server():
    """Run the coordination server in a separate thread."""
    if coordination_app is None:
        pytest.skip("Coordination server module not found")
        
    # Set up test client
    coordination_app.config['TESTING'] = True
    client = coordination_app.test_client()
    
    # Start the server in a separate thread
    server_thread = threading.Thread(target=coordination_app.run, 
                                     kwargs={'host': 'localhost', 'port': 5001})
    server_thread.daemon = True
    server_thread.start()
    
    # Give the server time to start
    time.sleep(0.1)
    
    yield client


# ------------------- Unit Tests -------------------

class TestArxivClient:
    """Tests for the ArXiv client."""
    
    @patch('requests.Session.get')
    def test_search(self, mock_get, mock_arxiv_response):
        """Test ArXiv search functionality."""
        mock_get.return_value = mock_arxiv_response
        
        client = ArxivClient()
        result = client.search('test query')
        
        assert result is not None
        assert mock_get.called
        mock_get.assert_called_with(client.base_url, params={
            'search_query': 'test query',
            'start': 0,
            'max_results': 100
        })
    
    @patch('requests.Session.get')
    def test_download_paper(self, mock_get, mock_pdf_response, temp_dir):
        """Test downloading a paper."""
        mock_get.return_value = mock_pdf_response
        
        client = ArxivClient()
        result = client.download_paper(TEST_ARXIV_ID, output_dir=temp_dir)
        
        assert result is not None
        assert os.path.exists(result)
        assert mock_get.called
        mock_get.assert_called_with(f"{client.pdf_base_url}{TEST_ARXIV_ID}.pdf", stream=True)
        
        # Clean up
        os.remove(result)
    
    @patch('requests.Session.get')
    def test_get_paper_metadata(self, mock_get, mock_arxiv_response):
        """Test getting paper metadata."""
        mock_get.return_value = mock_arxiv_response
        
        client = ArxivClient()
        result = client.get_paper_metadata(TEST_ARXIV_ID)
        
        assert result is not None
        assert result['id'] == TEST_ARXIV_ID
        assert result['title'] == 'Test Paper Title'
        assert 'abstract' in result
        assert 'authors' in result
        assert 'categories' in result
        assert mock_get.called


class TestLibgenClient:
    """Tests for the LibGen client."""
    
    @patch('requests.Session.get')
    def test_check_if_exists(self, mock_get, mock_libgen_response):
        """Test checking if a paper exists in LibGen."""
        mock_get.return_value = mock_libgen_response
        
        client = LibgenClient('http://example.com/api', 'http://example.com/upload')
        result = client.check_if_exists(title='Test Paper')
        
        assert result is False  # Paper does not exist
        assert mock_get.called
        mock_get.assert_called_with('http://example.com/api', params={'title': 'Test Paper'})
    
    def test_calculate_md5(self, temp_dir):
        """Test MD5 calculation."""
        # Create test file
        test_file = os.path.join(temp_dir, 'test.pdf')
        with open(test_file, 'wb') as f:
            f.write(TEST_PDF_CONTENT)
        
        client = LibgenClient('http://example.com/api', 'http://example.com/upload')
        result = client.calculate_md5(test_file)
        
        assert result is not None
        assert len(result) == 32  # MD5 should be 32 hex characters
    
    @patch('arxiv_backup.PdfReader')
    def test_extract_pdf_metadata(self, mock_pdfreader, temp_dir):
        """Test PDF metadata extraction."""
        # Create test file
        test_file = os.path.join(temp_dir, 'test.pdf')
        with open(test_file, 'wb') as f:
            f.write(TEST_PDF_CONTENT)
        
        # Mock PdfReader
        mock_pdf = MagicMock()
        mock_pdf.metadata = {
            '/Title': 'Test Paper Title',
            '/Author': 'Test Author',
            '/Creator': 'Test Creator',
            '/Producer': 'Test Producer',
            '/Subject': 'Test Subject'
        }
        mock_pdf.pages = [None] * 10  # 10 pages
        mock_pdfreader.return_value = mock_pdf
        
        client = LibgenClient('http://example.com/api', 'http://example.com/upload')
        result = client.extract_pdf_metadata(test_file)
        
        assert result is not None
        assert result['title'] == 'Test Paper Title'
        assert result['author'] == 'Test Author'
        assert result['num_pages'] == 10
    
    @patch('requests.Session.post')
    @patch('builtins.open')
    def test_upload_to_libgen(self, mock_open, mock_post, mock_libgen_response):
        """Test uploading a paper to LibGen."""
        mock_post.return_value = Mock(status_code=200)
        mock_open.return_value = BytesIO(TEST_PDF_CONTENT)
        
        client = LibgenClient('http://example.com/api', 'http://example.com/upload')
        client.check_if_exists = Mock(return_value=False)
        client.calculate_md5 = Mock(return_value='test_md5_hash')
        client.extract_pdf_metadata = Mock(return_value={
            'title': 'Test Paper Title',
            'author': 'Test Author',
            'num_pages': 10
        })
        
        metadata = {
            'id': TEST_ARXIV_ID,
            'title': 'Test Paper Title',
            'abstract': 'This is a test paper abstract',
            'authors': ['Test Author'],
            'categories': ['cs.AI'],
            'published': '2021-01-01T00:00:00Z',
            'updated': '2021-01-02T00:00:00Z',
            'pdf_url': f'https://arxiv.org/pdf/{TEST_ARXIV_ID}.pdf'
        }
        
        result = client.upload_to_libgen('/tmp/test.pdf', metadata)
        
        assert result is True
        assert mock_post.called


class TestMemoryMonitor:
    """Tests for the memory monitor."""
    
    def test_wait_for_resources(self):
        """Test waiting for memory resources."""
        monitor = MemoryMonitor(max_usage_fraction=0.8)
        monitor.current_usage = 0.5  # 50% usage
        
        # Should not block
        monitor.wait_for_resources()
        
        # Set usage above threshold and test with timeout
        monitor.current_usage = 0.9  # 90% usage
        monitor.pause_event.set()
        
        # Create a thread to clear the pause event after a short delay
        def clear_pause():
            time.sleep(0.1)
            monitor.pause_event.clear()
        
        threading.Thread(target=clear_pause).start()
        
        start_time = time.time()
        monitor.wait_for_resources()
        elapsed = time.time() - start_time
        
        assert elapsed >= 0.1  # Should have waited
        
        # Clean up
        monitor.stop()


class TestCoordinationClient:
    """Tests for the coordination client."""
    
    @patch('requests.Session.post')
    def test_register_paper(self, mock_post):
        """Test registering a paper with the coordination server."""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        mock_post.return_value = mock_response
        
        client = CoordinationClient('http://example.com')
        result = client.register_paper(TEST_ARXIV_ID)
        
        assert result is True
        assert mock_post.called
        mock_post.assert_called_with(
            'http://example.com/register',
            json={"arxiv_id": TEST_ARXIV_ID}
        )
    
    @patch('requests.Session.post')
    def test_complete_paper(self, mock_post):
        """Test marking a paper as completed."""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        client = CoordinationClient('http://example.com')
        result = client.complete_paper(TEST_ARXIV_ID, success=True)
        
        assert result is True
        assert mock_post.called
        mock_post.assert_called_with(
            'http://example.com/complete',
            json={"arxiv_id": TEST_ARXIV_ID, "success": True}
        )


# ------------------- Integration Tests -------------------

class TestArxivToLibgenWorker:
    """Integration tests for the ArXiv to LibGen worker."""
    
    def test_init(self, mock_tor_handler, mock_memory_monitor, mock_coordination_client):
        """Test worker initialization."""
        config = type('Config', (), {
            'use_tor': True,
            'tor_socks_port': 9050,
            'tor_control_port': 9051,
            'tor_control_password': 'password',
            'use_coordination_server': True,
            'coordination_server': 'http://example.com',
            'max_workers': 2,
            'max_memory_usage': 0.8,
            'memory_check_interval': 5,
            'wait_between_requests': 1.0,
            'libgen_api_endpoint': 'http://example.com/api',
            'libgen_upload_endpoint': 'http://example.com/upload',
            'keep_files': False,
            'output_dir': '/tmp',
            'use_torrents': False,
            'torrent_dir': '/tmp/torrents',
            'torrent_announce': None
        })
        
        worker = ArxivToLibgenWorker(config)
        
        assert worker.config == config
        assert worker.tor_handler is not None
        assert worker.memory_monitor is not None
        assert worker.coordination_client is not None
        assert worker.arxiv_client is not None
        assert worker.libgen_client is not None
    
    def test_process_paper(self, mock_arxiv_client, mock_libgen_client, 
                          mock_memory_monitor, mock_coordination_client):
        """Test processing a paper."""
        config = type('Config', (), {
            'use_tor': False,
            'use_coordination_server': True,
            'coordination_server': 'http://example.com',
            'max_workers': 2,
            'max_memory_usage': 0.8,
            'memory_check_interval': 5,
            'wait_between_requests': 0.1,
            'libgen_api_endpoint': 'http://example.com/api',
            'libgen_upload_endpoint': 'http://example.com/upload',
            'keep_files': False,
            'output_dir': '/tmp',
            'use_torrents': False,
            'torrent_dir': '/tmp/torrents',
            'torrent_announce': None
        })
        
        # Create worker with mocked components
        worker = ArxivToLibgenWorker(config)
        worker.arxiv_client = mock_arxiv_client
        worker.libgen_client = mock_libgen_client
        worker.memory_monitor = mock_memory_monitor
        worker.coordination_client = mock_coordination_client
        
        # Mock _process_single_paper method
        worker._process_single_paper = Mock(return_value=True)
        
        # Start worker and process paper
        worker.start()
        worker.process_paper(TEST_ARXIV_ID)
        
        # Wait for the queue to be processed
        worker.work_queue.join()
        
        # Verify
        worker._process_single_paper.assert_called_with(TEST_ARXIV_ID)
        
        # Clean up
        worker.stop()
    
    @patch('tempfile.mkdtemp')
    @patch('os.remove')
    @patch('shutil.rmtree')
    def test_process_single_paper(self, mock_rmtree, mock_remove, mock_mkdtemp, 
                                 mock_arxiv_client, mock_libgen_client, 
                                 mock_memory_monitor, mock_coordination_client):
        """Test processing a single paper from download to upload."""
        # Set up mocks
        mock_mkdtemp.return_value = '/tmp/test_dir'
        
        config = type('Config', (), {
            'use_tor': False,
            'use_coordination_server': True,
            'coordination_server': 'http://example.com',
            'max_workers': 2,
            'max_memory_usage': 0.8,
            'memory_check_interval': 5,
            'wait_between_requests': 0.1,
            'libgen_api_endpoint': 'http://example.com/api',
            'libgen_upload_endpoint': 'http://example.com/upload',
            'keep_files': False,
            'output_dir': '/tmp',
            'use_torrents': False,
            'torrent_dir': '/tmp/torrents',
            'torrent_announce': None
        })
        
        # Create worker with mocked components
        worker = ArxivToLibgenWorker(config)
        worker.arxiv_client = mock_arxiv_client
        worker.libgen_client = mock_libgen_client
        worker.memory_monitor = mock_memory_monitor
        worker.coordination_client = mock_coordination_client
        
        # Process the paper
        result = worker._process_single_paper(TEST_ARXIV_ID)
        
        # Verify
        assert result is True
        mock_arxiv_client.get_paper_metadata.assert_called_with(TEST_ARXIV_ID)
        mock_arxiv_client.download_paper.assert_called_with(TEST_ARXIV_ID, output_dir='/tmp/test_dir')
        mock_libgen_client.upload_to_libgen.assert_called()
        mock_coordination_client.complete_paper.assert_called_with(TEST_ARXIV_ID, success=True)
        mock_remove.assert_called()  # Should delete the file
        mock_rmtree.assert_called()  # Should clean up temp directory


# ------------------- Coordination Server Tests -------------------

@pytest.mark.skipif(coordination_app is None, reason="Coordination server module not found")
class TestCoordinationServer:
    """Tests for the coordination server."""
    
    def test_register_endpoint(self, coordination_server):
        """Test the register endpoint."""
        response = coordination_server.post(
            '/register',
            json={"arxiv_id": TEST_ARXIV_ID}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "ok"
        
        # Test registering the same paper again
        response = coordination_server.post(
            '/register',
            json={"arxiv_id": TEST_ARXIV_ID}
        )
        
        assert response.status_code == 409  # Conflict
    
    def test_complete_endpoint(self, coordination_server):
        """Test the complete endpoint."""
        # First register a paper
        coordination_server.post(
            '/register',
            json={"arxiv_id": f"{TEST_ARXIV_ID}2"}
        )
        
        # Then mark it as completed
        response = coordination_server.post(
            '/complete',
            json={"arxiv_id": f"{TEST_ARXIV_ID}2", "success": True}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "ok"
    
    def test_status_endpoint(self, coordination_server):
        """Test the status endpoint."""
        # Register and complete a paper
        coordination_server.post(
            '/register',
            json={"arxiv_id": f"{TEST_ARXIV_ID}3"}
        )
        coordination_server.post(
            '/complete',
            json={"arxiv_id": f"{TEST_ARXIV_ID}3", "success": True}
        )
        
        # Get status
        response = coordination_server.get('/status')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert f"{TEST_ARXIV_ID}3" in data
        assert data[f"{TEST_ARXIV_ID}3"]["status"] == "completed"
        
        # Get status for specific paper
        response = coordination_server.get(f'/status?arxiv_id={TEST_ARXIV_ID}3')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert f"{TEST_ARXIV_ID}3" in data
    
    def test_summary_endpoint(self, coordination_server):
        """Test the summary endpoint."""
        response = coordination_server.get('/summary')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "total" in data
        assert "processing" in data
        assert "completed" in data
        assert "failed" in data


# ------------------- Functional Tests -------------------

@pytest.mark.skipif(coordination_app is None, reason="Modules not found")
def test_complete_workflow_with_mocks():
    """Test a complete workflow with mocked external dependencies."""
    # This test simulates the entire workflow from ArXiv to LibGen
    # with all external dependencies mocked
    
    # Set up configuration
    config = type('Config', (), {
        'use_tor': False,
        'use_coordination_server': True,
        'coordination_server': 'http://localhost:5001',
        'max_workers': 2,
        'max_memory_usage': 0.8,
        'memory_check_interval': 5,
        'wait_between_requests': 0.1,
        'libgen_api_endpoint': 'http://example.com/api',
        'libgen_upload_endpoint': 'http://example.com/upload',
        'keep_files': False,
        'output_dir': tempfile.mkdtemp(),
        'use_torrents': False,
        'torrent_dir': tempfile.mkdtemp(),
        'torrent_announce': None
    })
    
    # Create mocks
    mock_arxiv_client = MagicMock(spec=ArxivClient)
    mock_arxiv_client.get_paper_metadata.return_value = {
        'id': TEST_ARXIV_ID,
        'title': 'Test Paper Title',
        'abstract': 'This is a test paper abstract',
        'authors': ['Test Author'],
        'categories': ['cs.AI'],
        'published': '2021-01-01T00:00:00Z',
        'updated': '2021-01-02T00:00:00Z',
        'pdf_url': f'https://arxiv.org/pdf/{TEST_ARXIV_ID}.pdf'
    }
    test_pdf_path = os.path.join(config.output_dir, f"{TEST_ARXIV_ID}.pdf")
    with open(test_pdf_path, 'wb') as f:
        f.write(TEST_PDF_CONTENT)
    mock_arxiv_client.download_paper.return_value = test_pdf_path
    
    mock_libgen_client = MagicMock(spec=LibgenClient)
    mock_libgen_client.check_if_exists.return_value = False
    mock_libgen_client.upload_to_libgen.return_value = True
    
    # Create worker with mocked components
    worker = ArxivToLibgenWorker(config)
    worker.arxiv_client = mock_arxiv_client
    worker.libgen_client = mock_libgen_client
    
    # Start worker and process paper
    worker.start()
    worker.process_paper(TEST_ARXIV_ID)
    
    # Wait for processing to complete
    worker.work_queue.join()
    
    # Verify
    mock_arxiv_client.get_paper_metadata.assert_called_with(TEST_ARXIV_ID)
    mock_arxiv_client.download_paper.assert_called_with(TEST_ARXIV_ID, output_dir=ANY)
    mock_libgen_client.upload_to_libgen.assert_called()
    
    # Clean up
    worker.stop()
    shutil.rmtree(config.output_dir, ignore_errors=True)
    shutil.rmtree(config.torrent_dir, ignore_errors=True)


# Run tests if executed directly
if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
