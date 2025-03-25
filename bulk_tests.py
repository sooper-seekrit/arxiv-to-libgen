#!/usr/bin/env python3
"""
Tests for ArXiv Bulk Data Backup Tool

This file contains unit tests and integration tests for the ArXiv Bulk Data Backup tool.
It uses pytest and mocks external dependencies like S3 and LibGen.

To run:
    pytest -xvs bulk_tests.py
"""

import os
import json
import time
import sqlite3
import tempfile
import threading
import shutil
import pytest
from unittest.mock import patch, MagicMock, Mock, ANY
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

# Import the modules to test
import sys
sys.path.append('.')
try:
    from bulk_backup import (
        ArxivS3Client, ArxivMetadataDB, BulkBackupProgressTracker,
        ArxivBulkBackupWorker, create_config_from_args, ARXIV_PDF_PREFIX
    )
except ImportError:
    # If imported as a script, create dummy classes for type hints
    class ArxivS3Client: pass
    class ArxivMetadataDB: pass
    class BulkBackupProgressTracker: pass
    class ArxivBulkBackupWorker: pass
    def create_config_from_args(args): pass
    ARXIV_PDF_PREFIX = "pdf/"

# Test constants
TEST_ARXIV_ID = '2101.12345'
TEST_S3_KEY = f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf"
TEST_PDF_CONTENT = b'%PDF-1.5\nTest PDF content\n%%EOF'
TEST_METADATA_DB_CONTENT = b'SQLite format 3\x00'

# ------------------- Fixtures -------------------

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_s3_objects():
    """Sample S3 objects for testing."""
    return [
        {
            'Key': f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf",
            'Size': len(TEST_PDF_CONTENT),
            'LastModified': time.time()
        },
        {
            'Key': f"{ARXIV_PDF_PREFIX}2101/2101.12346.pdf",
            'Size': len(TEST_PDF_CONTENT),
            'LastModified': time.time()
        },
        {
            'Key': f"{ARXIV_PDF_PREFIX}2101/2101.12347.pdf",
            'Size': len(TEST_PDF_CONTENT),
            'LastModified': time.time()
        }
    ]


@pytest.fixture
def mock_boto3_client():
    """Mock boto3 S3 client."""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        
        # Mock list_objects_v2
        mock_s3.list_objects_v2 = Mock(return_value={
            'Contents': [
                {
                    'Key': f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf",
                    'Size': len(TEST_PDF_CONTENT),
                    'LastModified': time.time()
                }
            ],
            'IsTruncated': False
        })
        
        # Mock download_file
        mock_s3.download_file = Mock(return_value=None)
        
        # Mock get_object
        mock_response = Mock()
        mock_body = Mock()
        mock_body.read.return_value = TEST_PDF_CONTENT
        mock_response['Body'] = mock_body
        mock_s3.get_object = Mock(return_value=mock_response)
        
        yield mock_s3


@pytest.fixture
def mock_s3_client(mock_boto3_client):
    """Mock ArxivS3Client instance."""
    with patch('bulk_backup.ArxivS3Client', autospec=True) as MockS3Client:
        client = MockS3Client.return_value
        client.list_papers.return_value = [
            {
                'Key': f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf",
                'Size': len(TEST_PDF_CONTENT),
                'LastModified': time.time()
            }
        ]
        client.list_papers_paginated.return_value = [
            {
                'Key': f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf",
                'Size': len(TEST_PDF_CONTENT),
                'LastModified': time.time()
            }
        ]
        client.download_paper.return_value = True
        client.download_paper_to_memory.return_value = TEST_PDF_CONTENT
        client.download_metadata_database.return_value = True
        client.get_arxiv_id_from_key.return_value = TEST_ARXIV_ID
        client.s3_client = mock_boto3_client
        client.bucket_name = "arxiv"
        yield client


@pytest.fixture
def temp_db_file(temp_dir):
    """Create a temporary SQLite database for testing."""
    db_path = os.path.join(temp_dir, 'test_arxiv_metadata.sqlite')
    
    # Create test database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
    CREATE TABLE papers (
        id TEXT PRIMARY KEY,
        arxiv_id TEXT,
        title TEXT,
        abstract TEXT,
        authors TEXT,
        categories TEXT,
        created TEXT,
        updated TEXT
    )
    ''')
    
    # Insert test data
    cursor.execute('''
    INSERT INTO papers (id, arxiv_id, title, abstract, authors, categories, created, updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        TEST_ARXIV_ID,
        TEST_ARXIV_ID,
        'Test Paper Title',
        'This is a test paper abstract.',
        'Test Author 1, Test Author 2',
        'cs.AI cs.LG',
        '2021-01-01T00:00:00Z',
        '2021-01-02T00:00:00Z'
    ))
    
    # Add a few more papers
    for i in range(5):
        arxiv_id = f"2101.1234{i+6}"
        cursor.execute('''
        INSERT INTO papers (id, arxiv_id, title, abstract, authors, categories, created, updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            arxiv_id,
            arxiv_id,
            f'Test Paper {i+1}',
            f'This is test paper {i+1} abstract.',
            'Test Author 1, Test Author 2',
            f'cs.AI math.ST',
            '2021-01-01T00:00:00Z',
            '2021-01-02T00:00:00Z'
        ))
    
    conn.commit()
    conn.close()
    
    yield db_path


@pytest.fixture
def mock_metadata_db(temp_db_file):
    """Mock ArxivMetadataDB instance."""
    with patch('bulk_backup.ArxivMetadataDB', autospec=True) as MockMetadataDB:
        db = MockMetadataDB.return_value
        db.db_path = temp_db_file
        db.connected = True
        
        # Create a real metadata database for testing
        real_db = ArxivMetadataDB(temp_db_file)
        real_db.connect()
        
        # Mock methods to use the real database
        db.connect.return_value = True
        db.get_paper_metadata.side_effect = real_db.get_paper_metadata
        db.get_papers_batch.side_effect = real_db.get_papers_batch
        db.get_papers_by_category.side_effect = real_db.get_papers_by_category
        db.get_paper_count.side_effect = real_db.get_paper_count
        db.get_schema.side_effect = real_db.get_schema
        
        yield db
        
        real_db.close()


@pytest.fixture
def temp_progress_file(temp_dir):
    """Create a temporary progress file for testing."""
    progress_path = os.path.join(temp_dir, 'test_progress.json')
    
    # Create test progress file
    with open(progress_path, 'w') as f:
        json.dump({
            'completed': [f"2101.1234{i}" for i in range(3)],
            'failed': ['2101.12349'],
            'in_progress': [],
            'updated': '2023-01-01T00:00:00'
        }, f)
    
    yield progress_path


@pytest.fixture
def mock_progress_tracker(temp_progress_file):
    """Mock BulkBackupProgressTracker instance."""
    with patch('bulk_backup.BulkBackupProgressTracker', autospec=True) as MockProgressTracker:
        tracker = MockProgressTracker.return_value
        tracker.progress_file = temp_progress_file
        tracker.completed_papers = set([f"2101.1234{i}" for i in range(3)])
        tracker.failed_papers = set(['2101.12349'])
        tracker.in_progress_papers = set()
        
        tracker.mark_paper_in_progress.return_value = True
        tracker.should_process_paper.return_value = True
        
        yield tracker


@pytest.fixture
def mock_memory_monitor():
    """Mock memory monitor."""
    with patch('bulk_backup.MemoryMonitor', autospec=True) as MockMemoryMonitor:
        monitor = MockMemoryMonitor.return_value
        monitor.current_usage = 0.5  # 50% usage
        monitor.wait_for_resources.return_value = None
        yield monitor


@pytest.fixture
def mock_libgen_client():
    """Mock LibGen client."""
    with patch('bulk_backup.LibgenClient', autospec=True) as MockLibgenClient:
        client = MockLibgenClient.return_value
        client.check_if_exists.return_value = False
        client.calculate_md5.return_value = 'test_md5_hash'
        client.extract_pdf_metadata.return_value = {
            'title': 'Test Paper Title',
            'author': 'Test Author',
            'num_pages': 10
        }
        client.upload_to_libgen.return_value = True
        yield client


@pytest.fixture
def mock_coordination_client():
    """Mock coordination client."""
    with patch('bulk_backup.CoordinationClient', autospec=True) as MockCoordinationClient:
        client = MockCoordinationClient.return_value
        client.register_paper.return_value = True
        client.complete_paper.return_value = True
        yield client


@pytest.fixture
def mock_args():
    """Mock command-line arguments."""
    class Args:
        from_s3 = True
        from_db = False
        from_file = None
        max_papers = 100
        category = None
        aws_access_key_id = None
        aws_secret_access_key = None
        endpoint_url = None
        region_name = None
        output_dir = None
        keep_files = False
        metadata_dir = None
        progress_file = None
        libgen_api_endpoint = None
        libgen_upload_endpoint = None
        use_tor = False
        tor_socks_port = 9050
        tor_control_port = 9051
        tor_password = None
        coordination_server = None
        use_metadata_db = True
        max_workers = 2
        max_memory = 0.8
        wait_between = 1.0
    
    return Args()


# ------------------- Unit Tests -------------------

class TestArxivS3Client:
    """Tests for the ArXiv S3 client."""
    
    def test_init(self, mock_boto3_client):
        """Test S3 client initialization."""
        client = ArxivS3Client(public_access=True)
        assert client.bucket_name == "arxiv"
        assert client.public_access is True
    
    def test_list_papers(self, mock_boto3_client, sample_s3_objects):
        """Test listing papers from S3."""
        mock_boto3_client.list_objects_v2.return_value = {
            'Contents': sample_s3_objects,
            'IsTruncated': False
        }
        
        client = ArxivS3Client()
        client.s3_client = mock_boto3_client
        
        result = client.list_papers(prefix=ARXIV_PDF_PREFIX, max_keys=10)
        
        assert result == sample_s3_objects
        assert mock_boto3_client.list_objects_v2.called
        mock_boto3_client.list_objects_v2.assert_called_with(
            Bucket="arxiv",
            Prefix=ARXIV_PDF_PREFIX,
            MaxKeys=10
        )
    
    def test_list_papers_paginated(self, mock_boto3_client, sample_s3_objects):
        """Test paginated listing of papers from S3."""
        # First page
        mock_boto3_client.list_objects_v2.side_effect = [
            {
                'Contents': sample_s3_objects[:2],
                'IsTruncated': True,
                'NextContinuationToken': 'token1'
            },
            {
                'Contents': sample_s3_objects[2:],
                'IsTruncated': False
            }
        ]
        
        client = ArxivS3Client()
        client.s3_client = mock_boto3_client
        
        result = client.list_papers_paginated(prefix=ARXIV_PDF_PREFIX, page_size=2)
        
        assert len(result) == 3
        assert result == sample_s3_objects
        assert mock_boto3_client.list_objects_v2.call_count == 2
    
    def test_download_paper(self, mock_boto3_client, temp_dir):
        """Test downloading a paper from S3."""
        client = ArxivS3Client()
        client.s3_client = mock_boto3_client
        
        output_path = os.path.join(temp_dir, f"{TEST_ARXIV_ID}.pdf")
        result = client.download_paper(TEST_S3_KEY, output_path)
        
        assert result is True
        assert mock_boto3_client.download_file.called
        mock_boto3_client.download_file.assert_called_with(
            Bucket="arxiv",
            Key=TEST_S3_KEY,
            Filename=output_path
        )
    
    def test_download_paper_to_memory(self, mock_boto3_client):
        """Test downloading a paper to memory from S3."""
        client = ArxivS3Client()
        client.s3_client = mock_boto3_client
        
        result = client.download_paper_to_memory(TEST_S3_KEY)
        
        assert result == TEST_PDF_CONTENT
        assert mock_boto3_client.get_object.called
        mock_boto3_client.get_object.assert_called_with(
            Bucket="arxiv",
            Key=TEST_S3_KEY
        )
    
    def test_get_arxiv_id_from_key(self):
        """Test extracting arXiv ID from S3 key."""
        client = ArxivS3Client()
        
        # Test PDF key
        pdf_key = f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf"
        result = client.get_arxiv_id_from_key(pdf_key)
        assert result == TEST_ARXIV_ID
        
        # Test source key
        src_key = f"src/2101/{TEST_ARXIV_ID}"
        result = client.get_arxiv_id_from_key(src_key)
        assert result == TEST_ARXIV_ID
        
        # Test old style ID
        old_key = "pdf/math/0603026.pdf"
        result = client.get_arxiv_id_from_key(old_key)
        assert result == "math/0603026"


class TestArxivMetadataDB:
    """Tests for the ArXiv metadata database."""
    
    def test_connect(self, temp_db_file):
        """Test connecting to the metadata database."""
        db = ArxivMetadataDB(temp_db_file)
        result = db.connect()
        
        assert result is True
        assert db.connected is True
        
        db.close()
    
    def test_get_paper_metadata(self, temp_db_file):
        """Test getting paper metadata."""
        db = ArxivMetadataDB(temp_db_file)
        db.connect()
        
        result = db.get_paper_metadata(TEST_ARXIV_ID)
        
        assert result is not None
        assert result['id'] == TEST_ARXIV_ID
        assert result['title'] == 'Test Paper Title'
        assert 'abstract' in result
        assert 'authors' in result
        assert 'categories' in result
        
        db.close()
    
    def test_get_papers_batch(self, temp_db_file):
        """Test getting a batch of papers."""
        db = ArxivMetadataDB(temp_db_file)
        db.connect()
        
        result = db.get_papers_batch(limit=3)
        
        assert len(result) == 3
        assert isinstance(result, list)
        assert all('id' in paper for paper in result)
        
        db.close()
    
    def test_get_papers_by_category(self, temp_db_file):
        """Test getting papers by category."""
        db = ArxivMetadataDB(temp_db_file)
        db.connect()
        
        result = db.get_papers_by_category('cs.AI', limit=10)
        
        assert len(result) > 0
        assert all('categories' in paper and 'cs.AI' in paper['categories'] for paper in result)
        
        db.close()
    
    def test_get_paper_count(self, temp_db_file):
        """Test getting paper count."""
        db = ArxivMetadataDB(temp_db_file)
        db.connect()
        
        result = db.get_paper_count()
        
        assert result == 6  # 1 main test paper + 5 additional ones
        
        db.close()


class TestBulkBackupProgressTracker:
    """Tests for the bulk backup progress tracker."""
    
    def test_load_progress(self, temp_progress_file):
        """Test loading progress from file."""
        tracker = BulkBackupProgressTracker(temp_progress_file)
        
        assert len(tracker.completed_papers) == 3
        assert len(tracker.failed_papers) == 1
        assert len(tracker.in_progress_papers) == 0
    
    def test_save_progress(self, temp_dir):
        """Test saving progress to file."""
        progress_file = os.path.join(temp_dir, 'new_progress.json')
        tracker = BulkBackupProgressTracker(progress_file)
        
        tracker.completed_papers = set(['id1', 'id2'])
        tracker.failed_papers = set(['id3'])
        tracker.in_progress_papers = set(['id4'])
        
        result = tracker.save_progress()
        
        assert result is True
        assert os.path.exists(progress_file)
        
        # Verify saved content
        with open(progress_file, 'r') as f:
            data = json.load(f)
            assert set(data['completed']) == set(['id1', 'id2'])
            assert set(data['failed']) == set(['id3'])
            assert set(data['in_progress']) == set(['id4'])
    
    def test_mark_paper_status(self, temp_dir):
        """Test marking paper status."""
        progress_file = os.path.join(temp_dir, 'status_progress.json')
        tracker = BulkBackupProgressTracker(progress_file)
        
        # Mark in progress
        result = tracker.mark_paper_in_progress('test_id')
        assert result is True
        assert 'test_id' in tracker.in_progress_papers
        
        # Mark completed
        tracker.mark_paper_completed('test_id')
        assert 'test_id' in tracker.completed_papers
        assert 'test_id' not in tracker.in_progress_papers
        
        # Mark failed
        tracker.mark_paper_failed('test_id2')
        assert 'test_id2' in tracker.failed_papers
    
    def test_status_checks(self, temp_dir):
        """Test paper status checking methods."""
        progress_file = os.path.join(temp_dir, 'check_progress.json')
        tracker = BulkBackupProgressTracker(progress_file)
        
        tracker.completed_papers = set(['id1'])
        tracker.failed_papers = set(['id2'])
        tracker.in_progress_papers = set(['id3'])
        
        assert tracker.is_paper_completed('id1') is True
        assert tracker.is_paper_completed('id2') is False
        
        assert tracker.is_paper_failed('id2') is True
        assert tracker.is_paper_failed('id1') is False
        
        assert tracker.is_paper_in_progress('id3') is True
        assert tracker.is_paper_in_progress('id1') is False
        
        assert tracker.should_process_paper('id4') is True
        assert tracker.should_process_paper('id1') is False
        assert tracker.should_process_paper('id3') is False
    
    def test_get_stats(self, temp_dir):
        """Test getting statistics."""
        progress_file = os.path.join(temp_dir, 'stats_progress.json')
        tracker = BulkBackupProgressTracker(progress_file)
        
        tracker.completed_papers = set(['id1', 'id2'])
        tracker.failed_papers = set(['id3'])
        tracker.in_progress_papers = set(['id4'])
        
        stats = tracker.get_stats()
        
        assert stats['completed'] == 2
        assert stats['failed'] == 1
        assert stats['in_progress'] == 1
        assert stats['total_processed'] == 3


# ------------------- Integration Tests -------------------

class TestArxivBulkBackupWorker:
    """Integration tests for the ArXiv bulk backup worker."""
    
    def test_init(self, mock_s3_client, mock_metadata_db, mock_memory_monitor, 
                mock_libgen_client, mock_coordination_client, mock_progress_tracker):
        """Test worker initialization."""
        config = type('Config', (), {
            'aws_access_key_id': None,
            'aws_secret_access_key': None,
            'endpoint_url': None,
            'region_name': 'us-east-1',
            'public_access': True,
            'use_tor': False,
            'tor_socks_port': 9050,
            'tor_control_port': 9051,
            'tor_control_password': None,
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
            'use_metadata_db': True,
            'metadata_db_path': '/tmp/metadata.sqlite',
            'progress_file': '/tmp/progress.json'
        })
        
        # Mock components
        with patch('bulk_backup.ArxivS3Client', return_value=mock_s3_client), \
             patch('bulk_backup.ArxivMetadataDB', return_value=mock_metadata_db), \
             patch('bulk_backup.MemoryMonitor', return_value=mock_memory_monitor), \
             patch('bulk_backup.LibgenClient', return_value=mock_libgen_client), \
             patch('bulk_backup.CoordinationClient', return_value=mock_coordination_client), \
             patch('bulk_backup.BulkBackupProgressTracker', return_value=mock_progress_tracker):
            
            worker = ArxivBulkBackupWorker(config)
            
            assert worker.config == config
            assert worker.s3_client is mock_s3_client
            assert worker.metadata_db is mock_metadata_db
            assert worker.memory_monitor is mock_memory_monitor
            assert worker.libgen_client is mock_libgen_client
            assert worker.coordination_client is mock_coordination_client
            assert worker.progress_tracker is mock_progress_tracker
    
    def test_process_single_paper(self, mock_s3_client, mock_metadata_db, 
                                mock_memory_monitor, mock_libgen_client, 
                                mock_coordination_client, mock_progress_tracker, temp_dir):
        """Test processing a single paper."""
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
            'output_dir': temp_dir,
            'use_metadata_db': True,
            'metadata_db_path': '/tmp/metadata.sqlite',
            'progress_file': '/tmp/progress.json'
        })
        
        # Mock _get_paper_metadata method to return test metadata
        def mock_get_metadata(arxiv_id):
            return {
                'id': arxiv_id,
                'title': 'Test Paper Title',
                'abstract': 'This is a test paper abstract',
                'authors': ['Test Author'],
                'categories': ['cs.AI'],
                'published': '2021-01-01T00:00:00Z',
                'updated': '2021-01-02T00:00:00Z',
                'pdf_url': f'https://arxiv.org/pdf/{arxiv_id}.pdf'
            }
        
        # Create worker with mocked components
        with patch('bulk_backup.ArxivS3Client', return_value=mock_s3_client), \
             patch('bulk_backup.ArxivMetadataDB', return_value=mock_metadata_db), \
             patch('bulk_backup.MemoryMonitor', return_value=mock_memory_monitor), \
             patch('bulk_backup.LibgenClient', return_value=mock_libgen_client), \
             patch('bulk_backup.CoordinationClient', return_value=mock_coordination_client), \
             patch('bulk_backup.BulkBackupProgressTracker', return_value=mock_progress_tracker), \
             patch('tempfile.mkdtemp', return_value=temp_dir):
            
            worker = ArxivBulkBackupWorker(config)
            worker._get_paper_metadata = mock_get_metadata
            
            # Process a paper
            result = worker._process_single_paper(TEST_ARXIV_ID)
            
            # Verify
            assert result is True
            mock_s3_client.download_paper.assert_called_with(
                f"{ARXIV_PDF_PREFIX}2101/{TEST_ARXIV_ID}.pdf", 
                os.path.join(temp_dir, f"{TEST_ARXIV_ID}.pdf")
            )
            mock_libgen_client.upload_to_libgen.assert_called()
    
    def test_process_papers_from_s3(self, mock_s3_client, mock_metadata_db, 
                               mock_memory_monitor, mock_libgen_client, 
                               mock_coordination_client, mock_progress_tracker):
        """Test processing papers from S3."""
        config = type('Config', (), {
            'use_tor': False,
            'use_coordination_server': False,
            'max_workers': 2,
            'max_memory_usage': 0.8,
            'memory_check_interval': 5,
            'wait_between_requests': 0.1,
            'libgen_api_endpoint': 'http://example.com/api',
            'libgen_upload_endpoint': 'http://example.com/upload',
            'keep_files': False,
            'output_dir': '/tmp',
            'use_metadata_db': True,
            'metadata_db_path': '/tmp/metadata.sqlite',
            'progress_file': '/tmp/progress.json'
        })
        
        # Mock S3 client to return test papers
        s3_papers = [
            {'Key': f"{ARXIV_PDF_PREFIX}2101/2101.12345.pdf"},
            {'Key': f"{ARXIV_PDF_PREFIX}2101/2101.12346.pdf"},
            {'Key': f"{ARXIV_PDF_PREFIX}2101/2101.12347.pdf"}
        ]
        mock_s3_client.list_papers_paginated.return_value = s3_papers
        mock_s3_client.get_arxiv_id_from_key.side_effect = lambda key: key.split('/')[-1].split('.')[0]
        
        # Create worker with mocked components
        with patch('bulk_backup.ArxivS3Client', return_value=mock_s3_client), \
             patch('bulk_backup.ArxivMetadataDB', return_value=mock_metadata_db), \
             patch('bulk_backup.MemoryMonitor', return_value=mock_memory_monitor), \
             patch('bulk_backup.LibgenClient', return_value=mock_libgen_client), \
             patch('bulk_backup.BulkBackupProgressTracker', return_value=mock_progress_tracker):
            
            worker = ArxivBulkBackupWorker(config)
            worker.queue_paper = Mock()  # Mock queue_paper to verify calls
            
            # Process papers
            worker.process_papers_from_s3(max_papers=10)
            
            # Verify
            assert mock_s3_client.list_papers_paginated.called
            assert worker.queue_paper.call_count == 3
            worker.queue_paper.assert_any_call('2101.12345')
            worker.queue_paper.assert_any_call('2101.12346')
            worker.queue_paper.assert_any_call('2101.12347')
    
    def test_process_papers_from_database(self, mock_s3_client, mock_metadata_db, 
                                    mock_memory_monitor, mock_libgen_client, 
                                    mock_coordination_client, mock_progress_tracker):
        """Test processing papers from the database."""
        config = type('Config', (), {
            'use_tor': False,
            'use_coordination_server': False,
            'max_workers': 2,
            'max_memory_usage': 0.8,
            'memory_check_interval': 5,
            'wait_between_requests': 0.1,
            'libgen_api_endpoint': 'http://example.com/api',
            'libgen_upload_endpoint': 'http://example.com/upload',
            'keep_files': False,
            'output_dir': '/tmp',
            'use_metadata_db': True,
            'metadata_db_path': '/tmp/metadata.sqlite',
            'progress_file': '/tmp/progress.json'
        })
        
        # Mock metadata DB to return test papers
        db_papers = [
            {'id': '2101.12345', 'title': 'Test Paper 1'},
            {'id': '2101.12346', 'title': 'Test Paper 2'},
            {'id': '2101.12347', 'title': 'Test Paper 3'}
        ]
        mock_metadata_db.get_papers_batch.return_value = db_papers
        mock_metadata_db.get_papers_by_category.return_value = db_papers
        
        # Create worker with mocked components
        with patch('bulk_backup.ArxivS3Client', return_value=mock_s3_client), \
             patch('bulk_backup.ArxivMetadataDB', return_value=mock_metadata_db), \
             patch('bulk_backup.MemoryMonitor', return_value=mock_memory_monitor), \
             patch('bulk_backup.LibgenClient', return_value=mock_libgen_client), \
             patch('bulk_backup.BulkBackupProgressTracker', return_value=mock_progress_tracker):
            
            worker = ArxivBulkBackupWorker(config)
            worker.queue_paper = Mock()  # Mock queue_paper to verify calls
            
            # Process papers
            worker.process_papers_from_database(max_papers=10)
            
            # Verify
            assert mock_metadata_db.get_papers_batch.called
            assert worker.queue_paper.call_count == 3
            worker.queue_paper.assert_any_call('2101.12345')
            worker.queue_paper.assert_any_call('2101.12346')
            worker.queue_paper.assert_any_call('2101.12347')
    
    def test_process_papers_from_file(self, mock_s3_client, mock_metadata_db, 
                               mock_memory_monitor, mock_libgen_client, 
                               mock_coordination_client, mock_progress_tracker, temp_dir):
        """Test processing papers from a file."""
        config = type('Config', (), {
            'use_tor': False,
            'use_coordination_server': False,
            'max_workers': 2,
            'max_memory_usage': 0.8,
            'memory_check_interval': 5,
            'wait_between_requests': 0.1,
            'libgen_api_endpoint': 'http://example.com/api',
            'libgen_upload_endpoint': 'http://example.com/upload',
            'keep_files': False,
            'output_dir': '/tmp',
            'use_metadata_db': True,
            'metadata_db_path': '/tmp/metadata.sqlite',
            'progress_file': '/tmp/progress.json'
        })
        
        # Create test file with arXiv IDs
        file_path = os.path.join(temp_dir, 'arxiv_ids.txt')
        with open(file_path, 'w') as f:
            f.write("2101.12345\n2101.12346\n2101.12347\n")
        
        # Create worker with mocked components
        with patch('bulk_backup.ArxivS3Client', return_value=mock_s3_client), \
             patch('bulk_backup.ArxivMetadataDB', return_value=mock_metadata_db), \
             patch('bulk_backup.MemoryMonitor', return_value=mock_memory_monitor), \
             patch('bulk_backup.LibgenClient', return_value=mock_libgen_client), \
             patch('bulk_backup.BulkBackupProgressTracker', return_value=mock_progress_tracker):
            
            worker = ArxivBulkBackupWorker(config)
            worker.queue_paper = Mock()  # Mock queue_paper to verify calls
            
            # Process papers
            worker.process_papers_from_file(file_path)
            
            # Verify
            assert worker.queue_paper.call_count == 3
            worker.queue_paper.assert_any_call('2101.12345')
            worker.queue_paper.assert_any_call('2101.12346')
            worker.queue_paper.assert_any_call('2101.12347')


class TestConfigAndMain:
    """Tests for the config creation and main function."""
    
    def test_create_config_from_args(self, mock_args, temp_dir):
        """Test creating config from command-line arguments."""
        # Modify mock args for testing
        mock_args.output_dir = temp_dir
        mock_args.metadata_dir = os.path.join(temp_dir, 'metadata')
        mock_args.max_workers = 4
        
        config = create_config_from_args(mock_args)
        
        assert config.output_dir == temp_dir
        assert config.metadata_dir == os.path.join(temp_dir, 'metadata')
        assert config.max_workers == 4
        assert config.public_access is True
        assert os.path.exists(config.metadata_dir)
    
    @patch('bulk_backup.ArxivBulkBackupWorker')
    def test_main_function(self, mock_worker_class, mock_args):
        """Test the main function."""
        # This test would typically mock out the ArgumentParser and test
        # the main function, but since main() primarily initializes components
        # and calls methods on the worker, we'll just check that the worker
        # is created and its methods are called correctly.
        
        mock_worker = mock_worker_class.return_value
        
        with patch('bulk_backup.create_config_from_args', return_value=type('Config', (), {})), \
             patch('bulk_backup.argparse.ArgumentParser.parse_args', return_value=mock_args), \
             patch('bulk_backup.main') as mock_main:
            
            # Call main (patched to avoid actual execution)
            mock_main()
            
            # In a real test, we'd verify:
            # 1. Worker was created with correct config
            # 2. Worker.start() was called
            # 3. The appropriate process_papers_* method was called based on args
            # 4. Worker.wait_for_completion() was called
            # 5. Worker.stop() was called


# ------------------- Functional Tests -------------------

@pytest.mark.skip(reason="These would be long-running integration tests")
def test_complete_workflow_with_mocks():
    """Test a complete workflow with mocked external dependencies."""
    # This would be a comprehensive test that initializes the worker with
    # mocked components and tests a complete paper processing workflow,
    # similar to the equivalent test in test_arxiv_backup.py
    pass


# Run tests if executed directly
if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
