#!/usr/bin/env python3
"""
ArXiv Bulk Data Backup Tool

This tool uses arXiv's bulk data access via Amazon S3 to:
- Download papers in bulk from arXiv's S3 buckets
- Process and upload them to Library Genesis
- Track progress to avoid duplicate work
- Coordinate between multiple instances

References:
- arXiv Bulk Data Access: https://info.arxiv.org/help/bulk_data_s3.html
"""

import os
import sys
import time
import json
import logging
import argparse
import tempfile
import threading
import multiprocessing
import queue
import sqlite3
import shutil
import hashlib
import traceback
import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set, Any, Union

# AWS/S3 access
import boto3
from botocore.exceptions import ClientError

# For PDF processing
try:
    from PyPDF2 import PdfReader
except ImportError:
    from PyPDF2 import PdfFileReader as PdfReader

# Standard libraries
import requests

# Import core functionality from original backup tool
try:
    from arxiv_backup import (
        LibgenClient, MemoryMonitor, CoordinationClient, TorNetwork
    )
except ImportError:
    # If not available, use dummy classes to keep type hints
    class LibgenClient: pass
    class MemoryMonitor: pass
    class CoordinationClient: pass
    class TorNetwork: pass
    print("Warning: Could not import from arxiv_backup.py. Some functionality will be limited.")
    print("Please ensure arxiv_backup.py is in the same directory or in your PYTHONPATH.")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("arxiv_bulk_backup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("arxiv_bulk_backup")

# Constants
ARXIV_S3_BUCKET = "arxiv"
ARXIV_PDF_PREFIX = "pdf/"
ARXIV_SRC_PREFIX = "src/"
ARXIV_METADATA_DB_KEY = "metadata/arxiv-metadata-oai-snapshot.sqlite"
DEFAULT_S3_ENDPOINT = "s3.amazonaws.com"
ARXIV_S3_REGION = "us-east-1"
MAX_PAPERS_PER_BATCH = 1000
BACKUP_PROGRESS_FILE = "arxiv_bulk_backup_progress.json"
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_BASE = 5  # seconds
DEFAULT_BATCH_SIZE = 100
MAX_MEMORY_USAGE = 0.8  # 80% of available memory
MEMORY_CHECK_INTERVAL = 5  # seconds
ARXIV_ID_PATTERN = r"\d{4}\.\d{4,5}(v\d+)?"  # Matches newer arXiv IDs like 2101.12345v1

class ArxivS3Client:
    """Client for accessing arXiv's bulk data on Amazon S3."""
    
    def __init__(
        self, 
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        region_name: str = ARXIV_S3_REGION,
        public_access: bool = True
    ):
        """
        Initialize the S3 client for arXiv bulk data access.
        
        Args:
            aws_access_key_id: AWS access key ID (not required for public access)
            aws_secret_access_key: AWS secret access key (not required for public access)
            endpoint_url: S3 endpoint URL (defaults to standard AWS S3)
            region_name: AWS region name
            public_access: If True, use public access method (no credentials needed)
        """
        self.public_access = public_access
        
        # Set up S3 client
        if public_access:
            # For public access, we use anonymous credentials
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                region_name=region_name,
                aws_access_key_id='',
                aws_secret_access_key='',
                config=boto3.session.Config(signature_version=boto3.session.UNSIGNED)
            )
        else:
            # For authenticated access (if you have credentials)
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        
        self.bucket_name = ARXIV_S3_BUCKET
        logger.info(f"Initialized S3 client for bucket: {self.bucket_name}")
    
    def list_papers(self, prefix: str = ARXIV_PDF_PREFIX, max_keys: int = 1000) -> List[Dict]:
        """
        List papers available in the S3 bucket with the given prefix.
        
        Args:
            prefix: S3 key prefix to list
            max_keys: Maximum number of keys to return
            
        Returns:
            List of dictionaries containing paper information
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                return response['Contents']
            return []
            
        except ClientError as e:
            logger.error(f"Error listing papers: {str(e)}")
            return []
    
    def list_papers_paginated(
        self, 
        prefix: str = ARXIV_PDF_PREFIX, 
        max_total: Optional[int] = None,
        page_size: int = 1000
    ) -> List[Dict]:
        """
        List papers available in the S3 bucket with pagination.
        
        Args:
            prefix: S3 key prefix to list
            max_total: Maximum total keys to return (None for all)
            page_size: Number of keys per page
            
        Returns:
            List of dictionaries containing paper information
        """
        results = []
        continuation_token = None
        total_retrieved = 0
        
        while True:
            try:
                if continuation_token:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=prefix,
                        MaxKeys=page_size,
                        ContinuationToken=continuation_token
                    )
                else:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=prefix,
                        MaxKeys=page_size
                    )
                
                if 'Contents' in response:
                    contents = response['Contents']
                    results.extend(contents)
                    total_retrieved += len(contents)
                    
                    if max_total and total_retrieved >= max_total:
                        # Truncate to max_total if specified
                        return results[:max_total]
                
                # Check if there are more results
                if not response.get('IsTruncated'):
                    break
                
                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    break
                    
            except ClientError as e:
                logger.error(f"Error listing papers with pagination: {str(e)}")
                break
        
        return results
    
    def download_paper(self, key: str, output_path: str) -> bool:
        """
        Download a paper from the S3 bucket.
        
        Args:
            key: S3 key of the paper to download
            output_path: Local path to save the paper
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Download the file
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=key,
                Filename=output_path
            )
            
            logger.info(f"Downloaded {key} to {output_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Error downloading paper {key}: {str(e)}")
            return False
    
    def download_paper_to_memory(self, key: str) -> Optional[bytes]:
        """
        Download a paper from the S3 bucket into memory.
        
        Args:
            key: S3 key of the paper to download
            
        Returns:
            Paper content as bytes, or None if download failed
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            # Get the file content
            paper_content = response['Body'].read()
            logger.info(f"Downloaded {key} to memory ({len(paper_content)} bytes)")
            return paper_content
            
        except ClientError as e:
            logger.error(f"Error downloading paper {key} to memory: {str(e)}")
            return None
    
    def download_metadata_database(self, output_path: str) -> bool:
        """
        Download arXiv metadata database from S3.
        
        Args:
            output_path: Local path to save the database
            
        Returns:
            True if download was successful, False otherwise
        """
        return self.download_paper(ARXIV_METADATA_DB_KEY, output_path)
    
    def get_arxiv_id_from_key(self, key: str) -> Optional[str]:
        """
        Extract arXiv ID from an S3 key.
        
        Args:
            key: S3 key of a paper
            
        Returns:
            arXiv ID if found, None otherwise
        """
        # Example PDF key format: pdf/1901/1901.00123.pdf
        # Example source key format: src/1901/1901.00123
        
        try:
            # Remove prefix and extension
            if key.startswith(ARXIV_PDF_PREFIX):
                key = key[len(ARXIV_PDF_PREFIX):]
            elif key.startswith(ARXIV_SRC_PREFIX):
                key = key[len(ARXIV_SRC_PREFIX):]
            
            # Remove extension if present
            if key.endswith('.pdf'):
                key = key[:-4]
            
            # Extract the ID - it should be in the last part of the path
            parts = key.split('/')
            arxiv_id = parts[-1]
            
            # Validate ID format
            import re
            if re.match(ARXIV_ID_PATTERN, arxiv_id):
                return arxiv_id
            else:
                # For older arXiv IDs (pre-2007) like math/0603026
                if '/' in parts[-1]:
                    return parts[-1]
            
            logger.warning(f"Could not extract valid arXiv ID from key: {key}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting arXiv ID from key {key}: {str(e)}")
            return None


class ArxivMetadataDB:
    """Interface for the arXiv metadata SQLite database."""
    
    def __init__(self, db_path: str):
        """
        Initialize the metadata database interface.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.connection = None
        self.connected = False
    
    def connect(self) -> bool:
        """
        Connect to the database.
        
        Returns:
            True if connection was successful, False otherwise
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            self.connected = True
            logger.info(f"Connected to arXiv metadata database at {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Error connecting to metadata database: {str(e)}")
            self.connected = False
            return False
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connected = False
    
    def get_paper_metadata(self, arxiv_id: str) -> Optional[Dict]:
        """
        Get metadata for a specific paper by arXiv ID.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            Dictionary of metadata or None if not found
        """
        if not self.connected:
            if not self.connect():
                return None
        
        try:
            cursor = self.connection.cursor()
            
            # Query for the paper
            cursor.execute(
                "SELECT * FROM papers WHERE id = ? OR arxiv_id = ?", 
                (arxiv_id, arxiv_id)
            )
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            
            logger.warning(f"Paper {arxiv_id} not found in metadata database")
            return None
            
        except sqlite3.Error as e:
            logger.error(f"Error querying metadata database for {arxiv_id}: {str(e)}")
            return None
    
    def get_papers_batch(self, start_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """
        Get a batch of papers from the database.
        
        Args:
            start_id: Starting arXiv ID (for pagination)
            limit: Maximum number of papers to return
            
        Returns:
            List of paper metadata dictionaries
        """
        if not self.connected:
            if not self.connect():
                return []
        
        try:
            cursor = self.connection.cursor()
            
            if start_id:
                cursor.execute(
                    "SELECT * FROM papers WHERE id > ? ORDER BY id LIMIT ?", 
                    (start_id, limit)
                )
            else:
                cursor.execute(
                    "SELECT * FROM papers ORDER BY id LIMIT ?", 
                    (limit,)
                )
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except sqlite3.Error as e:
            logger.error(f"Error querying metadata database for batch: {str(e)}")
            return []
    
    def get_papers_by_category(self, category: str, limit: int = 100) -> List[Dict]:
        """
        Get papers from a specific category.
        
        Args:
            category: arXiv category (e.g., 'cs.AI')
            limit: Maximum number of papers to return
            
        Returns:
            List of paper metadata dictionaries
        """
        if not self.connected:
            if not self.connect():
                return []
        
        try:
            cursor = self.connection.cursor()
            
            # Query for papers in the category
            cursor.execute(
                "SELECT * FROM papers WHERE categories LIKE ? ORDER BY id LIMIT ?", 
                (f"%{category}%", limit)
            )
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except sqlite3.Error as e:
            logger.error(f"Error querying metadata database for category {category}: {str(e)}")
            return []
    
    def get_paper_count(self) -> int:
        """
        Get the total number of papers in the database.
        
        Returns:
            Number of papers
        """
        if not self.connected:
            if not self.connect():
                return 0
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM papers")
            result = cursor.fetchone()
            return result[0] if result else 0
            
        except sqlite3.Error as e:
            logger.error(f"Error getting paper count: {str(e)}")
            return 0
    
    def get_schema(self) -> Dict[str, List[str]]:
        """
        Get the database schema.
        
        Returns:
            Dictionary mapping table names to lists of column names
        """
        if not self.connected:
            if not self.connect():
                return {}
        
        try:
            cursor = self.connection.cursor()
            schema = {}
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Get schema for each table
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in cursor.fetchall()]
                schema[table] = columns
            
            return schema
            
        except sqlite3.Error as e:
            logger.error(f"Error getting database schema: {str(e)}")
            return {}


class BulkBackupProgressTracker:
    """Tracks the progress of the bulk backup operation."""
    
    def __init__(self, progress_file: str = BACKUP_PROGRESS_FILE):
        """
        Initialize the progress tracker.
        
        Args:
            progress_file: Path to the progress file
        """
        self.progress_file = progress_file
        self.lock = threading.Lock()
        self.completed_papers = set()
        self.failed_papers = set()
        self.in_progress_papers = set()
        self.load_progress()
    
    def load_progress(self) -> bool:
        """
        Load progress from the progress file.
        
        Returns:
            True if progress was loaded successfully, False otherwise
        """
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.completed_papers = set(data.get('completed', []))
                    self.failed_papers = set(data.get('failed', []))
                    # Don't load in_progress from file - they might be stale
                    self.in_progress_papers = set()
                logger.info(f"Loaded progress: {len(self.completed_papers)} completed, "
                           f"{len(self.failed_papers)} failed papers")
                return True
            return False
        except Exception as e:
            logger.error(f"Error loading progress: {str(e)}")
            return False
    
    def save_progress(self) -> bool:
        """
        Save progress to the progress file.
        
        Returns:
            True if progress was saved successfully, False otherwise
        """
        try:
            with self.lock:
                with open(self.progress_file, 'w') as f:
                    json.dump({
                        'completed': list(self.completed_papers),
                        'failed': list(self.failed_papers),
                        'in_progress': list(self.in_progress_papers),
                        'updated': datetime.datetime.now().isoformat()
                    }, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving progress: {str(e)}")
            return False
    
    def mark_paper_in_progress(self, arxiv_id: str) -> bool:
        """
        Mark a paper as in progress.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            True if paper was not previously completed or failed, False otherwise
        """
        with self.lock:
            if arxiv_id in self.completed_papers or arxiv_id in self.failed_papers:
                return False
            self.in_progress_papers.add(arxiv_id)
            return True
    
    def mark_paper_completed(self, arxiv_id: str):
        """
        Mark a paper as completed.
        
        Args:
            arxiv_id: arXiv ID of the paper
        """
        with self.lock:
            self.completed_papers.add(arxiv_id)
            if arxiv_id in self.in_progress_papers:
                self.in_progress_papers.remove(arxiv_id)
            if arxiv_id in self.failed_papers:
                self.failed_papers.remove(arxiv_id)
    
    def mark_paper_failed(self, arxiv_id: str):
        """
        Mark a paper as failed.
        
        Args:
            arxiv_id: arXiv ID of the paper
        """
        with self.lock:
            self.failed_papers.add(arxiv_id)
            if arxiv_id in self.in_progress_papers:
                self.in_progress_papers.remove(arxiv_id)
    
    def is_paper_completed(self, arxiv_id: str) -> bool:
        """
        Check if a paper has been completed.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            True if paper is marked as completed, False otherwise
        """
        with self.lock:
            return arxiv_id in self.completed_papers
    
    def is_paper_failed(self, arxiv_id: str) -> bool:
        """
        Check if a paper has failed.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            True if paper is marked as failed, False otherwise
        """
        with self.lock:
            return arxiv_id in self.failed_papers
    
    def is_paper_in_progress(self, arxiv_id: str) -> bool:
        """
        Check if a paper is in progress.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            True if paper is marked as in progress, False otherwise
        """
        with self.lock:
            return arxiv_id in self.in_progress_papers
    
    def should_process_paper(self, arxiv_id: str) -> bool:
        """
        Check if a paper should be processed.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            True if paper should be processed, False otherwise
        """
        with self.lock:
            return (arxiv_id not in self.completed_papers and 
                   arxiv_id not in self.in_progress_papers)
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the backup progress.
        
        Returns:
            Dictionary of statistics
        """
        with self.lock:
            return {
                'completed': len(self.completed_papers),
                'failed': len(self.failed_papers),
                'in_progress': len(self.in_progress_papers),
                'total_processed': len(self.completed_papers) + len(self.failed_papers)
            }
    
    def get_failed_papers(self) -> List[str]:
        """
        Get the list of failed papers.
        
        Returns:
            List of arXiv IDs of failed papers
        """
        with self.lock:
            return list(self.failed_papers)
    
    def reset_failed_papers(self):
        """Reset the list of failed papers to allow retrying."""
        with self.lock:
            self.failed_papers = set()
            self.save_progress()


class ArxivBulkBackupWorker:
    """Worker for the bulk backup operation."""
    
    def __init__(self, config):
        """
        Initialize the bulk backup worker.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.progress_tracker = BulkBackupProgressTracker(config.progress_file)
        self.memory_monitor = MemoryMonitor(
            max_usage_fraction=config.max_memory_usage,
            check_interval=config.memory_check_interval
        )
        
        # Initialize S3 client
        self.s3_client = ArxivS3Client(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
            public_access=config.public_access
        )
        
        # Initialize metadata database if needed
        self.metadata_db = None
        if config.use_metadata_db:
            if not os.path.exists(config.metadata_db_path):
                logger.info(f"Metadata database not found at {config.metadata_db_path}, downloading...")
                os.makedirs(os.path.dirname(config.metadata_db_path), exist_ok=True)
                self.s3_client.download_metadata_database(config.metadata_db_path)
            
            self.metadata_db = ArxivMetadataDB(config.metadata_db_path)
            self.metadata_db.connect()
        
        # Initialize LibGen client
        self.tor_handler = None
        if config.use_tor:
            self.tor_handler = TorNetwork(
                socks_port=config.tor_socks_port,
                control_port=config.tor_control_port,
                password=config.tor_control_password
            )
        
        self.libgen_client = LibgenClient(
            api_endpoint=config.libgen_api_endpoint,
            upload_endpoint=config.libgen_upload_endpoint,
            use_tor=config.use_tor,
            tor_handler=self.tor_handler
        )
        
        # Initialize coordination client if needed
        self.coordination_client = None
        if config.use_coordination_server:
            self.coordination_client = CoordinationClient(config.coordination_server)
        
        # Work queue and thread pool
        self.work_queue = queue.Queue()
        self.workers = []
        self.stop_event = threading.Event()
        
        # Statistics
        self.stats = {
            'papers_processed': 0,
            'papers_uploaded': 0,
            'papers_failed': 0,
            'start_time': time.time()
        }
    
    def start(self):
        """Start the backup worker threads."""
        self.memory_monitor.start()
        
        # Start worker threads
        for i in range(self.config.max_workers):
            worker_thread = threading.Thread(
                target=self._worker_thread,
                name=f"Worker-{i}",
                daemon=True
            )
            worker_thread.start()
            self.workers.append(worker_thread)
        
        logger.info(f"Started {len(self.workers)} worker threads")
    
    def stop(self):
        """Stop the backup worker threads."""
        logger.info("Stopping backup worker...")
        self.stop_event.set()
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=2.0)
        
        # Stop memory monitor
        if self.memory_monitor:
            self.memory_monitor.stop()
        
        # Close metadata database
        if self.metadata_db:
            self.metadata_db.close()
        
        # Save progress
        self.progress_tracker.save_progress()
        
        # Log final statistics
        duration = time.time() - self.stats['start_time']
        logger.info(f"Backup completed in {duration:.2f} seconds")
        logger.info(f"Papers processed: {self.stats['papers_processed']}")
        logger.info(f"Papers uploaded: {self.stats['papers_uploaded']}")
        logger.info(f"Papers failed: {self.stats['papers_failed']}")
    
    def queue_paper(self, arxiv_id: str):
        """
        Queue a paper for processing.
        
        Args:
            arxiv_id: arXiv ID of the paper
        """
        if self.progress_tracker.should_process_paper(arxiv_id):
            self.work_queue.put(arxiv_id)
            self.progress_tracker.mark_paper_in_progress(arxiv_id)
    
    def _worker_thread(self):
        """Worker thread for processing papers."""
        while not self.stop_event.is_set():
            try:
                # Get the next paper to process
                try:
                    arxiv_id = self.work_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                # Check if we should process this paper (if using coordination)
                if self.coordination_client:
                    if not self.coordination_client.register_paper(arxiv_id):
                        logger.info(f"Skipping {arxiv_id} as it's being processed by another instance")
                        self.work_queue.task_done()
                        continue
                
                # Process the paper
                success = self._process_single_paper(arxiv_id)
                
                # Update statistics
                self.stats['papers_processed'] += 1
                if success:
                    self.stats['papers_uploaded'] += 1
                    self.progress_tracker.mark_paper_completed(arxiv_id)
                else:
                    self.stats['papers_failed'] += 1
                    self.progress_tracker.mark_paper_failed(arxiv_id)
                
                # Mark task as done
                self.work_queue.task_done()
                
                # Save progress occasionally
                if self.stats['papers_processed'] % 10 == 0:
                    self.progress_tracker.save_progress()
                
                # Change Tor identity occasionally if using Tor
                if (self.config.use_tor and self.tor_handler and 
                    random.random() < 0.1):  # 10% chance after each paper
                    self.tor_handler.new_identity()
                
            except Exception as e:
                logger.error(f"Error in worker thread: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(1)  # Sleep on error to avoid hammering
    
    def _process_single_paper(self, arxiv_id: str) -> bool:
        """
        Process a single paper from download to upload.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            True if processing was successful, False otherwise
        """
        success = False
        temp_dir = None
        file_path = None
        
        try:
            # Check memory usage before proceeding
            self.memory_monitor.wait_for_resources()
            
            # Get metadata
            metadata = self._get_paper_metadata(arxiv_id)
            if not metadata:
                logger.error(f"Failed to get metadata for {arxiv_id}")
                return False
            
            # Create temporary directory
            temp_dir = tempfile.mkdtemp()
            
            # Download paper
            s3_key = f"{ARXIV_PDF_PREFIX}{arxiv_id[:4]}/{arxiv_id}.pdf"
            file_path = os.path.join(temp_dir, f"{arxiv_id}.pdf")
            
            download_success = self.s3_client.download_paper(s3_key, file_path)
            if not download_success:
                logger.error(f"Failed to download {arxiv_id}")
                return False
            
            # Upload to LibGen
            success = self.libgen_client.upload_to_libgen(file_path, metadata)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing paper {arxiv_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
        finally:
            # Report to coordination server
            if self.coordination_client:
                self.coordination_client.complete_paper(arxiv_id, success=success)
            
            # Delete file if needed
            if file_path and success and not self.config.keep_files:
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting file {file_path}: {str(e)}")
            
            # Clean up temp directory
            if temp_dir:
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception as e:
                    logger.error(f"Error removing temp directory {temp_dir}: {str(e)}")
    
    def _get_paper_metadata(self, arxiv_id: str) -> Optional[Dict]:
        """
        Get metadata for a paper.
        
        Args:
            arxiv_id: arXiv ID of the paper
            
        Returns:
            Dictionary of metadata or None if not found
        """
        # Try to get metadata from database first
        if self.metadata_db:
            db_metadata = self.metadata_db.get_paper_metadata(arxiv_id)
            if db_metadata:
                # Convert database metadata to the format expected by LibGen client
                return {
                    'id': arxiv_id,
                    'title': db_metadata.get('title', ''),
                    'abstract': db_metadata.get('abstract', ''),
                    'authors': db_metadata.get('authors', '').split(','),
                    'categories': db_metadata.get('categories', '').split(' '),
                    'published': db_metadata.get('created', ''),
                    'updated': db_metadata.get('updated', ''),
                    'pdf_url': f'https://arxiv.org/pdf/{arxiv_id}.pdf'
                }
        
        # If no database or paper not found in database, try to fetch from arXiv API
        try:
            # Use the original arxiv_client to get metadata
            from arxiv_backup import ArxivClient
            arxiv_client = ArxivClient(use_tor=self.config.use_tor, tor_handler=self.tor_handler)
            return arxiv_client.get_paper_metadata(arxiv_id)
        except (ImportError, Exception) as e:
            logger.error(f"Error getting metadata from API for {arxiv_id}: {str(e)}")
            
            # Construct minimal metadata as fallback
            return {
                'id': arxiv_id,
                'title': f"arXiv:{arxiv_id}",
                'abstract': f"No abstract available for {arxiv_id}",
                'authors': ["Unknown"],
                'categories': ["Unknown"],
                'published': datetime.datetime.now().isoformat(),
                'updated': datetime.datetime.now().isoformat(),
                'pdf_url': f'https://arxiv.org/pdf/{arxiv_id}.pdf'
            }
    
    def process_papers_from_s3(self, max_papers: Optional[int] = None, 
                              category_filter: Optional[str] = None):
        """
        Process papers by listing them from S3.
        
        Args:
            max_papers: Maximum number of papers to process (None for all)
            category_filter: Only process papers in this category
        """
        logger.info(f"Starting bulk processing from S3")
        
        # List papers from S3
        papers = self.s3_client.list_papers_paginated(
            prefix=ARXIV_PDF_PREFIX,
            max_total=max_papers,
            page_size=1000
        )
        
        logger.info(f"Found {len(papers)} papers in S3")
        
        # Queue papers for processing
        count = 0
        for paper in papers:
            key = paper['Key']
            arxiv_id = self.s3_client.get_arxiv_id_from_key(key)
            
            if not arxiv_id:
                continue
            
            # If category filter is specified, check metadata
            if category_filter and self.metadata_db:
                metadata = self.metadata_db.get_paper_metadata(arxiv_id)
                if not metadata or category_filter not in metadata.get('categories', ''):
                    continue
            
            self.queue_paper(arxiv_id)
            count += 1
            
            if max_papers and count >= max_papers:
                break
        
        logger.info(f"Queued {count} papers for processing")
    
    def process_papers_from_database(self, max_papers: Optional[int] = None,
                                   category_filter: Optional[str] = None):
        """
        Process papers by querying the metadata database.
        
        Args:
            max_papers: Maximum number of papers to process (None for all)
            category_filter: Only process papers in this category
        """
        if not self.metadata_db:
            logger.error("Metadata database not available")
            return
        
        logger.info(f"Starting bulk processing from metadata database")
        
        # Get papers from database
        if category_filter:
            papers = self.metadata_db.get_papers_by_category(
                category=category_filter,
                limit=max_papers or 1000000
            )
        else:
            # Process in batches to avoid loading too many papers at once
            papers = []
            batch_size = min(1000, max_papers or 1000)
            last_id = None
            
            while True:
                batch = self.metadata_db.get_papers_batch(
                    start_id=last_id,
                    limit=batch_size
                )
                
                if not batch:
                    break
                
                papers.extend(batch)
                last_id = batch[-1]['id']
                
                if max_papers and len(papers) >= max_papers:
                    papers = papers[:max_papers]
                    break
        
        logger.info(f"Found {len(papers)} papers in database")
        
        # Queue papers for processing
        count = 0
        for paper in papers:
            arxiv_id = paper.get('id') or paper.get('arxiv_id')
            if not arxiv_id:
                continue
            
            self.queue_paper(arxiv_id)
            count += 1
        
        logger.info(f"Queued {count} papers for processing")
    
    def process_papers_from_file(self, file_path: str):
        """
        Process papers listed in a file.
        
        Args:
            file_path: Path to a file containing arXiv IDs, one per line
        """
        logger.info(f"Starting bulk processing from file {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                arxiv_ids = [line.strip() for line in f if line.strip()]
            
            logger.info(f"Found {len(arxiv_ids)} papers in file")
            
            # Queue papers for processing
            count = 0
            for arxiv_id in arxiv_ids:
                self.queue_paper(arxiv_id)
                count += 1
            
            logger.info(f"Queued {count} papers for processing")
            
        except Exception as e:
            logger.error(f"Error reading paper IDs from file {file_path}: {str(e)}")
    
    def wait_for_completion(self, timeout: Optional[float] = None):
        """
        Wait for all queued papers to be processed.
        
        Args:
            timeout: Maximum time to wait in seconds (None for no timeout)
        """
        try:
            self.work_queue.join()
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error waiting for completion: {str(e)}")


def create_config_from_args(args):
    """
    Create a configuration object from command-line arguments.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        Configuration object
    """
    # Set up output directory
    output_dir = args.output_dir
    if not output_dir:
        output_dir = tempfile.mkdtemp()
    os.makedirs(output_dir, exist_ok=True)
    
    # Set up metadata database directory
    metadata_dir = args.metadata_dir
    if not metadata_dir:
        metadata_dir = os.path.join(output_dir, "metadata")
    os.makedirs(metadata_dir, exist_ok=True)
    
    # Create config object
    config = type('Config', (), {
        # S3 settings
        'aws_access_key_id': args.aws_access_key_id,
        'aws_secret_access_key': args.aws_secret_access_key,
        'endpoint_url': args.endpoint_url,
        'region_name': args.region_name or ARXIV_S3_REGION,
        'public_access': not (args.aws_access_key_id and args.aws_secret_access_key),
        
        # Tor settings
        'use_tor': args.use_tor,
        'tor_socks_port': args.tor_socks_port or 9050,
        'tor_control_port': args.tor_control_port or 9051,
        'tor_control_password': args.tor_password,
        
        # Coordination settings
        'use_coordination_server': bool(args.coordination_server),
        'coordination_server': args.coordination_server,
        
        # Memory settings
        'max_memory_usage': args.max_memory or MAX_MEMORY_USAGE,
        'memory_check_interval': MEMORY_CHECK_INTERVAL,
        
        # LibGen settings
        'libgen_api_endpoint': args.libgen_api_endpoint or "http://libgen.rs/json.php",
        'libgen_upload_endpoint': args.libgen_upload_endpoint or "http://libgen.rs/upload/",
        
        # File settings
        'keep_files': args.keep_files,
        'output_dir': output_dir,
        
        # Worker settings
        'max_workers': args.max_workers or min(multiprocessing.cpu_count(), 5),
        'wait_between_requests': args.wait_between or 1.0,
        
        # Metadata settings
        'use_metadata_db': args.use_metadata_db,
        'metadata_db_path': os.path.join(metadata_dir, "arxiv-metadata.sqlite"),
        
        # Progress settings
        'progress_file': args.progress_file or BACKUP_PROGRESS_FILE,
    })
    
    return config


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='ArXiv Bulk Data Backup Tool')
    
    # Input sources
    input_group = parser.add_argument_group('Input Sources')
    input_group.add_argument('--from-s3', action='store_true', help='Process papers directly from S3')
    input_group.add_argument('--from-db', action='store_true', 
                           help='Process papers from the metadata database')
    input_group.add_argument('--from-file', help='Process papers listed in a file')
    input_group.add_argument('--max-papers', type=int, help='Maximum number of papers to process')
    input_group.add_argument('--category', help='Only process papers in this category')
    
    # S3 settings
    s3_group = parser.add_argument_group('S3 Settings')
    s3_group.add_argument('--aws-access-key-id', help='AWS access key ID')
    s3_group.add_argument('--aws-secret-access-key', help='AWS secret access key')
    s3_group.add_argument('--endpoint-url', help='S3 endpoint URL')
    s3_group.add_argument('--region-name', help='AWS region name')
    
    # Output settings
    output_group = parser.add_argument_group('Output Settings')
    output_group.add_argument('--output-dir', help='Directory to save downloaded files')
    output_group.add_argument('--keep-files', action='store_true', 
                            help='Keep downloaded files after upload')
    output_group.add_argument('--metadata-dir', help='Directory for metadata database')
    output_group.add_argument('--progress-file', help='Path to progress file')
    
    # LibGen settings
    libgen_group = parser.add_argument_group('LibGen Settings')
    libgen_group.add_argument('--libgen-api-endpoint', help='LibGen API endpoint')
    libgen_group.add_argument('--libgen-upload-endpoint', help='LibGen upload endpoint')
    
    # Tor settings
    tor_group = parser.add_argument_group('Tor Settings')
    tor_group.add_argument('--use-tor', action='store_true', 
                         help='Use Tor network for communications')
    tor_group.add_argument('--tor-socks-port', type=int, default=9050, help='Tor SOCKS port')
    tor_group.add_argument('--tor-control-port', type=int, default=9051, help='Tor control port')
    tor_group.add_argument('--tor-password', help='Tor control password')
    
    # Coordination settings
    coordination_group = parser.add_argument_group('Coordination Settings')
    coordination_group.add_argument('--coordination-server', help='URL of coordination server')
    
    # Metadata settings
    metadata_group = parser.add_argument_group('Metadata Settings')
    metadata_group.add_argument('--use-metadata-db', action='store_true', 
                              help='Use metadata database')
    
    # Worker settings
    worker_group = parser.add_argument_group('Worker Settings')
    worker_group.add_argument('--max-workers', type=int, 
                            help='Maximum worker threads')
    worker_group.add_argument('--max-memory', type=float, 
                            help='Maximum memory usage (0.0-1.0)')
    worker_group.add_argument('--wait-between', type=float, 
                            help='Wait time between requests')
    
    args = parser.parse_args()
    
    # Check for required arguments
    if not any([args.from_s3, args.from_db, args.from_file]):
        parser.error("At least one input source is required: --from-s3, --from-db, or --from-file")
    
    # Create config
    config = create_config_from_args(args)
    
    # Create and start worker
    worker = ArxivBulkBackupWorker(config)
    worker.start()
    
    try:
        # Process papers from the selected source
        if args.from_s3:
            worker.process_papers_from_s3(
                max_papers=args.max_papers,
                category_filter=args.category
            )
        
        if args.from_db:
            worker.process_papers_from_database(
                max_papers=args.max_papers,
                category_filter=args.category
            )
        
        if args.from_file:
            worker.process_papers_from_file(args.from_file)
        
        # Wait for all papers to be processed
        worker.wait_for_completion()
        
        logger.info("All papers have been processed")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        worker.stop()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
