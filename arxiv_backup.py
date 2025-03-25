#!/usr/bin/env python3
"""
ArXiv to LibGen Backup Tool

This tool:
- Downloads PDFs from arxiv.org
- Uploads them to Library Genesis with complete metadata
- Deletes files after successful upload (unless specified otherwise)
- Supports Tor network routing
- Uses multithreading for parallel processing
- Monitors memory usage to stay below 80% of free memory
- Coordinates between instances using either a server or torrents
"""

import argparse
import os
import sys
import time
import threading
import queue
import logging
import tempfile
import hashlib
import json
import random
import requests
import shutil
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs
import psutil
import xml.etree.ElementTree as ET

# For Tor network support
import socks
from stem import Signal
from stem.control import Controller

# PDF metadata extraction
try:
    from PyPDF2 import PdfReader
except ImportError:
    from PyPDF2 import PdfFileReader as PdfReader

# Torrents
try:
    import libtorrent as lt
    HAVE_LIBTORRENT = True
except ImportError:
    HAVE_LIBTORRENT = False

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("arxiv_backup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("arxiv_backup")

# Global settings
MAX_MEMORY_USAGE = 0.8  # 80% of available memory
MEMORY_CHECK_INTERVAL = 5  # seconds
COORDINATION_SERVER = os.environ.get("COORDINATION_SERVER", "http://localhost:5000")
USE_COORDINATION_SERVER = os.environ.get("USE_COORDINATION_SERVER", "false").lower() == "true"
USE_TOR = os.environ.get("USE_TOR", "false").lower() == "true"
TOR_SOCKS_PORT = int(os.environ.get("TOR_SOCKS_PORT", 9050))
TOR_CONTROL_PORT = int(os.environ.get("TOR_CONTROL_PORT", 9051))
TOR_CONTROL_PASSWORD = os.environ.get("TOR_CONTROL_PASSWORD", "")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 5))
WAIT_BETWEEN_REQUESTS = float(os.environ.get("WAIT_BETWEEN_REQUESTS", 1.0))
LIBGEN_API_ENDPOINT = os.environ.get("LIBGEN_API_ENDPOINT", "http://libgen.rs/json.php")
LIBGEN_UPLOAD_ENDPOINT = os.environ.get("LIBGEN_UPLOAD_ENDPOINT", "http://libgen.rs/upload/")


class MemoryMonitor:
    """Monitors and controls memory usage to stay below the specified threshold."""
    
    def __init__(self, max_usage_fraction=0.8, check_interval=5):
        self.max_usage_fraction = max_usage_fraction
        self.check_interval = check_interval
        self.pause_event = threading.Event()
        self._stop_event = threading.Event()
        self._monitor_thread = threading.Thread(target=self._monitor_memory)
        self._monitor_thread.daemon = True
        self.current_usage = 0

    def start(self):
        """Start the memory monitoring thread."""
        self._monitor_thread.start()

    def stop(self):
        """Stop the memory monitoring thread."""
        self._stop_event.set()
        self._monitor_thread.join()

    def _monitor_memory(self):
        """Monitor memory usage and pause operations if threshold is exceeded."""
        while not self._stop_event.is_set():
            svmem = psutil.virtual_memory()
            used_percent = svmem.percent / 100.0
            self.current_usage = used_percent
            
            if used_percent > self.max_usage_fraction:
                if not self.pause_event.is_set():
                    logger.warning(f"Memory usage at {used_percent*100:.2f}%, pausing operations")
                    self.pause_event.set()
            else:
                if self.pause_event.is_set():
                    logger.info(f"Memory usage at {used_percent*100:.2f}%, resuming operations")
                    self.pause_event.clear()
            
            time.sleep(self.check_interval)

    def wait_for_resources(self):
        """Block until memory resources are available."""
        if self.pause_event.is_set():
            logger.info("Waiting for memory resources to become available")
            self.pause_event.wait()
            logger.info("Resources available, continuing")


class TorNetwork:
    """Handles connections and identity management through the Tor network."""
    
    def __init__(self, socks_port=9050, control_port=9051, password=""):
        self.socks_port = socks_port
        self.control_port = control_port
        self.password = password
        self.session = requests.Session()
        
    def setup_tor_proxy(self):
        """Configure requests to use Tor as a proxy."""
        self.session.proxies = {
            'http': f'socks5h://127.0.0.1:{self.socks_port}',
            'https': f'socks5h://127.0.0.1:{self.socks_port}'
        }
        return self.session
        
    def new_identity(self):
        """Request a new Tor identity (circuit)."""
        try:
            with Controller.from_port(port=self.control_port) as controller:
                if self.password:
                    controller.authenticate(password=self.password)
                else:
                    controller.authenticate()
                controller.signal(Signal.NEWNYM)
                logger.info("New Tor identity established")
                return True
        except Exception as e:
            logger.error(f"Error establishing new Tor identity: {e}")
            return False


class CoordinationClient:
    """Client for the coordination server to prevent duplicate downloads/uploads."""
    
    def __init__(self, server_url):
        self.server_url = server_url
        self.session = requests.Session()
    
    def register_paper(self, arxiv_id):
        """Register that we're downloading this paper to avoid duplicates."""
        try:
            response = self.session.post(
                f"{self.server_url}/register",
                json={"arxiv_id": arxiv_id}
            )
            if response.status_code == 200:
                result = response.json()
                return result.get("status") == "ok"
            return False
        except Exception as e:
            logger.error(f"Error registering paper with coordination server: {e}")
            return False
    
    def complete_paper(self, arxiv_id, success=True):
        """Mark a paper as completed (uploaded to LibGen)."""
        try:
            response = self.session.post(
                f"{self.server_url}/complete",
                json={"arxiv_id": arxiv_id, "success": success}
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error completing paper with coordination server: {e}")
            return False


class ArxivClient:
    """Client for downloading papers and metadata from arXiv."""
    
    def __init__(self, use_tor=False, tor_handler=None):
        self.base_url = "https://export.arxiv.org/api/query"
        self.pdf_base_url = "https://arxiv.org/pdf/"
        self.tor_handler = tor_handler
        
        if use_tor and tor_handler:
            self.session = tor_handler.setup_tor_proxy()
        else:
            self.session = requests.Session()
    
    def search(self, query, start=0, max_results=100):
        """Search arxiv for papers matching the query."""
        params = {
            'search_query': query,
            'start': start,
            'max_results': max_results
        }
        
        response = self.session.get(self.base_url, params=params)
        
        if response.status_code == 200:
            return ET.fromstring(response.content)
        else:
            logger.error(f"ArXiv API error: {response.status_code}")
            return None
    
    def download_paper(self, arxiv_id, output_dir=None):
        """Download a paper from arxiv by ID."""
        if not output_dir:
            output_dir = tempfile.mkdtemp()
            
        # Ensure the ID is in the correct format for the PDF URL
        arxiv_id = arxiv_id.replace('http://arxiv.org/abs/', '')
        arxiv_id = arxiv_id.replace('https://arxiv.org/abs/', '')
        
        pdf_url = f"{self.pdf_base_url}{arxiv_id}.pdf"
        output_path = os.path.join(output_dir, f"{arxiv_id.replace('/', '_')}.pdf")
        
        try:
            response = self.session.get(pdf_url, stream=True)
            
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                logger.info(f"Downloaded {arxiv_id} to {output_path}")
                return output_path
            else:
                logger.error(f"Error downloading {arxiv_id}: HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error downloading {arxiv_id}: {e}")
            return None
    
    def get_paper_metadata(self, arxiv_id):
        """Get metadata for a paper from arxiv."""
        try:
            # Ensure the ID is in the correct format for the API
            arxiv_id = arxiv_id.replace('http://arxiv.org/abs/', '')
            arxiv_id = arxiv_id.replace('https://arxiv.org/abs/', '')
            
            params = {
                'id_list': arxiv_id
            }
            
            response = self.session.get(self.base_url, params=params)
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                ns = {'atom': 'http://www.w3.org/2005/Atom'}
                entry = root.find('.//atom:entry', ns)
                
                if entry is not None:
                    metadata = {
                        'id': arxiv_id,
                        'title': entry.find('./atom:title', ns).text.strip(),
                        'abstract': entry.find('./atom:summary', ns).text.strip(),
                        'authors': [author.find('./atom:name', ns).text for author in entry.findall('./atom:author', ns)],
                        'categories': [c.get('term') for c in entry.findall('./atom:category', ns)],
                        'published': entry.find('./atom:published', ns).text,
                        'updated': entry.find('./atom:updated', ns).text,
                        'pdf_url': f"{self.pdf_base_url}{arxiv_id}.pdf"
                    }
                    return metadata
            
            logger.error(f"Error getting metadata for {arxiv_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting metadata for {arxiv_id}: {e}")
            return None


class LibgenClient:
    """Client for interacting with Library Genesis API and uploading papers."""
    
    def __init__(self, api_endpoint, upload_endpoint, use_tor=False, tor_handler=None):
        self.api_endpoint = api_endpoint
        self.upload_endpoint = upload_endpoint
        self.tor_handler = tor_handler
        
        if use_tor and tor_handler:
            self.session = tor_handler.setup_tor_proxy()
        else:
            self.session = requests.Session()
    
    def check_if_exists(self, title=None, author=None, md5=None):
        """Check if a paper already exists in LibGen."""
        params = {}
        if title:
            params['title'] = title
        if author:
            params['author'] = author
        if md5:
            params['md5'] = md5
        
        try:
            response = self.session.get(self.api_endpoint, params=params)
            if response.status_code == 200:
                result = response.json()
                return len(result) > 0
            return False
        except Exception as e:
            logger.error(f"Error checking LibGen: {e}")
            return False
    
    def calculate_md5(self, file_path):
        """Calculate MD5 hash of a file."""
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()
    
    def extract_pdf_metadata(self, file_path):
        """Extract metadata from PDF file."""
        try:
            with open(file_path, 'rb') as f:
                pdf = PdfReader(f)
                info = pdf.metadata
                if info:
                    metadata = {
                        'title': info.get('/Title', ''),
                        'author': info.get('/Author', ''),
                        'creator': info.get('/Creator', ''),
                        'producer': info.get('/Producer', ''),
                        'subject': info.get('/Subject', ''),
                        'num_pages': len(pdf.pages)
                    }
                    return metadata
                return None
        except Exception as e:
            logger.error(f"Error extracting PDF metadata: {e}")
            return None
    
    def upload_to_libgen(self, file_path, arxiv_metadata):
        """Upload a file to LibGen with metadata."""
        # This implementation follows the LibGen upload guide
        try:
            # First check if paper already exists
            author_string = ', '.join(arxiv_metadata['authors'])
            exists = self.check_if_exists(
                title=arxiv_metadata['title'],
                author=author_string
            )
            
            if exists:
                logger.info(f"Paper already exists in LibGen: {arxiv_metadata['title']}")
                return True
            
            # Calculate MD5 to check for duplicates and for upload
            md5 = self.calculate_md5(file_path)
            exists_by_md5 = self.check_if_exists(md5=md5)
            
            if exists_by_md5:
                logger.info(f"Paper with same MD5 already exists in LibGen: {md5}")
                return True
            
            # Extract PDF metadata to supplement arxiv metadata
            pdf_metadata = self.extract_pdf_metadata(file_path)
            
            # Prepare upload data according to LibGen requirements
            files = {
                'file': (os.path.basename(file_path), open(file_path, 'rb'), 'application/pdf')
            }
            
            # Format date as YYYY-MM-DD
            pub_date = arxiv_metadata['published'][:10]
            
            data = {
                'title': arxiv_metadata['title'],
                'author': author_string,
                'year': pub_date[:4],  # Extract year from published date
                'publisher': 'arXiv',
                'journal': 'arXiv',
                'volume': '',
                'issue': arxiv_metadata['id'],
                'series': ', '.join(arxiv_metadata['categories']),
                'edition': '',
                'isbn': '',
                'doi': '',
                'url': f"https://arxiv.org/abs/{arxiv_metadata['id']}",
                'language': 'English',  # Assuming English for simplicity
                'pages': pdf_metadata.get('num_pages', '') if pdf_metadata else '',
                'format': 'pdf',
                'library': 'arXiv',
                'file_type': 'application/pdf',
                'commentary': arxiv_metadata['abstract'],
                'oriental': '0',
                'color': '0',
                'cleaned': '0',
                'dpi': '0',
                'searchable': '1',
                'bookmarked': '0',
                'scanned': '0'
            }
            
            response = self.session.post(
                self.upload_endpoint,
                files=files,
                data=data
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully uploaded to LibGen: {arxiv_metadata['title']}")
                return True
            else:
                logger.error(f"Error uploading to LibGen: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading to LibGen: {e}")
            return False


class TorrentCoordinator:
    """Handles torrent-based coordination between instances."""
    
    def __init__(self, torrent_dir, announce_urls=None):
        if not HAVE_LIBTORRENT:
            logger.warning("libtorrent not installed. Torrent functionality disabled.")
            
        self.torrent_dir = torrent_dir
        os.makedirs(self.torrent_dir, exist_ok=True)
        
        self.announce_urls = announce_urls or [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.coppersurfer.tk:6969/announce",
            "udp://tracker.leechers-paradise.org:6969/announce"
        ]
        
        self.session = None
        self.torrents = {}  # Handle -> info_hash
        
        if HAVE_LIBTORRENT:
            self.session = lt.session()
            self.session.listen_on(6881, 6891)
            
            settings = {
                'announce_to_all_tiers': True,
                'announce_to_all_trackers': True,
                'connection_speed': 100,
                'auto_manage_startup_deferred': False,
                'auto_manage_interval': 5
            }
            self.session.set_settings(settings)
    
    def create_torrent(self, file_path):
        """Create a torrent file for the given file."""
        if not HAVE_LIBTORRENT:
            return None
            
        torrent_file = os.path.join(
            self.torrent_dir,
            os.path.basename(file_path) + ".torrent"
        )
        
        try:
            # Create torrent info
            fs = lt.file_storage()
            lt.add_files(fs, file_path)
            
            # Create torrent
            creator = lt.create_torrent(fs)
            for url in self.announce_urls:
                creator.add_tracker(url)
            
            # Set properties
            creator.set_creator("ArXiv Backup Tool")
            creator.set_comment(f"ArXiv backup of {os.path.basename(file_path)}")
            
            # Generate torrent data
            lt.set_piece_hashes(creator, os.path.dirname(file_path))
            torrent = creator.generate()
            
            # Save torrent file
            with open(torrent_file, 'wb') as f:
                f.write(lt.bencode(torrent))
            
            logger.info(f"Created torrent: {torrent_file}")
            return torrent_file
        except Exception as e:
            logger.error(f"Error creating torrent: {e}")
            return None
    
    def seed_file(self, file_path, torrent_file=None):
        """Start seeding a file using the torrent."""
        if not HAVE_LIBTORRENT:
            return None
            
        try:
            # Create torrent if not provided
            if not torrent_file:
                torrent_file = self.create_torrent(file_path)
                if not torrent_file:
                    return None
            
            # Add torrent for seeding
            with open(torrent_file, 'rb') as f:
                torrent_data = f.read()
            
            info = lt.torrent_info(lt.bdecode(torrent_data))
            info_hash = str(info.info_hash())
            
            if info_hash in self.torrents.values():
                logger.info(f"Already seeding: {info_hash}")
                return info_hash
            
            # Create handle based on the torrent
            params = {
                'save_path': os.path.dirname(file_path),
                'ti': info,
                'seed_mode': True
            }
            handle = self.session.add_torrent(params)
            
            self.torrents[handle] = info_hash
            logger.info(f"Started seeding: {os.path.basename(file_path)} ({info_hash})")
            
            return info_hash
        except Exception as e:
            logger.error(f"Error seeding file: {e}")
            return None
    
    def check_if_being_seeded(self, info_hash):
        """Check if a file is already being seeded in the swarm."""
        if not HAVE_LIBTORRENT:
            return False
            
        return info_hash in self.torrents.values()
    
    def stop_all(self):
        """Stop all torrents and close session."""
        if not HAVE_LIBTORRENT or not self.session:
            return
            
        for handle in list(self.torrents.keys()):
            self.session.remove_torrent(handle)
        self.torrents.clear()
        logger.info("Stopped all torrents")


class ArxivToLibgenWorker:
    """Main worker class that orchestrates the backup process."""
    
    def __init__(self, config):
        self.config = config
        self.tor_handler = None
        self.memory_monitor = None
        self.coordination_client = None
        self.torrent_coordinator = None
        
        # Initialize components
        if config.use_tor:
            self.tor_handler = TorNetwork(
                socks_port=config.tor_socks_port,
                control_port=config.tor_control_port,
                password=config.tor_control_password
            )
        
        if config.use_coordination_server:
            self.coordination_client = CoordinationClient(config.coordination_server)
        
        if config.use_torrents and HAVE_LIBTORRENT:
            self.torrent_coordinator = TorrentCoordinator(
                config.torrent_dir,
                announce_urls=config.torrent_announce
            )
        
        self.memory_monitor = MemoryMonitor(
            max_usage_fraction=config.max_memory_usage,
            check_interval=config.memory_check_interval
        )
        
        self.arxiv_client = ArxivClient(
            use_tor=config.use_tor,
            tor_handler=self.tor_handler
        )
        
        self.libgen_client = LibgenClient(
            api_endpoint=config.libgen_api_endpoint,
            upload_endpoint=config.libgen_upload_endpoint,
            use_tor=config.use_tor,
            tor_handler=self.tor_handler
        )
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self.work_queue = queue.Queue()
        self.stop_event = threading.Event()
    
    def start(self):
        """Start all components and worker threads."""
        self.memory_monitor.start()
        if self.tor_handler:
            self.tor_handler.setup_tor_proxy()
        
        # Start worker threads
        for _ in range(self.config.max_workers):
            threading.Thread(target=self._worker_thread, daemon=True).start()
    
    def stop(self):
        """Stop all components and worker threads."""
        self.stop_event.set()
        if self.memory_monitor:
            self.memory_monitor.stop()
        if self.torrent_coordinator:
            self.torrent_coordinator.stop_all()
        self.executor.shutdown(wait=False)
    
    def process_paper(self, arxiv_id):
        """Queue a paper for processing."""
        self.work_queue.put(arxiv_id)
    
    def _worker_thread(self):
        """Worker thread that processes papers from the queue."""
        while not self.stop_event.is_set():
            try:
                arxiv_id = self.work_queue.get(timeout=1)
                if not arxiv_id:
                    continue
                
                # Check if we should process this paper (if using coordination)
                if self.coordination_client:
                    if not self.coordination_client.register_paper(arxiv_id):
                        logger.info(f"Skipping {arxiv_id} as it's being processed by another instance")
                        self.work_queue.task_done()
                        continue
                
                # Process the paper
                self._process_single_paper(arxiv_id)
                
                # Mark task as done
                self.work_queue.task_done()
                
                # Sleep a bit to avoid hammering the services
                time.sleep(random.uniform(
                    self.config.wait_between_requests * 0.5,
                    self.config.wait_between_requests * 1.5
                ))
                
                # Change Tor identity occasionally if using Tor
                if (self.config.use_tor and self.tor_handler and 
                    random.random() < 0.1):  # 10% chance after each paper
                    self.tor_handler.new_identity()
                
            except queue.Empty:
                # No work to do
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in worker thread: {e}")
                time.sleep(1)  # Sleep on error to avoid hammering
    
    def _process_single_paper(self, arxiv_id):
        """Process a single paper from download to upload."""
        success = False
        temp_dir = None
        file_path = None
        
        try:
            # Check memory usage before proceeding
            self.memory_monitor.wait_for_resources()
            
            # Get metadata
            metadata = self.arxiv_client.get_paper_metadata(arxiv_id)
            if not metadata:
                logger.error(f"Failed to get metadata for {arxiv_id}")
                return False
            
            # Create temporary directory
            temp_dir = tempfile.mkdtemp()
            
            # Download paper
            file_path = self.arxiv_client.download_paper(arxiv_id, output_dir=temp_dir)
            if not file_path:
                logger.error(f"Failed to download {arxiv_id}")
                return False
            
            # If using torrents, create and seed the torrent
            if self.torrent_coordinator and HAVE_LIBTORRENT:
                torrent_file = self.torrent_coordinator.create_torrent(file_path)
                if torrent_file:
                    self.torrent_coordinator.seed_file(file_path, torrent_file)
            
            # Upload to LibGen
            success = self.libgen_client.upload_to_libgen(file_path, metadata)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing paper {arxiv_id}: {e}")
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
                    logger.error(f"Error deleting file {file_path}: {e}")
            
            # Clean up temp directory
            if temp_dir:
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception as e:
                    logger.error(f"Error removing temp directory {temp_dir}: {e}")


def create_coordination_server():
    """Create a simple Flask server for coordination between instances."""
    from flask import Flask, request, jsonify
    
    app = Flask(__name__)
    papers_status = {}
    
    @app.route('/register', methods=['POST'])
    def register_paper():
        data = request.json
        arxiv_id = data.get('arxiv_id')
        
        if not arxiv_id:
            return jsonify({"status": "error", "message": "No arxiv_id provided"}), 400
        
        if arxiv_id in papers_status and papers_status[arxiv_id]['status'] == 'processing':
            return jsonify({"status": "error", "message": "Already being processed"}), 409
        
        papers_status[arxiv_id] = {
            'status': 'processing',
            'timestamp': time.time()
        }
        
        return jsonify({"status": "ok"})
    
    @app.route('/complete', methods=['POST'])
    def complete_paper():
        data = request.json
        arxiv_id = data.get('arxiv_id')
        success = data.get('success', False)
        
        if not arxiv_id:
            return jsonify({"status": "error", "message": "No arxiv_id provided"}), 400
        
        papers_status[arxiv_id] = {
            'status': 'completed' if success else 'failed',
            'timestamp': time.time()
        }
        
        return jsonify({"status": "ok"})
    
    @app.route('/status', methods=['GET'])
    def get_status():
        return jsonify(papers_status)
    
    return app


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='ArXiv to LibGen backup tool')
    parser.add_argument('--query', help='ArXiv search query')
    parser.add_argument('--ids', help='Comma-separated list of ArXiv IDs to process')
    parser.add_argument('--id-file', help='File containing ArXiv IDs, one per line')
    parser.add_argument('--keep-files', action='store_true', help='Keep downloaded files after upload')
    parser.add_argument('--output-dir', help='Directory to save downloaded files')
    parser.add_argument('--max-results', type=int, default=100, help='Maximum number of results to process from search')
    parser.add_argument('--use-tor', action='store_true', help='Use Tor network for communications')
    parser.add_argument('--tor-socks-port', type=int, default=9050, help='Tor SOCKS port')
    parser.add_argument('--tor-control-port', type=int, default=9051, help='Tor control port')
    parser.add_argument('--tor-password', help='Tor control password')
    parser.add_argument('--coordination-server', help='URL of coordination server')
    parser.add_argument('--run-coordination-server', action='store_true', help='Run coordination server')
    parser.add_argument('--coordination-port', type=int, default=5000, help='Port for coordination server')
    parser.add_argument('--max-workers', type=int, default=5, help='Maximum worker threads')
    parser.add_argument('--max-memory', type=float, default=0.8, help='Maximum memory usage (0.0-1.0)')
    parser.add_argument('--wait-between', type=float, default=1.0, help='Wait time between requests')
    parser.add_argument('--use-torrents', action='store_true', help='Use torrents for coordination')
    parser.add_argument('--torrent-dir', help='Directory for torrent files')
    parser.add_argument('--torrent-announce', help='Comma-separated announce URLs for torrents')
    
    args = parser.parse_args()
    
    # Check for required arguments
    if not (args.query or args.ids or args.id_file):
        parser.error("At least one of --query, --ids, or --id-file is required")
    
    # Configure output directory
    output_dir = args.output_dir
    if not output_dir:
        output_dir = tempfile.mkdtemp()
    os.makedirs(output_dir, exist_ok=True)
    
    # Configure settings
    config = type('Config', (), {
        'use_tor': args.use_tor or USE_TOR,
        'tor_socks_port': args.tor_socks_port or TOR_SOCKS_PORT,
        'tor_control_port': args.tor_control_port or TOR_CONTROL_PORT,
        'tor_control_password': args.tor_password or TOR_CONTROL_PASSWORD,
        'use_coordination_server': bool(args.coordination_server) or USE_COORDINATION_SERVER,
        'coordination_server': args.coordination_server or COORDINATION_SERVER,
        'max_workers': args.max_workers or MAX_WORKERS,
        'max_memory_usage': args.max_memory or MAX_MEMORY_USAGE,
        'memory_check_interval': MEMORY_CHECK_INTERVAL,
        'wait_between_requests': args.wait_between or WAIT_BETWEEN_REQUESTS,
        'libgen_api_endpoint': LIBGEN_API_ENDPOINT,
        'libgen_upload_endpoint': LIBGEN_UPLOAD_ENDPOINT,
        'keep_files': args.keep_files,
        'output_dir': output_dir,
        'use_torrents': args.use_torrents,
        'torrent_dir': args.torrent_dir or os.path.join(output_dir, 'torrents'),
        'torrent_announce': args.torrent_announce.split(",") if args.torrent_announce else None
    })
    
    # Start coordination server if requested
    if args.run_coordination_server:
        try:
            from threading import Thread
            from flask import Flask
            app = create_coordination_server()
            Thread(target=lambda: app.run(host='0.0.0.0', port=args.coordination_port), daemon=True).start()
            logger.info(f"Started coordination server on port {args.coordination_port}")
        except ImportError:
            logger.error("Flask not installed. Cannot run coordination server.")
            sys.exit(1)
    
    # Initialize worker
    worker = ArxivToLibgenWorker(config)
    worker.start()
    
    try:
        # Process ArXiv IDs from various sources
        arxiv_ids = []
        
        # From direct IDs argument
        if args.ids:
            arxiv_ids.extend(args.ids.split(','))
        
        # From ID file
        if args.id_file:
            with open(args.id_file, 'r') as f:
                arxiv_ids.extend([line.strip() for line in f if line.strip()])
        
        # From search query
        if args.query:
            arxiv_client = worker.arxiv_client
            results = arxiv_client.search(args.query, max_results=args.max_results)
            
            if results:
                ns = {'atom': 'http://www.w3.org/2005/Atom'}
                entries = results.findall('.//atom:entry', ns)
                for entry in entries:
                    id_el = entry.find('./atom:id', ns)
                    if id_el is not None:
                        # Extract ID from the arxiv URL
                        arxiv_url = id_el.text.strip()
                        arxiv_id = arxiv_url.split('/')[-1]
                        arxiv_ids.append(arxiv_id)
        
        # Process all IDs
        logger.info(f"Processing {len(arxiv_ids)} ArXiv papers")
        for arxiv_id in arxiv_ids:
            worker.process_paper(arxiv_id)
        
        # Wait for processing to complete
        worker.work_queue.join()
        logger.info("All papers have been processed")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        worker.stop()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
