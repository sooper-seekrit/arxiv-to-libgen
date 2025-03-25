# ArXiv Backup Tool

The client is found in `arxiv_backup.py` the server in `coordination_server.py`.
For bulk backups read [./BULK.md](BULK.md).

First we document the client, then the server.

## Client

Python tool for backing up arXiv papers to Library Genesis in case the worst happens.

### Core Functionality
- Downloads PDFs from arXiv.org using their API
- Extracts metadata from both the arXiv API and PDF files
- Uploads papers to LibGen with complete metadata
- Deletes files after successful upload (unless `--keep-files` is specified)

### Technical Features
- **Tor Network Support**: Routes all traffic through Tor when `--use-tor` is enabled
- **Multithreading**: Processes multiple papers concurrently with configurable thread count
- **Memory Management**: Monitors memory usage and pauses operations when reaching 80% of free memory
- **Coordination**: Prevents duplicate uploads via either:
  - A central coordination server
  - A peer-to-peer torrent-based system

### Usage Examples

Basic usage with a search query:
```bash
python arxiv_backup.py --query "quantum computing" --max-results 20
```

Process specific arXiv IDs over Tor:
```bash
python arxiv_backup.py --ids "2101.01234,2101.56789" --use-tor --tor-password "your_password"
```

Process IDs from a file with coordination:
```bash
python arxiv_backup.py --id-file arxiv_ids.txt --coordination-server "http://server:5000"
```

Use torrent-based coordination:
```bash
python arxiv_backup.py --query "machine learning" --use-torrents
```

Run a coordination server and process papers:
```bash
python arxiv_backup.py --query "neural networks" --run-coordination-server
```

### Installation

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up Tor if needed (optional):
   ```bash
   sudo apt-get install tor
   sudo systemctl start tor
   ```

## Server

The server can be run independently from the main arXiv backup tool.  This server tracks which arXiv papers are being processed by different instances of the backup tool, preventing duplicate work and ensuring efficient resource usage.

### Features

- **RESTful API**: Endpoints for paper registration, completion, and status checking
- **Web Interface**: Provides a human-readable status dashboard
- **Timeout Handling**: Automatically detects and marks stalled paper processing
- **State Persistence**: Periodically saves state to disk for recovery after restarts
- **Summary Statistics**: Provides overview of processing status and success rates

### API Endpoints

- `/register` (POST): Register a paper as being processed
- `/complete` (POST): Mark a paper as completed or failed
- `/status` (GET): Get the status of all papers or a specific paper
- `/summary` (GET): Get processing statistics
- `/reset` (POST): Admin function to reset paper status

### Usage

Run the server independently:
```bash
python coordination_server.py --host 0.0.0.0 --port 5000 --admin-key your_secret_key
```

Then configure the main backup tool to use this server:
```bash
python arxiv_backup.py --query "machine learning" --coordination-server "http://server:5000"
```

### Security

The `/reset` endpoint requires an admin key for security. Set it using the `--admin-key` parameter or the `ADMIN_KEY` environment variable.


## Notes

- The LibGen upload implementation follows the guidelines from [their wiki page](https://wiki.mhut.org/content:how_to_upload)
- For torrent functionality, the `libtorrent-python` package is required
- The coordination server uses Flask and can be run in the same process or separately
- Both the coordination server and torrent functionality should help ensure that only one instance uploads each paper
