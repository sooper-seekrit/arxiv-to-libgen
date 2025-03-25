# ArXiv Bulk Data Backup Tool

First read [the readme](./README.md) and install requirements etc.

The tool `bulk_backup.py` leverages arXiv's S3-based bulk data access as described in the [documentation](https://info.arxiv.org/help/bulk_data_s3.html).
This tool is designed to efficiently back up large numbers of arXiv papers to Library Genesis while maintaining compatibility with `axriv_backup.py`'s features.

### Key Features

1. **Direct S3 Access**: Connects to arXiv's public S3 buckets to download papers in bulk
2. **Metadata Database Integration**: Downloads and uses arXiv's metadata SQLite database for efficient paper processing
3. **Multiple Input Sources**:
   - S3 bucket listing (most efficient for mass backups)
   - SQLite metadata database queries (allows filtering by category)
   - File-based ID lists (for targeted backups)
4. **Efficient Processing**:
   - Multithreading for parallel downloads and uploads
   - Memory monitoring to prevent out-of-memory errors
   - Progress tracking with resume capability
5. **All Original Features**:
   - Tor network routing
   - Coordination between instances
   - Memory usage monitoring
   - LibGen uploads with complete metadata

### Usage Examples

**Basic S3-based backup**:
```bash
python bulk_backup.py --from-s3 --max-papers 1000
```

**Category-specific backup using the metadata database**:
```bash
python bulk_backup.py --from-db --category cs.AI --use-metadata-db
```

**Process a list of specific arXiv IDs over Tor**:
```bash
python bulk_backup.py --from-file arxiv_ids.txt --use-tor
```

**Full-scale backup with coordination**:
```bash
python bulk_backup.py --from-s3 --use-metadata-db --coordination-server http://server:5000
```

### Key Components

1. **ArxivS3Client**: Handles S3 bucket access, paper listing, and downloading
2. **ArxivMetadataDB**: Interface to the arXiv metadata SQLite database
3. **BulkBackupProgressTracker**: Tracks backup progress and handles resumption
4. **ArxivBulkBackupWorker**: Orchestrates the backup process with multiple threads
