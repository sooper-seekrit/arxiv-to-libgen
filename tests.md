# Tests for ArXiv Backup Tools

Comprehensive test suites for 
- ArXiv Backup Tool
- Coordination Server
- Bulk Backup Tool

## Bulk Test Structure

### Unit Tests

1. **ArxivS3Client Tests**
   - Testing S3 client initialization
   - Listing papers (both standard and paginated)
   - Downloading papers (to file and memory)
   - Extracting arXiv IDs from S3 keys (including legacy format support)

2. **ArxivMetadataDB Tests**
   - Database connection handling
   - Metadata retrieval for individual papers
   - Batch operations
   - Category filtering queries

3. **BulkBackupProgressTracker Tests**
   - Loading/saving progress from disk
   - Paper status management (completed, failed, in-progress)
   - Statistics generation

### Integration Tests

1. **ArxivBulkBackupWorker Tests**
   - Worker initialization and component setup
   - Single paper processing workflow
   - Bulk processing from multiple sources:
     - S3 bucket listing
     - Metadata database queries
     - File-based ID lists

2. **Config and Main Function Tests**
   - Command-line argument processing
   - Configuration object creation

### Test Fixtures

The tests use a variety of fixtures to isolate external dependencies:

1. **Mocked S3 Client** - Prevents actual AWS API calls
2. **In-memory SQLite Database** - Creates a real database with test data
3. **Temporary Progress Files** - For testing serialization/deserialization
4. **Mocked LibGen and Coordination Clients** - Reuses components from original tests

### Test Coverage

These tests cover all major components and functionality specific to the bulk backup tool:

- S3 access patterns
- Metadata database usage
- Progress tracking and resumability
- Integration between components
- Error handling and edge cases

## test_arxiv_backup Structure

1. **Unit Tests**: Test individual components in isolation
   - ArxivClient (search, download, metadata retrieval)
   - LibgenClient (paper existence checks, MD5 calculation, PDF metadata extraction, uploads)
   - MemoryMonitor (resource management)
   - CoordinationClient (paper registration and completion)

2. **Integration Tests**: Test interactions between components
   - ArxivToLibgenWorker (initialization, paper processing)
   - Coordination Server API endpoints (register, complete, status)

3. **Functional Tests**: Test complete workflows
   - End-to-end paper processing with mocked external services

### Key Features

- **Mocked Dependencies**: All external services (ArXiv API, LibGen, Tor) are mocked to avoid real network calls
- **Temporary File Handling**: Creates and cleans up temporary directories
- **Isolation**: Tests remain isolated from each other
- **Configurations**: Includes pytest configuration in conftest.py

### Running the Tests

To run all tests:
```bash
pytest -xvs test_arxiv_backup.py
```

To run specific test classes:
```bash
pytest -xvs test_arxiv_backup.py::TestArxivClient
```

To run with coverage report:
```bash
pytest -xvs --cov=arxiv_backup --cov=coordination_server test_arxiv_backup.py
```

### Notable Test Cases

- **Memory Management**: Tests MemoryMonitor's ability to pause operations when memory usage is too high
- **Tor Integration**: Tests TorNetwork's functionality for routing requests through Tor
- **Coordination Server**: Tests the RESTful API and status tracking
- **Error Handling**: Tests various failure scenarios and error handling
- **Complete Workflow**: Simulates the entire process from downloading a paper to uploading it to LibGen
