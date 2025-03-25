# Tests for ArXiv Backup Tool

Comprehensive test suites for both the ArXiv Backup Tool and the Coordination Server.

## Test Structure

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

## Key Features

- **Mocked Dependencies**: All external services (ArXiv API, LibGen, Tor) are mocked to avoid real network calls
- **Temporary File Handling**: Creates and cleans up temporary directories
- **Isolation**: Tests remain isolated from each other
- **Configurations**: Includes pytest configuration in conftest.py

## Running the Tests

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

## Notable Test Cases

- **Memory Management**: Tests MemoryMonitor's ability to pause operations when memory usage is too high
- **Tor Integration**: Tests TorNetwork's functionality for routing requests through Tor
- **Coordination Server**: Tests the RESTful API and status tracking
- **Error Handling**: Tests various failure scenarios and error handling
- **Complete Workflow**: Simulates the entire process from downloading a paper to uploading it to LibGen
