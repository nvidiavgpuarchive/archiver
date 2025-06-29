<div align="center">
  <img width="500px" alt="logo" src="logo_archiver.png"/></a>
  <br/>
</div>

## Overview

![Demo](demo.gif)

This project is a Python 3.12 utility for downloading, verifying, and managing drivers and related files from various
sources, and optionally uploading them to a remote archive. It includes functionality for chunked asynchronous
downloads, metadata handling, archiving through various storage backends, and optional email integration for
notifications. The architecture is modular, with dedicated components for database handling, email operations,
interactions with storage providers, and portal-like operations for organizing metadata.

- Asynchronous, chunked file downloads with range support
- Pluggable storage and metadata management
- Email client integration for notifications or workflow automation
- Parallel and robust workflow with graceful shutdown handling
- Integration with remote storage (e.g., archival backend)
- Detailed logging and progress tracking

## Setup Instructions

### Prerequisites

- **Python 3.12.8** is required.
- macOS Sonoma (aarch64) recommended for maximum compatibility.
- [uv package manager](https://github.com/astral-sh/uv), which is compatible with `poetry` CLI syntax.  
  (If you don't have `uv`, please [see the docs](https://github.com/astral-sh/uv#getting-started))

### Installation

1. **Clone the Repository**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```

2. **Install Dependencies**
    ```bash
    uv install
    # or to only install dependencies
    uv sync
    ```

3. **Configuration**  
   Rename the `config.example/` dir into `config/` and edit it to suit your need.


4. **Running the Application**
    ```bash
    uv run python main.py --help 
    ```
