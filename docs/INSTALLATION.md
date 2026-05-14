# Installation & Setup Guide

This guide walks through installing and running the Autonomous Engineering Runtime locally.

---

# Requirements

## Supported Environment

- Python 3.11+
- Linux, macOS, or Windows
- asyncio-compatible environment

---

# Clone Repository

```bash
git clone https://github.com/NahstyDev/autonomous-engineering-runtime.git

cd autonomous-engineering-runtime
```

---

# Create Virtual Environment

## Windows

```bash
python -m venv .venv

.venv\Scripts\activate
```

---

## Linux / macOS

```bash
python3 -m venv .venv

source .venv/bin/activate
```

---

# Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Verify Installation

Run the test suite:

```bash
pytest
```

If installation succeeds, the runtime test suite should execute successfully.

---

# Running the Runtime

## Start Runtime With Default Configuration

```bash
python -m autonomous_runtime
```

---

## Start Runtime With Custom Runtime Name

```bash
python -m autonomous_runtime --name dev-runtime
```

---

## Override Worker Count

```bash
python -m autonomous_runtime --workers 8
```

---

## Override Log Level

```bash
python -m autonomous_runtime --log-level DEBUG
```

---

## Start Using Config File

```bash
python -m autonomous_runtime --config config.json
```

---

# Runtime Data Directories

The runtime automatically provisions runtime directories during bootstrap.

Default structure:

```text
runtime_data/
├── artifacts/
├── checkpoints/
├── journal/
└── runtime.db
```

---

# Environment Variables

The runtime supports configuration through `RUNTIME_*` environment variables.

Examples:

```bash
RUNTIME_ENV=local
RUNTIME_WORKER_COUNT=4
RUNTIME_MAX_CONCURRENT_TASKS=16
RUNTIME_LOG_LEVEL=INFO
RUNTIME_DATA_DIR=./runtime_data
```

---

# Running Integration Tests

Run all tests:

```bash
pytest
```

Run verbose output:

```bash
pytest -v
```

Run a specific test file:

```bash
pytest tests/test_runtime.py
```

---

# Common Development Workflow

## Activate Virtual Environment

```bash
# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate
```

---

## Install Updated Dependencies

```bash
pip install -r requirements.txt
```

---

## Run Tests

```bash
pytest
```

---

## Start Runtime

```bash
python -m autonomous_runtime
```

---

# Troubleshooting

## Python Version Errors

Verify Python version:

```bash
python --version
```

Python 3.11 or newer is required.

---

## Missing Dependencies

Reinstall dependencies:

```bash
pip install -r requirements.txt
```

---

## Virtual Environment Not Activated

Ensure the virtual environment is active before running the runtime.

---

## Permission Issues

If filesystem provisioning fails:

- Verify write access to the project directory
- Verify runtime data directory permissions
- Avoid protected system directories

---

# Current Runtime Status

The current implementation includes:

- Runtime bootstrap system
- Lifecycle supervision
- Event bus
- Workflow scheduling
- Async worker queue
- Concurrency management
- Orchestration engine
- Integration testing infrastructure

Persistence, replay, and recovery systems are currently under active development.