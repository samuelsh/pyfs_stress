[![Tests](https://github.com/samuelsh/pyfs_stress/actions/workflows/tests.yml/badge.svg?branch=devel)](https://github.com/samuelsh/pyfs_stress/actions/workflows/tests.yml)

# pyfs_stress

Multi-client filesystem load and stress testing tool built on ZeroMQ.

Useful for anyone developing a new filesystem or testing an existing one.

## Use Cases

1. **Load & Stress** -- Run dozens of clients, each with multiple I/O processes, against your file server simultaneously.
2. **Race Conditions** -- Client X tries to access a file that client Y is removing while client Z is reading it.
3. **Data Corruption Detection** -- The controller tracks the expected state of every file and directory, catching lost files or bad data.

## Features

- Multi-client load & stress (tested with 60+ clients)
- Data integrity and corruption monitoring via in-memory state tracking and xxHash checksums
- NFS (v3, v4, v4.1) and SMB (v1, v2, v3) mounts
- Filesystem operations: `mkdir`, `list`, `delete`, `touch`, `stat`, `read`, `write`, `rename`, `move`, `truncate`
- Random and sequential reads/writes
- NFS advisory file locking (native `fcntl` and Redis-backed application locks)
- Centralized PUB/SUB logging across all clients
- Configurable workloads and weighted operation profiles (JSON)
- Reproducible runs via `--seed` and operation journaling (JSONL)
- Fail-fast mode via `--strict`

## Architecture

```
┌──────────────────┐       ZMQ        ┌──────────────┐
│  fileops_server   │◄────────────────►│   client N    │
│  (controller)     │  PUSH/PULL +     │  (dynamo)     │
│                   │  PUB/SUB logs    │               │
│  - DirTree state  │                  │  - fs ops     │
│  - job dispatch   │                  │  - locking    │
│  - result verify  │                  │  - checksums  │
└──────────────────┘                  └──────────────┘
```

| Directory          | Purpose                                             |
|--------------------|-----------------------------------------------------|
| `server/`          | Controller, async job dispatch, response validation |
| `client/`          | Client worker (dynamo), mounter, file operations    |
| `tree/`            | Thread-safe in-memory DirTree for expected state    |
| `config/`          | Ports, paths, Redis settings (all env-configurable) |
| `logger/`          | ZMQ PUB/SUB and console loggers                    |
| `utils/`           | SSH, IP, shell, filesystem utilities                |
| `io_tools/`        | Standalone I/O micro-benchmarks and stress scripts  |
| `workloads/`       | JSON workload profiles                              |
| `tests/`           | Pytest test suite                                   |

## Requirements

- Python 3.10+
- Redis server (for application-level byte-range locking)
- Passwordless SSH between controller and clients

## Quick Start

```bash
# Clone and install dependencies
git clone https://github.com/samuelsh/pyfs_stress.git
cd pyfs_stress
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Copy and edit the config template
cp server/config.json.example server/config.json
# Edit server/config.json with your credentials and workload

# Run the controller
./fileops_server.py my-file-server \
    -c client1 client2 client3 \
    -e /export \
    -m nfs3 \
    --start_vip 10.0.0.1 --end_vip 10.0.0.10
```

## Usage

```
usage: fileops_server.py [-h] [-c CLIENTS [CLIENTS ...]] [-e EXPORT]
                         [--start_vip START_VIP] [--end_vip END_VIP]
                         [--tenants]
                         [-m {nfs3,nfs4,nfs4.1,smb1,smb2,smb3}]
                         [-l {native,application,off}]
                         [--seed SEED] [--strict]
                         cluster

positional arguments:
  cluster               File server name or IP

optional arguments:
  -h, --help            show this help message and exit
  -c, --clients         Space separated list of clients
  -e, --export          NFS export name (default: /)
  --start_vip           Start VIP address range
  --end_vip             End VIP address range
  --tenants             Enable multi-tenancy
  -m, --mtype           Mount type (default: nfs3)
  -l, --locking         Locking type (default: native)
  --seed                Random seed for reproducibility
  --strict              Fail fast on first unexpected filesystem error
```

## Configuration

All infrastructure paths and ports are configurable via environment variables.
Default values match the original QA lab setup; override them for your environment.

| Variable | Default | Description |
|----------|---------|-------------|
| `CTRL_MSG_PORT` | `5557` | ZMQ controller message port |
| `CLIENT_MSG_PORT` | `5558` | ZMQ client message port |
| `PUBSUB_LOGGER_PORT` | `5559` | ZMQ PUB/SUB logger port |
| `MAX_FILES_PER_DIR` | `10000` | Max files per directory |
| `MAX_WORKERS_PER_CLIENT` | `32` | Worker processes per client |
| `DYNAMO_PATH` | `~/qa/dynamo` | Remote deployment path |
| `DYNAMO_BIN_PATH` | `~/qa/dynamo/client/dynamo_starter.py` | Remote client binary |
| `CLIENT_MOUNT_POINT` | `/mnt/test_workdir` | Client mount point |
| `REDIS_SERVER` | `10.27.50.31` | Redis host for app locking |
| `REDIS_PORT` | `6379` | Redis port |
| `SSH_PATH` | `ssh` | SSH binary path |

See `config/__init__.py` and `config/redis_config.py` for the full list.

## Running Tests

```bash
source .venv/bin/activate
pytest tests/ -v
```

Tests run automatically on push/PR via GitHub Actions (Python 3.10, 3.11, 3.12).

## System Requirements

- **Controller**: 8 GB+ RAM recommended
- **Clients**: 4 GB+ RAM each
- File server must support NFS or SMB
- At least 2 machines (1 controller + 1 client)

## License

This project is under active development. Feel free to test it, open issues, or submit pull requests.
