import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fileops_server import get_args, load_config


# ---------------------------------------------------------------------------
# get_args
# ---------------------------------------------------------------------------

class TestGetArgs:
    def test_minimal_required_args(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['fileops_server.py', 'my-cluster'])
        args = get_args()
        assert args.cluster == 'my-cluster'
        assert args.clients is None
        assert args.export == '/'
        assert args.mtype == 'nfs3'
        assert args.locking == 'native'
        assert args.seed is None
        assert args.strict is False

    def test_all_args(self, monkeypatch):
        monkeypatch.setattr('sys.argv', [
            'fileops_server.py', 'cluster01',
            '-c', 'client1', 'client2',
            '-e', '/vol1',
            '--start_vip', '10.0.0.1',
            '--end_vip', '10.0.0.5',
            '--tenants',
            '-m', 'nfs4',
            '-l', 'application',
            '--seed', '42',
            '--strict',
        ])
        args = get_args()
        assert args.cluster == 'cluster01'
        assert args.clients == ['client1', 'client2']
        assert args.export == '/vol1'
        assert args.start_vip == '10.0.0.1'
        assert args.end_vip == '10.0.0.5'
        assert args.tenants is True
        assert args.mtype == 'nfs4'
        assert args.locking == 'application'
        assert args.seed == 42
        assert args.strict is True

    def test_invalid_mtype_rejected(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['fileops_server.py', 'c', '-m', 'cifs'])
        with pytest.raises(SystemExit):
            get_args()

    def test_invalid_locking_rejected(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['fileops_server.py', 'c', '-l', 'magic'])
        with pytest.raises(SystemExit):
            get_args()

    def test_missing_cluster_rejected(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['fileops_server.py'])
        with pytest.raises(SystemExit):
            get_args()

    def test_seed_is_int(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['fileops_server.py', 'c', '--seed', '999'])
        args = get_args()
        assert args.seed == 999
        assert isinstance(args.seed, int)


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_valid_json(self, tmp_path, monkeypatch):
        config_data = {
            "workload": "test_workload",
            "access": {
                "server": {"user": "root", "password": "secret"},
                "client": {"user": "admin", "password": "pw"},
            },
        }
        config_dir = tmp_path / "server"
        config_dir.mkdir()
        (config_dir / "config.json").write_text(json.dumps(config_data))
        monkeypatch.chdir(tmp_path)

        result = load_config()
        assert result["workload"] == "test_workload"
        assert result["access"]["server"]["user"] == "root"

    def test_missing_config_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError):
            load_config()

    def test_malformed_json_raises(self, tmp_path, monkeypatch):
        config_dir = tmp_path / "server"
        config_dir.mkdir()
        (config_dir / "config.json").write_text("{bad json!!!")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(json.JSONDecodeError):
            load_config()


# ---------------------------------------------------------------------------
# Seed determinism (extracted logic from main)
# ---------------------------------------------------------------------------

class TestSeedLogic:
    """The seed logic from main():
        seed = args.seed if args.seed is not None else int(time.time() * 1000) % (2**31)
    """

    def test_explicit_seed_used(self):
        import random
        seed = 42
        random.seed(seed)
        seq_a = [random.randint(0, 1000) for _ in range(10)]
        random.seed(seed)
        seq_b = [random.randint(0, 1000) for _ in range(10)]
        assert seq_a == seq_b

    def test_auto_seed_within_range(self):
        import time
        seed = int(time.time() * 1000) % (2**31)
        assert 0 <= seed < 2**31


# ---------------------------------------------------------------------------
# OperationJournal
# ---------------------------------------------------------------------------

class TestOperationJournal:
    def test_creates_file_and_records(self, tmp_path):
        from server.journal import OperationJournal
        journal = OperationJournal(output_dir=str(tmp_path))
        assert os.path.isfile(journal.path)

        journal.record(1, "mkdir", {"target": "/d1"})
        journal.record(2, "touch", {"target": "/d1/f1"})
        journal.close()

        with open(journal.path) as f:
            lines = f.readlines()
        assert len(lines) == 2
        entry = json.loads(lines[0])
        assert entry["job_id"] == 1
        assert entry["action"] == "mkdir"
        assert entry["data"]["target"] == "/d1"

    def test_journal_path_contains_timestamp(self, tmp_path):
        from server.journal import OperationJournal
        journal = OperationJournal(output_dir=str(tmp_path))
        assert "journal_" in os.path.basename(journal.path)
        assert journal.path.endswith(".jsonl")
        journal.close()

    def test_records_are_valid_jsonl(self, tmp_path):
        from server.journal import OperationJournal
        journal = OperationJournal(output_dir=str(tmp_path))
        for i in range(20):
            journal.record(i, "write", {"offset": i * 4096})
        journal.close()

        with open(journal.path) as f:
            for line in f:
                entry = json.loads(line)
                assert "ts" in entry
                assert "job_id" in entry
