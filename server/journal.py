"""
Append-only operation journal for reproducibility.
Each dispatched job is written as a single JSON line.
"""
import json
import os
import time


class OperationJournal:
    def __init__(self, output_dir="logs"):
        os.makedirs(output_dir, exist_ok=True)
        ts = time.strftime("%Y-%m-%d_%H%M%S")
        self._path = os.path.join(output_dir, f"journal_{ts}.jsonl")
        self._fh = open(self._path, "a")

    @property
    def path(self):
        return self._path

    def record(self, job_id, action, data):
        entry = {
            "ts": time.strftime("%Y/%m/%d %H:%M:%S"),
            "job_id": job_id,
            "action": action,
            "data": data,
        }
        self._fh.write(json.dumps(entry, default=str) + "\n")
        self._fh.flush()

    def close(self):
        self._fh.close()
