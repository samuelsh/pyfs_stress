"""
Implements CSV Writer object to dump all imcoming file operations to CSV
"""
import queue
import csv

CSV_PATH = 'logs'
CSV_NAME = 'pyfstress_stats.csv'


class CSVWriter:
    def __init__(self, csv_writer_queue, stop_event):
        self.stop_event = stop_event
        self.csv_writer_queue = csv_writer_queue
        with open("/".join([CSV_PATH, CSV_NAME]), 'w') as f:
            writer = csv.DictWriter(f, fieldnames=['Client', 'Operation', 'Result', 'Duration', 'Timestamp'])
            writer.writeheader()

    def run(self):
        with open("/".join([CSV_PATH, CSV_NAME]), 'a') as f:
            writer = csv.writer(f)
            while not self.stop_event.is_set():
                try:
                    worker_id, message = self.csv_writer_queue.get(timeout=0.1)
                    writer.writerow([worker_id, message['action'], message['result'], message['data']['duration'],
                                    message['timestamp']])
                except queue.Empty:
                    pass
                except KeyError:
                    pass
                except KeyboardInterrupt:
                    self.stop_event.set()
