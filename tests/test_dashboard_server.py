import csv
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen

from dashboard.server import DashboardServer
from pipeline.load_revenues import load


class DashboardServerTests(unittest.TestCase):
    def test_http_dashboard_reads_warehouse(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            csv_path = Path(root) / "revenues.csv"
            db_path = Path(root) / "warehouse.duckdb"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
                writer.writerow(["1", "2022-01-01", "Film A", "100", "10", "Studio X"])
            load(csv_path, db_path)

            server = DashboardServer(("127.0.0.1", 0), db_path)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_port}"
                with urlopen(base + "/") as response:
                    page = response.read()
                    self.assertIn(b"Box office ranking", page)
                    self.assertIn(b"Movie match review", page)
                    self.assertIn(b"Confirmed film ranking", page)
                with urlopen(base + "/api/meta") as response:
                    meta = json.load(response)
                self.assertEqual(meta["first_date"], "2022-01-01")
                with urlopen(base + "/api/dashboard?limit=10") as response:
                    data = json.load(response)
                self.assertEqual(data["selected"]["revenue"], 100)
                self.assertEqual(data["movies"][0]["movie"], "Film A")
                self.assertEqual(data["coverage"], 0)
                self.assertEqual(data["films"], [])
                with urlopen(base + "/api/review") as response:
                    review = json.load(response)
                self.assertEqual(review, {"total": 0, "cases": []})
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
