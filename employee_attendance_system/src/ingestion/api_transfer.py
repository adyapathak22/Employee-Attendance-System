"""
src/ingestion/api_transfer.py
TASK 2 — Networking Task
Simulates secure data transfer between systems using HTTP REST APIs.
Demonstrates HTTP vs FTP concepts, request/response lifecycle,
headers, authentication, and data flow analysis.
"""
import requests
import json
import time
import hashlib
import hmac
import base64
import logging
from datetime import datetime
from typing import Any
import urllib.parse

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# 1. Simulated Attendance API Client (HTTP)
# ──────────────────────────────────────────────────────────────────
class AttendanceAPIClient:
    """
    Simulates an HTTP API client for attendance data transfer.
    In production this would connect to an actual server endpoint.
    """
    BASE_URL = "https://jsonplaceholder.typicode.com"  # public mock API

    def __init__(self, api_key: str = "demo_key_12345"):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-API-Key": api_key,
            "User-Agent": "AttendanceSystem/1.0",
        })
        self.request_log = []

    def _sign_request(self, payload: str) -> str:
        """HMAC-SHA256 signing — demonstrates secure data transfer."""
        secret = self.api_key.encode()
        sig = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
        return sig

    def get_employee_records(self, employee_id: int = 1) -> dict:
        """GET request — fetch employee data (simulated via public API)."""
        url = f"{self.BASE_URL}/users/{employee_id}"
        log.info(f"GET {url}")
        start = time.time()
        resp = self.session.get(url, timeout=10)
        elapsed = round((time.time() - start) * 1000, 2)

        self._log_request("GET", url, resp.status_code, elapsed)
        resp.raise_for_status()
        log.info(f"  ↳ Status: {resp.status_code} | Latency: {elapsed}ms | Size: {len(resp.content)} bytes")
        return resp.json()

    def post_attendance_record(self, record: dict) -> dict:
        """POST request — push attendance record to remote server."""
        url = f"{self.BASE_URL}/posts"
        payload = json.dumps(record)
        signature = self._sign_request(payload)
        self.session.headers["X-Signature"] = signature

        log.info(f"POST {url} | Payload size: {len(payload)} bytes")
        start = time.time()
        resp = self.session.post(url, data=payload, timeout=10)
        elapsed = round((time.time() - start) * 1000, 2)

        self._log_request("POST", url, resp.status_code, elapsed)
        resp.raise_for_status()
        log.info(f"  ↳ Status: {resp.status_code} | Latency: {elapsed}ms")
        return resp.json()

    def _log_request(self, method, url, status, latency_ms):
        self.request_log.append({
            "timestamp": datetime.now().isoformat(),
            "method": method,
            "url": url,
            "status_code": status,
            "latency_ms": latency_ms,
        })

    def get_network_report(self) -> dict:
        if not self.request_log:
            return {}
        latencies = [r["latency_ms"] for r in self.request_log]
        return {
            "total_requests": len(self.request_log),
            "successful": sum(1 for r in self.request_log if 200 <= r["status_code"] < 300),
            "avg_latency_ms": round(sum(latencies) / len(latencies), 2),
            "max_latency_ms": max(latencies),
            "min_latency_ms": min(latencies),
        }


# ──────────────────────────────────────────────────────────────────
# 2. Protocol Comparison: HTTP vs FTP
# ──────────────────────────────────────────────────────────────────
PROTOCOL_COMPARISON = {
    "HTTP/HTTPS": {
        "Port": "80 / 443",
        "Use Case": "REST APIs, web data transfer",
        "Security": "TLS/SSL encryption (HTTPS)",
        "Stateless": True,
        "Authentication": "API Keys, OAuth2, JWT",
        "Best For": "Real-time attendance API pushes",
    },
    "FTP/SFTP": {
        "Port": "21 / 22",
        "Use Case": "Bulk file transfer (CSVs, exports)",
        "Security": "Plain (FTP) / SSH encrypted (SFTP)",
        "Stateless": False,
        "Authentication": "Username/Password, SSH Keys",
        "Best For": "Nightly bulk attendance dumps",
    },
    "gRPC": {
        "Port": "443 (HTTP/2)",
        "Use Case": "High-performance microservice calls",
        "Security": "TLS",
        "Stateless": True,
        "Authentication": "mTLS, tokens",
        "Best For": "High-frequency check-in streaming",
    },
}


# ──────────────────────────────────────────────────────────────────
# 3. Simulated Packet Capture / Data Flow Analysis
# ──────────────────────────────────────────────────────────────────
def simulate_packet_capture(num_packets: int = 10):
    """Simulate packet-level logging (what Wireshark would capture)."""
    log.info("\n── Simulated Packet Capture ──────────────────────────────")
    packets = []
    for i in range(num_packets):
        p = {
            "packet_no": i + 1,
            "src_ip": f"10.0.0.{i+1}",
            "dst_ip": "192.168.1.100",
            "protocol": "TCP/HTTPS",
            "src_port": 49152 + i,
            "dst_port": 443,
            "seq_no": 1000 + i * 100,
            "ack_no": 2000 + i * 100,
            "flags": "PSH+ACK",
            "payload_bytes": 512 + i * 10,
            "timestamp": datetime.now().isoformat(),
        }
        packets.append(p)
        log.info(
            f"  PKT {p['packet_no']:03d} | {p['src_ip']}:{p['src_port']} → "
            f"{p['dst_ip']}:{p['dst_port']} | {p['flags']} | {p['payload_bytes']}B"
        )
    return packets


def explain_data_flow():
    """Explain how attendance data flows securely through the system."""
    flow = """
    ╔══════════════════════════════════════════════════════════════╗
    ║         Secure Attendance Data Flow                         ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  1. Employee scans badge / checks in via mobile app         ║
    ║     └─ Device sends HTTPS POST to API Gateway (TLS 1.3)     ║
    ║                                                              ║
    ║  2. API Gateway validates JWT token                          ║
    ║     └─ Checks signature, expiry, scopes                      ║
    ║                                                              ║
    ║  3. Payload is HMAC-signed for integrity                     ║
    ║     └─ Server verifies no tampering occurred                 ║
    ║                                                              ║
    ║  4. Record written to transactional DB (OLTP)                ║
    ║     └─ Async event pushed to Kafka topic                     ║
    ║                                                              ║
    ║  5. Kafka consumer writes to Data Lake / Warehouse           ║
    ║     └─ Batch jobs aggregate for dashboards                   ║
    ║                                                              ║
    ║  Security Layers: TLS + JWT + HMAC + IP whitelisting        ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(flow)


def main():
    log.info("=== Task 2: Networking & Data Transfer ===\n")

    # HTTP API Demo
    client = AttendanceAPIClient()

    try:
        log.info("── Fetching employee records via HTTP GET ──")
        employee = client.get_employee_records(1)
        log.info(f"  Employee fetched: {employee.get('name')} ({employee.get('email')})")

        log.info("\n── Posting attendance record via HTTP POST ──")
        record = {
            "employee_id": "EMP0042",
            "date": "2025-01-15",
            "status": "Present",
            "check_in": "09:02:00",
            "check_out": "18:15:00",
            "hours_worked": 9.22,
        }
        response = client.post_attendance_record(record)
        log.info(f"  Record posted, server ID: {response.get('id')}")
    except requests.RequestException as e:
        log.warning(f"  API call failed (network unavailable in sandbox): {e}")

    # Protocol comparison
    log.info("\n── Protocol Comparison ──")
    for proto, details in PROTOCOL_COMPARISON.items():
        log.info(f"  {proto}: Port={details['Port']} | Auth={details['Authentication']}")

    # Packet simulation
    simulate_packet_capture(5)

    # Data flow explanation
    explain_data_flow()

    # Network report
    report = client.get_network_report()
    if report:
        log.info(f"\n── Network Report: {report}")

    log.info("\n✅ Task 2 Complete!")


if __name__ == "__main__":
    main()
