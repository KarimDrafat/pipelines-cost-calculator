"""
SQLite database layer.

Schema
------
services         — service name, category, human-readable metadata
pricing_params   — cost model parameters (base cost, scaling exponents)
calculations     — saved pipeline configurations for history / comparison
"""

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "telemetry_costs.db"

# ---------------------------------------------------------------------------
# Seed data  (name, category, description, notes, base_cost, fixed_fraction,
#             device_exp, window_exp)
# Base costs are at 10 M devices / 5-min ingestion window.
# ---------------------------------------------------------------------------
_SERVICES: list[tuple] = [
    # ── Ingestion ───────────────────────────────────────────────────────────
    (
        "Lambda Primary Consumer",
        "ingestion",
        "AWS Lambda as primary message consumer",
        "Batched invocations (batch≈200). Compute $0.0000166667/GB-s + $0.20/1M requests. "
        "Adjusted to real Lambda compute rates.",
        40_000, 0.02, 1.00, 1.00,
    ),
    (
        "MSK Kafka",
        "ingestion",
        "Amazon Managed Streaming for Apache Kafka",
        "2× kafka.m5.large brokers (fixed ~$4.5 K) + variable storage & throughput. "
        "Corrected broker count; was previously overestimated.",
        11_400, 0.40, 0.85, 0.55,
    ),
    (
        "IoT Core + KDS + Lambda",
        "ingestion",
        "AWS IoT Core + Kinesis Data Streams + Lambda",
        "IoT Core charges $0.08/1M msgs AND $0.08/1M connection-minutes — both scale with "
        "devices × frequency. Most expensive at high device counts.",
        82_500, 0.005, 1.00, 1.00,
    ),
    (
        "KDS On-Demand + Lambda",
        "ingestion",
        "Kinesis Data Streams On-Demand + Lambda Consumer",
        "KDS On-Demand $0.08/1M PUT records + Lambda compute. "
        "Corrected with actual KDS write pricing.",
        37_200, 0.02, 1.00, 0.90,
    ),
    (
        "KDS + Firehose + KDA Flink",
        "ingestion",
        "KDS On-Demand + Kinesis Firehose + Kinesis Data Analytics (Flink)",
        "Best value for stateful stream processing. Fixed KDA KPU cost (~28 %). "
        "Firehose format-conversion charge included.",
        13_900, 0.28, 0.95, 0.72,
    ),
    # ── Processing ──────────────────────────────────────────────────────────
    (
        "Delta Live Tables",
        "processing",
        "Databricks Delta Live Tables",
        "DLT Jobs compute DBUs (~$0.20/DBU). Incremental processing with automatic "
        "lineage & quality checks. Good for ML feature pipelines.",
        6_800, 0.12, 0.82, 0.38,
    ),
    (
        "Databricks Workflows",
        "processing",
        "Databricks Workflows (Jobs)",
        "Orchestrated Databricks jobs at a lower DBU rate than DLT. "
        "Best for scheduled batch aggregations.",
        4_500, 0.08, 0.78, 0.32,
    ),
    (
        "Structured Streaming",
        "processing",
        "Apache Spark Structured Streaming on Databricks",
        "Continuous micro-batch streaming. Cluster stays alive 24/7; "
        "cost scales with throughput and cluster size.",
        5_400, 0.18, 0.88, 0.48,
    ),
    # ── Storage ─────────────────────────────────────────────────────────────
    (
        "S3 Data Lake",
        "storage",
        "Amazon S3 with Parquet format and Hive partitioning",
        "Parquet + partitioning cuts storage to ~$0.023/GB and reduces Athena scan "
        "by ~95 % vs raw JSON. Total stored data scales with devices, not frequency.",
        1_700, 0.05, 1.00, 0.00,
    ),
    (
        "DynamoDB",
        "storage",
        "Amazon DynamoDB On-Demand — hot / live device state",
        "Stores latest telemetry reading + threshold state per device. "
        "On-Demand WCU cost scales with update frequency. Use alongside S3, not instead.",
        3_800, 0.00, 1.00, 0.85,
    ),
    # ── Analytics ───────────────────────────────────────────────────────────
    (
        "Databricks SQL Warehouse",
        "analytics",
        "Databricks Serverless SQL Warehouse",
        "Serverless, auto-idles between queries (~$0.70/DBU serverless). "
        "Cost driven by data scanned, not ingestion frequency.",
        1_700, 0.08, 0.68, 0.08,
    ),
    (
        "Amazon Athena",
        "analytics",
        "Amazon Athena — serverless interactive SQL",
        "$5/TB scanned. Parquet + partitioning cuts scan volume by ~95 %. "
        "Cheapest option for infrequent or ad-hoc queries.",
        320, 0.00, 0.88, 0.00,
    ),
]

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db() -> None:
    os.makedirs(DB_PATH.parent, exist_ok=True)
    conn = _connect()
    _create_schema(conn)
    _seed_if_empty(conn)
    conn.close()


def get_services_by_category(category: str) -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        """
        SELECT s.name, s.category, s.description, s.notes,
               p.base_cost, p.fixed_fraction, p.device_exp, p.window_exp
        FROM   services s
        JOIN   pricing_params p ON p.service_id = s.id
        WHERE  s.category = ?
        ORDER  BY p.base_cost ASC
        """,
        (category,),
    ).fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def get_all_services() -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        """
        SELECT s.name, s.category, s.description, s.notes,
               p.base_cost, p.fixed_fraction, p.device_exp, p.window_exp
        FROM   services s
        JOIN   pricing_params p ON p.service_id = s.id
        ORDER  BY s.category, p.base_cost ASC
        """
    ).fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def save_calculation(
    num_devices: int,
    window_minutes: int,
    ingestion: str,
    processing: str,
    storage_list: list[str],
    analytics: str,
    total_cost: float,
) -> None:
    conn = _connect()
    conn.execute(
        """
        INSERT INTO calculations
            (num_devices, window_minutes, ingestion_service, processing_service,
             storage_services, analytics_service, total_cost)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            num_devices,
            window_minutes,
            ingestion,
            processing,
            ", ".join(storage_list),
            analytics,
            round(total_cost, 2),
        ),
    )
    conn.commit()
    conn.close()


def get_calculation_history(limit: int = 30) -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        """
        SELECT id, timestamp, num_devices, window_minutes,
               ingestion_service, processing_service, storage_services,
               analytics_service, total_cost
        FROM   calculations
        ORDER  BY timestamp DESC
        LIMIT  ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    cols = [
        "id", "timestamp", "num_devices", "window_minutes",
        "ingestion_service", "processing_service", "storage_services",
        "analytics_service", "total_cost",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS services (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL UNIQUE,
            category    TEXT    NOT NULL,
            description TEXT,
            notes       TEXT
        );

        CREATE TABLE IF NOT EXISTS pricing_params (
            service_id      INTEGER PRIMARY KEY,
            base_cost       REAL    NOT NULL,
            fixed_fraction  REAL    NOT NULL,
            device_exp      REAL    NOT NULL,
            window_exp      REAL    NOT NULL,
            FOREIGN KEY (service_id) REFERENCES services(id)
        );

        CREATE TABLE IF NOT EXISTS calculations (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           TEXT    DEFAULT (datetime('now')),
            num_devices         INTEGER NOT NULL,
            window_minutes      INTEGER NOT NULL,
            ingestion_service   TEXT,
            processing_service  TEXT,
            storage_services    TEXT,
            analytics_service   TEXT,
            total_cost          REAL
        );
        """
    )
    conn.commit()


def _seed_if_empty(conn: sqlite3.Connection) -> None:
    if conn.execute("SELECT COUNT(*) FROM services").fetchone()[0] > 0:
        return
    for row in _SERVICES:
        name, cat, desc, notes, base, ff, d_exp, w_exp = row
        cur = conn.execute(
            "INSERT INTO services (name, category, description, notes) VALUES (?, ?, ?, ?)",
            (name, cat, desc, notes),
        )
        conn.execute(
            "INSERT INTO pricing_params (service_id, base_cost, fixed_fraction, device_exp, window_exp) "
            "VALUES (?, ?, ?, ?, ?)",
            (cur.lastrowid, base, ff, d_exp, w_exp),
        )
    conn.commit()


def _row_to_dict(row: tuple) -> dict:
    return {
        "name":           row[0],
        "category":       row[1],
        "description":    row[2],
        "notes":          row[3],
        "base_cost":      row[4],
        "fixed_fraction": row[5],
        "device_exp":     row[6],
        "window_exp":     row[7],
    }
