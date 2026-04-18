"""
Cost calculation engine for telemetry pipeline services.

All base costs are calibrated to 10M devices + 5-minute ingestion window.
Formula: cost = fixed_cost + variable_cost * (devices/10M)^device_exp * (5/window)^window_exp
"""

BASE_DEVICES = 10_000_000
BASE_WINDOW = 5


def calculate_cost(service: dict, num_devices: int, window_minutes: int) -> float:
    base = service["base_cost"]
    ff = service["fixed_fraction"]
    d_exp = service["device_exp"]
    w_exp = service["window_exp"]

    fixed = base * ff
    variable = base * (1 - ff)

    device_factor = (num_devices / BASE_DEVICES) ** d_exp
    window_factor = (BASE_WINDOW / window_minutes) ** w_exp

    return fixed + variable * device_factor * window_factor


def msgs_per_month(num_devices: int, window_minutes: int) -> int:
    """Messages ingested per month: each device sends one msg per window."""
    return int(num_devices * (30 * 24 * 60 / window_minutes))


def msgs_per_sec(num_devices: int, window_minutes: int) -> float:
    return num_devices / (window_minutes * 60)


def format_cost(amount: float) -> str:
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.0f}"


PIPELINE_PRESETS = {
    "Real-Time Tracking + Threshold Detection": {
        "ingestion": "KDS + Firehose + KDA Flink",
        "processing": "Structured Streaming",
        "storage": ["S3 Data Lake", "DynamoDB"],
        "analytics": "Databricks SQL Warehouse",
        "description": (
            "Low-latency pipeline optimised for sub-second threshold alerts. "
            "KDA Flink handles CEP rules; DynamoDB holds live device state."
        ),
    },
    "Statistical Metrics + ML": {
        "ingestion": "KDS On-Demand + Lambda",
        "processing": "Delta Live Tables",
        "storage": ["S3 Data Lake"],
        "analytics": "Databricks SQL Warehouse",
        "description": (
            "Micro-batch pipeline for feature engineering and ML training. "
            "DLT manages incremental updates; SQL Warehouse serves BI dashboards."
        ),
    },
    "Cost-Optimised": {
        "ingestion": "KDS + Firehose + KDA Flink",
        "processing": "Databricks Workflows",
        "storage": ["S3 Data Lake"],
        "analytics": "Amazon Athena",
        "description": (
            "Lowest-cost combination for moderate workloads. "
            "Athena on Parquet cuts query cost by ~95% vs raw JSON."
        ),
    },
    "Custom": {
        "ingestion": "KDS + Firehose + KDA Flink",
        "processing": "Structured Streaming",
        "storage": ["S3 Data Lake"],
        "analytics": "Amazon Athena",
        "description": "Pick any combination below.",
    },
}
