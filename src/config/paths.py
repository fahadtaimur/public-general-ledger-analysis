"""
This module defines local and S3 paths used throughout the project.
"""

from pathlib import Path

# Local paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "gl" / "01_raw"
SILVER_DATA_DIR = DATA_DIR / "gl" / "02_silver"
GOLD_DATA_DIR = DATA_DIR / "gl" / "03_gold"

# S3 Paths
S3_BUCKET_NAME = "oklahoma-gl-test"
S3_RAW_DIR = "raw"
S3_SILVER_DIR = "silver"
S3_GOLD_DIR = "gold"
