"""
src/transformation/transform_pipeline.py
Runs all transformation tasks in sequence: Tasks 3, 4, 5
"""
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.ingestion.csv_merger import main as csv_main
from src.transformation.pandas_analysis import main as pandas_main

if __name__ == "__main__":
    log.info("=== Running Full Transformation Pipeline ===")
    log.info("\nStep 1: CSV Merge & Clean (Task 3)")
    csv_main()
    log.info("\nStep 2: Pandas + NumPy Analysis (Task 5)")
    pandas_main()
    log.info("\n✅ Transformation pipeline complete!")
