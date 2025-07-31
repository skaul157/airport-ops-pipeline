#!/usr/bin/env python
"""
Data quality script using Great Expectations.

This module validates the Silver layer to ensure that the data conforms to
expected ranges and formats.  It uses Great Expectations to programmatically
define an expectation suite and run it against a sample of the data.  Results
are printed to stdout and can be exported to JSON for further reporting.

Usage:

```
python data_quality.py --input_path data/staging/silver/flights_enriched
```
"""

import argparse
import logging
from pyspark.sql import SparkSession
import great_expectations as ge


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run data quality checks with Great Expectations.")
    parser.add_argument("--input_path", required=True, help="Path to Silver table (Delta)")
    parser.add_argument("--sample_rows", type=int, default=10000, help="Number of rows to sample for validation")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (
        SparkSession.builder
        .appName("AirportOpsDataQuality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    logging.info("Loading Silver table from %s", args.input_path)
    df = spark.read.format("delta").load(args.input_path)
    # sample to Pandas for GE; for large datasets use PandasOnSpark or GE's Spark integration
    sample_pdf = df.limit(args.sample_rows).toPandas()
    ge_df = ge.from_pandas(sample_pdf)

    # Define expectations
    expectation_suite = ge_df.get_expectation_suite(discard_failed_expectations=False)
    # Flight ID should be unique
    ge_df.expect_column_values_to_be_unique("flight_id")
    # Baggage count >= 0
    ge_df.expect_column_values_to_be_between("baggage_count", min_value=0)
    # Departure delays within reasonable range
    ge_df.expect_column_values_to_be_between("departure_delay_mins", min_value=-15, max_value=180)
    # Arrival delays within reasonable range
    ge_df.expect_column_values_to_be_between("arrival_delay_mins", min_value=-30, max_value=240)
    # On‑time flag is boolean
    ge_df.expect_column_values_to_be_in_set("on_time", [True, False])
    # Incident count >= 0
    ge_df.expect_column_values_to_be_between("incident_count", min_value=0)

    # Run validation
    results = ge_df.validate()
    logging.info("Data quality results: %s", results)
    spark.stop()


if __name__ == "__main__":
    main()