#!/usr/bin/env python
"""
Ingestion script for the airport operations data pipeline.

This script simulates reading flight and incident data from external sources and
lands them into a raw **Bronze** layer on Azure Data Lake Storage (or local
filesystem).  In production the sources would be Azure Event Hubs, Kafka or
ServiceNow REST APIs.  The script uses PySpark to parallelize ingestion and
write data in a scalable format (Delta or Parquet).

Usage:

```
python ingestion.py \
    --input_flights data/raw/flights.csv \
    --input_incidents data/raw/incidents.json \
    --output_path data/staging/bronze
```

"""

import argparse
import logging
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw flight and incident data.")
    parser.add_argument("--input_flights", required=True, help="Path to flights CSV file")
    parser.add_argument("--input_incidents", required=True, help="Path to incidents JSON file")
    parser.add_argument("--output_path", required=True, help="Output directory for Bronze data")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (
        SparkSession.builder
        .appName("AirportOpsIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    logging.info("Reading flights from %s", args.input_flights)
    flights_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args.input_flights)
    )
    logging.info("Reading incidents from %s", args.input_incidents)
    incidents_df = spark.read.json(args.input_incidents)

    # Write out as separate Delta tables in Bronze layer
    flights_output = f"{args.output_path}/flights"
    incidents_output = f"{args.output_path}/incidents"
    logging.info("Writing flights to %s", flights_output)
    (
        flights_df
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(flights_output)
    )
    logging.info("Writing incidents to %s", incidents_output)
    (
        incidents_df
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(incidents_output)
    )
    logging.info("Ingestion complete.\nRows: flights=%d, incidents=%d", flights_df.count(), incidents_df.count())
    spark.stop()


if __name__ == "__main__":
    main()