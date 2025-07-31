#!/usr/bin/env python
"""
Transformation script for the airport operations data pipeline.

This script reads the Bronze layer (raw flights and incidents) and produces
two outputs:

* **Silver Layer** – cleaned and enriched data with proper schemas and derived
  columns.  It joins flights and incidents on `flight_id` to compute an
  incident count per flight.  Timestamp columns are converted to Spark
  TimestampType, delays are cast to integer and a boolean flag identifies
  on‑time flights.
* **Gold Layer** – aggregated KPIs such as average delays, incident rates and
  baggage throughput.  These metrics are written as Delta tables for BI
  consumption.

Usage:

```
python transformation.py \
    --bronze_path data/staging/bronze \
    --silver_path data/staging/silver \
    --gold_path data/gold
```
"""

import argparse
import logging
from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform Bronze data into Silver and Gold layers.")
    parser.add_argument("--bronze_path", required=True, help="Input Bronze directory containing flights and incidents")
    parser.add_argument("--silver_path", required=True, help="Output directory for Silver tables")
    parser.add_argument("--gold_path", required=True, help="Output directory for Gold tables")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (
        SparkSession.builder
        .appName("AirportOpsTransformation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Read Bronze tables
    flights_df = spark.read.format("delta").load(f"{args.bronze_path}/flights")
    incidents_df = spark.read.format("delta").load(f"{args.bronze_path}/incidents")

    # Convert string columns to proper types
    flights_clean = (
        flights_df
        .withColumn("scheduled_departure", F.to_timestamp("scheduled_departure"))
        .withColumn("actual_departure", F.to_timestamp("actual_departure"))
        .withColumn("scheduled_arrival", F.to_timestamp("scheduled_arrival"))
        .withColumn("actual_arrival", F.to_timestamp("actual_arrival"))
        .withColumn("departure_delay_mins", F.col("departure_delay_mins").cast("int"))
        .withColumn("arrival_delay_mins", F.col("arrival_delay_mins").cast("int"))
        .withColumn("baggage_count", F.col("baggage_count").cast("int"))
        .withColumn("on_time", F.when((F.col("departure_delay_mins") <= 15) & (F.col("arrival_delay_mins") <= 15), F.lit(True)).otherwise(F.lit(False)))
    )

    # Compute incident counts per flight
    incident_counts = (
        incidents_df.groupBy("flight_id").agg(F.count("incident_type").alias("incident_count"))
    )
    flights_enriched = flights_clean.join(incident_counts, on="flight_id", how="left").fillna({"incident_count": 0})

    # Write Silver table
    silver_output = f"{args.silver_path}/flights_enriched"
    logging.info("Writing Silver table to %s", silver_output)
    (
        flights_enriched
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_output)
    )

    # Create Gold aggregations
    # Average departure delay by destination
    avg_delay_by_dest = (
        flights_enriched.groupBy("dest")
        .agg(F.avg("departure_delay_mins").alias("avg_departure_delay"))
    )
    # On‑time performance by destination
    ontime_stats = (
        flights_enriched.groupBy("dest")
        .agg(F.sum(F.when(F.col("on_time"), 1).otherwise(0)).alias("on_time_flights"),
             F.count("flight_id").alias("total_flights"))
        .withColumn("on_time_rate", F.col("on_time_flights") / F.col("total_flights"))
    )
    # Incident rate by gate
    incident_rate_by_gate = (
        flights_enriched.groupBy("gate_id")
        .agg(F.sum("incident_count").alias("total_incidents"),
             F.count("flight_id").alias("flights_handled"))
        .withColumn("incident_rate", F.col("total_incidents") / F.col("flights_handled"))
    )
    # Baggage throughput per airport and date
    baggage_throughput = (
        flights_enriched.groupBy("origin", "date")
        .agg(F.sum("baggage_count").alias("total_baggage"))
    )

    # Write Gold tables
    gold_paths = {
        "avg_delay_by_dest": avg_delay_by_dest,
        "on_time_stats": ontime_stats,
        "incident_rate_by_gate": incident_rate_by_gate,
        "baggage_throughput": baggage_throughput,
    }
    for name, df in gold_paths.items():
        out_path = f"{args.gold_path}/{name}"
        logging.info("Writing %s to %s", name, out_path)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(out_path)
        )

    logging.info("Transformation complete.  Silver and Gold tables created.")
    spark.stop()


if __name__ == "__main__":
    main()