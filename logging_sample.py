"""
Example of instrumenting your pipeline code with structured logging and
telemetry.  While production systems may integrate with Azure Monitor
or Application Insights, this script demonstrates a minimal pattern
using Python’s built‑in ``logging`` module.  You can extend it to
emit metrics and traces to Azure Monitor or other observability tools.
"""

import logging
import time

logger = logging.getLogger("airport_pipeline")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def process_batch(batch_id: int) -> None:
    """Simulate processing a batch and logging metrics."""
    start = time.time()
    logger.info("Starting processing batch %s", batch_id)
    try:
        # simulate work
        time.sleep(1.0)
        record_count = 1000  # placeholder for number of records processed
        # log a custom metric (would be sent to Azure Monitor in production)
        logger.info("batch_processed", extra={"records": record_count})
        elapsed = time.time() - start
        logger.info(
            "Completed batch %s in %.2f seconds (records=%s)", batch_id, elapsed, record_count
        )
    except Exception as exc:
        logger.error("Error processing batch %s: %s", batch_id, exc, exc_info=True)
        # In production you might raise or push this exception to a monitoring system


if __name__ == "__main__":
    for i in range(3):
        process_batch(i)