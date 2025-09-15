"""
json_consumer_schroder.py

Consume JSON messages from a Kafka topic and visualize the live
distribution by a categorical field (default: 'category') as a pie chart.

Run this from Windows PowerShell (so the chart window opens).
Keep your Kafka broker running in WSL.

Environment:
- PROJECT_TOPIC (preferred) or BUZZ_TOPIC — Kafka topic to consume
- BUZZ_CONSUMER_GROUP_ID — Kafka consumer group id
Optional:
- PROJECT_CATEGORY_FIELD — e.g., 'category' (default) or 'keyword_mentioned'
"""

#####################################
# Import Modules
#####################################

# Standard Library
import os
import json
from collections import defaultdict
from typing import Any

# External packages
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# Local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment
#####################################
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """
    Prefer the producer's env var name so producer/consumer match by default.
    Falls back to BUZZ_TOPIC and finally a sane default.
    """
    topic = os.getenv("PROJECT_TOPIC") or os.getenv("BUZZ_TOPIC") or "buzzline-topic"
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "json_consumer_schroder")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_category_field() -> str:
    # choose which categorical field we aggregate by
    field = os.getenv("PROJECT_CATEGORY_FIELD", "category")
    logger.info(f"Grouping by field: {field}")
    return field

#####################################
# Data structures
#####################################
category_counts = defaultdict(int)
CATEGORY_FIELD = get_category_field()

#####################################
# Live visuals
#####################################
fig, ax = plt.subplots(figsize=(6, 6))
plt.ion()  # interactive mode on so we can update the chart

def _collapse_small(labels, sizes, min_share=0.05):
    """Group small slices into 'other' for readability."""
    total = sum(sizes) or 1
    keep_labs, keep_sizes = [], []
    other = 0
    # Sort largest → smallest so we only collapse tail slices
    for lab, sz in sorted(zip(labels, sizes), key=lambda p: p[1], reverse=True):
        if (sz / total) < min_share and len(keep_labs) >= 2:
            other += sz
        else:
            keep_labs.append(lab)
            keep_sizes.append(sz)
    if other > 0:
        keep_labs.append("other")
        keep_sizes.append(other)
    return keep_labs, keep_sizes

def update_chart():
    """Update the live pie chart with latest category counts."""
    ax.clear()

    if category_counts:
        labels = list(category_counts.keys())
        sizes  = list(category_counts.values())
        labels, sizes = _collapse_small(labels, sizes, min_share=0.05)
    else:
        labels = ["waiting"]
        sizes  = [1]

    ax.set_title(f"Live Share by '{CATEGORY_FIELD}'")
    ax.pie(sizes, labels=labels, autopct="%1.0f%%", startangle=90)
    ax.axis("equal")  # keep it circular

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Message processing
#####################################
def _coerce_to_dict(payload: Any) -> dict | None:
    """
    Accept bytes, str, or dict and return a dict (or None on failure).
    - bytes -> UTF-8 decode -> JSON parse
    - str   -> JSON parse
    - dict  -> pass through
    """
    try:
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")
        if isinstance(payload, str):
            return json.loads(payload)
        logger.error(f"Unsupported payload type: {type(payload)}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {payload!r}")
        return None
    except Exception as e:
        logger.error(f"Error coercing payload to dict: {e}")
        return None

def process_message(payload: Any) -> None:
    """
    Parse one Kafka record value (bytes/str/dict), increment the chosen category,
    and refresh the chart.
    """
    msg = _coerce_to_dict(payload)
    if not msg:
        return

    try:
        # Normalize category (or keyword) so cases/spaces don’t split slices
        raw = msg.get(CATEGORY_FIELD, "unknown")
        if isinstance(raw, str):
            cat = raw.strip().lower() or "unknown"
        else:
            cat = str(raw) if raw is not None else "unknown"

        category_counts[cat] += 1
        logger.info(f"Updated {CATEGORY_FIELD} counts: {dict(category_counts)}")

        update_chart()
        logger.debug("Chart updated successfully.")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main
#####################################
def main() -> None:
    logger.info("START consumer (pie by category).")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer connecting to topic '{topic}' in group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    # Draw an initial chart so a window appears immediately
    update_chart()

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for record in consumer:
            # record.value may be bytes/str/dict depending on consumer config
            logger.debug(f"Received at offset {getattr(record, 'offset', '?')}: {type(record.value)}")
            process_message(record.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################
if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
