"""
json_consumer_schroder.py

Consume JSON messages from a Kafka topic and visualize the live
distribution by a categorical field (default: 'category') as a pie chart.

Run this from Windows PowerShell (so the chart window opens).
Keep your Kafka broker running in WSL.

Environment:
- BUZZ_TOPIC (Kafka topic) — used by get_kafka_topic()
- BUZZ_CONSUMER_GROUP_ID — used by get_kafka_consumer_group_id()
Optional:
- PROJECT_CATEGORY_FIELD — set to 'keyword_mentioned' to switch grouping
"""

#####################################
# Import Modules
#####################################

# Standard Library
import os
import json
from collections import defaultdict

# External packages
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# Local modules (same utilities as the example)
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
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
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
plt.ion()  # interactive mode on so we can update

def update_chart():
    """Update the live pie chart with latest category counts."""
    ax.clear()

    if category_counts:
        labels = list(category_counts.keys())
        sizes  = list(category_counts.values())
    else:
        labels = ["waiting"]
        sizes  = [1]

    ax.set_title(f"Live Share by '{CATEGORY_FIELD}'")
    # startangle=90 for a cleaner look; autopct for % labels
    ax.pie(sizes, labels=labels, autopct="%1.0f%%", startangle=90)
    ax.axis("equal")  # keep it circular

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Message processing
#####################################
def process_message(message: str) -> None:
    """
    Parse one JSON string, increment the chosen category,
    and refresh the chart.
    """
    try:
        logger.debug(f"Raw message: {message}")
        msg: dict = json.loads(message)
        logger.info(f"Processed JSON message: {msg}")

        if not isinstance(msg, dict):
            logger.error(f"Expected a dict but got: {type(msg)}")
            return

        # Pick the category field (e.g., 'category' or 'keyword_mentioned')
        cat = msg.get(CATEGORY_FIELD, "unknown")
        category_counts[cat] += 1
        logger.info(f"Updated {CATEGORY_FIELD} counts: {dict(category_counts)}")

        update_chart()
        logger.info("Chart updated successfully.")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
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

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for record in consumer:
            message_str = record.value  # example utils deliver str
            logger.debug(f"Received at offset {record.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################
if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
