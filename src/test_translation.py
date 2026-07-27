"""
Demo script: publishes a sample TRANSLATION_REQUEST message and listens
for the corresponding TRANSLATION_RESPONSE.

Usage:
    python test_publish.py

Requires the worker (translation_worker.py) to be running and consuming
from TRANSLATION_REQUEST_QUEUE, and RabbitMQ + Ollama to be up
(see docker-compose.yml).
"""

import os
import json
import uuid
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone
import rabbitmq

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TEST_PUBLISH")

SAMPLE_TEXT = "Patient reports mild discomfort during chewing on the lower right side."

def build_sample_message():
    return {
        "id": str(uuid.uuid4()),
        "type": "TRANSLATION_REQUEST",
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "originalText": SAMPLE_TEXT,
            "entity": "PATIENT",
            "entityId": "1",
            "field": "description",
        },
    }


def on_response(ch, method, properties, body):
    response = json.loads(body)
    logger.info("Received response:\n%s", json.dumps(response, indent=2, ensure_ascii=False))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.stop_consuming()


def main():
    exchange = os.getenv("TRANSLATION_EXCHANGE")
    request_key = os.getenv("TRANSLATION_REQUEST_KEY")
    response_queue = os.getenv("TRANSLATION_RESPONSE_QUEUE")

    mq = rabbitmq.RabbitMQ(exchange)

    message = build_sample_message()
    logger.info("Publishing sample request: %s", message["payload"]["originalText"])
    mq.publish(request_key, json.dumps(message, ensure_ascii=False))

    logger.info("Waiting for response on '%s'...", response_queue)
    mq.consume(queue_name=response_queue, callback=on_response)


if __name__ == "__main__":
    main()