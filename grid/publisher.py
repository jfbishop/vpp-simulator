"""
grid/publisher.py

Publishes the synthetic baseline grid load to MQTT every N seconds.
The coordinator subscribes to this topic to include baseline load
in its net load calculation.

Topic: vpp/grid/load
"""

import json
import time
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os

from grid.baseline import get_baseline_load, get_dispatch_threshold

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

logger = logging.getLogger("grid-publisher")
PUBLISH_INTERVAL_SEC = 10


def run():
    broker = os.getenv("MQTT_BROKER", "localhost")
    port = int(os.getenv("MQTT_PORT", 1883))

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(broker, port)
    client.loop_start()

    logger.info(f"Grid publisher started | broker: {broker}:{port}")

    while True:
        load_mw = get_baseline_load()
        threshold_mw = get_dispatch_threshold()

        payload = json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "load_mw": load_mw,
            "dispatch_threshold_mw": threshold_mw,
        })

        client.publish("vpp/grid/load", payload)
        logger.info(f"Baseline load: {load_mw:.2f} MW")

        time.sleep(PUBLISH_INTERVAL_SEC)


if __name__ == "__main__":
    run()