"""
influx_writer.py

Subscribes to all VPP asset state and grid load topics and writes
every message to InfluxDB as a time series data point.

Runs as a standalone process alongside the simulation — assets and
the coordinator have no knowledge of InfluxDB. This separation means:
    - Assets stay simple and focused on MQTT
    - Persistence can be added/removed without touching asset code
    - InfluxDB schema changes only require changes here

Topic subscriptions:
    vpp/assets/+/state  — all asset state messages
    vpp/grid/load       — baseline grid load

InfluxDB measurements:
    asset_state   — one measurement for all asset types
                    tags:  asset_id, asset_type
                    fields: all numeric fields from state message
    grid_load     — baseline load curve
                    fields: load_mw, dispatch_threshold_mw

Field handling:
    - Numeric fields are written as InfluxDB fields (queryable values)
    - String fields (mode, hvac_stage) are written as tags (indexed)
    - Boolean-like integer fields (plugged_in, dispatch_active etc.)
      are written as integer fields
    - Non-numeric, non-tag fields are skipped
"""

import json
import time
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

logger = logging.getLogger("influx-writer")

# ---------------------------------------------------------------------------
# InfluxDB configuration
# ---------------------------------------------------------------------------

INFLUX_URL    = os.getenv("INFLUXDB_URL")
INFLUX_TOKEN  = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG    = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))

# ---------------------------------------------------------------------------
# Field classification
# Fields in this set are written as InfluxDB tags (string, indexed)
# Everything else numeric goes as a field (float or int)
# Fields in SKIP_FIELDS are not written at all
# ---------------------------------------------------------------------------

# String fields that should be InfluxDB tags (indexed, filterable)
TAG_FIELDS = {
    "mode",
    "hvac_stage",
}

# Fields to skip entirely — either redundant with tags or non-numeric
# asset_id and asset_type are already tags on the point itself
# timestamp is handled explicitly from the message envelope
# departure_time is an ISO string — skip for now
SKIP_FIELDS = {
    "asset_id",
    "asset_type",
    "timestamp",
    "departure_time",
}

# Integer fields — written as int rather than float
# Keeps Grafana queries cleaner for boolean-like fields
INT_FIELDS = {
    "plugged_in",
    "dispatch_active",
    "grid_connected",
    "islanding_available",
    "safe_to_pause",
}


# ---------------------------------------------------------------------------
# Point builders
# ---------------------------------------------------------------------------

def build_asset_point(data: dict) -> Point:
    """
    Builds an InfluxDB Point from an asset state message.

    Uses the timestamp from the message envelope rather than write time
    so data is accurate even if there is write latency.

    Tags: asset_id, asset_type, plus any TAG_FIELDS present in message
    Fields: all numeric fields not in SKIP_FIELDS or TAG_FIELDS
    """
    asset_id   = data.get("asset_id", "unknown")
    asset_type = data.get("asset_type", "unknown")
    timestamp  = data.get("timestamp")

    point = (
        Point("asset_state")
        .tag("asset_id", asset_id)
        .tag("asset_type", asset_type)
    )

    # Parse timestamp from ISO string
    if timestamp:
        try:
            dt = datetime.fromisoformat(timestamp)
            point = point.time(dt, "s")
        except ValueError:
            point = point.time(datetime.now(timezone.utc), "s")
    else:
        point = point.time(datetime.now(timezone.utc), "s")

    # Write all remaining fields
    for key, value in data.items():
        if key in SKIP_FIELDS:
            continue

        if key in TAG_FIELDS:
            # String fields become additional tags
            if value is not None:
                point = point.tag(key, str(value))
            continue

        if isinstance(value, bool):
            point = point.field(key, int(value))
        elif key in INT_FIELDS:
            try:
                point = point.field(key, int(value))
            except (TypeError, ValueError):
                pass
        elif isinstance(value, (int, float)):
            try:
                point = point.field(key, float(value))
            except (TypeError, ValueError):
                pass
        # Skip strings and other non-numeric types

    return point


def build_grid_point(data: dict) -> Point:
    """
    Builds an InfluxDB Point from a grid load message.

    Separate measurement from asset_state so grid load can be
    queried independently in Grafana without filtering by asset_type.
    """
    timestamp = data.get("timestamp")

    point = Point("grid_load")

    if timestamp:
        try:
            dt = datetime.fromisoformat(timestamp)
            point = point.time(dt, "s")
        except ValueError:
            point = point.time(datetime.now(timezone.utc), "s")
    else:
        point = point.time(datetime.now(timezone.utc), "s")

    load_mw = data.get("load_mw")
    threshold_mw = data.get("dispatch_threshold_mw")

    if load_mw is not None:
        point = point.field("load_mw", float(load_mw))
    if threshold_mw is not None:
        point = point.field("dispatch_threshold_mw", float(threshold_mw))

    return point


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class InfluxWriter:
    """
    Subscribes to MQTT and writes all state messages to InfluxDB.

    Uses synchronous writes for simplicity — each message is written
    immediately on receipt. For high-frequency production use, batch
    writes would be more efficient, but for this simulation the
    message rate is low enough that synchronous is fine.
    """

    def __init__(self):
        # InfluxDB client
        self._influx = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
        )
        self._write_api = self._influx.write_api(write_options=SYNCHRONOUS)

        # MQTT client
        self._mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message

        self._write_count = 0
        self._error_count = 0

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.subscribe("vpp/assets/+/state")
            client.subscribe("vpp/grid/load")
            logger.info("Connected — subscribed to asset states and grid load")
        else:
            logger.error(f"MQTT connection failed: {rc}")

    def _on_message(self, client, userdata, msg):
        """
        Receives MQTT message, builds InfluxDB point, writes immediately.
        Logs write count periodically.
        """
        try:
            data = json.loads(msg.payload.decode())
            topic = msg.topic

            if topic == "vpp/grid/load":
                point = build_grid_point(data)
            else:
                point = build_asset_point(data)

            self._write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=point,
            )

            self._write_count += 1

            # Log every 50 writes so we know it's working without spamming
            if self._write_count % 50 == 0:
                logger.info(
                    f"Write count: {self._write_count} | "
                    f"errors: {self._error_count}"
                )

        except json.JSONDecodeError as e:
            self._error_count += 1
            logger.error(f"JSON decode error on {msg.topic}: {e}")
        except Exception as e:
            self._error_count += 1
            logger.error(f"Write error on {msg.topic}: {e}")

    def run(self):
        """Connects and runs forever, writing all messages to InfluxDB."""
        self._mqtt.connect(MQTT_BROKER, MQTT_PORT)
        logger.info(
            f"InfluxDB writer started | "
            f"broker: {MQTT_BROKER}:{MQTT_PORT} | "
            f"bucket: {INFLUX_BUCKET}"
        )

        try:
            self._mqtt.loop_forever()
        except KeyboardInterrupt:
            logger.info(
                f"Writer stopping | "
                f"total writes: {self._write_count} | "
                f"errors: {self._error_count}"
            )
        finally:
            self._write_api.close()
            self._influx.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    writer = InfluxWriter()
    writer.run()