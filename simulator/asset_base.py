"""
simulator/asset_base.py

Abstract base class for all VPP asset simulators. Handles MQTT connection,
state publishing, and dispatch signal subscription. Individual asset types
inherit from this class and implement get_state() and on_dispatch().

Each asset:
  - Publishes its state to:  vpp/assets/{asset_id}/state
  - Listens for signals on:  vpp/assets/{asset_id}/dispatch
  - State is published as JSON every PUBLISH_INTERVAL_SEC seconds
"""

import json
import time
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from grid.sim_clock import SimClock, TIME_SCALE


import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)


class AssetBase(ABC):
    """
    Abstract base class for all VPP asset simulators.

    Subclasses must implement:
        get_state()    -> dict   — current asset state (power draw, SoC, etc.)
        on_dispatch()  -> None   — respond to a dispatch signal from coordinator

    Args:
        asset_id:    Unique identifier, e.g. "bess-01" or "ev-charger-03"
        asset_type:  Human readable type label, e.g. "bess", "ev_charger"
        publish_interval_sec: How often to publish state (default 15s, 
                              represents 15-min intervals in compressed time)
    """

    # 15 sim minutes = 15 * 60 / TIME_SCALE real seconds = 2.5 real seconds
    PUBLISH_INTERVAL_SEC = (15 * 60) / TIME_SCALE  # ~2.5 seconds at 360x

    # Simulated seconds per publish interval — use this for physics calculations
    PUBLISH_INTERVAL_SIM_SEC = 15 * 60  # always 15 sim-minutes regardless of scale


    def __init__(self, asset_id: str, asset_type: str):
        self.asset_id = asset_id
        self.asset_type = asset_type
        self.logger = logging.getLogger(asset_id)

        # MQTT topic structure
        # Mirrors industrial SCADA conventions where topics encode
        # hierarchy: system/subsystem/device/datatype
        self.state_topic = f"vpp/assets/{asset_id}/state"
        self.dispatch_topic = f"vpp/assets/{asset_id}/dispatch"

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._running = False

    # ------------------------------------------------------------------
    # Abstract methods — every subclass must implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def get_state(self) -> dict:
        """
        Returns the current state of the asset as a dictionary.
        This is what gets published to MQTT and written to InfluxDB.

        Must include at minimum:
            power_kw (float): positive = consuming, negative = exporting

        Example for a BESS:
            {
                "power_kw": -50.0,      # discharging (negative = export)
                "state_of_charge": 0.72  # 72% charged
            }
        """
        pass

    @abstractmethod
    def on_dispatch(self, signal: dict) -> None:
        """
        Called when the coordinator sends a dispatch signal to this asset.
        Subclass should update internal state to reflect the commanded action.

        Args:
            signal: dict parsed from the dispatch message JSON, e.g.:
                    {"command": "curtail", "target_kw": 20.0}
                    {"command": "discharge", "target_kw": 50.0}
                    {"command": "normal"}
        """
        pass

    # ------------------------------------------------------------------
    # MQTT connection handlers
    # ------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.logger.info(f"Connected to broker — subscribing to {self.dispatch_topic}")
            client.subscribe(self.dispatch_topic)
        else:
            self.logger.error(f"Connection failed with code {rc}")

    def _on_message(self, client, userdata, msg):
        """
        Receives dispatch signals from the coordinator.
        Parses the JSON payload and calls on_dispatch().
        """
        try:
            signal = json.loads(msg.payload.decode())
            self.logger.info(f"Dispatch received: {signal}")
            self.on_dispatch(signal)
        except json.JSONDecodeError as e:
            self.logger.error(f"Could not parse dispatch signal: {e}")

    # ------------------------------------------------------------------
    # State publishing
    # ------------------------------------------------------------------

    def _build_message(self) -> str:
        """
        Wraps get_state() output with standard metadata fields
        and serializes to JSON for MQTT publish.
        """
        state = self.get_state()

        # Add standard envelope fields to every message
        # This mirrors real telemetry conventions where every message
        # carries its own identity and timestamp for traceability
        message = {
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "timestamp": SimClock.now().isoformat(),
            **state   # merge in the asset-specific fields
        }

        return json.dumps(message)

    def publish_state(self) -> None:
        """Publishes current state to the asset's MQTT state topic."""
        payload = self._build_message()
        self._client.publish(self.state_topic, payload)
        self.logger.debug(f"Published: {payload}")

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Connects to the MQTT broker and starts the publish loop.
        Publishes state every PUBLISH_INTERVAL_SEC seconds.
        Runs until stopped.

        This is a blocking call — run each asset in its own thread
        or process. See run_simulation.py for how this is orchestrated.
        """
        broker = os.getenv("MQTT_BROKER", "localhost")
        port = int(os.getenv("MQTT_PORT", 1883))

        self._client.connect(broker, port)
        self._client.loop_start()  # handles network traffic in background thread
        self._running = True

        self.logger.info(f"Asset {self.asset_id} ({self.asset_type}) started")

        try:
            while self._running:
                self.publish_state()
                time.sleep(self.PUBLISH_INTERVAL_SEC)
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Gracefully stops the publish loop and disconnects from broker."""
        self._running = False
        self._client.loop_stop()
        self._client.disconnect()
        self.logger.info(f"Asset {self.asset_id} stopped")