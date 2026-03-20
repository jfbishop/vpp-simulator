"""
Quick test: verify industrial load profile, islanding dispatch,
minimum duration enforcement, and reconnection.
"""

import json
import time
import threading
import paho.mqtt.client as mqtt
from simulator.industrial_load import IndustrialLoadAsset
from dotenv import load_dotenv
import os

load_dotenv()

BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT"))


def start_listener():
    def on_connect(client, userdata, flags, rc, properties=None):
        client.subscribe("vpp/assets/+/state")
        print("[LISTENER] Subscribed to vpp/assets/+/state\n")

    def on_message(client, userdata, msg):
        data = json.loads(msg.payload.decode())
        print(f"[LISTENER] Topic: {msg.topic}")
        print(f"           power_kw: {data.get('power_kw')} | "
              f"baseline_load_kw: {data.get('baseline_load_kw')} | "
              f"grid_connected: {data.get('grid_connected')}")
        print(f"           mode: {data.get('mode')} | "
              f"islanding_available: {data.get('islanding_available')} | "
              f"time_until_reconnect: {data.get('time_until_reconnect_sec')}s")
        print()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    client.loop_forever()


if __name__ == "__main__":
    listener_thread = threading.Thread(target=start_listener, daemon=True)
    listener_thread.start()
    time.sleep(0.5)

    IndustrialLoadAsset.PUBLISH_INTERVAL_SEC = 3

    # Short min island duration for testing (10s represents ~2hrs real time)
    industrial = IndustrialLoadAsset(
        asset_id="industrial-01",
        peak_load_kw=500.0,
        min_load_kw=150.0,
        min_island_duration_sec=10.0,
    )

    asset_thread = threading.Thread(target=industrial.run, daemon=True)
    asset_thread.start()
    time.sleep(2)

    print(">>> Running normally — watching load profile...\n")
    time.sleep(4)

    print(">>> Sending island command (load shed)...\n")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT)
    client.publish(
        "vpp/assets/industrial-01/dispatch",
        json.dumps({"command": "island"})
    )
    time.sleep(4)

    # Try to reconnect too early — should be denied
    print(">>> Attempting early reconnect (should be denied)...\n")
    client.publish(
        "vpp/assets/industrial-01/dispatch",
        json.dumps({"command": "reconnect"})
    )
    time.sleep(8)

    # Now reconnect after minimum duration
    print(">>> Reconnecting after minimum duration...\n")
    client.publish(
        "vpp/assets/industrial-01/dispatch",
        json.dumps({"command": "reconnect"})
    )
    time.sleep(4)

    print("Test complete.")