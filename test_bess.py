"""
Quick test: run a single BESS asset and watch it publish state,
then send it a dispatch signal and verify it responds.
"""

import json
import time
import threading
import paho.mqtt.client as mqtt
from simulator.bess import BessAsset
from dotenv import load_dotenv
import os

load_dotenv()

BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT"))

# --- Listener: prints state messages published to vpp/assets/+/state ---

def start_listener():
    def on_connect(client, userdata, flags, rc, properties=None):
        # Subscribe only to state topics, not dispatch topics
        # vpp/assets/+/state  — the + is a single-level wildcard
        # This means "any asset ID, but only the /state subtopic"
        # Compare to # which would match everything including /dispatch
        client.subscribe("vpp/assets/+/state")
        print("[LISTENER] Subscribed to vpp/assets/+/state\n")

    def on_message(client, userdata, msg):
        data = json.loads(msg.payload.decode())
        print(f"[LISTENER] Topic: {msg.topic}")
        print(f"           power_kw: {data.get('power_kw')} | "
              f"charge_kw: {data.get('charge_kw')} | "
              f"discharge_kw: {data.get('discharge_kw')} | "
              f"dispatchable_kw: {data.get('dispatchable_kw')}")
        print(f"           SoC: {data.get('state_of_charge_pct')}% | "
              f"mode: {data.get('mode')} | "
              f"dispatch_active: {data.get('dispatch_active')} | "
              f"energy_available: {data.get('energy_available_kwh')} kWh")
        print()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    client.loop_forever()

# --- Main ---

if __name__ == "__main__":
    # Start listener in background
    listener_thread = threading.Thread(target=start_listener, daemon=True)
    listener_thread.start()
    time.sleep(0.5)

    # Override publish interval for testing so we don't wait 15 seconds
    # between state messages. We set this on the class before instantiating
    # so all instances created in this test use the shorter interval.
    # In production run_simulation.py this line is simply absent and the
    # class default of 15 seconds is used.
    BessAsset.PUBLISH_INTERVAL_SEC = 3

    # Create a 200kW / 400kWh BESS, starting at 50% SoC
    bess = BessAsset(
        asset_id="bess-01",
        power_rating_kw=200.0,
        energy_rating_kwh=400.0,
        initial_soc=0.5
    )

    # Run asset in background thread
    asset_thread = threading.Thread(target=bess.run, daemon=True)
    asset_thread.start()
    time.sleep(2)

    # Send a discharge dispatch signal
    print(">>> Sending discharge signal...\n")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT)
    client.publish(
        "vpp/assets/bess-01/dispatch",
        json.dumps({"command": "discharge", "target_kw": 150.0})
    )
    time.sleep(4)  # wait long enough for at least one state publish after dispatch

    # Send charge signal to test bidirectional behavior
    print(">>> Sending charge signal...\n")
    client.publish(
        "vpp/assets/bess-01/dispatch",
        json.dumps({"command": "charge", "target_kw": 100.0})
    )
    time.sleep(4)

    # Send idle signal
    print(">>> Sending idle signal...\n")
    client.publish(
        "vpp/assets/bess-01/dispatch",
        json.dumps({"command": "idle"})
    )
    time.sleep(4)

    print("Test complete.")