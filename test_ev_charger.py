"""
Quick test: verify EV charger autonomous charging, V2G dispatch,
stop_charge (load shed), and away/plugged-in behavior.
"""

import json
import time
import threading
import paho.mqtt.client as mqtt
from simulator.ev_charger import EvChargerAsset
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
              f"charge_kw: {data.get('charge_kw')} | "
              f"discharge_kw: {data.get('discharge_kw')}")
        print(f"           SoC: {data.get('state_of_charge_pct')}% | "
              f"mode: {data.get('mode')} | "
              f"plugged_in: {data.get('plugged_in')} | "
              f"dispatch_active: {data.get('dispatch_active')}")
        print(f"           dispatchable_kw: {data.get('dispatchable_kw')} | "
              f"safe_to_pause: {data.get('safe_to_pause')}")
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

    EvChargerAsset.PUBLISH_INTERVAL_SEC = 3

    # Start at 40% SoC, driver minimum 30%, plugged in now
    # We force plugged_in by setting plugged_in_hour to current texas hour
    # so we don't have to wait for the schedule window
    from datetime import datetime, timezone
    texas_hour = (datetime.now(timezone.utc).hour - 5) % 24
    
    ev = EvChargerAsset(
        asset_id="ev-01",
        charge_rate_kw=7.2,
        battery_kwh=60.0,
        initial_soc=0.4,
        driver_min_soc=0.3,
        plugged_in_hour=texas_hour,   # plugged in right now
        departure_hour=(texas_hour + 10) % 24,  # departs in 10 hours
    )

    asset_thread = threading.Thread(target=ev.run, daemon=True)
    asset_thread.start()
    time.sleep(2)

    # At 40% SoC with 10hrs until departure, should be autonomously charging
    print(">>> Should be autonomously charging...\n")
    time.sleep(4)

    # Send V2G discharge signal
    print(">>> Sending V2G discharge signal...\n")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT)
    client.publish(
        "vpp/assets/ev-01/dispatch",
        json.dumps({"command": "discharge", "target_kw": 7.2})
    )
    time.sleep(4)

    # Send stop_charge (load shed)
    print(">>> Sending stop_charge (load shed)...\n")
    client.publish(
        "vpp/assets/ev-01/dispatch",
        json.dumps({"command": "stop_charge"})
    )
    time.sleep(4)

    # Return to autonomous
    print(">>> Returning to autonomous charging...\n")
    client.publish(
        "vpp/assets/ev-01/dispatch",
        json.dumps({"command": "auto"})
    )
    time.sleep(4)

    print("Test complete.")