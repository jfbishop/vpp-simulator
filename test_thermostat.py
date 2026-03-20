"""
Quick test: verify thermostat autonomous behavior, curtailment dispatch,
thermal lag effect on load, and setpoint restoration.
"""

import json
import time
import threading
import paho.mqtt.client as mqtt
from simulator.thermostat import ThermostatAsset
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
              f"hvac_stage: {data.get('hvac_stage')} | "
              f"mode: {data.get('mode')}")
        print(f"           setpoint: {data.get('setpoint_f')}°F | "
              f"indoor: {data.get('current_temp_f')}°F | "
              f"outdoor: {data.get('outdoor_temp_f')}°F")
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

    ThermostatAsset.PUBLISH_INTERVAL_SEC = 3

    thermostat = ThermostatAsset(
        asset_id="thermostat-01",
        normal_setpoint_f=72.0,
        hvac_capacity_kw=3.5,
    )

    asset_thread = threading.Thread(target=thermostat.run, daemon=True)
    asset_thread.start()
    time.sleep(2)

    print(">>> Running normally — watching load build up...\n")
    time.sleep(9)  # let indoor temp drift up and HVAC respond

    print(">>> Sending curtailment signal (+4°F setpoint)...\n")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT)
    client.publish(
        "vpp/assets/thermostat-01/dispatch",
        json.dumps({"command": "curtail", "setpoint_offset_f": 4.0})
    )
    time.sleep(9)  # watch load gradually drop due to thermal lag

    print(">>> Restoring normal setpoint...\n")
    client.publish(
        "vpp/assets/thermostat-01/dispatch",
        json.dumps({"command": "normal"})
    )
    time.sleep(6)

    print("Test complete.")