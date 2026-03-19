import time
import threading
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os

load_dotenv()

BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT"))
TOPIC = "vpp/test"

# --- Subscriber ---

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[SUB] Connected to broker")
        client.subscribe(TOPIC)
        print(f"[SUB] Subscribed to {TOPIC}")
    else:
        print(f"[SUB] Connection failed with code {rc}")

def on_message(client, userdata, msg):
    print(f"[SUB] Received message on {msg.topic}: {msg.payload.decode()}")

def run_subscriber():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    client.loop_forever()

# --- Publisher ---

def run_publisher():
    time.sleep(1)  # give subscriber a moment to connect first
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT)
    print("[PUB] Connected to broker")

    for i in range(5):
        message = f"test message {i+1}"
        client.publish(TOPIC, message)
        print(f"[PUB] Sent: {message}")
        time.sleep(1)

# --- Main ---

if __name__ == "__main__":
    # Run subscriber in background thread, publisher in main thread
    sub_thread = threading.Thread(target=run_subscriber, daemon=True)
    sub_thread.start()

    run_publisher()

    time.sleep(1)  # let the last message print before exiting
    print("Done!")