import paho.mqtt.client as mqtt
import pymongo
import json
import time

# --- Configuration ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_COMMAND_TOPIC = "golf/stroke/command"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "golf_ball_db"

# --- MongoDB Setup ---
try:
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    db = mongo_client[DB_NAME]
    mongo_client.server_info()
except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    exit(1)

def get_devices():
    return sorted([c for c in db.list_collection_names() if not c.startswith('system.') and c != 'assignments' and c != 'active_devices'])

def reset_device(client, device_id):
    # CRITICAL: separators=(',', ':') removes spaces so ESP32 can read it
    payload = {"cmd": "RESET", "device": device_id}
    json_payload = json.dumps(payload, separators=(',', ':'))
    
    # Send MQTT Signal
    client.publish(MQTT_COMMAND_TOPIC, json_payload)
    print(f"📡 Sent to ESP32: {json_payload}")

    # Reset MongoDB values
    targets = get_devices() if device_id == "all" else [device_id]
    for t in targets:
        db[t].update_one({"device": t}, {"$set": {"count": 0}})
        print(f"💾 Reset MongoDB: {t}")

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_start()

    print("\n--- AUTO RESET TEST ---")
    devices = get_devices()
    for dev in devices:
        print(f"Resetting {dev}...")
        reset_device(client, dev)
    
    time.sleep(2)
    client.loop_stop()

if __name__ == "__main__":
    main()
