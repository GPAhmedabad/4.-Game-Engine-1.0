import paho.mqtt.client as mqtt
import pymongo
import json
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading

SESSION_ID = f"v{int(time.time())}"
print(f"🚀 Starting Ball Server Session: {SESSION_ID}")

# --- Configuration ---
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "golf/stroke/count"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "golf_ball_db"
COLLECTION_NAME = "active_devices"
ASSIGNER_RES_TOPIC = "4/nfc-light/controller/res"
PLAYER_ASSIGN_TOPIC = "golf/player/assign/v2"

# --- Global State for High-Speed Access ---
ball_cache = {} 
pending_player_name = None 

# --- MongoDB Setup ---
try:
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    db = mongo_client[DB_NAME]
    devices_col = db[COLLECTION_NAME]
    mongo_client.server_info() 
    print(f"✅ Connected to MongoDB at {MONGO_URI}")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    mongo_client = None

# --- High-Speed Cache Sync ---
ball_cache = {} 
def init_cache():
    if mongo_client:
        print("📥 Initializing cache from MongoDB...")
        for col_name in db.list_collection_names():
            if col_name == "active_devices": continue
            doc = db[col_name].find_one({}, {"_id": 0})
            if doc and "device" in doc:
                # Ensure timestamp is a string for consistent sorting and JSON response
                ts = doc.get("timestamp")
                if isinstance(ts, datetime):
                    doc["timestamp"] = ts.isoformat()
                ball_cache[doc["device"]] = doc
        print(f"✅ Cache initialized with {len(ball_cache)} devices")

init_cache()

# --- Flask App Setup ---
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}) # Full CORS support


from flask import send_from_directory, request
import os

@app.route('/', methods=['GET'])
def index():
    print(f"🏠 Serving Dashboard to {request.remote_addr}")
    return send_from_directory(os.getcwd(), "Theme 2 Update.html")

@app.before_request
def log_request_info():
    pass # Silent logging

# --- Focus/Visible State ---
scanned_visible_ids = set() # Balls scanned on Station 1

@app.route('/balls', methods=['GET'])
def get_balls():
    # 1. Fetch all assignments from the "Master List"
    assignments = {}
    if mongo_client:
        try:
            assign_list = list(db["assignments"].find({}, {"_id": 0}))
            for a in assign_list:
                # Store multiple keys for the same assignment to ensure we match it
                raw_id = a.get("ball_id", "").lower().strip()
                assignments[raw_id] = a
                # Also store just the number if possible
                num_only = "".join(filter(str.isdigit, raw_id))
                if num_only:
                    assignments[f"id{num_only}"] = a
                    assignments[num_only] = a
        except Exception as e:
            print(f"⚠️ Assignment Fetch Error: {e}")

    # 2. Build the list of balls to show
    now = datetime.now(timezone.utc)
    display_balls = []
    
    for bid, ball in ball_cache.items():
        bid_lower = bid.lower()
        short_id = bid_lower.split("-")[-1] # e.g. "id3"
        num_only = "".join(filter(str.isdigit, short_id))
        
        # Try matching in order of priority
        match = assignments.get(bid_lower) or assignments.get(short_id) or assignments.get(num_only)
        
        if match:
            ball["player_name"] = match.get("player_name")
            ball["assigner"] = match.get("assigner")
        
        is_assigned = ball.get("player_name") and ball.get("player_name") != "UNASSIGNED"
        ts_str = ball.get("timestamp")
        is_recent = False
        
        if ts_str:
            try:
                ts = ts_str if isinstance(ts_str, datetime) else datetime.fromisoformat(ts_str)
                if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
                if (now - ts).total_seconds() < 3600:
                    is_recent = True
            except: pass

        if is_assigned or is_recent:
            display_balls.append(ball)

    # 3. FILTER: Only show if it has been scanned on Station 1
    global scanned_visible_ids
    display_balls = [b for b in display_balls if b.get("device") in scanned_visible_ids]

    display_balls.sort(key=lambda x: (1 if x.get("player_name") else 0, str(x.get("timestamp") or "")), reverse=True)
    return jsonify(display_balls)

@app.route('/assign_player', methods=['POST'])
def assign_player():
    global pending_player_name
    from flask import request
    data = request.get_json()
    name = data.get("name") if data else None
    
    if name:
        pending_player_name = name
        print(f"👤 Pending Assignment: {name}")
        return jsonify({"status": "Waiting for ball scan..."})
    return jsonify({"error": "No name provided"}), 400

# --- MQTT Handlers ---
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"✅ Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC)
        client.subscribe(PLAYER_ASSIGN_TOPIC)
        client.subscribe("+/nfc-light/controller/res") # Listen for ALL scans for Focus Mode
    else:
        print(f"❌ Failed to connect, code {reason_code}")

def on_message(client, userdata, msg):
    global pending_player_name, focus_ball_id, focus_expiry
    try:
        topic = msg.topic
        payload = msg.payload.decode()

        # --- SCAN-TO-SHOW LOGIC ---
        if topic == "1/nfc-light/controller/res":
            scanned_content = payload.strip()
            print(f"🔦 Station 1 Scan: {scanned_content}")
            
            # Find which ball in our cache matches this scan
            content_lower = scanned_content.lower()
            for bid in ball_cache:
                if content_lower in bid.lower() or bid.lower() in content_lower:
                    scanned_visible_ids.add(bid) # Add to persistent visible list
                    print(f"➕ Dashboard Added: {bid}")
                    break
            return

        # Handle Player Assignment (sent from browser via MQTT)
        if topic == PLAYER_ASSIGN_TOPIC:
            try:
                assign_data = json.loads(payload)
                player_name = assign_data.get("name", "").strip()
                scanned_data = assign_data.get("scanned_data", "").strip()

                if not player_name or not scanned_data:
                    print(f"⚠️ Invalid assignment payload: {payload}")
                    return

                print(f"🎯 Assignment: Player='{player_name}', NFC Scanned='{scanned_data}'")

                import re
                matched_ball = None
                scanned_lower = scanned_data.lower().strip()
                
                print(f"🎯 ASSIGNMENT REQUEST: Player='{player_name}'")
                print(f"📡 RAW SCANNED DATA: '{scanned_data}'")
                print(f"🗃️ KNOWN BALLS: {list(ball_cache.keys())}")

                # 1. Handle raw numeric scans (e.g. "3")
                if scanned_lower.isdigit():
                    target_suffix = f"id{scanned_lower}"
                    for bid in ball_cache:
                        if bid.lower().endswith(target_suffix):
                            matched_ball = bid
                            print(f"✅ Found match via raw number: {matched_ball}")
                            break
                
                # 2. Handle "idX" pattern scans (e.g. "id3")
                if not matched_ball:
                    id_match = re.search(r"id(\d+)", scanned_lower)
                    if id_match:
                        target_suffix = f"id{id_match.group(1)}"
                        for bid in ball_cache:
                            if bid.lower().endswith(target_suffix):
                                matched_ball = bid
                                print(f"✅ Found match via ID pattern: {matched_ball}")
                                break
                
                # 3. Last resort: Exact substring check
                if not matched_ball:
                    for bid in ball_cache:
                        short_id = bid.lower().split("-")[-1] # e.g. "id1"
                        if short_id == scanned_lower or bid.lower() == scanned_lower:
                            matched_ball = bid
                            print(f"✅ Found match via exact ID check: {matched_ball}")
                            break

                if not matched_ball:
                    print(f"❌ NO MATCH FOUND for '{scanned_data}'")
                    return

                print(f"✅ Matched: '{scanned_data}' → {matched_ball} → Player '{player_name}'")

                if mongo_client:
                    # Clear old assignment for this ball
                    db[matched_ball].update_many({}, {"$unset": {"player_name": ""}})
                    # Save new assignment to the ball's own collection
                    db[matched_ball].update_one(
                        {"device": matched_ball},
                        {"$set": {
                            "player_name": player_name,
                            "assigner": "ASSIGNER"
                        }},
                        upsert=True
                    )
                    
                    # LOGGING: Update existing or create new (No Duplicates)
                    clean_ball_id = matched_ball.split("-")[-1].upper() # e.g. "ID3"
                    db["assignments"].update_one(
                        {"ball_id": clean_ball_id},
                        {"$set": {
                            "player_name": player_name,
                            "assigner": "ASSIGNER",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "session": SESSION_ID
                        }},
                        upsert=True
                    )
                    print(f"💾 [{SESSION_ID}] MongoDB Updated: {clean_ball_id} → {player_name}")

                # Update cache in real-time
                if matched_ball not in ball_cache:
                    ball_cache[matched_ball] = {"device": matched_ball, "count": 0}
                
                ball_cache[matched_ball]["player_name"] = player_name
                ball_cache[matched_ball]["assigner"] = "ASSIGNER"
                ball_cache[matched_ball]["timestamp"] = datetime.now(timezone.utc).isoformat()

            except Exception as e:
                print(f"❌ Assignment Error: {e}")
            return


        data = json.loads(payload)
        
        device_id = data.get("device", "unknown")
        data["timestamp"] = datetime.now(timezone.utc).isoformat()
        
        # Update high-speed cache
        ball_cache[device_id] = data
        
        if mongo_client:
            # This logic ensures only ONE document exists for this device collection
            db[device_id].update_one(
                {"device": device_id}, 
                {"$set": data}, 
                upsert=True
            )
            print(f"🔄 Count Updated: {device_id} -> {data['count']}")
            
    except Exception as e:
        print(f"❌ MQTT Processing Error: {e}")

def run_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    print("🚀 Starting MQTT Listener...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"❌ MQTT Connection Error: {e}")

if __name__ == "__main__":
    # Start MQTT in a background thread
    mqtt_thread = threading.Thread(target=run_mqtt, daemon=True)
    mqtt_thread.start()

    # Start Flask API
    print("🌐 Starting API Server on http://localhost:5002")
    app.run(host='0.0.0.0', port=5002, debug=False)
