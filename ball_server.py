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

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "golf_ball_db"
COLLECTION_NAME = "active_devices"
ASSIGNER_RES_TOPIC = "4/nfc-light/controller/res"
PLAYER_ASSIGN_TOPIC = "golf/player/assign/v2"

# --- Global State for High-Speed Access ---
ball_cache = {} 
pending_player_name = None 
mqtt_client = None # Global MQTT client for publishing
MQTT_COMMAND_TOPIC = "golf/stroke/command"
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

    display_balls.sort(key=lambda x: (1 if x.get("player_name") else 0, str(x.get("timestamp") or "")), reverse=True)
    return jsonify(display_balls)

@app.route('/lookup', methods=['POST'])
def lookup_by_nfc():
    """NFC sends raw number like '3' -> find ID3 in MongoDB assignments -> return player name."""
    import re
    data = request.get_json()
    scanned = (data.get("scanned_data", "") if data else "").strip()
    if not scanned:
        return jsonify({"error": "No scanned_data provided"}), 400

    scanned_lower = scanned.lower()
    print(f"🔍 [LOOKUP] Raw NFC: '{scanned}'")

    # Build the canonical ball ID from the raw scan
    # "3"          -> "ID3"
    # "id3"        -> "ID3"
    # "esp32c3-id3" -> "ID3"
    num_only = "".join(filter(str.isdigit, scanned_lower))
    if num_only:
        target_id = f"ID{num_only}"
    else:
        target_id = scanned_lower.split("-")[-1].upper()

    print(f"🎯 [LOOKUP] Targeting: '{target_id}'")

    # Go directly to MongoDB assignments — no ball_cache needed
    assignment = None
    if mongo_client:
        # Try exact match: ball_id = "ID3"
        assignment = db["assignments"].find_one(
            {"ball_id": {"$regex": f"^{re.escape(target_id)}$", "$options": "i"}},
            {"_id": 0}
        )
        # Fallback: any doc whose ball_id contains the digit(s)
        if not assignment and num_only:
            assignment = db["assignments"].find_one(
                {"ball_id": {"$regex": num_only}},
                {"_id": 0}
            )

    if not assignment:
        print(f"❌ [LOOKUP] No assignment found for '{target_id}'")
        return jsonify({"error": "Not registered", "scanned": scanned, "tried": target_id}), 404

    player_name = assignment.get("player_name", "UNASSIGNED")
    ball_id     = assignment.get("ball_id", target_id)
    assigner    = assignment.get("assigner", "SYSTEM")

    # Optional: resolve full device name from live cache
    full_device = None
    for bid in ball_cache:
        if num_only and bid.lower().endswith(f"id{num_only}"):
            full_device = bid
            break

    print(f"✅ [LOOKUP] {ball_id} -> '{player_name}'")

    # Resolve target device ID
    target_device = full_device or ball_id

    # Get do_reset flag from request (default to True)
    do_reset = data.get("do_reset", True) if data else True

    # --- AUTO RESET LOGIC ---
    current_count = 0
    if target_device in ball_cache:
        current_count = ball_cache[target_device].get("count", 0)

    if do_reset and target_device:
        reset_ball(target_device)
        print(f"📊 [LOOKUP] {target_device} reset to 0 (T-Point).")
        current_count = 0
    else:
        print(f"📊 [LOOKUP] {target_device} score is {current_count} (Goal/View).")

    # Check for pending bonus in MongoDB
    bonus_active = False
    if mongo_client:
        bonus_doc = db["assignments"].find_one({"ball_id": ball_id})
        if bonus_doc and bonus_doc.get("bonus_pending"):
            bonus_active = True
            # Clear bonus once looked up (it's being "used" or it's a fresh start at T-Point)
            db["assignments"].update_one({"ball_id": ball_id}, {"$set": {"bonus_pending": False}})
            print(f"🎁 [LOOKUP] Bonus activated for {ball_id}")

    return jsonify({
        "ball_id":     ball_id,
        "device":      target_device,
        "player_name": player_name,
        "assigner":    assigner,
        "count":       current_count,
        "bonus":       bonus_active
    })

def reset_ball(device_id):
    """Sends RESET command to ESP32 and resets MongoDB/Cache."""
    global mqtt_client
    if not device_id: return

    # 1. Send MQTT Reset Command (Exact match to user logic)
    if mqtt_client:
        try:
            payload = {"cmd": "RESET", "device": device_id}
            json_payload = json.dumps(payload, separators=(',', ':'))
            mqtt_client.publish(MQTT_COMMAND_TOPIC, json_payload)
            print(f"📡 [RESET] Sent to ESP32: {json_payload}")
        except Exception as e:
            print(f"❌ [RESET] MQTT Publish Error: {e}")

    # 2. Reset MongoDB (Exact match to user logic)
    if mongo_client:
        try:
            # We use the collection named after the device
            db[device_id].update_one({"device": device_id}, {"$set": {"count": 0}})
            print(f"💾 [RESET] Reset MongoDB: {device_id}")
        except Exception as e:
            print(f"⚠️ [RESET] MongoDB Reset Error: {e}")

    # 3. Reset Cache
    if device_id in ball_cache:
        ball_cache[device_id]["count"] = 0
        print(f"📥 [RESET] Cache Reset: {device_id}")

@app.route('/record_bonus', methods=['POST'])
def record_bonus():
    data = request.get_json()
    scanned = data.get("scanned_data", "")
    if not scanned: return jsonify({"error": "No data"}), 400

    print(f"🎁 [BONUS] 📥 NEW SCAN RECEIVED: '{scanned}'")

    # Extract digits for fallback
    num_only = "".join(filter(str.isdigit, scanned))
    
    if mongo_client:
        # Fetch ALL assignments to do a manual fuzzy match if needed
        all_assignments = list(db["assignments"].find({}, {"_id": 0}))
        print(f"🎁 [BONUS] Checking against {len(all_assignments)} assignments...")
        
        match = None
        
        # 1. Try exact match (case insensitive)
        for a in all_assignments:
            bid = a.get("ball_id", "").upper()
            if bid == scanned.upper():
                match = a
                print(f"🎁 [BONUS] Found Exact Match: {bid}")
                break
        
        # 2. Try numeric match (if scan is '3' and ball is 'ID3')
        if not match and num_only:
            for a in all_assignments:
                bid = a.get("ball_id", "")
                bid_nums = "".join(filter(str.isdigit, bid))
                if bid_nums == num_only:
                    match = a
                    print(f"🎁 [BONUS] Found Numeric Match: {bid} (via {num_only})")
                    break
        
        # 3. Try partial match (if '3' is inside 'ID3')
        if not match:
            for a in all_assignments:
                bid = a.get("ball_id", "").upper()
                if scanned.upper() in bid or bid in scanned.upper():
                    match = a
                    print(f"🎁 [BONUS] Found Partial Match: {bid}")
                    break

        if match:
            real_ball_id = match.get("ball_id")
            player_name = match.get("player_name", "UNKNOWN")
            
            # Mark it
            db["assignments"].update_one(
                {"ball_id": real_ball_id},
                {"$set": {"bonus_pending": True}}
            )
            
            print(f"✅ [BONUS] SUCCESS: {player_name} ({real_ball_id}) awarded bonus!")
            return jsonify({
                "status": "Bonus recorded",
                "ball": real_ball_id,
                "player": player_name
            })
    
    print(f"❌ [BONUS] FAILED: No player found for scan '{scanned}'")
    return jsonify({"error": "Ball not assigned"}), 404

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


        # Final fallback: Try to parse as JSON for device updates
        try:
            data = json.loads(payload)
            if not isinstance(data, dict):
                # If it's just a number or string, it's not a device update payload
                return
            
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
                print(f"🔄 Count Updated: {device_id} -> {data.get('count', 0)}")
        except json.JSONDecodeError:
            # Not a JSON message, ignore or log if necessary
            pass
            
    except Exception as e:
        print(f"❌ MQTT Processing Error: {e}")

def run_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    print("🚀 Starting MQTT Listener...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"❌ MQTT Connection Error: {e}")

if __name__ == "__main__":
    # Start MQTT in a background thread
    mqtt_thread = threading.Thread(target=run_mqtt, daemon=True)
    mqtt_thread.start()

    # Start Flask API
    print(f"🌐 Starting Cloud API Server on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
