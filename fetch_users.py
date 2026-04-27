"""
fetch_users.py - Requests and prints all registered users from the Database
"""

import json
import os
import time
from pathlib import Path


def _load_env_file() -> None:
    for candidate in (Path(__file__).with_name("locker_pi.env"), Path(__file__).with_name(".env")):
        if not candidate.exists():
            continue
        for raw_line in candidate.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
        break


_load_env_file()

MISSING_RUNTIME_DEPS: list[str] = []

try:
    from awscrt import mqtt
except ModuleNotFoundError:
    mqtt = None
    MISSING_RUNTIME_DEPS.append("awscrt")

try:
    from awsiot import mqtt_connection_builder
except ModuleNotFoundError:
    mqtt_connection_builder = None
    MISSING_RUNTIME_DEPS.append("awsiot")


def ensure_runtime_dependencies():
    if MISSING_RUNTIME_DEPS:
        names = ", ".join(MISSING_RUNTIME_DEPS)
        raise RuntimeError(
            f"Missing runtime dependencies: {names}. Install the AWS IoT SDK packages before running fetch_users.py."
        )

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT  = os.getenv("LOCKER_AWS_ENDPOINT",  "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH     = os.getenv("LOCKER_CERT_PATH",     "/home/uindykennel/certs/locker-pi-001.cert.pem")
KEY_PATH      = os.getenv("LOCKER_KEY_PATH",      "/home/uindykennel/certs/locker-pi-001.private.key")
CA_PATH       = os.getenv("LOCKER_CA_PATH",       "/home/uindykennel/certs/AmazonRootCA1.pem")

# Must match the Pi's allowed Client ID. 
# Make sure locker_main.py is NOT running!
CLIENT_ID     = os.getenv("LOCKER_CLIENT_ID",     "locker-pi-001") 

# ─────────────────────────────────────────────────────────────────────────────
# CALLBACK: What to do when the DB sends a user
# ─────────────────────────────────────────────────────────────────────────────
def on_user_received(topic, payload, **kwargs):
    try:
        msg = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
        
        # Only process USER_UPDATE messages
        event_type = msg.get("eventType", msg.get("event", "")).strip().upper()
        if event_type == "USER_UPDATE":
            name  = msg.get("name", "Unknown Name")
            card  = msg.get("cardId", msg.get("card_number", "Unknown Card"))
            email = msg.get("email", "No Email")
            
            print(f"👤 User Found: {name}")
            print(f"   ├─ Card ID: {card}")
            print(f"   └─ Email:   {email}\n")
            
    except Exception as exc:
        print(f"[Error parsing incoming message]: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRIPT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ensure_runtime_dependencies()
    print("Connecting to AWS IoT...")
    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT, 
        cert_filepath=CERT_PATH, 
        pri_key_filepath=KEY_PATH, 
        ca_filepath=CA_PATH,
        client_id=CLIENT_ID, 
        clean_session=False, 
        keep_alive_secs=30
    )
    
    connection.connect().result()
    print("Connected!\n")
    time.sleep(2) # Allow session to settle

    # 1. Subscribe to the Database Updates topic
    print("Subscribing to database updates...")
    sub_future, packet_id = connection.subscribe(
        topic="users/db/update",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_user_received
    )
    sub_future.result()

    # 2. Publish the SYNC_REQUEST to trigger the backend to send the data
    print("Requesting user list from the cloud database...\n")
    print("="*50)
    
    sync_payload = {
        "eventType": "SYNC_REQUEST",
        "piClientId": CLIENT_ID
    }
    
    pub_future, packet_id = connection.publish(
        topic=f"lockers/pi/{CLIENT_ID}/status",
        payload=json.dumps(sync_payload),
        qos=mqtt.QoS.AT_LEAST_ONCE
    )
    pub_future.result()

    # 3. Wait to receive the incoming messages
    # (Adjust this time if your database takes longer to respond)
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        pass # Allow user to exit early with Ctrl+C

    print("="*50)
    print("Finished listening for users. Disconnecting...")
    
    connection.disconnect().result()

if __name__ == "__main__":
    main()
