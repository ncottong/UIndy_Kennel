"""
initialize_database_dictionaries.py - Pushes initial state to the Locker Pi via MQTT
"""

import json
import os
import time
from datetime import datetime
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
            "Missing runtime dependencies: "
            f"{names}. Install the AWS IoT SDK packages before running initialize_database_dictionaries.py."
        )

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG (Matches your pi_locker_main.py environment)
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT  = os.getenv("LOCKER_AWS_ENDPOINT",  "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH     = os.getenv("LOCKER_CERT_PATH",     "/home/uindykennel/certs/locker-pi-001.cert.pem")
KEY_PATH      = os.getenv("LOCKER_KEY_PATH",      "/home/uindykennel/certs/locker-pi-001.private.key")
CA_PATH       = os.getenv("LOCKER_CA_PATH",       "/home/uindykennel/certs/AmazonRootCA1.pem")

# Ensure the main locker script is STOPPED when running this to avoid Client ID conflicts
CLIENT_ID     = os.getenv("LOCKER_CLIENT_ID",     "locker-pi-001") 

def main():
    ensure_runtime_dependencies()
    print("Connecting to AWS IoT...")
    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT, 
        cert_filepath=CERT_PATH, 
        pri_key_filepath=KEY_PATH, 
        ca_filepath=CA_PATH,
        client_id=CLIENT_ID, 
        clean_session=False,  # FIX: Set to False to match previous session states
        keep_alive_secs=30
    )
    
    connection.connect().result()
    print("Connected!\n")
    
    # FIX: Wait a moment for the AWS session resumption to fully settle before blasting messages
    time.sleep(2)

    # 1. Format the date: January 1st of the current year at noon.
    current_year = datetime.now().year
    reg_date = f"{current_year}-01-01 12:00:00"

    # 2. Upload User Data
    user_payload = {
        "eventType": "USER_UPDATE",
        "name": "Noah Cottongim",
        "cardId": "1902565315657405",
        "email": "noah.cottongim@example.com", 
        "registrationDate": reg_date
    }
    
    print(f"Publishing USER_UPDATE for {user_payload['name']}...")
    publish_future, packet_id = connection.publish(
        topic="users/db/update",
        payload=json.dumps(user_payload),
        qos=mqtt.QoS.AT_LEAST_ONCE
    )
    publish_future.result()


    # 3. Upload Locker Data
    # We will activate Lockers 1 through 4 so the Pi can build its cache.
    for i in range(1, 5):
        locker_id = f"Locker {i}"
        locker_payload = {
            "eventType": "LOCKER_UPDATE",
            "lockerId": locker_id,
            "active": True,
            "occupied": False
        }
        
        print(f"Publishing LOCKER_UPDATE to activate {locker_id}...")
        publish_future, packet_id = connection.publish(
            topic="lockers/db/update",
            payload=json.dumps(locker_payload),
            qos=mqtt.QoS.AT_LEAST_ONCE
        )
        publish_future.result()
        time.sleep(0.5) # Give the backend a brief moment to process each message

    print("\nAll placeholder values pushed successfully.")
    
    # Disconnect cleanly
    connection.disconnect().result()
    print("Disconnected.")

if __name__ == "__main__":
    main()
