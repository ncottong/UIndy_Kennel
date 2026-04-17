"""
initialize_database_dictionaries.py - Pushes initial state to the Locker Pi via MQTT
"""

import json
import os
import time
from datetime import datetime
from awscrt import mqtt
from awsiot import mqtt_connection_builder

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG (Matches your pi_locker_main.py environment)
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT  = os.getenv("LOCKER_AWS_ENDPOINT",  "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH     = os.getenv("LOCKER_CERT_PATH",     "/home/uindykennel/locker_code/certs/locker-pi-001.cert.pem")
KEY_PATH      = os.getenv("LOCKER_KEY_PATH",      "/home/uindykennel/locker_code/certs/locker-pi-001.private.key")
CA_PATH       = os.getenv("LOCKER_CA_PATH",       "/home/uindykennel/locker_code/certs/AmazonRootCA1.pem")

# Ensure the main locker script is STOPPED when running this to avoid Client ID conflicts
CLIENT_ID     = os.getenv("LOCKER_CLIENT_ID",     "locker-pi-001") 

def main():
    print("Connecting to AWS IoT...")
    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT, 
        cert_filepath=CERT_PATH, 
        pri_key_filepath=KEY_PATH, 
        ca_filepath=CA_PATH,
        client_id=CLIENT_ID, 
        clean_session=True, 
        keep_alive_secs=30
    )
    
    connection.connect().result()
    print("Connected!\n")

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
    # FIX: Unpack the tuple returned by publish() before calling .result()
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
        # FIX: Unpack the tuple here as well
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