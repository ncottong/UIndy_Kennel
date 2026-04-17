"""
pi_locker_main.py  –  Smart Locker Controller (DFRobot CH423 Edition)
======================================================================
Follows locker_outline.md exactly. No local CSV files; 100% memory and MQTT.
"""

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import smbus
from awscrt import mqtt
from awsiot import mqtt_connection_builder
from evdev import InputDevice, categorize, ecodes

# DFRobot_CH423.py must be in the same directory (or on PYTHONPATH)
import DFRobot_CH423 as df_module

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG 
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT  = os.getenv("LOCKER_AWS_ENDPOINT",  "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH     = os.getenv("LOCKER_CERT_PATH",     "/home/uindykennel/locker_code/certs/locker-pi-001.cert.pem")
KEY_PATH      = os.getenv("LOCKER_KEY_PATH",      "/home/uindykennel/locker_code/certs/locker-pi-001.private.key")
CA_PATH       = os.getenv("LOCKER_CA_PATH",       "/home/uindykennel/locker_code/certs/AmazonRootCA1.pem")
CLIENT_ID     = os.getenv("LOCKER_CLIENT_ID",     "locker-pi-001")
HID_DEVICE    = os.getenv("LOCKER_HID_DEVICE",    "/dev/input/by-id/usb-EFFON-RD_EFFON-event-kbd")

# Timing constants
UNLOCK_WINDOW_SECONDS     = 15   
DOOR_CLOSE_WINDOW_SECONDS = 30   
HEARTBEAT_SECONDS         = 60
CARD_RETRY_SECONDS        = 5
MQTT_RETRY_SECONDS        = 30

# ─────────────────────────────────────────────────────────────────────────────
# HARDWARE CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
def _build_hardware_dict() -> dict:
    hw = {}
    board_bus     = {0: 1,    1: 2,    2: 3}
    board_address = {0: 0x20, 1: 0x21, 2: 0x22}
    for board_idx in range(3):
        for slot in range(8):
            locker_num = board_idx * 8 + slot + 1
            hw[f"Locker {locker_num}"] = {
                "Bus":        board_bus[board_idx],
                "Address":    board_address[board_idx],
                "Light Pin":  f"eGPO{slot * 2}",
                "Latch Pin":  f"eGPO{slot * 2 + 1}",
                "Sensor Pin": f"eGPIO{slot}",
            }
    return hw

LOCKER_HARDWARE: dict = _build_hardware_dict()

# ─────────────────────────────────────────────────────────────────────────────
# CH423 BOARD WRAPPER
# ─────────────────────────────────────────────────────────────────────────────
class CH423Board(df_module.DFRobot_CH423):
    """DFRobot CH423 with a configurable I2C bus number."""
    def __init__(self, bus_num: int):
        self._args      = 0
        self._mode      = [0] * 8
        self._cbs       = [0] * 8
        self._int_value = 0
        self._gpo0_7    = 0
        self._gpo8_15   = 0
        self._bus_num   = bus_num
        self._bus       = smbus.SMBus(bus_num)

def _pin_int(name: str) -> int:
    return getattr(df_module.DFRobot_CH423, name)

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────
_boards:      dict = {}
_boards_lock: threading.Lock = threading.Lock()

mqtt_conn         = None
mqtt_ready        = threading.Event()
mqtt_publish_lock = threading.Lock()

# ── locker_status_dict (outline §General Information) ─────────────────────────
# All 24 lockers start inactive / unoccupied per outline.
locker_status_dict: dict = {
    f"Locker {i}": {
        "Admin Unlocked": False,
        "Door Shut":      True,
        "Active":         False,
        "Occupied":       False,
        "Occupant": {
            "Name":       None,
            "Card ID":    None,
            "Email":      None,
            "Entry Date": None,
        },
    }
    for i in range(2, 25)
}

temp_user_dict: dict = {
    "Locker 1": {
        "Admin Unlocked": False,
        "Door Shut":      True,
        "Active":         True,
        "Occupied":       True,
        "Occupant": {
            "Name":       "Noah Cottongim",
            "Card ID":    1902565315657405,
            "Email":      "cottongimn@uindy.edu",
            "Entry Date": "1776-07-04 12:00:00",
        },
    }
}

locker_status_lock = threading.Lock()

# ── registered_user_dict (outline §General Information) ───────────────────────
registered_user_dict: dict = {
    "Noah Cottongim": {
        "Card ID": 1902565315657405,
        "Email": "cottongimn@uindy.edu",
        "Registration Time": "1776-07-04 12:00:00"
    }
}
user_dict_lock = threading.Lock()

# ── Locker cache ──────────────────────────────────────────────────────────────
locker_cache: list = []
cache_lock = threading.Lock()

locker_op_locks: dict = {f"Locker {i}": threading.Lock() for i in range(1, 25)}
open_door_lockers:     set = set()
open_door_lock: threading.Lock = threading.Lock()

KEY_MAP = {
    "KEY_0": "0", "KEY_1": "1", "KEY_2": "2", "KEY_3": "3", "KEY_4": "4",
    "KEY_5": "5", "KEY_6": "6", "KEY_7": "7", "KEY_8": "8", "KEY_9": "9",
    "KEY_KP0": "0", "KEY_KP1": "1", "KEY_KP2": "2", "KEY_KP3": "3", "KEY_KP4": "4",
    "KEY_KP5": "5", "KEY_KP6": "6", "KEY_KP7": "7", "KEY_KP8": "8", "KEY_KP9": "9",
    "KEY_SPACE": " ",
}
MODIFIER_KEYS = {
    "KEY_LEFTSHIFT", "KEY_RIGHTSHIFT", "KEY_LEFTCTRL", "KEY_RIGHTCTRL",
    "KEY_LEFTALT", "KEY_RIGHTALT", "KEY_LEFTMETA", "KEY_RIGHTMETA",
    "KEY_CAPSLOCK", "KEY_NUMLOCK",
}

# ─────────────────────────────────────────────────────────────────────────────
# BOARD MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
def get_board(locker_id: str) -> "CH423Board | None":
    hw = LOCKER_HARDWARE.get(locker_id)
    if hw is None: return None
    bus_num = hw["Bus"]
    with _boards_lock:
        if bus_num not in _boards:
            try:
                board = CH423Board(bus_num)
                board.begin(gpio_mode=df_module.DFRobot_CH423.eINPUT,
                            gpo_mode=df_module.DFRobot_CH423.ePUSH_PULL)
                _boards[bus_num] = board
            except Exception as exc:
                print(f"[HW] Failed to init CH423 on bus {bus_num}: {exc}")
                return None
        return _boards[bus_num]

# ─────────────────────────────────────────────────────────────────────────────
# LOCKER CONTROL FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
def unlock_locker(locker_id: str):
    board = get_board(locker_id)
    if board:
        pin = _pin_int(LOCKER_HARDWARE[locker_id]["Latch Pin"])
        try: board.gpo_digital_write(pin, 1)
        except Exception: pass

def lock_locker(locker_id: str):
    board = get_board(locker_id)
    if board:
        pin = _pin_int(LOCKER_HARDWARE[locker_id]["Latch Pin"])
        try: board.gpo_digital_write(pin, 0)
        except Exception: pass

def _set_light(locker_id: str, on: bool):
    board = get_board(locker_id)
    if board:
        pin = _pin_int(LOCKER_HARDWARE[locker_id]["Light Pin"])
        try: board.gpo_digital_write(pin, 1 if on else 0)
        except Exception: pass

def blink_light(locker_id: str, elapsed: float):
    _set_light(locker_id, int(elapsed) % 2 == 0)

def is_door_closed(locker_id: str) -> bool:
    board = get_board(locker_id)
    if board is None: return True
    pin = _pin_int(LOCKER_HARDWARE[locker_id]["Sensor Pin"])
    try: return board.gpio_digital_read(pin) == 1
    except Exception: return True

def apply_hardware_state(locker_id: str):
    with locker_status_lock:
        status         = locker_status_dict.get(locker_id, {})
        active         = status.get("Active", False)
        occupied       = status.get("Occupied", False)
        admin_unlocked = status.get("Admin Unlocked", False)

    if not active:
        _set_light(locker_id, False)
        lock_locker(locker_id)
        return

    if admin_unlocked: unlock_locker(locker_id)
    else: lock_locker(locker_id)
    _set_light(locker_id, occupied or admin_unlocked)

# ─────────────────────────────────────────────────────────────────────────────
# USER CHECK FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
def is_card_registered(card_id: str) -> "str | bool":
    with user_dict_lock:
        for name, info in registered_user_dict.items():
            if str(info.get("Card ID", "")) == str(card_id):
                return name
    return False

def is_card_assigned(card_id: str) -> "str | bool":
    with locker_status_lock:
        for lid, status in locker_status_dict.items():
            if (status.get("Active") and status.get("Occupied") and
                    str(status.get("Occupant", {}).get("Card ID", "")) == str(card_id)):
                return lid
    return False

def is_locker_available() -> "str | bool":
    with cache_lock:
        if locker_cache: return locker_cache[0]["Locker ID"]
    return False

# ─────────────────────────────────────────────────────────────────────────────
# CACHE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────
def cache_initialize():
    with locker_status_lock:
        available = [
            lid for lid, status in locker_status_dict.items()
            if status.get("Active") and not status.get("Occupied")
        ]
    with cache_lock:
        locker_cache.clear()
        for lid in available:
            locker_cache.append({
                "Locker ID":              lid,
                "Previous User":          None,
                "Previous User Card ID":  None,
                "Previous User Email":    None,
            })
    print(f"[CACHE] Initialised with {len(locker_cache)} locker(s)")

def cache_add(locker_id: str, prev_user: "str | None",
              prev_card_id: "str | None", prev_email: "str | None"):
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["Locker ID"] != locker_id]
        locker_cache.append({
            "Locker ID":              locker_id,
            "Previous User":          prev_user,
            "Previous User Card ID":  prev_card_id,
            "Previous User Email":    prev_email,
        })
    print(f"[CACHE] Added {locker_id} to end")

def cache_remove(locker_id: str):
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["Locker ID"] != locker_id]
    print(f"[CACHE] Removed {locker_id}")

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

# ─────────────────────────────────────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────────────────────────────────────
def publish_event(locker_id: str, event_type: str, card_id: str = "", extra: dict = None):
    if mqtt_conn is None or not mqtt_ready.is_set(): return
    with locker_status_lock:
        status = locker_status_dict.get(locker_id, {})
        occupied  = status.get("Occupied", False)
        door_shut = status.get("Door Shut", True)

    payload = {
        "lockerId":   locker_id,
        "state":      ("OCCUPIED" if occupied else ("CLOSED" if door_shut else "OPENED")),
        "eventType":  event_type,
        "userId":     card_id,
        "occupied":   occupied,
        "doorShut":   door_shut,
        "timestamp":  utc_now_iso(),
        "piClientId": CLIENT_ID,
    }
    if extra: payload.update(extra)

    topic = f"lockers/{locker_id.replace(' ', '_')}/status"
    try:
        with mqtt_publish_lock:
            mqtt_conn.publish(topic=topic, payload=json.dumps(payload), qos=mqtt.QoS.AT_LEAST_ONCE)
        print(f"[MQTT] → {topic} | {event_type}")
    except Exception as exc: pass

def publish_unregistered(card_id: str):
    if mqtt_conn is None or not mqtt_ready.is_set(): return
    payload = {
        "eventType":  "UNREGISTERED_USER_CARD_ID",
        "cardId":     card_id,
        "timestamp":  utc_now_iso(),
        "piClientId": CLIENT_ID,
    }
    try:
        with mqtt_publish_lock:
            mqtt_conn.publish(topic="users/unregistered", payload=json.dumps(payload), qos=mqtt.QoS.AT_LEAST_ONCE)
    except Exception: pass

def on_cloud_command(topic, payload, **kwargs):
    try:
        msg        = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
        event_type = msg.get("eventType", msg.get("event", "")).strip().upper()

        if event_type in ("ADMIN_UNLOCK", "UNLOCK", "OPEN"):
            _handle_admin_unlock(msg.get("lockerId", ""), msg.get("requestedBy") or msg.get("userId") or "ADMIN")
        elif event_type == "USER_UPDATE":
            _handle_user_update(msg)
        elif event_type == "LOCKER_UPDATE":
            _handle_locker_update(msg)
    except Exception as exc:
        print(f"[MQTT] on_cloud_command error: {exc}")

def _handle_admin_unlock(locker_id: str, actor: str):
    with locker_status_lock:
        if locker_id not in locker_status_dict or not locker_status_dict[locker_id].get("Active"): return
    threading.Thread(target=_admin_unlock_sequence, args=(locker_id,), daemon=True).start()

def _handle_user_update(msg: dict):
    name     = msg.get("name",    "").strip()
    card_id  = str(msg.get("cardId", msg.get("card_number", ""))).strip()
    email    = msg.get("email",   "").strip().lower()
    reg_date = msg.get("registrationDate", now_str())

    if not name or not card_id or not email: return

    with user_dict_lock:
        for existing_name, info in list(registered_user_dict.items()):
            if info.get("Email", "").lower() == email:
                if reg_date <= info.get("Registration Date", ""): return
                del registered_user_dict[existing_name]
                break
        registered_user_dict[name] = {
            "Card ID":           card_id,
            "Email":             email,
            "Registration Date": reg_date,
        }

def _handle_locker_update(msg: dict):
    locker_id = msg.get("lockerId", "")
    if locker_id not in locker_status_dict: return

    with locker_status_lock:
        status       = locker_status_dict[locker_id]
        was_occupied = status.get("Occupied", False)
        was_active   = status.get("Active", False)

        if "active" in msg:
            status["Active"] = bool(msg["active"])
            # If newly active and unoccupied, add to cache
            if status["Active"] and not was_active and not status["Occupied"]:
                threading.Thread(target=cache_add, args=(locker_id, None, None, None), daemon=True).start()

        if "occupied" in msg:
            new_occupied = bool(msg["occupied"])
            if was_occupied and not new_occupied:
                prev_name, prev_card_id, prev_email = status["Occupant"].get("Name"), status["Occupant"].get("Card ID"), status["Occupant"].get("Email")
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}
                threading.Thread(target=cache_add, args=(locker_id, prev_name, prev_card_id, prev_email), daemon=True).start()
            elif not was_occupied and new_occupied:
                threading.Thread(target=cache_remove, args=(locker_id,), daemon=True).start()
                status["Occupied"] = True
                status["Occupant"] = {
                    "Name":       msg.get("occupantName"),
                    "Card ID":    msg.get("occupantCardId"),
                    "Email":      msg.get("occupantEmail"),
                    "Entry Date": msg.get("entryDate", now_str()),
                }
    apply_hardware_state(locker_id)

def on_connection_interrupted(connection, error, **kwargs):
    mqtt_ready.clear()

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    mqtt_ready.set()

def connect_mqtt():
    global mqtt_conn
    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT, cert_filepath=CERT_PATH, pri_key_filepath=KEY_PATH, ca_filepath=CA_PATH,
        client_id=CLIENT_ID, clean_session=False, keep_alive_secs=30,
        on_connection_interrupted=on_connection_interrupted, on_connection_resumed=on_connection_resumed,
    )
    connection.connect().result()
    mqtt_conn = connection
    mqtt_ready.set()

    for i in range(1, 25):
        topic = f"lockers/Locker_{i}/cmd"
        mqtt_conn.subscribe(topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_cloud_command)
    for topic in ("lockers/db/update", "users/db/update"):
        mqtt_conn.subscribe(topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_cloud_command)

    _mqtt_publish_raw(f"lockers/pi/{CLIENT_ID}/status", {"eventType": "STARTUP", "piClientId": CLIENT_ID, "timestamp": utc_now_iso()})
    print("[MQTT] Connected")

def request_db_sync():
    """Request the cloud/DB to push down all current user and locker states."""
    print("[SYNC] Requesting database state over MQTT...")
    _mqtt_publish_raw(f"lockers/pi/{CLIENT_ID}/status", {"eventType": "SYNC_REQUEST", "piClientId": CLIENT_ID})
    # Wait a moment for incoming payloads to populate dictionaries
    time.sleep(3)

def _mqtt_publish_raw(topic: str, payload: dict):
    if mqtt_conn is None or not mqtt_ready.is_set(): return
    try:
        with mqtt_publish_lock:
            mqtt_conn.publish(topic=topic, payload=json.dumps(payload), qos=mqtt.QoS.AT_LEAST_ONCE)
    except Exception: pass

def mqtt_maintainer_loop():
    while True:
        if mqtt_conn is None:
            try: connect_mqtt()
            except Exception: mqtt_ready.clear()
        time.sleep(MQTT_RETRY_SECONDS)

def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        _mqtt_publish_raw(f"lockers/pi/{CLIENT_ID}/status", {"eventType": "HEARTBEAT", "piClientId": CLIENT_ID, "timestamp": utc_now_iso()})

# ─────────────────────────────────────────────────────────────────────────────
# OPEN-DOOR MONITOR 
# ─────────────────────────────────────────────────────────────────────────────
def open_door_monitor():
    while True:
        time.sleep(0.5)
        with open_door_lock: snapshot = set(open_door_lockers)
        for lid in snapshot:
            if is_door_closed(lid):
                with locker_status_lock: locker_status_dict[lid]["Door Shut"] = True
                with open_door_lock: open_door_lockers.discard(lid)
                apply_hardware_state(lid)
                publish_event(lid, "DOOR_CLOSED_IDLE")

# ─────────────────────────────────────────────────────────────────────────────
# CARD READER
# ─────────────────────────────────────────────────────────────────────────────
def decode_card_key(keycode) -> str:
    if isinstance(keycode, list):
        for candidate in keycode:
            result = decode_card_key(candidate)
            if result: return result
        return ""
    keycode = str(keycode)
    if keycode in MODIFIER_KEYS: return ""
    if keycode in {"KEY_ENTER", "KEY_KPENTER"}: return "\n"
    if keycode in KEY_MAP: return KEY_MAP[keycode]
    if keycode.startswith("KEY_"):
        token = keycode[4:]
        if len(token) == 1 and token.isprintable(): return token
    return ""

def wait_for_card_reader():
    while not Path(HID_DEVICE).exists(): time.sleep(CARD_RETRY_SECONDS)

def read_card_reader() -> "str | None":
    if not Path(HID_DEVICE).exists():
        time.sleep(CARD_RETRY_SECONDS)
        return None
    try:
        device, raw = InputDevice(HID_DEVICE), ""
        for event in device.read_loop():
            if event.type != ecodes.EV_KEY: continue
            data = categorize(event)
            if data.keystate != 1: continue
            ch = decode_card_key(data.keycode)
            if ch == "\n": return raw if raw else None
            if ch: raw += ch
    except Exception:
        time.sleep(CARD_RETRY_SECONDS)
        return None

def validate_card(raw: str) -> "str | None":
    if len(raw) != 18: return None
    middle = raw[1:17]
    if not middle.isdigit(): return None
    return middle

# ─────────────────────────────────────────────────────────────────────────────
# UNLOCK SEQUENCE
# ─────────────────────────────────────────────────────────────────────────────
def unlock_sequence(locker_id: str, card_id: str, user_name: str, was_occupied: bool) -> bool:
    op_lock = locker_op_locks[locker_id]
    if not op_lock.acquire(blocking=False): return False

    try:
        unlock_locker(locker_id)
        publish_event(locker_id, "UNLOCK_STARTED", card_id)

        start, door_opened = time.time(), False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            blink_light(locker_id, time.time() - start)
            if not is_door_closed(locker_id):
                door_opened = True
                break
            time.sleep(0.05)

        if not door_opened:
            lock_locker(locker_id)
            apply_hardware_state(locker_id)
            publish_event(locker_id, "UNLOCK_TIMEOUT", card_id)
            return False

        publish_event(locker_id, "DOOR_OPENED", card_id)

        if was_occupied:
            with locker_status_lock:
                status = locker_status_dict[locker_id]
                prev_email = status["Occupant"].get("Email")
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}
                status["Admin Unlocked"] = False
            with user_dict_lock:
                user_email = registered_user_dict.get(user_name, {}).get("Email", prev_email)
            cache_add(locker_id, user_name, card_id, user_email)
            _set_light(locker_id, False)
        else:
            with user_dict_lock:
                user_email = registered_user_dict.get(user_name, {}).get("Email")
            with locker_status_lock:
                status = locker_status_dict[locker_id]
                status["Occupied"] = True
                status["Occupant"] = {"Name": user_name, "Card ID": card_id, "Email": user_email, "Entry Date": now_str()}
                status["Admin Unlocked"] = False
            cache_remove(locker_id)
            _set_light(locker_id, True)

        close_start, door_closed = time.time(), False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if is_door_closed(locker_id):
                door_closed = True
                break
            time.sleep(0.05)

        lock_locker(locker_id)
        with locker_status_lock:
            locker_status_dict[locker_id]["Door Shut"] = door_closed
            if not door_closed:
                with open_door_lock: open_door_lockers.add(locker_id)

        publish_event(locker_id, "RELEASED" if was_occupied else "ASSIGNED" if door_closed else "DOOR_LEFT_OPEN", card_id)
        return True

    finally:
        op_lock.release()

def _admin_unlock_sequence(locker_id: str):
    op_lock = locker_op_locks[locker_id]
    if not op_lock.acquire(blocking=False): return

    try:
        unlock_locker(locker_id)
        publish_event(locker_id, "ADMIN_UNLOCK_STARTED")

        start, door_opened = time.time(), False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            blink_light(locker_id, time.time() - start)
            if not is_door_closed(locker_id):
                door_opened = True
                break
            time.sleep(0.05)

        if not door_opened:
            lock_locker(locker_id)
            apply_hardware_state(locker_id)
            publish_event(locker_id, "ADMIN_UNLOCK_TIMEOUT")
            return

        with locker_status_lock:
            status = locker_status_dict[locker_id]
            occupied = status.get("Occupied", False)
            prev_name, prev_cid, prev_email = status["Occupant"].get("Name"), status["Occupant"].get("Card ID"), status["Occupant"].get("Email")
            if occupied:
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}

        if occupied:
            cache_add(locker_id, prev_name, prev_cid, prev_email)
            _set_light(locker_id, False)
            publish_event(locker_id, "ADMIN_RELEASED", prev_cid or "")

        close_start, door_closed = time.time(), False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if is_door_closed(locker_id):
                door_closed = True
                break
            time.sleep(0.05)

        lock_locker(locker_id)
        with locker_status_lock:
            locker_status_dict[locker_id]["Door Shut"] = door_closed
            if not door_closed:
                with open_door_lock: open_door_lockers.add(locker_id)
        publish_event(locker_id, "ADMIN_DOOR_CLOSED" if door_closed else "ADMIN_DOOR_LEFT_OPEN")
    finally:
        op_lock.release()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP STEPS
# ─────────────────────────────────────────────────────────────────────────────
def check_locker_status_updates():
    with locker_status_lock:
        pending_admin = [lid for lid, s in locker_status_dict.items() if s.get("Active") and s.get("Admin Unlocked")]
        for lid in pending_admin: locker_status_dict[lid]["Admin Unlocked"] = False
    for lid in pending_admin:
        threading.Thread(target=_admin_unlock_sequence, args=(lid,), daemon=True).start()

def handle_card_swipe(card_id: str):
    user_name = is_card_registered(card_id)
    if not user_name:
        publish_unregistered(card_id)
        return
    assigned_locker = is_card_assigned(card_id)
    if assigned_locker:
        unlock_sequence(assigned_locker, card_id, user_name, was_occupied=True)
    else:
        available_locker = is_locker_available()
        if not available_locker:
            publish_event("SYSTEM", "NO_LOCKER_AVAILABLE", card_id)
            return
        unlock_sequence(available_locker, card_id, user_name, was_occupied=False)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # Setup MQTT and request full DB Sync first thing
    threading.Thread(target=mqtt_maintainer_loop, daemon=True).start()
    
    # Wait until MQTT is connected before sending the sync request
    while not mqtt_ready.is_set(): time.sleep(0.5)
    
    request_db_sync()

    # Build FIFO cache from newly synced state
    cache_initialize()

    # Initialise CH423 boards for every active locker
    with locker_status_lock:
        active_lids = [lid for lid, s in locker_status_dict.items() if s.get("Active")]
    for lid in active_lids:
        get_board(lid)
        apply_hardware_state(lid)

    with locker_status_lock:
        for lid, s in locker_status_dict.items():
            if s.get("Active") and not s.get("Door Shut", True):
                with open_door_lock: open_door_lockers.add(lid)

    threading.Thread(target=heartbeat_loop,       daemon=True).start()
    threading.Thread(target=open_door_monitor,    daemon=True).start()

    try:
        while True:
            try:
                check_locker_status_updates()
                wait_for_card_reader()
                raw = read_card_reader()
                if not raw: continue
                card_id = validate_card(raw)
                if card_id is None: continue
                handle_card_swipe(card_id)
            except Exception as exc:
                time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        with locker_status_lock:
            all_active = [lid for lid, s in locker_status_dict.items() if s.get("Active")]
        for lid in all_active:
            try:
                _set_light(lid, False)
                lock_locker(lid)
            except Exception: pass

if __name__ == "__main__":
    main()