"""
pi_locker_main.py  –  Smart Locker Controller (DFRobot CH423 Edition)
======================================================================
Follows locker_outline.md exactly.

Hardware
--------
Up to 3 DFRobot CH423 I2C IO expanders on separate I2C buses.
  • Bus 1 / Address 0x20  →  Lockers  1–8
  • Bus 2 / Address 0x21  →  Lockers  9–16
  • Bus 3 / Address 0x22  →  Lockers 17–24

Each locker uses:
  • 2 GPO output pins  →  light (even) and latch (odd)
  • 1 GPIO input  pin  →  door sensor (HIGH = closed, LOW = open)

Database
--------
Persistent state: lockers.csv / registered_users.csv  (same paths & column
  layout as the previous pi_locker_main.py – no CSV changes required).
Events / admin commands: AWS IoT MQTT (same credentials & topics as before).

State model (locker_outline.md)
--------------------------------
locker_status_dict   – 24 lockers; all start inactive/unoccupied; synced from
                       CSV on boot and updated via MQTT callbacks thereafter.
registered_user_dict – starts empty; synced from CSV; updated live via MQTT.
locker_cache         – FIFO list of available lockers; index 0 = next to assign.
"""

import csv
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
from DFRobot_CH423 import DFRobot_CH423


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  (credentials & paths identical to old pi_locker_main.py)
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT  = os.getenv("LOCKER_AWS_ENDPOINT",  "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH     = os.getenv("LOCKER_CERT_PATH",     "/home/uindykennel/locker_code/certs/locker-pi-001.cert.pem")
KEY_PATH      = os.getenv("LOCKER_KEY_PATH",      "/home/uindykennel/locker_code/certs/locker-pi-001.private.key")
CA_PATH       = os.getenv("LOCKER_CA_PATH",       "/home/uindykennel/locker_code/certs/AmazonRootCA1.pem")
CLIENT_ID     = os.getenv("LOCKER_CLIENT_ID",     "locker-pi-001")
HID_DEVICE    = os.getenv("LOCKER_HID_DEVICE",    "/dev/input/by-id/usb-EFFON-RD_EFFON-event-kbd")
LOCKERS_FILE  = os.getenv("LOCKER_LOCKERS_FILE",  "/home/uindykennel/locker_code/lockers.csv")
USERS_FILE    = os.getenv("LOCKER_USERS_FILE",    "/home/uindykennel/locker_code/registered_users.csv")

# Timing constants (saved at top of code per outline)
UNLOCK_WINDOW_SECONDS     = 15   # seconds to open the door after unlock
DOOR_CLOSE_WINDOW_SECONDS = 30   # seconds to close the door before flagging open
HEARTBEAT_SECONDS         = 60
CARD_RETRY_SECONDS        = 5
MQTT_RETRY_SECONDS        = 30

# CSV column layouts – kept identical to old script so no file changes are needed
LOCKER_FIELDNAMES = ["locker_id", "reed_pin", "led_pin", "solenoid_pin",
                     "state", "assigned_user", "time_assigned"]
USER_FIELDNAMES   = ["name", "card_number", "email", "time_registered"]


# ─────────────────────────────────────────────────────────────────────────────
# HARDWARE CONFIGURATION
# All 24 lockers are defined here even if not yet wired (outline requirement).
#
# Pin pattern (repeats identically on each of the 3 boards):
#   Slot 0  →  Light: eGPO0,  Latch: eGPO1,  Sensor: eGPIO0
#   Slot 1  →  Light: eGPO2,  Latch: eGPO3,  Sensor: eGPIO1
#   ...
#   Slot 7  →  Light: eGPO14, Latch: eGPO15, Sensor: eGPIO7
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
# The DFRobot_CH423 driver hard-codes smbus.SMBus(1).  This subclass lets us
# open any bus number while reusing all the driver methods unchanged.
# ─────────────────────────────────────────────────────────────────────────────
class CH423Board(DFRobot_CH423):
    """DFRobot CH423 with a configurable I2C bus number."""

    def __init__(self, bus_num: int):
        # Initialise internal state manually to avoid the parent __init__
        # opening smbus(1) before we can replace it.
        self._args      = 0
        self._mode      = [0] * 8
        self._cbs       = [0] * 8
        self._int_value = 0
        self._gpo0_7    = 0
        self._gpo8_15   = 0
        self._bus_num   = bus_num
        self._bus       = smbus.SMBus(bus_num)


def _pin_int(name: str) -> int:
    """Convert a CH423 pin name string (e.g. 'eGPO3', 'eGPIO2') to its integer constant."""
    return getattr(DFRobot_CH423, name)


# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

# CH423 board instances, keyed by I2C bus number; created on first use.
_boards:      dict = {}
_boards_lock: threading.Lock = threading.Lock()

# ── MQTT ──────────────────────────────────────────────────────────────────────
mqtt_conn         = None
mqtt_ready        = threading.Event()
mqtt_publish_lock = threading.Lock()

# ── locker_status_dict (outline §General Information) ─────────────────────────
# All 24 lockers start inactive / unoccupied (outline requirement).
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
    for i in range(1, 25)
}
locker_status_lock = threading.Lock()

# ── registered_user_dict (outline §General Information) ───────────────────────
# Starts empty (outline requirement – Pi is read-only, synced from DB on boot).
registered_user_dict: dict = {}
user_dict_lock = threading.Lock()

# ── Locker cache (outline §General Information) ────────────────────────────────
# FIFO list – index 0 is assigned next; new entries appended to the end.
# Entry format:
#   {"Locker ID": str, "Previous User": str|None,
#    "Previous User Card ID": str|None, "Previous User Email": str|None}
locker_cache: list = []
cache_lock = threading.Lock()

# Per-locker operation locks (prevent concurrent unlock sequences on one locker)
locker_op_locks: dict = {f"Locker {i}": threading.Lock() for i in range(1, 25)}

# Lockers whose doors were not closed within DOOR_CLOSE_WINDOW_SECONDS
open_door_lockers:     set = set()
open_door_lock: threading.Lock = threading.Lock()

# ── Card reader key map (from old script) ─────────────────────────────────────
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
    """Return (and lazily create) the CH423Board for the given locker's I2C bus."""
    hw = LOCKER_HARDWARE.get(locker_id)
    if hw is None:
        print(f"[HW] No hardware config for {locker_id!r}")
        return None
    bus_num = hw["Bus"]
    with _boards_lock:
        if bus_num not in _boards:
            try:
                board = CH423Board(bus_num)
                # GPIO pins = input (door sensors); GPO pins = push-pull output
                board.begin(gpio_mode=DFRobot_CH423.eINPUT,
                            gpo_mode=DFRobot_CH423.ePUSH_PULL)
                _boards[bus_num] = board
                print(f"[HW] CH423 board initialised on I2C bus {bus_num}")
            except Exception as exc:
                print(f"[HW] Failed to init CH423 on bus {bus_num}: {exc}")
                return None
        return _boards[bus_num]


# ─────────────────────────────────────────────────────────────────────────────
# LOCKER CONTROL FUNCTIONS  (outline §Locker Control Functions)
# ─────────────────────────────────────────────────────────────────────────────
def unlock_locker(locker_id: str):
    """Open (energise) the latch of the given locker."""
    board = get_board(locker_id)
    if board is None:
        return
    pin = _pin_int(LOCKER_HARDWARE[locker_id]["Latch Pin"])
    try:
        board.gpo_digital_write(pin, 1)
    except Exception as exc:
        print(f"[HW] unlock_locker({locker_id}): {exc}")


def lock_locker(locker_id: str):
    """Release (de-energise) the latch of the given locker."""
    board = get_board(locker_id)
    if board is None:
        return
    pin = _pin_int(LOCKER_HARDWARE[locker_id]["Latch Pin"])
    try:
        board.gpo_digital_write(pin, 0)
    except Exception as exc:
        print(f"[HW] lock_locker({locker_id}): {exc}")


def _set_light(locker_id: str, on: bool):
    """Turn the locker's indicator light on or off."""
    board = get_board(locker_id)
    if board is None:
        return
    pin = _pin_int(LOCKER_HARDWARE[locker_id]["Light Pin"])
    try:
        board.gpo_digital_write(pin, 1 if on else 0)
    except Exception as exc:
        print(f"[HW] _set_light({locker_id}, {on}): {exc}")


def blink_light(locker_id: str, elapsed: float):
    """Blink the locker's light at 1 Hz based on elapsed time (seconds)."""
    _set_light(locker_id, int(elapsed) % 2 == 0)


def is_door_closed(locker_id: str) -> bool:
    """
    Return True if the door sensor reads CLOSED.
    CH423 GPIO pins are HIGH when floating (internal pull-up = door closed)
    and LOW when the reed switch pulls the line down (door open).
    """
    board = get_board(locker_id)
    if board is None:
        return True   # Assume closed if hardware unavailable
    pin = _pin_int(LOCKER_HARDWARE[locker_id]["Sensor Pin"])
    try:
        return board.gpio_digital_read(pin) == 1
    except Exception as exc:
        print(f"[HW] is_door_closed({locker_id}): {exc}")
        return True


def apply_hardware_state(locker_id: str):
    """
    Drive the light and latch to the correct levels for the locker's current
    locker_status_dict entry.  Called after any state change.
    """
    with locker_status_lock:
        status         = locker_status_dict.get(locker_id, {})
        active         = status.get("Active", False)
        occupied       = status.get("Occupied", False)
        admin_unlocked = status.get("Admin Unlocked", False)

    if not active:
        _set_light(locker_id, False)
        lock_locker(locker_id)
        return

    # Latch: energised only when Admin Unlocked
    if admin_unlocked:
        unlock_locker(locker_id)
    else:
        lock_locker(locker_id)

    # Light: on while occupied or while admin-unlocked
    _set_light(locker_id, occupied or admin_unlocked)


# ─────────────────────────────────────────────────────────────────────────────
# USER CHECK FUNCTIONS  (outline §User Check Functions)
# ─────────────────────────────────────────────────────────────────────────────
def is_card_registered(card_id: str) -> "str | bool":
    """Return the user's name if card_id is in registered_user_dict, else False."""
    with user_dict_lock:
        for name, info in registered_user_dict.items():
            if str(info.get("Card ID", "")) == str(card_id):
                return name
    return False


def is_card_assigned(card_id: str) -> "str | bool":
    """Return the locker_id that card_id is assigned to, or False."""
    with locker_status_lock:
        for lid, status in locker_status_dict.items():
            if (status.get("Active") and status.get("Occupied") and
                    str(status.get("Occupant", {}).get("Card ID", "")) == str(card_id)):
                return lid
    return False


def is_locker_available() -> "str | bool":
    """Return the first locker_id in the cache (next to be assigned), or False."""
    with cache_lock:
        if locker_cache:
            return locker_cache[0]["Locker ID"]
    return False


# ─────────────────────────────────────────────────────────────────────────────
# CACHE OPERATIONS  (outline §General Information)
# ─────────────────────────────────────────────────────────────────────────────
def cache_initialize():
    """
    Build cache at startup: all active, unoccupied lockers in CSV order.
    Locks are acquired separately (not nested) to avoid deadlock.
    """
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
    print(f"[CACHE] Initialised with {len(locker_cache)} locker(s): "
          f"{[e['Locker ID'] for e in locker_cache]}")


def cache_add(locker_id: str, prev_user: "str | None",
              prev_card_id: "str | None", prev_email: "str | None"):
    """Append a vacated locker to the END of the cache (FIFO)."""
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["Locker ID"] != locker_id]
        locker_cache.append({
            "Locker ID":              locker_id,
            "Previous User":          prev_user,
            "Previous User Card ID":  prev_card_id,
            "Previous User Email":    prev_email,
        })
    print(f"[CACHE] Added {locker_id} to end  "
          f"| cache: {[e['Locker ID'] for e in locker_cache]}")


def cache_remove(locker_id: str):
    """Remove a locker from the cache once it has been assigned."""
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["Locker ID"] != locker_id]
    print(f"[CACHE] Removed {locker_id}  "
          f"| cache: {[e['Locker ID'] for e in locker_cache]}")


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _normalise_locker_id(raw: str) -> str:
    """Convert raw CSV locker_id (e.g. '1' or 'Locker 1') to 'Locker N' form."""
    raw = raw.strip()
    return raw if raw.startswith("Locker ") else f"Locker {raw}"


# ─────────────────────────────────────────────────────────────────────────────
# CSV I/O  (same file paths & column layout as old pi_locker_main.py)
# ─────────────────────────────────────────────────────────────────────────────
def setup_files():
    """Create CSV files with correct headers if they don't already exist."""
    for path, fieldnames in ((LOCKERS_FILE, LOCKER_FIELDNAMES),
                              (USERS_FILE,   USER_FIELDNAMES)):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            with p.open("w", newline="", encoding="utf-8") as fh:
                csv.DictWriter(fh, fieldnames=fieldnames).writeheader()
            print(f"[INIT] Created {path}")
        else:
            print(f"[INIT] Found   {path}")


def sync_users_from_csv():
    """
    Database Sync step 1 (outline §Initialization):
    Load registered_users.csv into registered_user_dict.
    Duplicate e-mails: keep the entry with the most recent time_registered.
    """
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    except Exception as exc:
        print(f"[SYNC] Cannot read {USERS_FILE}: {exc}")
        return

    # Group by lower-cased e-mail; keep newest registration per e-mail address
    email_map: dict = {}
    for row in rows:
        email = row.get("email", "").strip().lower()
        t_str = row.get("time_registered", "").strip()
        if not email:
            continue
        if email not in email_map or t_str > email_map[email].get("time_registered", ""):
            email_map[email] = row

    new_dict: dict = {}
    for email, row in email_map.items():
        name = row.get("name", "").strip()
        if not name:
            continue
        new_dict[name] = {
            "Card ID":           row.get("card_number", "").strip(),
            "Email":             row.get("email",       "").strip(),
            "Registration Date": row.get("time_registered", "").strip(),
        }

    with user_dict_lock:
        registered_user_dict.clear()
        registered_user_dict.update(new_dict)

    print(f"[SYNC] {len(new_dict)} user(s) loaded from {USERS_FILE}")


def sync_lockers_from_csv():
    """
    Database Sync step 2 (outline §Initialization):
    Load lockers.csv into locker_status_dict.
    • Lockers present in the CSV are treated as Active=True.
    • Occupied/Occupant fields are derived from assigned_user and the user dict.
    • Door Shut is updated from live sensor readings.
    • Ignores reed_pin/led_pin/solenoid_pin columns – hardware is CH423-based.
    """
    try:
        with open(LOCKERS_FILE, "r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    except Exception as exc:
        print(f"[SYNC] Cannot read {LOCKERS_FILE}: {exc}")
        return

    # Snapshot the user dict without holding both locks simultaneously
    with user_dict_lock:
        user_snapshot = {
            name: dict(info) for name, info in registered_user_dict.items()
        }

    with locker_status_lock:
        for row in rows:
            lid = _normalise_locker_id(row.get("locker_id", ""))
            if lid not in locker_status_dict:
                print(f"[SYNC] Unknown locker in CSV: {lid!r} – skipping")
                continue

            assigned_card = row.get("assigned_user", "NONE").strip()
            occupied      = assigned_card not in ("NONE", "", None)

            # Resolve occupant details from the user registry
            occupant_name  = None
            occupant_email = None
            entry_date     = None
            if occupied:
                for name, info in user_snapshot.items():
                    if str(info.get("Card ID", "")) == assigned_card:
                        occupant_name  = name
                        occupant_email = info.get("Email")
                        break
                entry_date = row.get("time_assigned") or None

            locker_status_dict[lid].update({
                "Admin Unlocked": False,
                "Door Shut":      True,   # refreshed from sensor below
                "Active":         True,   # locker is in CSV → admin has activated it
                "Occupied":       occupied,
                "Occupant": {
                    "Name":       occupant_name,
                    "Card ID":    assigned_card if occupied else None,
                    "Email":      occupant_email,
                    "Entry Date": entry_date,
                },
            })

            # Flag lockers whose door was left open at last shutdown
            state = row.get("state", "CLOSED").strip().upper()
            if state == "OPENED":
                locker_status_dict[lid]["Door Shut"] = False

    # Refresh door state from live hardware (outside the lock to avoid deadlock)
    with locker_status_lock:
        active_lockers = [
            lid for lid, s in locker_status_dict.items() if s.get("Active")
        ]

    for lid in active_lockers:
        closed = is_door_closed(lid)
        with locker_status_lock:
            # Only override if the CSV said door was shut; don't overwrite OPENED flag
            if locker_status_dict[lid]["Door Shut"]:
                locker_status_dict[lid]["Door Shut"] = closed
        if not closed:
            with open_door_lock:
                open_door_lockers.add(lid)

    n_active = sum(1 for s in locker_status_dict.values() if s.get("Active"))
    print(f"[SYNC] {n_active} locker(s) loaded from {LOCKERS_FILE}")


def save_locker_to_csv(locker_id: str):
    """
    Persist one locker's current state back to lockers.csv.
    All other rows are preserved unchanged.
    """
    locker_num = locker_id.replace("Locker ", "").strip()

    with locker_status_lock:
        status    = locker_status_dict.get(locker_id, {})
        occupied  = status.get("Occupied", False)
        card_id   = (status.get("Occupant") or {}).get("Card ID") or "NONE"
        t_assign  = (status.get("Occupant") or {}).get("Entry Date") or "NONE"
        door_shut = status.get("Door Shut", True)

    if not door_shut:
        state = "OPENED"
    elif occupied:
        state = "OCCUPIED"
    else:
        state = "CLOSED"

    try:
        rows: list = []
        try:
            with open(LOCKERS_FILE, "r", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
        except Exception:
            pass  # File may not exist yet

        found = False
        for row in rows:
            if row.get("locker_id", "").strip() == locker_num:
                row["state"]         = state
                row["assigned_user"] = card_id
                row["time_assigned"] = t_assign
                found = True
                break

        if not found:
            rows.append({
                "locker_id":     locker_num,
                "reed_pin":      "",
                "led_pin":       "",
                "solenoid_pin":  "",
                "state":         state,
                "assigned_user": card_id,
                "time_assigned": t_assign,
            })

        with open(LOCKERS_FILE, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=LOCKER_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

    except Exception as exc:
        print(f"[CSV] save_locker_to_csv({locker_id}): {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────────────────────────────────────
def publish_event(locker_id: str, event_type: str,
                  card_id: str = "", extra: dict = None):
    """Publish a locker event to AWS IoT.  Silently skips if offline."""
    if mqtt_conn is None or not mqtt_ready.is_set():
        print(f"[MQTT] Offline – skipping {event_type} for {locker_id}")
        return

    with locker_status_lock:
        status = locker_status_dict.get(locker_id, {})
        occupied  = status.get("Occupied", False)
        door_shut = status.get("Door Shut", True)

    payload: dict = {
        "lockerId":   locker_id,
        "state":      ("OCCUPIED" if occupied else ("CLOSED" if door_shut else "OPENED")),
        "eventType":  event_type,
        "userId":     card_id,
        "occupied":   occupied,
        "doorShut":   door_shut,
        "timestamp":  utc_now_iso(),
        "piClientId": CLIENT_ID,
    }
    if extra:
        payload.update(extra)

    # Use the same topic pattern as the old script
    topic = f"lockers/{locker_id.replace(' ', '_')}/status"
    try:
        with mqtt_publish_lock:
            result = mqtt_conn.publish(topic=topic,
                                       payload=json.dumps(payload),
                                       qos=mqtt.QoS.AT_LEAST_ONCE)
        future = result[0] if isinstance(result, tuple) else result
        if hasattr(future, "result"):
            future.result(timeout=10)
        print(f"[MQTT] → {topic} | {event_type}")
    except Exception as exc:
        print(f"[MQTT] publish failed for {locker_id}: {exc}")


def publish_unregistered(card_id: str):
    """Outline: send unregistered card ID to the database."""
    if mqtt_conn is None or not mqtt_ready.is_set():
        print(f"[MQTT] Offline – cannot send UNREGISTERED_USER_CARD_ID {card_id}")
        return
    payload = {
        "eventType":  "UNREGISTERED_USER_CARD_ID",
        "cardId":     card_id,
        "timestamp":  utc_now_iso(),
        "piClientId": CLIENT_ID,
    }
    try:
        with mqtt_publish_lock:
            mqtt_conn.publish(topic="users/unregistered",
                              payload=json.dumps(payload),
                              qos=mqtt.QoS.AT_LEAST_ONCE)
        print(f"[MQTT] Unregistered card published: {card_id}")
    except Exception as exc:
        print(f"[MQTT] publish_unregistered failed: {exc}")


# ── MQTT callbacks ─────────────────────────────────────────────────────────────

def on_cloud_command(topic, payload, **kwargs):
    """
    Handles commands from the admin website / database via MQTT.

    Supported eventType values in the JSON payload:
      ADMIN_UNLOCK   – admin unlock a locker (lockerId required)
      UNLOCK / OPEN  – legacy alias for ADMIN_UNLOCK
      USER_UPDATE    – new or updated user registration
      LOCKER_UPDATE  – locker status change from the website
    """
    try:
        msg        = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
        event_type = msg.get("eventType", msg.get("event", "")).strip().upper()

        if event_type in ("ADMIN_UNLOCK", "UNLOCK", "OPEN"):
            locker_id = msg.get("lockerId", "")
            actor     = msg.get("requestedBy") or msg.get("userId") or "ADMIN"
            _handle_admin_unlock(locker_id, actor)

        elif event_type == "USER_UPDATE":
            _handle_user_update(msg)

        elif event_type == "LOCKER_UPDATE":
            _handle_locker_update(msg)

    except Exception as exc:
        print(f"[MQTT] on_cloud_command error: {exc}")


def _handle_admin_unlock(locker_id: str, actor: str):
    """
    Set Admin Unlocked = True for locker and immediately spawn the unlock
    sequence in a background thread (so the main loop isn't blocked).
    """
    with locker_status_lock:
        if locker_id not in locker_status_dict:
            print(f"[ADMIN] Unknown locker: {locker_id!r}")
            return
        if not locker_status_dict[locker_id].get("Active"):
            print(f"[ADMIN] {locker_id} is not active – ignoring")
            return

    print(f"[ADMIN] Admin unlock requested for {locker_id} by {actor!r}")
    threading.Thread(
        target=_admin_unlock_sequence,
        args=(locker_id,),
        daemon=True,
    ).start()


def _handle_user_update(msg: dict):
    """
    Outline §Main Loop step 2:
    Receive a new/updated user registration from the database.
    Duplicate e-mails: keep the most recently registered card ID.
    """
    name     = msg.get("name",    "").strip()
    card_id  = str(msg.get("cardId", msg.get("card_number", ""))).strip()
    email    = msg.get("email",   "").strip().lower()
    reg_date = msg.get("registrationDate", now_str())

    if not name or not card_id or not email:
        print("[USER] USER_UPDATE missing required fields – ignored")
        return

    with user_dict_lock:
        # Remove any existing entry that shares the same e-mail if this one is newer
        for existing_name, info in list(registered_user_dict.items()):
            if info.get("Email", "").lower() == email:
                if reg_date <= info.get("Registration Date", ""):
                    print(f"[USER] Existing registration for {email} is newer – ignored")
                    return
                del registered_user_dict[existing_name]
                break
        registered_user_dict[name] = {
            "Card ID":           card_id,
            "Email":             email,
            "Registration Date": reg_date,
        }
    print(f"[USER] Registered/updated: {name} ({email})")


def _handle_locker_update(msg: dict):
    """
    Outline §Main Loop step 1 (database-initiated changes):
    Receive a locker status change from the website and update local state.
    """
    locker_id = msg.get("lockerId", "")
    if locker_id not in locker_status_dict:
        print(f"[LOCKER_UPDATE] Unknown locker: {locker_id!r}")
        return

    with locker_status_lock:
        status      = locker_status_dict[locker_id]
        was_occupied = status.get("Occupied", False)

        # Active flag may be toggled by the admin
        if "active" in msg:
            status["Active"] = bool(msg["active"])

        if "occupied" in msg:
            new_occupied = bool(msg["occupied"])
            if was_occupied and not new_occupied:
                # Admin unassigned the locker via the website
                prev_name    = status["Occupant"].get("Name")
                prev_card_id = status["Occupant"].get("Card ID")
                prev_email   = status["Occupant"].get("Email")
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None,
                                       "Email": None, "Entry Date": None}
                # cache_add must be called outside locker_status_lock
                # schedule it by passing to a short thread
                threading.Thread(
                    target=cache_add,
                    args=(locker_id, prev_name, prev_card_id, prev_email),
                    daemon=True,
                ).start()

            elif not was_occupied and new_occupied:
                # Admin assigned the locker via the website
                threading.Thread(target=cache_remove, args=(locker_id,), daemon=True).start()
                status["Occupied"] = True
                status["Occupant"] = {
                    "Name":       msg.get("occupantName"),
                    "Card ID":    msg.get("occupantCardId"),
                    "Email":      msg.get("occupantEmail"),
                    "Entry Date": msg.get("entryDate", now_str()),
                }

    apply_hardware_state(locker_id)
    save_locker_to_csv(locker_id)


def on_connection_interrupted(connection, error, **kwargs):
    mqtt_ready.clear()
    print(f"[MQTT] Connection interrupted: {error}")


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    mqtt_ready.set()
    print(f"[MQTT] Connection resumed: return_code={return_code}")


def connect_mqtt():
    global mqtt_conn
    for label, path in (("cert", CERT_PATH), ("key", KEY_PATH), ("CA", CA_PATH)):
        if not Path(path).exists():
            raise FileNotFoundError(f"Missing {label} file: {path}")

    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT,
        cert_filepath=CERT_PATH,
        pri_key_filepath=KEY_PATH,
        ca_filepath=CA_PATH,
        client_id=CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
    )
    connection.connect().result()
    mqtt_conn = connection
    mqtt_ready.set()

    # Subscribe to per-locker command topics (same as old pi_locker_main.py)
    for i in range(1, 25):
        lid   = f"Locker {i}"
        topic = f"lockers/{lid.replace(' ', '_')}/cmd"
        mqtt_conn.subscribe(topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE,
                            callback=on_cloud_command)

    # Optional broadcast topics (database/website can use these to push updates)
    for topic in ("lockers/db/update", "users/db/update"):
        mqtt_conn.subscribe(topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE,
                            callback=on_cloud_command)

    # Announce startup
    _mqtt_publish_raw(
        f"lockers/pi/{CLIENT_ID}/status",
        {"eventType": "STARTUP", "piClientId": CLIENT_ID, "timestamp": utc_now_iso()},
    )
    print("[MQTT] Connected and subscribed")


def _mqtt_publish_raw(topic: str, payload: dict):
    """Fire-and-forget raw MQTT publish (used internally)."""
    if mqtt_conn is None or not mqtt_ready.is_set():
        return
    try:
        with mqtt_publish_lock:
            mqtt_conn.publish(topic=topic, payload=json.dumps(payload),
                              qos=mqtt.QoS.AT_LEAST_ONCE)
    except Exception as exc:
        print(f"[MQTT] _mqtt_publish_raw to {topic}: {exc}")


def mqtt_maintainer_loop():
    """Background thread: reconnects MQTT whenever the connection drops."""
    while True:
        if mqtt_conn is None:
            try:
                connect_mqtt()
            except Exception as exc:
                print(f"[MQTT] Connect failed: {exc}. Retrying in {MQTT_RETRY_SECONDS}s")
                mqtt_ready.clear()
        time.sleep(MQTT_RETRY_SECONDS)


def heartbeat_loop():
    """Background thread: publishes a heartbeat every HEARTBEAT_SECONDS."""
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        _mqtt_publish_raw(
            f"lockers/pi/{CLIENT_ID}/status",
            {"eventType": "HEARTBEAT", "piClientId": CLIENT_ID, "timestamp": utc_now_iso()},
        )


# ─────────────────────────────────────────────────────────────────────────────
# OPEN-DOOR MONITOR  (background thread – outline §Idle State)
# ─────────────────────────────────────────────────────────────────────────────
def open_door_monitor():
    """
    Watches lockers whose door was not closed within DOOR_CLOSE_WINDOW_SECONDS.
    When a door finally closes, updates Door Shut state and persists to CSV.
    """
    while True:
        time.sleep(0.5)
        with open_door_lock:
            snapshot = set(open_door_lockers)

        for lid in snapshot:
            if is_door_closed(lid):
                with locker_status_lock:
                    locker_status_dict[lid]["Door Shut"] = True
                with open_door_lock:
                    open_door_lockers.discard(lid)
                apply_hardware_state(lid)
                save_locker_to_csv(lid)
                publish_event(lid, "DOOR_CLOSED_IDLE")
                print(f"[MONITOR] {lid} door closed → Door Shut = True")


# ─────────────────────────────────────────────────────────────────────────────
# CARD READER  (identical to old pi_locker_main.py)
# ─────────────────────────────────────────────────────────────────────────────
def decode_card_key(keycode) -> str:
    if isinstance(keycode, list):
        for candidate in keycode:
            result = decode_card_key(candidate)
            if result:
                return result
        return ""
    keycode = str(keycode)
    if keycode in MODIFIER_KEYS:
        return ""
    if keycode in {"KEY_ENTER", "KEY_KPENTER"}:
        return "\n"
    if keycode in KEY_MAP:
        return KEY_MAP[keycode]
    if keycode.startswith("KEY_"):
        token = keycode[4:]
        if len(token) == 1 and token.isprintable():
            return token
    return ""


def wait_for_card_reader():
    """Block until the HID card reader device appears."""
    while not Path(HID_DEVICE).exists():
        print(f"[CARD] Waiting for reader: {HID_DEVICE}")
        time.sleep(CARD_RETRY_SECONDS)


def read_card_reader() -> "str | None":
    """Block until a card is swiped; return the raw accumulated string or None."""
    if not Path(HID_DEVICE).exists():
        time.sleep(CARD_RETRY_SECONDS)
        return None
    try:
        device = InputDevice(HID_DEVICE)
        raw    = ""
        for event in device.read_loop():
            if event.type != ecodes.EV_KEY:
                continue
            data = categorize(event)
            if data.keystate != 1:   # key-down only
                continue
            ch = decode_card_key(data.keycode)
            if ch == "\n":
                return raw if raw else None
            if ch:
                raw += ch
    except Exception as exc:
        print(f"[CARD] Reader error: {exc}")
        time.sleep(CARD_RETRY_SECONDS)
        return None


def validate_card(raw: str) -> "str | None":
    """
    Validate raw card swipe string (same rules as old script):
      • Must be exactly 18 characters.
      • Characters 1–16 (0-indexed) must be numeric.
    Returns the 16-digit card ID, or None if invalid.
    """
    if len(raw) != 18:
        print(f"[CARD] Ignored: expected 18 chars, got {len(raw)}")
        return None
    middle = raw[1:17]
    if not middle.isdigit():
        print(f"[CARD] Ignored: middle 16 chars are not numeric")
        return None
    return middle


# ─────────────────────────────────────────────────────────────────────────────
# UNLOCK SEQUENCE  (outline §Main Loop step 3 sub-steps)
# ─────────────────────────────────────────────────────────────────────────────
def unlock_sequence(locker_id: str, card_id: str, user_name: str,
                    was_occupied: bool, source: str = "CARD") -> bool:
    """
    Full locker interaction sequence for a single locker/user pair.

    was_occupied=True  → EXIT sequence (user retrieving belongings)
    was_occupied=False → ENTRY sequence (user depositing belongings)

    Returns True if the door was opened within the unlock window, else False.
    """
    op_lock = locker_op_locks[locker_id]
    if not op_lock.acquire(blocking=False):
        print(f"[BUSY] {locker_id} already in a sequence – ignoring")
        return False

    try:
        print(f"\n{'─'*60}")
        print(f" Sequence | {locker_id} | {user_name} | card={card_id} | src={source}")
        print(f"{'─'*60}")

        # ── Step 1: Unlock latch, blink light; wait for door to open ─────────
        unlock_locker(locker_id)
        publish_event(locker_id, "UNLOCK_STARTED", card_id)
        print(f"[UNLOCK] {locker_id} unlocked. Waiting {UNLOCK_WINDOW_SECONDS}s …")

        start       = time.time()
        door_opened = False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            blink_light(locker_id, time.time() - start)
            if not is_door_closed(locker_id):
                door_opened = True
                break
            time.sleep(0.05)

        if not door_opened:
            # Door did not open in time → re-lock and restore light
            lock_locker(locker_id)
            apply_hardware_state(locker_id)
            publish_event(locker_id, "UNLOCK_TIMEOUT", card_id)
            print(f"[TIMEOUT] Door not opened within {UNLOCK_WINDOW_SECONDS}s. Re-locked.")
            return False

        # ── Step 2: Door opened → update occupancy ───────────────────────────
        print(f"[DOOR] {locker_id} door opened by {user_name}")
        publish_event(locker_id, "DOOR_OPENED", card_id)

        if was_occupied:
            # EXIT: unassign the locker
            with locker_status_lock:
                status = locker_status_dict[locker_id]
                prev_card_id = status["Occupant"].get("Card ID")
                prev_email   = status["Occupant"].get("Email")
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None,
                                       "Email": None, "Entry Date": None}
                status["Admin Unlocked"] = False

            # Append to end of cache (FIFO – outline requirement)
            with user_dict_lock:
                user_email = registered_user_dict.get(user_name, {}).get("Email", prev_email)
            cache_add(locker_id, user_name, card_id, user_email)
            _set_light(locker_id, False)
            print(f"[RELEASE] {locker_id} unassigned (was {user_name})")

        else:
            # ENTRY: assign the locker to this user
            with user_dict_lock:
                user_info  = registered_user_dict.get(user_name, {})
                user_email = user_info.get("Email")

            with locker_status_lock:
                status = locker_status_dict[locker_id]
                status["Occupied"] = True
                status["Occupant"] = {
                    "Name":       user_name,
                    "Card ID":    card_id,
                    "Email":      user_email,
                    "Entry Date": now_str(),
                }
                status["Admin Unlocked"] = False

            cache_remove(locker_id)
            _set_light(locker_id, True)
            print(f"[ASSIGN] {locker_id} assigned to {user_name}")

        # ── Step 3: Hold latch open; wait up to 30s for door to close ────────
        print(f"[DOOR] Holding latch. Waiting {DOOR_CLOSE_WINDOW_SECONDS}s for close …")
        close_start = time.time()
        door_closed = False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if is_door_closed(locker_id):
                door_closed = True
                break
            time.sleep(0.05)

        lock_locker(locker_id)

        with locker_status_lock:
            locker_status_dict[locker_id]["Door Shut"] = door_closed
            if not door_closed:
                # Flag for the open-door monitor
                with open_door_lock:
                    open_door_lockers.add(locker_id)

        if door_closed:
            print(f"[DOOR] Door closed. Latch released.")
            event = "RELEASED" if was_occupied else "ASSIGNED"
        else:
            print(f"[OPENED] Door not closed after {DOOR_CLOSE_WINDOW_SECONDS}s. "
                  f"{locker_id} flagged as Door Shut = False.")
            event = "DOOR_LEFT_OPEN"

        save_locker_to_csv(locker_id)
        publish_event(locker_id, event, card_id)
        print(f"[DONE] Sequence complete | occupied="
              f"{locker_status_dict[locker_id]['Occupied']}\n")
        return True

    finally:
        op_lock.release()


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN UNLOCK SEQUENCE  (triggered by MQTT admin command)
# ─────────────────────────────────────────────────────────────────────────────
def _admin_unlock_sequence(locker_id: str):
    """
    Outline §Main Loop step 1 (Admin Unlocked = True):
    Unlock the latch and blink the light.  If the door opens and the locker
    was occupied, release the occupant and push to cache.
    Wait up to DOOR_CLOSE_WINDOW_SECONDS for door to close; flag if not.
    """
    op_lock = locker_op_locks[locker_id]
    if not op_lock.acquire(blocking=False):
        print(f"[BUSY] {locker_id} already in a sequence (admin unlock)")
        return

    try:
        print(f"\n[ADMIN] Unlock sequence for {locker_id}")
        unlock_locker(locker_id)
        publish_event(locker_id, "ADMIN_UNLOCK_STARTED")

        start       = time.time()
        door_opened = False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            blink_light(locker_id, time.time() - start)
            if not is_door_closed(locker_id):
                door_opened = True
                break
            time.sleep(0.05)

        if not door_opened:
            lock_locker(locker_id)
            apply_hardware_state(locker_id)
            print(f"[ADMIN] Door not opened within {UNLOCK_WINDOW_SECONDS}s. Re-locked.")
            publish_event(locker_id, "ADMIN_UNLOCK_TIMEOUT")
            return

        # Door opened – release occupant if assigned
        with locker_status_lock:
            status     = locker_status_dict[locker_id]
            occupied   = status.get("Occupied", False)
            prev_name  = status["Occupant"].get("Name")
            prev_cid   = status["Occupant"].get("Card ID")
            prev_email = status["Occupant"].get("Email")
            if occupied:
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None,
                                       "Email": None, "Entry Date": None}

        if occupied:
            cache_add(locker_id, prev_name, prev_cid, prev_email)
            _set_light(locker_id, False)
            print(f"[ADMIN] {locker_id} released (was {prev_name})")
            publish_event(locker_id, "ADMIN_RELEASED", prev_cid or "")

        # Wait for door to close
        close_start = time.time()
        door_closed = False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if is_door_closed(locker_id):
                door_closed = True
                break
            time.sleep(0.05)

        lock_locker(locker_id)

        with locker_status_lock:
            locker_status_dict[locker_id]["Door Shut"] = door_closed
            if not door_closed:
                with open_door_lock:
                    open_door_lockers.add(locker_id)

        save_locker_to_csv(locker_id)
        event = "ADMIN_DOOR_CLOSED" if door_closed else "ADMIN_DOOR_LEFT_OPEN"
        publish_event(locker_id, event)
        print(f"[ADMIN] Sequence complete for {locker_id}")

    finally:
        op_lock.release()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP STEPS
# ─────────────────────────────────────────────────────────────────────────────
def check_locker_status_updates():
    """
    Outline §Main Loop step 1:
    Re-apply hardware state for any lockers whose status was updated since the
    last iteration.  Admin unlock sequences are launched by MQTT callbacks
    immediately via threads; this call handles any edge-case queued updates.
    """
    # Snapshot active lockers and check for any Admin Unlocked that slipped through
    with locker_status_lock:
        pending_admin = [
            lid for lid, s in locker_status_dict.items()
            if s.get("Active") and s.get("Admin Unlocked")
        ]
        for lid in pending_admin:
            locker_status_dict[lid]["Admin Unlocked"] = False

    for lid in pending_admin:
        threading.Thread(target=_admin_unlock_sequence, args=(lid,), daemon=True).start()


def handle_card_swipe(card_id: str):
    """
    Outline §Main Loop step 3:
    Full card swipe handling flow.
    """
    print(f"\n[SCAN] Card: {card_id}")

    # Step 3.2 – Is card registered?
    user_name = is_card_registered(card_id)
    if not user_name:
        print(f"[SCAN] Card {card_id} is not registered → UNREGISTERED_USER_CARD_ID")
        publish_unregistered(card_id)
        return

    print(f"[SCAN] Registered as: {user_name}")

    # Step 3.2.2 – Does this user have an assigned locker?
    assigned_locker = is_card_assigned(card_id)
    if assigned_locker:
        # Exit sequence: user is collecting their belongings
        print(f"[SCAN] {user_name} has {assigned_locker} → EXIT sequence")
        unlock_sequence(assigned_locker, card_id, user_name, was_occupied=True)
    else:
        # Entry sequence: find next available locker from cache
        available_locker = is_locker_available()
        if not available_locker:
            print(f"[SCAN] No lockers available for {user_name}")
            publish_event("SYSTEM", "NO_LOCKER_AVAILABLE", card_id)
            return
        print(f"[SCAN] {user_name} → {available_locker} (from cache) → ENTRY sequence")
        unlock_sequence(available_locker, card_id, user_name, was_occupied=False)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 60)
    print("  SMART LOCKER SYSTEM  –  DFRobot CH423 Edition")
    print("═" * 60 + "\n")

    # ── Initialisation (outline §Initialization §Database Sync) ───────────────
    setup_files()

    # DB Sync step 1: registered users (must run before lockers so occupant
    # names can be resolved during locker sync)
    sync_users_from_csv()

    # DB Sync step 2: locker status
    sync_lockers_from_csv()

    # Build FIFO cache from synced state
    cache_initialize()

    # Initialise CH423 boards for every active locker's bus
    with locker_status_lock:
        active_lids = [lid for lid, s in locker_status_dict.items() if s.get("Active")]

    for lid in active_lids:
        get_board(lid)  # Creates board instance if not yet open
        apply_hardware_state(lid)

    # Flag open doors for the monitor thread
    with locker_status_lock:
        for lid, s in locker_status_dict.items():
            if s.get("Active") and not s.get("Door Shut", True):
                with open_door_lock:
                    open_door_lockers.add(lid)

    print(f"[INIT] Active lockers : {active_lids}")
    print(f"[INIT] Open doors     : {list(open_door_lockers)}")

    # ── Background threads ─────────────────────────────────────────────────────
    threading.Thread(target=mqtt_maintainer_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop,       daemon=True).start()
    threading.Thread(target=open_door_monitor,    daemon=True).start()

    # ── Main loop ─────────────────────────────────────────────────────────────
    try:
        while True:
            try:
                # Step 1: Process any pending locker status changes (admin unlock, etc.)
                check_locker_status_updates()

                # Step 2: registered_user_dict updates are handled live by MQTT callbacks
                #         (_handle_user_update). Nothing extra needed here.

                # Step 3: Wait for a card swipe and process it
                wait_for_card_reader()
                raw = read_card_reader()
                if not raw:
                    continue

                card_id = validate_card(raw)
                if card_id is None:
                    continue

                handle_card_swipe(card_id)

            except Exception as exc:
                print(f"[ERROR] Main loop recovered from: {exc}")
                time.sleep(1)

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] KeyboardInterrupt received. Cleaning up …")

    finally:
        # Safe shutdown: turn off all lights and lock all latches
        with locker_status_lock:
            all_active = [lid for lid, s in locker_status_dict.items() if s.get("Active")]
        for lid in all_active:
            try:
                _set_light(lid, False)
                lock_locker(lid)
            except Exception:
                pass
        print("[SHUTDOWN] Hardware cleared. Goodbye.")


if __name__ == "__main__":
    main()