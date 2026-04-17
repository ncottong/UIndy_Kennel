"""
pi_locker_main.py  –  Smart Locker Controller
Follows the system outline exactly.

CSV formats
  lockers.csv         : locker_id, reed_pin, led_pin, solenoid_pin, state, assigned_user, time_assigned
  registered_users.csv: name, card_number, email, time_registered

Locker states (state column)
  CLOSED   – door shut and locker is UNOCCUPIED (available)
  OCCUPIED – door shut and locker is in use (assigned_user != NONE)
  OPENED   – door physically left open after the 30-second close window expired

Cache
  Ordered list of {"locker_id": ..., "card_number": ...}
  Index 0 = top (most-recently vacated), index -1 = bottom (least-recently vacated)
  – When a locker is released the entry is inserted at the top.
  – When a locker is assigned the entry for that locker is removed.
  – A returning user (card found in cache) is sent back to their old locker.
  – A new/unknown user gets the bottom entry (least-recently vacated).
"""

import csv
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import RPi.GPIO as GPIO
from awscrt import mqtt
from awsiot import mqtt_connection_builder
from evdev import InputDevice, categorize, ecodes


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT  = os.getenv("LOCKER_AWS_ENDPOINT",  "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH     = os.getenv("LOCKER_CERT_PATH",     "/home/uindykennel/locker_code/certs/locker-pi-001.cert.pem")
KEY_PATH      = os.getenv("LOCKER_KEY_PATH",      "/home/uindykennel/locker_code/certs/locker-pi-001.private.key")
CA_PATH       = os.getenv("LOCKER_CA_PATH",       "/home/uindykennel/locker_code/certs/AmazonRootCA1.pem")
CLIENT_ID     = os.getenv("LOCKER_CLIENT_ID",     "locker-pi-001")
HID_DEVICE    = os.getenv("LOCKER_HID_DEVICE",    "/dev/input/by-id/usb-EFFON-RD_EFFON-event-kbd")
LOCKERS_FILE  = os.getenv("LOCKER_LOCKERS_FILE",  "/home/uindykennel/locker_code/lockers.csv")
USERS_FILE    = os.getenv("LOCKER_USERS_FILE",    "/home/uindykennel/locker_code/registered_users.csv")

UNLOCK_WINDOW_SECONDS       = 15   # time to open the door after unlock
DOOR_CLOSE_WINDOW_SECONDS   = 30   # time to close the door before it is flagged OPENED
REGISTRATION_TIMEOUT_SECONDS = 60  # how long to wait for a QR-code registration
HEARTBEAT_SECONDS           = 60
CARD_RETRY_SECONDS          = 5
MQTT_RETRY_SECONDS          = 30

LOCKER_FIELDNAMES = ["locker_id", "reed_pin", "led_pin", "solenoid_pin",
                     "state", "assigned_user", "time_assigned"]
USER_FIELDNAMES   = ["name", "card_number", "email", "time_registered"]


# ─────────────────────────────────────────────────────────────────────────────
# GPIO
# ─────────────────────────────────────────────────────────────────────────────
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

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
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────
mqtt_conn          = None
mqtt_ready         = threading.Event()
mqtt_publish_lock  = threading.Lock()

lockers: list = []          # list[Locker], populated at startup

# Cache: index-0 = top (most-recently vacated)
locker_cache: list = []     # list[{"locker_id": str, "card_number": str}]
cache_lock = threading.Lock()

# Lockers whose doors are physically left open (30-second timer expired)
opened_lockers: set = set()
opened_lockers_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def decode_card_key(keycode) -> str:
    """Convert a raw evdev keycode to its character, or '' if unrecognised."""
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


# ─────────────────────────────────────────────────────────────────────────────
# LOCKER CLASS
# ─────────────────────────────────────────────────────────────────────────────
class Locker:
    """
    Represents one physical locker.

    state values  : CLOSED | OCCUPIED | OPENED
    assigned_user : card_number string, or "NONE" when unoccupied
    """

    def __init__(self, row: dict):
        self.id             = str(row["locker_id"]).strip()
        self.reed_pin       = int(row["reed_pin"])
        self.led_pin        = int(row["led_pin"])
        self.solenoid_pin   = int(row["solenoid_pin"])
        self.state          = str(row.get("state", "CLOSED")).strip().upper()
        self.assigned_user  = str(row.get("assigned_user", "NONE")).strip()
        self.time_assigned  = str(row.get("time_assigned", "NONE")).strip()
        self.operation_lock = threading.Lock()

        GPIO.setup(self.reed_pin,     GPIO.IN,  pull_up_down=GPIO.PUD_UP)
        GPIO.setup(self.led_pin,      GPIO.OUT)
        GPIO.setup(self.solenoid_pin, GPIO.OUT)
        self._apply_hardware_state()

    # ── Properties ──────────────────────────────────────────────────────────
    @property
    def is_occupied(self) -> bool:
        return self.assigned_user not in ("NONE", "", None)

    # ── Hardware helpers ─────────────────────────────────────────────────────
    def _unlock(self):
        GPIO.output(self.solenoid_pin, GPIO.HIGH)

    def _lock(self):
        GPIO.output(self.solenoid_pin, GPIO.LOW)

    def _led_on(self):
        GPIO.output(self.led_pin, GPIO.HIGH)

    def _led_off(self):
        GPIO.output(self.led_pin, GPIO.LOW)

    def _led_blink(self, elapsed: float):
        level = GPIO.HIGH if int(elapsed * 2) % 2 == 0 else GPIO.LOW
        GPIO.output(self.led_pin, level)

    def _apply_hardware_state(self):
        """Drive LED and solenoid from the current state / occupancy."""
        self._lock()
        if self.state == "CLOSED" and not self.is_occupied:
            self._led_off()                 # available
        elif self.state in ("OCCUPIED", "OPENED"):
            self._led_on()                  # in use or door left open
        elif self.state == "CLOSED" and self.is_occupied:
            self._led_on()                  # closed but occupied
        else:
            self._led_off()

    def is_door_open(self) -> bool:
        return GPIO.input(self.reed_pin) == GPIO.LOW

    # ── Serialisation ────────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "locker_id":     self.id,
            "reed_pin":      self.reed_pin,
            "led_pin":       self.led_pin,
            "solenoid_pin":  self.solenoid_pin,
            "state":         self.state,
            "assigned_user": self.assigned_user,
            "time_assigned": self.time_assigned,
        }


# ─────────────────────────────────────────────────────────────────────────────
# CSV I/O
# ─────────────────────────────────────────────────────────────────────────────
def setup_files():
    """Create CSV files with correct headers if they do not already exist."""
    for path, fieldnames in ((LOCKERS_FILE, LOCKER_FIELDNAMES),
                              (USERS_FILE,   USER_FIELDNAMES)):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            with p.open("w", newline="", encoding="utf-8") as fh:
                csv.DictWriter(fh, fieldnames=fieldnames).writeheader()
            print(f"[INIT] Created {path}")
        else:
            print(f"[INIT] Found {path}")


def load_lockers_from_csv() -> list:
    rows = []
    with open(LOCKERS_FILE, "r", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return [Locker(row) for row in rows]


def save_all_lockers():
    with open(LOCKERS_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=LOCKER_FIELDNAMES)
        writer.writeheader()
        for locker in lockers:
            writer.writerow(locker.to_dict())


def is_registered_user(card_number: str) -> bool:
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if row.get("card_number", "").strip() == card_number:
                    return True
    except Exception as exc:
        print(f"[ERROR] Reading users file: {exc}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# CACHE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────
def cache_initialize():
    """
    Populate the cache at startup with all CLOSED, unoccupied lockers.
    The order is the file order; card_number is NONE for unclaimed slots.
    """
    with cache_lock:
        for locker in lockers:
            if locker.state == "CLOSED" and not locker.is_occupied:
                locker_cache.append({"locker_id": locker.id, "card_number": "NONE"})
    print(f"[CACHE] Initialised with {len(locker_cache)} available locker(s): "
          f"{[e['locker_id'] for e in locker_cache]}")


def cache_add_top(locker_id: str, card_number: str):
    """
    Insert a vacated locker at the top (index 0) of the cache.
    Any existing entry for that locker is first removed.
    """
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["locker_id"] != locker_id]
        locker_cache.insert(0, {"locker_id": locker_id, "card_number": card_number})
    print(f"[CACHE] Added locker {locker_id!r} to top  (prev_user={card_number})")
    print(f"[CACHE] Current cache: {[e['locker_id'] for e in locker_cache]}")


def cache_remove(locker_id: str):
    """Remove a locker from the cache once it has been assigned."""
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["locker_id"] != locker_id]
    print(f"[CACHE] Removed locker {locker_id!r}")
    print(f"[CACHE] Current cache: {[e['locker_id'] for e in locker_cache]}")


def cache_pick_for_user(card_number: str):
    """
    Return (locker_id, entry) for the best available locker:
      – If the user's card_number appears in the cache → return that entry.
      – Otherwise → return the bottom entry (least-recently vacated).
      – If the cache is empty → return (None, None).
    """
    with cache_lock:
        if not locker_cache:
            return None, None
        for entry in locker_cache:
            if entry["card_number"] == card_number:
                return entry["locker_id"], entry
        # Not found: use last (bottom) entry
        return locker_cache[-1]["locker_id"], locker_cache[-1]


# ─────────────────────────────────────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────────────────────────────────────
def publish_status(locker_id: str, state: str, assigned_user: str,
                   event_type: str, user_id: str = "", source: str = "PI"):
    if mqtt_conn is None or not mqtt_ready.is_set():
        print(f"[MQTT] Offline – skipping event {event_type} for {locker_id}")
        return
    payload = {
        "lockerId":     locker_id,
        "state":        state,
        "assignedUser": assigned_user or "NONE",
        "eventType":    event_type,
        "userId":       user_id or "",
        "timestamp":    utc_now_iso(),
        "piClientId":   CLIENT_ID,
        "source":       source,
    }
    topic = f"lockers/{locker_id}/status"
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
        print(f"[ERROR] publish_status failed for {locker_id}: {exc}")


def on_connection_interrupted(connection, error, **kwargs):
    mqtt_ready.clear()
    print(f"[MQTT] Interrupted: {error}")


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    mqtt_ready.set()
    print(f"[MQTT] Resumed: return_code={return_code}")


def connect_mqtt():
    global mqtt_conn
    for label, path in (("cert", CERT_PATH), ("key", KEY_PATH), ("CA", CA_PATH)):
        if not Path(path).exists():
            raise FileNotFoundError(f"Missing {label}: {path}")

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

    for locker in lockers:
        topic = f"lockers/{locker.id}/cmd"
        mqtt_conn.subscribe(topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE,
                            callback=on_cloud_command)

    # Publish startup snapshot
    publish_status(CLIENT_ID, "ONLINE", "", "HEARTBEAT", source="PI")
    for locker in lockers:
        publish_status(locker.id, locker.state, locker.assigned_user,
                       "SYNC_STATE", locker.assigned_user, source="PI")
    print("[MQTT] Connected and snapshot published")


def on_cloud_command(topic, payload, **kwargs):
    """Handle remote unlock commands from the cloud."""
    try:
        msg      = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
        locker_id = msg.get("lockerId", "")
        actor_id  = msg.get("userId") or msg.get("requestedBy") or "CLOUD"
        target    = next((l for l in lockers if l.id == locker_id), None)
        if target is None:
            publish_status(locker_id or "UNKNOWN", "UNKNOWN", "", "UNKNOWN_LOCKER",
                           actor_id, source="CLOUD")
            return
        threading.Thread(
            target=open_locker_sequence,
            args=(target, actor_id),
            kwargs={"source": "CLOUD"},
            daemon=True,
        ).start()
    except Exception as exc:
        print(f"[ERROR] on_cloud_command: {exc}")


def mqtt_maintainer_loop():
    while True:
        if mqtt_conn is None:
            try:
                connect_mqtt()
            except Exception as exc:
                print(f"[MQTT] Connect failed: {exc}. Retrying in {MQTT_RETRY_SECONDS}s")
                mqtt_ready.clear()
        time.sleep(MQTT_RETRY_SECONDS)


def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        publish_status(CLIENT_ID, "ONLINE", "", "HEARTBEAT", source="PI")


# ─────────────────────────────────────────────────────────────────────────────
# CARD READER
# ─────────────────────────────────────────────────────────────────────────────
def wait_for_card_reader():
    while not Path(HID_DEVICE).exists():
        print(f"[CARD] Waiting for reader: {HID_DEVICE}")
        time.sleep(CARD_RETRY_SECONDS)


def read_card_reader() -> str | None:
    """
    Block until a card is swiped, then return the raw accumulated string
    (everything up to the Enter keystroke). Returns None on error.
    """
    if not Path(HID_DEVICE).exists():
        time.sleep(CARD_RETRY_SECONDS)
        return None
    try:
        device  = InputDevice(HID_DEVICE)
        raw     = ""
        for event in device.read_loop():
            if event.type != ecodes.EV_KEY:
                continue
            data = categorize(event)
            if data.keystate != 1:        # only key-down events
                continue
            ch = decode_card_key(data.keycode)
            if ch == "\n":
                return raw if raw else None
            if ch:
                raw += ch
    except Exception as exc:
        print(f"[ERROR] Card reader: {exc}")
        time.sleep(CARD_RETRY_SECONDS)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CARD VALIDATION  (outline §"Check if card is valid")
# ─────────────────────────────────────────────────────────────────────────────
def validate_card(raw: str) -> str | None:
    """
    Outline rules:
      1. Must be exactly 18 characters long.
      2. The middle 16 characters (indices 1–16) must be numeric.
    Returns the 16-digit card number, or None if invalid.
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
# REGISTRATION  (outline §"wait for user to register")
# ─────────────────────────────────────────────────────────────────────────────
def wait_for_registration(card_number: str) -> bool:
    """
    Prompt the user to scan the registration QR code and poll until
    the card_number appears in registered_users.csv, or until timeout.
    Returns True if registered successfully, False otherwise.

    Replace the print/publish calls with physical display output as needed.
    """
    print(f"[REGISTER] Card {card_number} is not registered.")
    print(f"[REGISTER] Please scan the QR code to register (timeout {REGISTRATION_TIMEOUT_SECONDS}s).")
    publish_status("REGISTER", "PENDING", "", "REGISTRATION_REQUIRED",
                   card_number, source="CARD")

    # TODO: Activate physical QR-code display here if hardware is present.

    deadline = time.time() + REGISTRATION_TIMEOUT_SECONDS
    while time.time() < deadline:
        if is_registered_user(card_number):
            print(f"[REGISTER] Card {card_number} successfully registered.")
            publish_status("REGISTER", "COMPLETE", "", "REGISTRATION_SUCCESS",
                           card_number, source="CARD")
            return True
        time.sleep(1)

    print(f"[REGISTER] Registration timed out for {card_number}.")
    publish_status("REGISTER", "FAILED", "", "REGISTRATION_TIMEOUT",
                   card_number, source="CARD")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# LOCKER UNLOCK SEQUENCE  (outline §"Locker unlocking process")
# ─────────────────────────────────────────────────────────────────────────────
def open_locker_sequence(locker: Locker, card_number: str,
                         source: str = "CARD") -> bool:
    """
    Full unlock sequence for one locker/user pair.
    Follows the outline exactly:
      – Unlock + blink for up to UNLOCK_WINDOW_SECONDS.
      – On door open: update state, cache, LED.
      – Wait up to DOOR_CLOSE_WINDOW_SECONDS for door to close.
      – Set final state (CLOSED or OPENED) and lock solenoid.
    """
    if not locker.operation_lock.acquire(blocking=False):
        print(f"[BUSY] Locker {locker.id} is already in a sequence")
        publish_status(locker.id, locker.state, locker.assigned_user,
                       "BUSY", card_number, source=source)
        return False

    try:
        print(f"\n{'─'*55}")
        print(f" Sequence  locker={locker.id}  user={card_number}  src={source}")
        print(f"{'─'*55}")

        was_occupied = locker.is_occupied   # True  → user is collecting belongings
                                            # False → user is depositing belongings

        # ── Step 1: Unlock solenoid, blink LED, wait up to 15s for door open ──
        locker._unlock()
        publish_status(locker.id, locker.state, locker.assigned_user,
                       "UNLOCK_STARTED", card_number, source=source)
        print(f"[UNLOCK] Locker {locker.id} unlocked. Waiting {UNLOCK_WINDOW_SECONDS}s for door open…")

        start        = time.time()
        door_opened  = False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            locker._led_blink(time.time() - start)
            if locker.is_door_open():
                door_opened = True
                break
            time.sleep(0.05)

        if not door_opened:
            # Door not opened → lock and return to idle
            print(f"[TIMEOUT] Door not opened within {UNLOCK_WINDOW_SECONDS}s. Locking.")
            locker._lock()
            locker._apply_hardware_state()      # restore LED to correct state
            publish_status(locker.id, locker.state, locker.assigned_user,
                           "UNLOCK_TIMEOUT", card_number, source=source)
            return False

        # ── Step 2: Door opened ───────────────────────────────────────────────
        print(f"[DOOR] Locker {locker.id} opened by {card_number}")
        publish_status(locker.id, locker.state, locker.assigned_user,
                       "DOOR_OPENED", card_number, source=source)

        if not was_occupied:
            # ── Assigning: locker was previously UNOCCUPIED ──
            locker.state         = "OCCUPIED"
            locker.assigned_user = card_number
            locker.time_assigned = now_str()
            locker._led_on()                        # turn light ON
            cache_remove(locker.id)                 # remove from cache
            print(f"[ASSIGN] Locker {locker.id} → OCCUPIED by {card_number}")

        else:
            # ── Releasing: locker was previously OCCUPIED ──
            prev_user            = locker.assigned_user
            locker.assigned_user = "NONE"
            locker.time_assigned = "NONE"
            locker._led_off()                       # turn light OFF
            cache_add_top(locker.id, prev_user)     # push to top of cache
            print(f"[RELEASE] Locker {locker.id} → UNOCCUPIED (was {prev_user})")

        # ── Step 3: Wait up to 30s for door to close ─────────────────────────
        print(f"[DOOR] Waiting {DOOR_CLOSE_WINDOW_SECONDS}s for door to close…")
        close_start  = time.time()
        door_closed  = False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if not locker.is_door_open():
                door_closed = True
                break
            time.sleep(0.05)

        locker._lock()

        if door_closed:
            # Door closed properly
            locker.state = "CLOSED" if not locker.is_occupied else "OCCUPIED"
            # Outline: "Update locker in locker database as CLOSED"
            # We store OCCUPIED for an occupied-closed locker so it is
            # distinguishable from a free locker (both have door closed).
            print(f"[DOOR] Door closed. Final state: {locker.state}")
            event = "ASSIGNED" if not was_occupied else "RELEASED"
        else:
            # 30-second timer expired, door still open
            locker.state = "OPENED"
            with opened_lockers_lock:
                opened_lockers.add(locker.id)
            print(f"[OPENED] Door still open after {DOOR_CLOSE_WINDOW_SECONDS}s. "
                  f"Locker {locker.id} flagged OPENED.")
            event = "DOOR_LEFT_OPEN"

        save_all_lockers()
        publish_status(locker.id, locker.state, locker.assigned_user,
                       event, card_number, source=source)
        print(f" Sequence complete  state={locker.state}  user={locker.assigned_user}")
        print(f"{'─'*55}\n")
        return True

    finally:
        locker.operation_lock.release()


# ─────────────────────────────────────────────────────────────────────────────
# IDLE STATE – OPENED-LOCKER MONITOR  (outline §"Idle state")
# ─────────────────────────────────────────────────────────────────────────────
def opened_lockers_monitor():
    """
    Background thread.
    Continuously checks the list of OPENED lockers.
    If an opened locker's door is now closed:
      – Update locker database state to CLOSED.
      – Remove from the opened_lockers list.
    """
    while True:
        time.sleep(0.5)
        with opened_lockers_lock:
            snapshot = set(opened_lockers)

        for locker_id in snapshot:
            locker = next((l for l in lockers if l.id == locker_id), None)
            if locker is None:
                with opened_lockers_lock:
                    opened_lockers.discard(locker_id)
                continue
            if not locker.is_door_open():
                # Door is now closed
                locker.state = "CLOSED"
                locker._apply_hardware_state()
                save_all_lockers()
                with opened_lockers_lock:
                    opened_lockers.discard(locker_id)
                print(f"[IDLE] Locker {locker_id} door closed. State → CLOSED.")
                publish_status(locker_id, locker.state, locker.assigned_user,
                               "DOOR_CLOSED_IDLE", "", source="PI")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────
def handle_card_swipe(card_number: str):
    """
    Outline §"Check locker database for card number" and onwards.
    """
    print(f"[SCAN] Card number: {card_number}")
    publish_status("SCANNER", "SCAN", "", "USER_SCAN", card_number, source="CARD")

    # ── 1. Check locker database for this card number ────────────────────────
    owner_locker = next((l for l in lockers if l.assigned_user == card_number), None)
    if owner_locker is not None:
        # Card found in locker database → proceed directly to unlock sequence
        print(f"[LOOKUP] Card found in locker DB → locker {owner_locker.id}")
        open_locker_sequence(owner_locker, card_number, source="CARD")
        return

    # ── 2. Not in locker DB → check registered users ─────────────────────────
    if not is_registered_user(card_number):
        # Card not registered → prompt QR-code registration
        print(f"[LOOKUP] Card not in registered users DB → awaiting registration")
        wait_for_registration(card_number)
        # Regardless of outcome, return to idle
        return

    # ── 3. Registered user without a locker → find one in the cache ──────────
    print(f"[LOOKUP] Card found in registered users DB")
    locker_id, entry = cache_pick_for_user(card_number)

    if locker_id is None:
        print(f"[CACHE] No lockers available. Returning to idle.")
        publish_status("SCANNER", "NONE", "", "NO_LOCKER_AVAILABLE",
                       card_number, source="CARD")
        return

    target = next((l for l in lockers if l.id == locker_id), None)
    if target is None:
        print(f"[ERROR] Cache entry refers to unknown locker {locker_id!r}")
        cache_remove(locker_id)
        return

    print(f"[CACHE] Assigning locker {locker_id} to {card_number}")
    open_locker_sequence(target, card_number, source="CARD")


def main():
    global lockers

    print("\n" + "═"*55)
    print("  SMART LOCKER SYSTEM  –  PI CONTROLLER")
    print("═"*55 + "\n")

    # ── Startup ───────────────────────────────────────────────────────────────
    setup_files()
    lockers = load_lockers_from_csv()
    print(f"[INIT] Loaded {len(lockers)} locker(s): {[l.id for l in lockers]}")

    # Initialise cache from locker CSV
    cache_initialize()

    # Flag any OPENED lockers so the idle monitor picks them up immediately
    for locker in lockers:
        if locker.state == "OPENED":
            with opened_lockers_lock:
                opened_lockers.add(locker.id)

    # ── Background threads ────────────────────────────────────────────────────
    threading.Thread(target=mqtt_maintainer_loop,    daemon=True).start()
    threading.Thread(target=heartbeat_loop,          daemon=True).start()
    threading.Thread(target=opened_lockers_monitor,  daemon=True).start()

    # ── Card reader loop (idle state) ─────────────────────────────────────────
    try:
        while True:
            try:
                wait_for_card_reader()
                raw = read_card_reader()
                if not raw:
                    continue

                # ── Card validation ──
                card_number = validate_card(raw)
                if card_number is None:
                    # Invalid swipe – ignore and wait for next swipe
                    continue

                handle_card_swipe(card_number)

            except Exception as exc:
                print(f"[ERROR] Main loop recovered from: {exc}")
                time.sleep(1)

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] KeyboardInterrupt received. Cleaning up.")
    finally:
        GPIO.cleanup()
        print("[SHUTDOWN] GPIO cleaned up. Goodbye.")


if __name__ == "__main__":
    main()
