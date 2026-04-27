import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, request

import DFRobot_CH423 as df_module


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
    import smbus
except ModuleNotFoundError:
    smbus = None
    MISSING_RUNTIME_DEPS.append("smbus")

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

try:
    from evdev import InputDevice, categorize, ecodes
except ModuleNotFoundError:
    InputDevice = categorize = ecodes = None
    MISSING_RUNTIME_DEPS.append("evdev")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
AWS_ENDPOINT = os.getenv("LOCKER_AWS_ENDPOINT", "a3cym5dx6wtyuv-ats.iot.us-east-2.amazonaws.com")
CERT_PATH    = os.getenv("LOCKER_CERT_PATH",    "/home/uindykennel/certs/locker-pi-001.cert.pem")
KEY_PATH     = os.getenv("LOCKER_KEY_PATH",     "/home/uindykennel/certs/locker-pi-001.private.key")
CA_PATH      = os.getenv("LOCKER_CA_PATH",      "/home/uindykennel/certs/AmazonRootCA1.pem")
CLIENT_ID    = os.getenv("LOCKER_CLIENT_ID",    "locker-pi-001")
HID_DEVICE   = os.getenv("LOCKER_HID_DEVICE",   "/dev/input/by-id/usb-EFFON-RD_EFFON-event-kbd")
API_BASE_URL = os.getenv("LOCKER_API_BASE_URL", "").rstrip("/")
DEVICE_TOKEN = os.getenv("LOCKER_DEVICE_TOKEN", "")
QUEUE_ID     = os.getenv("LOCKER_QUEUE_ID",     "main-hall").strip() or "main-hall"

UNLOCK_WINDOW_SECONDS     = 15
DOOR_CLOSE_WINDOW_SECONDS = 30
HEARTBEAT_SECONDS         = 60
CARD_RETRY_SECONDS        = 5
MQTT_RETRY_SECONDS        = 30
MQTT_STALE_SECS           = int(os.getenv("LOCKER_MQTT_STALE_SECS",    "120"))
USER_SYNC_SECONDS         = int(os.getenv("LOCKER_USER_SYNC_SECONDS",  "300"))
HTTP_TIMEOUT_SECONDS      = int(os.getenv("LOCKER_HTTP_TIMEOUT_SECS",  "10"))


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


LIGHT_ACTIVE_LOW         = _env_flag("LOCKER_LIGHT_ACTIVE_LOW", False)
LATCH_ACTIVE_LOW         = _env_flag("LOCKER_LATCH_ACTIVE_LOW", False)
SENSOR_CLOSED_ACTIVE_LOW = _env_flag("LOCKER_SENSOR_CLOSED_ACTIVE_LOW", False)


# ─────────────────────────────────────────────────────────────────────────────
# HARDWARE CONFIGURATION  (CH423 boards)
# ─────────────────────────────────────────────────────────────────────────────
def _build_hardware_dict() -> dict:
    hw = {}
    # Pi 3B has one hardware I2C bus (bus 1).  All three CH423 boards share
    # that bus and are differentiated by their hardware-strapped I2C address.
    board_bus     = {0: 1,    1: 1,    2: 1}       # all on bus 1
    board_address = {0: 0x20, 1: 0x21, 2: 0x22}    # ADDR[1:0] strapping
    for board_idx in range(3):
        for slot in range(8):
            n = board_idx * 8 + slot + 1
            hw[f"Locker {n}"] = {
                "Bus":        board_bus[board_idx],
                "Address":    board_address[board_idx],
                # Even GPO pins → latches,  odd GPO pins → lights
                "Latch Pin":  f"eGPO{slot * 2}",
                "Light Pin":  f"eGPO{slot * 2 + 1}",
                "Sensor Pin": f"eGPIO{slot}",
            }
    return hw

LOCKER_HARDWARE: dict = _build_hardware_dict()


# ─────────────────────────────────────────────────────────────────────────────
# CH423 BOARD WRAPPER
# ─────────────────────────────────────────────────────────────────────────────
class CH423Board(df_module.DFRobot_CH423):
    def __init__(self, bus_num: int, address: int):
        self._args      = 0
        self._mode      = [0] * 8
        self._cbs       = [0] * 8
        self._int_value = 0
        self._gpo0_7    = 0
        self._gpo8_15   = 0
        self._bus_num   = bus_num
        # Both attribute names used across DFRobot library versions:
        self._addr      = address
        self.I2C_ADDR   = address
        self._bus       = smbus.SMBus(bus_num)

def _pin_int(name: str) -> int:
    return getattr(df_module.DFRobot_CH423, name)


def ensure_runtime_dependencies():
    if MISSING_RUNTIME_DEPS:
        names = ", ".join(MISSING_RUNTIME_DEPS)
        raise RuntimeError(
            "Missing runtime dependencies: "
            f"{names}. Install the Raspberry Pi packages and Python modules before running locker_main.py."
        )


def _output_level(active: bool, active_low: bool) -> int:
    return 0 if (active and active_low) else 1 if (active or active_low) else 0


# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────
_boards:      dict           = {}
_boards_lock: threading.Lock = threading.Lock()

mqtt_conn:            object          = None
mqtt_ready:           threading.Event = threading.Event()
mqtt_publish_lock:    threading.Lock  = threading.Lock()
mqtt_last_ready_time: float           = 0.0

# All 24 lockers start inactive and unoccupied; Lambda/HTTP sync activates them.
locker_status_dict: dict = {
    f"Locker {i}": {
        "Admin Unlocked": False,
        "Door Shut":      True,
        "Active":         False,
        "Occupied":       False,
        "Occupant": {"Name": None, "Card ID": None, "Email": None, "Entry Date": None},
    }
    for i in range(1, 25)
}
locker_status_lock = threading.Lock()

registered_user_dict: dict = {}
user_dict_lock = threading.Lock()

locker_cache: list = []
cache_lock = threading.Lock()

locker_op_locks: dict   = {f"Locker {i}": threading.Lock() for i in range(1, 25)}
open_door_lockers: set  = set()
open_door_lock          = threading.Lock()

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
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def sanitize(value) -> str:
    return str(value or "").strip()

def normalize_swipe_id(value) -> str:
    text   = sanitize(value)
    digits = "".join(c for c in text if c.isdigit())
    if len(digits) == 16:
        return digits
    if len(text) == 18 and len(digits) >= 16:
        return digits[-16:]
    return text

def cloud_locker_id(local_id: str) -> str:
    """'Locker 3'  →  '3'  (matches Lambda's DynamoDB key format)."""
    match = re.fullmatch(r"(?:locker[\s_-]*)?0*(\d+)", sanitize(local_id), flags=re.IGNORECASE)
    return str(int(match.group(1))) if match else sanitize(local_id)

def local_locker_id(cloud_id: str) -> str:
    """'3' or 'Locker_3'  →  'Locker 3'  (local dict key)."""
    match = re.fullmatch(r"(?:locker[\s_-]*)?0*(\d+)", sanitize(cloud_id), flags=re.IGNORECASE)
    if match:
        candidate = f"Locker {int(match.group(1))}"
        if candidate in locker_status_dict:
            return candidate
    return sanitize(cloud_id)


# ─────────────────────────────────────────────────────────────────────────────
# BOARD MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
def get_board(locker_id: str) -> "CH423Board | None":
    hw = LOCKER_HARDWARE.get(locker_id)
    if hw is None:
        return None
    bus_num   = hw["Bus"]
    address   = hw["Address"]
    board_key = (bus_num, address)          # unique per physical board
    with _boards_lock:
        if board_key not in _boards:
            print(f"[HW] Initializing CH423 on I2C bus {bus_num} "
                  f"addr 0x{address:02X} for {locker_id}...")
            try:
                board = CH423Board(bus_num, address)
                board.begin(
                    gpio_mode=df_module.DFRobot_CH423.eINPUT,
                    gpo_mode=df_module.DFRobot_CH423.ePUSH_PULL,
                )
                # Drive all 16 GPO pins LOW and configure all 8 GPIO pins as
                # inputs before any locker state is applied.  This prevents
                # latches from firing on power-up due to undefined pin state.
                for i in range(16):
                    board.gpo_digital_write(_pin_int(f"eGPO{i}"), 0)
                _boards[board_key] = board
                print(f"[HW] CH423 bus {bus_num} addr 0x{address:02X} ready")
            except Exception as exc:
                print(f"[HW] Failed to init CH423 bus {bus_num} "
                      f"addr 0x{address:02X}: {exc}")
                return None
        return _boards[board_key]


# ─────────────────────────────────────────────────────────────────────────────
# LOCKER CONTROL FUNCTIONS  (unchanged from teammate's code)
# ─────────────────────────────────────────────────────────────────────────────
def unlock_locker(locker_id: str):
    board = get_board(locker_id)
    if board:
        pin = _pin_int(LOCKER_HARDWARE[locker_id]["Latch Pin"])
        try:
            board.gpo_digital_write(pin, _output_level(True, LATCH_ACTIVE_LOW))
        except Exception as exc:
            print(f"[HW] Error unlocking {locker_id}: {exc}")

def lock_locker(locker_id: str):
    board = get_board(locker_id)
    if board:
        pin = _pin_int(LOCKER_HARDWARE[locker_id]["Latch Pin"])
        try:
            board.gpo_digital_write(pin, _output_level(False, LATCH_ACTIVE_LOW))
        except Exception as exc:
            print(f"[HW] Error locking {locker_id}: {exc}")

def _set_light(locker_id: str, on: bool):
    board = get_board(locker_id)
    if board:
        pin = _pin_int(LOCKER_HARDWARE[locker_id]["Light Pin"])
        try:
            board.gpo_digital_write(pin, _output_level(on, LIGHT_ACTIVE_LOW))
        except Exception:
            pass

def blink_light(locker_id: str, elapsed: float):
    _set_light(locker_id, int(elapsed) % 2 == 0)

def is_door_closed(locker_id: str) -> bool:
    board = get_board(locker_id)
    if board is None:
        return True
    pin = _pin_int(LOCKER_HARDWARE[locker_id]["Sensor Pin"])
    try:
        level = board.gpio_digital_read(pin)
        return level == 0 if SENSOR_CLOSED_ACTIVE_LOW else level == 1
    except Exception:
        return True

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
    if admin_unlocked:
        unlock_locker(locker_id)
    else:
        lock_locker(locker_id)
    _set_light(locker_id, occupied or admin_unlocked)

def initialize_hardware_state(locker_id: str):
    """
    Startup-only alternative to apply_hardware_state().
    Latches are ALWAYS driven LOW (locked) regardless of any Admin Unlocked
    flag left in the database from a previous session.  Lights reflect the
    current occupancy so a locker that was assigned before the Pi rebooted
    still shows its indicator.
    """
    with locker_status_lock:
        status   = locker_status_dict.get(locker_id, {})
        active   = status.get("Active", False)
        occupied = status.get("Occupied", False)
        # Clear any stale Admin Unlocked state — it must be re-sent by the
        # dashboard after boot, not blindly re-activated from the DB.
        status["Admin Unlocked"] = False
    if not active:
        _set_light(locker_id, False)
        lock_locker(locker_id)
        return
    lock_locker(locker_id)             # always start locked
    _set_light(locker_id, occupied)    # light on only if locker is in use


# ─────────────────────────────────────────────────────────────────────────────
# USER / LOCKER STATE CHECKS
# ─────────────────────────────────────────────────────────────────────────────
def is_card_registered(card_id: str) -> "str | bool":
    norm = normalize_swipe_id(card_id)
    with user_dict_lock:
        for name, info in registered_user_dict.items():
            if normalize_swipe_id(info.get("Card ID", "")) == norm:
                return name
    return False

def is_card_assigned(card_id: str) -> "str | bool":
    norm = normalize_swipe_id(card_id)
    with locker_status_lock:
        for lid, status in locker_status_dict.items():
            if (status.get("Active") and status.get("Occupied") and
                    normalize_swipe_id(status.get("Occupant", {}).get("Card ID", "")) == norm):
                return lid
    return False

def is_locker_available() -> "str | bool":
    with cache_lock:
        if locker_cache:
            return locker_cache[0]["Locker ID"]
    print("[CACHE] Cache empty — rebuilding from locker state")
    cache_initialize()
    with cache_lock:
        if locker_cache:
            return locker_cache[0]["Locker ID"]
    return False


# ─────────────────────────────────────────────────────────────────────────────
# CACHE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────
def cache_initialize():
    with locker_status_lock:
        available = [
            lid for lid, s in locker_status_dict.items()
            if s.get("Active") and not s.get("Occupied")
        ]
    with cache_lock:
        locker_cache.clear()
        for lid in available:
            locker_cache.append({
                "Locker ID":             lid,
                "Previous User":         None,
                "Previous User Card ID": None,
                "Previous User Email":   None,
            })
    print(f"[CACHE] Initialized with {len(locker_cache)} available locker(s)")

def cache_add(locker_id: str, prev_user, prev_card_id, prev_email):
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["Locker ID"] != locker_id]
        locker_cache.append({
            "Locker ID":             locker_id,
            "Previous User":         prev_user,
            "Previous User Card ID": prev_card_id,
            "Previous User Email":   prev_email,
        })
    print(f"[CACHE] {locker_id} added to available cache")

def cache_remove(locker_id: str):
    with cache_lock:
        locker_cache[:] = [e for e in locker_cache if e["Locker ID"] != locker_id]
    print(f"[CACHE] {locker_id} removed from available cache")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP SYNC  — GET /pi/sync
# ─────────────────────────────────────────────────────────────────────────────
def http_sync(reason: str = "periodic") -> bool:
    """
    Pulls the full user list and locker states from Lambda.
    Called on boot and periodically as a safety net.
    """
    if not API_BASE_URL:
        return False

    headers = {"Accept": "application/json"}
    if DEVICE_TOKEN:
        headers["x-device-token"] = DEVICE_TOKEN

    try:
        req = request.Request(f"{API_BASE_URL}/pi/sync", headers=headers, method="GET")
        with request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.URLError as exc:
        print(f"[HTTP] Sync network error ({reason}): {exc.reason}")
        return False
    except Exception as exc:
        print(f"[HTTP] Sync failed ({reason}): {exc}")
        return False

    # ── Install users ──────────────────────────────────────────────────────
    users_loaded = 0
    with user_dict_lock:
        for user in payload.get("authorizedUsers", []):
            if sanitize(user.get("status", "ACTIVE")).upper() != "ACTIVE":
                continue
            swipe = normalize_swipe_id(user.get("swipeId") or user.get("cardId", ""))
            name  = sanitize(user.get("fullName") or user.get("name", ""))
            if not swipe or not name:
                continue
            registered_user_dict[name] = {
                "Card ID":           swipe,
                "Email":             sanitize(user.get("email", "")).lower(),
                "Registration Date": sanitize(user.get("updatedAt") or user.get("createdAt", "")),
            }
            users_loaded += 1

    # ── Install locker states ──────────────────────────────────────────────
    lockers_loaded = 0
    for locker in payload.get("lockers", []):
        lid = local_locker_id(sanitize(locker.get("lockerId", "")))
        if lid not in locker_status_dict:
            continue
        # Lambda state: 1=available, 2=assigned, 3=opening
        state         = int(locker.get("state", 1) or 1)
        occupied      = state >= 2
        assigned_user = sanitize(locker.get("assignedUser", ""))

        with locker_status_lock:
            status       = locker_status_dict[lid]
            was_active   = status.get("Active", False)
            was_occupied = status.get("Occupied", False)
            status["Active"] = True

            if occupied and not was_occupied:
                status["Occupied"] = True
                status["Occupant"] = {
                    "Name":       assigned_user,
                    "Card ID":    None,
                    "Email":      None,
                    "Entry Date": sanitize(locker.get("lastUpdated", now_str())),
                }
            elif not occupied and was_occupied:
                prev = status["Occupant"]
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}
                cache_add(lid, prev.get("Name"), prev.get("Card ID"), prev.get("Email"))
            elif not was_active and not occupied:
                cache_add(lid, None, None, None)

        apply_hardware_state(lid)
        lockers_loaded += 1

    print(f"[HTTP] Sync OK ({reason}): {users_loaded} users, {lockers_loaded} lockers")
    return True


def cloud_sync_loop():
    """Background thread — re-syncs via HTTP every USER_SYNC_SECONDS as a fallback."""
    while True:
        time.sleep(USER_SYNC_SECONDS)
        try:
            http_sync(reason="background")
        except Exception as exc:
            print(f"[HTTP] Background sync error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# MQTT PUBLISH
# ─────────────────────────────────────────────────────────────────────────────
def _mqtt_publish(topic: str, payload: dict):
    if mqtt_conn is None or not mqtt_ready.is_set():
        print(f"[MQTT] Not ready — dropped publish to {topic}")
        return
    try:
        with mqtt_publish_lock:
            mqtt_conn.publish(
                topic=topic,
                payload=json.dumps(payload),
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )
        print(f"[MQTT] -> {topic} | {payload.get('eventType', '?')}")
    except Exception as exc:
        print(f"[MQTT] Publish error on {topic}: {exc}")

def publish_event(locker_id: str, event_type: str, card_id: str = "", extra: dict = None):
    """Publish a locker status event on  lockers/{number}/status."""
    cid = cloud_locker_id(locker_id)
    with locker_status_lock:
        status   = locker_status_dict.get(locker_id, {})
        occupied = status.get("Occupied", False)

    payload = {
        "lockerId":   cid,
        "state":      2 if occupied else 1,
        "eventType":  event_type,
        "userId":     card_id,
        "timestamp":  utc_now_iso(),
        "piClientId": CLIENT_ID,
        "queueId":    QUEUE_ID,
        "source":     "PI",
    }
    if extra:
        payload.update(extra)
    _mqtt_publish(f"lockers/{cid}/status", payload)

def publish_registration_required(swipe_id: str):
    """
    Unknown card swiped — notify Lambda so it appears as a pending swipe
    on the staff dashboard and the student can self-register via QR code.
    """
    _mqtt_publish("lockers/SCANNER/status", {
        "lockerId":   "SCANNER",
        "state":      0,
        "eventType":  "REGISTRATION_REQUIRED",
        "swipeId":    swipe_id,
        "queueId":    QUEUE_ID,
        "timestamp":  utc_now_iso(),
        "piClientId": CLIENT_ID,
        "source":     "CARD",
    })


# ─────────────────────────────────────────────────────────────────────────────
# MQTT COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
def _handle_unlock_command(msg: dict):
    """
    Lambda dashboard sends: {"action": "unlock", "lockerId": "1", "userId": "..."}
    Legacy/direct tools may send: {"eventType": "ADMIN_UNLOCK", "lockerId": "Locker 1"}
    Both are handled here.
    """
    locker_id = local_locker_id(sanitize(msg.get("lockerId", "")))
    actor     = sanitize(msg.get("userId") or msg.get("requestedBy") or "ADMIN")
    action    = sanitize(msg.get("action", "unlock")).lower()

    with locker_status_lock:
        if locker_id not in locker_status_dict or not locker_status_dict[locker_id].get("Active"):
            print(f"[CMD] Ignoring command for inactive/unknown locker: {locker_id}")
            return

    print(f"[CMD] Admin unlock: {locker_id} by {actor} (action={action})")
    threading.Thread(
        target=_admin_unlock_sequence,
        args=(locker_id, actor, action),
        daemon=True,
    ).start()

def _handle_user_update(msg: dict):
    """Real-time single-user push from Lambda after registration / admin card-link."""
    swipe    = normalize_swipe_id(msg.get("cardId") or msg.get("swipeId", ""))
    name     = sanitize(msg.get("name") or msg.get("fullName", ""))
    email    = sanitize(msg.get("email", "")).lower()
    reg_date = sanitize(msg.get("registrationDate") or msg.get("updatedAt", now_str()))

    if not swipe or not name:
        print("[SYNC] Ignoring USER_UPDATE — missing swipe or name")
        return

    print(f"[SYNC] USER_UPDATE: {name} (swipe={swipe})")
    with user_dict_lock:
        # Remove any duplicate entry for this email, keep the newest
        for existing_name, info in list(registered_user_dict.items()):
            if email and info.get("Email", "").lower() == email:
                if reg_date <= info.get("Registration Date", ""):
                    return
                del registered_user_dict[existing_name]
                break
        registered_user_dict[name] = {
            "Card ID":           swipe,
            "Email":             email,
            "Registration Date": reg_date,
        }

def _handle_locker_update(msg: dict):
    """
    LOCKER_UPDATE from lockers/db/update — used by the init helper tool
    and any future admin utility that pushes locker state directly over MQTT.
    """
    locker_id = local_locker_id(sanitize(msg.get("lockerId", "")))
    if locker_id not in locker_status_dict:
        return

    print(f"[SYNC] LOCKER_UPDATE: {locker_id}")
    with locker_status_lock:
        status       = locker_status_dict[locker_id]
        was_active   = status.get("Active", False)
        was_occupied = status.get("Occupied", False)

        if "active" in msg:
            status["Active"] = bool(msg["active"])
            if status["Active"] and not was_active and not status["Occupied"]:
                threading.Thread(target=cache_add, args=(locker_id, None, None, None), daemon=True).start()

        if "occupied" in msg:
            new_occ = bool(msg["occupied"])
            if was_occupied and not new_occ:
                prev = status["Occupant"]
                status["Occupied"] = False
                status["Occupant"] = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}
                threading.Thread(
                    target=cache_add,
                    args=(locker_id, prev.get("Name"), prev.get("Card ID"), prev.get("Email")),
                    daemon=True,
                ).start()
            elif not was_occupied and new_occ:
                threading.Thread(target=cache_remove, args=(locker_id,), daemon=True).start()
                status["Occupied"] = True
                status["Occupant"] = {
                    "Name":       msg.get("occupantName"),
                    "Card ID":    msg.get("occupantCardId"),
                    "Email":      msg.get("occupantEmail"),
                    "Entry Date": msg.get("entryDate", now_str()),
                }

    apply_hardware_state(locker_id)

def on_cloud_command(topic, payload, **kwargs):
    try:
        msg        = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
        event_type = sanitize(msg.get("eventType", msg.get("event", ""))).upper()
        action     = sanitize(msg.get("action", "")).lower()

        print(f"[MQTT] <- {topic} | event={event_type or '—'}  action={action or '—'}")

        # Dashboard remote unlock (Lambda sends action:"unlock")
        if action in ("unlock", "open_only"):
            _handle_unlock_command(msg)

        # Legacy direct-MQTT unlock commands
        elif event_type in ("ADMIN_UNLOCK", "UNLOCK", "OPEN"):
            _handle_unlock_command({**msg, "action": "unlock"})

        # Real-time user push from Lambda after registration or admin save
        elif event_type == "USER_UPDATE":
            _handle_user_update(msg)

        # Locker state push from init tool or admin utilities
        elif event_type == "LOCKER_UPDATE":
            _handle_locker_update(msg)

    except Exception as exc:
        print(f"[MQTT] Command handler error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# MQTT CONNECTION
# ─────────────────────────────────────────────────────────────────────────────
def _subscribe_all_topics(connection):
    """Subscribe to locker command topics and user/locker update feeds."""
    # Locker command topics — Lambda uses bare numeric IDs ("1", "2", ...)
    for i in range(1, 25):
        topic = f"lockers/{i}/cmd"
        future, _ = connection.subscribe(
            topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_cloud_command
        )
        future.result()

    # Broadcast channels
    for topic in ("users/db/update", "lockers/db/update", "lockers/system/cmd"):
        future, _ = connection.subscribe(
            topic=topic, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_cloud_command
        )
        future.result()

    print("[MQTT] Subscribed to all topics")

def on_connection_interrupted(connection, error, **kwargs):
    global mqtt_conn, mqtt_last_ready_time
    print(f"[MQTT] Connection interrupted: {error}")
    mqtt_ready.clear()
    # Set to None so the maintainer loop rebuilds the connection object.
    with mqtt_publish_lock:
        mqtt_conn = None

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    global mqtt_conn, mqtt_last_ready_time
    print(f"[MQTT] Connection resumed (session_present={session_present})")
    mqtt_conn = connection
    mqtt_ready.set()
    mqtt_last_ready_time = time.time()
    if not session_present:
        # AWS discarded our persistent subscriptions — re-subscribe now.
        print("[MQTT] No previous session — re-subscribing to all topics")
        try:
            _subscribe_all_topics(connection)
        except Exception as exc:
            print(f"[MQTT] Re-subscribe failed: {exc}")

def connect_mqtt():
    global mqtt_conn, mqtt_last_ready_time

    for label, path in (
        ("certificate", CERT_PATH),
        ("private key", KEY_PATH),
        ("root CA",     CA_PATH),
    ):
        if not Path(path).exists():
            raise FileNotFoundError(f"Missing {label}: {path}")

    print(f"[MQTT] Connecting to {AWS_ENDPOINT}...")
    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_ENDPOINT,
        cert_filepath=CERT_PATH,
        pri_key_filepath=KEY_PATH,
        ca_filepath=CA_PATH,
        client_id=CLIENT_ID,
        clean_session=True,     # prevents stale-session UNEXPECTED_HANGUP errors
        keep_alive_secs=300,    # 5-min keep-alive survives normal network hiccups
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
    )
    connection.connect().result()
    mqtt_conn = connection
    mqtt_ready.set()
    mqtt_last_ready_time = time.time()
    print("[MQTT] Connected")

    _subscribe_all_topics(connection)

    # STARTUP tells Lambda to push all active users to this Pi immediately via MQTT.
    _mqtt_publish("lockers/SYSTEM/status", {
        "eventType":  "STARTUP",
        "piClientId": CLIENT_ID,
        "queueId":    QUEUE_ID,
        "timestamp":  utc_now_iso(),
    })

def mqtt_maintainer_loop():
    """
    Handles two recovery cases:
      1. mqtt_conn is None  — first connect, or interrupted callback cleared it.
      2. mqtt_conn set but offline for > MQTT_STALE_SECS — SDK reconnect is stuck.
    """
    global mqtt_conn
    while True:
        time.sleep(MQTT_RETRY_SECONDS)

        with mqtt_publish_lock:
            conn = mqtt_conn

        if conn is None:
            try:
                connect_mqtt()
            except Exception as exc:
                mqtt_ready.clear()
                print(f"[MQTT] Reconnect failed: {exc}")
            continue

        if not mqtt_ready.is_set():
            stale = time.time() - mqtt_last_ready_time
            if stale >= MQTT_STALE_SECS:
                print(f"[MQTT] Stale for {stale:.0f}s — forcing reconnect")
                try:
                    conn.disconnect()
                except Exception:
                    pass
                with mqtt_publish_lock:
                    mqtt_conn = None

def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        _mqtt_publish("lockers/SYSTEM/status", {
            "eventType":  "HEARTBEAT",
            "piClientId": CLIENT_ID,
            "timestamp":  utc_now_iso(),
        })


# ─────────────────────────────────────────────────────────────────────────────
# OPEN-DOOR MONITOR
# ─────────────────────────────────────────────────────────────────────────────
def open_door_monitor():
    while True:
        time.sleep(0.5)
        with open_door_lock:
            snapshot = set(open_door_lockers)
        for lid in snapshot:
            if is_door_closed(lid):
                print(f"[MONITOR] {lid} door closed (was left open)")
                with locker_status_lock:
                    locker_status_dict[lid]["Door Shut"] = True
                with open_door_lock:
                    open_door_lockers.discard(lid)
                apply_hardware_state(lid)
                publish_event(lid, "DOOR_CLOSED_IDLE")


# ─────────────────────────────────────────────────────────────────────────────
# CARD READER
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
    if not Path(HID_DEVICE).exists():
        print(f"[CARD] Waiting for USB reader at {HID_DEVICE}...")
    while not Path(HID_DEVICE).exists():
        time.sleep(CARD_RETRY_SECONDS)

def read_card_reader() -> "str | None":
    if not Path(HID_DEVICE).exists():
        time.sleep(CARD_RETRY_SECONDS)
        return None
    try:
        device, raw = InputDevice(HID_DEVICE), ""
        for event in device.read_loop():
            if event.type != ecodes.EV_KEY:
                continue
            data = categorize(event)
            if data.keystate != 1:
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
    normalized = normalize_swipe_id(raw)
    if not normalized.isdigit() or len(normalized) != 16:
        print(f"[CARD] Invalid card: '{raw}'")
        return None
    return normalized


# ─────────────────────────────────────────────────────────────────────────────
# UNLOCK SEQUENCES
# ─────────────────────────────────────────────────────────────────────────────
def unlock_sequence(locker_id: str, card_id: str, user_name: str, was_occupied: bool) -> bool:
    op_lock = locker_op_locks[locker_id]
    if not op_lock.acquire(blocking=False):
        print(f"[SYS] {locker_id} busy — ignoring swipe")
        return False

    try:
        print(f"[SYS] Unlock sequence: {locker_id} | {user_name} | releasing={was_occupied}")
        unlock_locker(locker_id)
        publish_event(locker_id, "UNLOCK_STARTED", card_id)

        start, door_opened = time.time(), False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            blink_light(locker_id, time.time() - start)
            if not is_door_closed(locker_id):
                door_opened = True
                print(f"[SYS] {locker_id} door opened")
                break
            time.sleep(0.05)

        if not door_opened:
            print(f"[SYS] {locker_id} timeout — door not opened")
            lock_locker(locker_id)
            apply_hardware_state(locker_id)
            publish_event(locker_id, "TIMEOUT", card_id)
            return False

        publish_event(locker_id, "DOOR_OPENED", card_id)

        if was_occupied:
            with locker_status_lock:
                status     = locker_status_dict[locker_id]
                prev_email = status["Occupant"].get("Email")
                status["Occupied"]       = False
                status["Admin Unlocked"] = False
                status["Occupant"]       = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}
            with user_dict_lock:
                user_email = registered_user_dict.get(user_name, {}).get("Email", prev_email)
            cache_add(locker_id, user_name, card_id, user_email)
            _set_light(locker_id, False)
        else:
            with user_dict_lock:
                user_email = registered_user_dict.get(user_name, {}).get("Email")
            with locker_status_lock:
                status = locker_status_dict[locker_id]
                status["Occupied"]       = True
                status["Admin Unlocked"] = False
                status["Occupant"]       = {
                    "Name":       user_name,
                    "Card ID":    card_id,
                    "Email":      user_email,
                    "Entry Date": now_str(),
                }
            cache_remove(locker_id)
            _set_light(locker_id, True)

        close_start, door_closed = time.time(), False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if is_door_closed(locker_id):
                door_closed = True
                print(f"[SYS] {locker_id} door closed")
                break
            time.sleep(0.05)

        lock_locker(locker_id)
        with locker_status_lock:
            locker_status_dict[locker_id]["Door Shut"] = door_closed
            if not door_closed:
                with open_door_lock:
                    open_door_lockers.add(locker_id)

        event_type = "RELEASED" if was_occupied else ("ASSIGNED" if door_closed else "DOOR_LEFT_OPEN")
        publish_event(locker_id, event_type, card_id)
        return True

    finally:
        op_lock.release()

def _admin_unlock_sequence(locker_id: str, actor: str = "ADMIN", action: str = "unlock"):
    op_lock = locker_op_locks[locker_id]
    if not op_lock.acquire(blocking=False):
        print(f"[SYS] {locker_id} busy — admin unlock dropped")
        return

    try:
        print(f"[SYS] Admin unlock: {locker_id} by {actor}")
        unlock_locker(locker_id)
        publish_event(locker_id, "UNLOCK_STARTED", actor)

        start, door_opened = time.time(), False
        while time.time() - start < UNLOCK_WINDOW_SECONDS:
            blink_light(locker_id, time.time() - start)
            if not is_door_closed(locker_id):
                door_opened = True
                print(f"[SYS] {locker_id} door opened (admin)")
                break
            time.sleep(0.05)

        if not door_opened:
            print(f"[SYS] Admin unlock timeout: {locker_id}")
            lock_locker(locker_id)
            apply_hardware_state(locker_id)
            publish_event(locker_id, "TIMEOUT", actor)
            return

        with locker_status_lock:
            status   = locker_status_dict[locker_id]
            occupied = status.get("Occupied", False)
            prev     = dict(status["Occupant"])
            if occupied and action == "unlock":
                status["Occupied"]       = False
                status["Admin Unlocked"] = False
                status["Occupant"]       = {"Name": None, "Card ID": None, "Email": None, "Entry Date": None}

        if occupied and action == "unlock":
            cache_add(locker_id, prev.get("Name"), prev.get("Card ID"), prev.get("Email"))
            _set_light(locker_id, False)
            publish_event(locker_id, "RELEASED", prev.get("Card ID") or actor)

        close_start, door_closed = time.time(), False
        while time.time() - close_start < DOOR_CLOSE_WINDOW_SECONDS:
            if is_door_closed(locker_id):
                door_closed = True
                print(f"[SYS] {locker_id} door closed (admin)")
                break
            time.sleep(0.05)

        lock_locker(locker_id)
        with locker_status_lock:
            locker_status_dict[locker_id]["Door Shut"] = door_closed
            if not door_closed:
                with open_door_lock:
                    open_door_lockers.add(locker_id)

        publish_event(locker_id, "DOOR_CLOSED" if door_closed else "DOOR_LEFT_OPEN", actor)

    finally:
        op_lock.release()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def check_locker_status_updates():
    """Fire any pending admin unlocks that arrived via MQTT while the loop was busy."""
    with locker_status_lock:
        pending = [
            lid for lid, s in locker_status_dict.items()
            if s.get("Active") and s.get("Admin Unlocked")
        ]
        for lid in pending:
            locker_status_dict[lid]["Admin Unlocked"] = False
    for lid in pending:
        threading.Thread(target=_admin_unlock_sequence, args=(lid,), daemon=True).start()

def handle_card_swipe(card_id: str):
    print(f"[SCAN] Card swiped: {card_id}")
    user_name = is_card_registered(card_id)

    if not user_name:
        print("[SCAN] Unregistered card — notifying dashboard")
        publish_registration_required(card_id)
        return

    print(f"[SCAN] Authorized: {user_name}")
    assigned = is_card_assigned(card_id)

    if assigned:
        print(f"[SCAN] Releasing {assigned}")
        unlock_sequence(assigned, card_id, user_name, was_occupied=True)
    else:
        available = is_locker_available()
        if not available:
            print("[SCAN] No available lockers")
            publish_event("SYSTEM", "ACCESS_DENIED", card_id)
            return
        print(f"[SCAN] Assigning {available}")
        unlock_sequence(available, card_id, user_name, was_occupied=False)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ensure_runtime_dependencies()
    print("==========================================")
    print("   UINDY KENNEL LOCKER CONTROLLER")
    print("==========================================")
    print(f"  Endpoint  : {AWS_ENDPOINT}")
    print(f"  Client ID : {CLIENT_ID}")
    print(f"  API URL   : {API_BASE_URL or '(not set)'}")
    print(f"  Queue ID  : {QUEUE_ID}")
    print(f"  HID Device: {HID_DEVICE}")
    print(f"  Light Low : {LIGHT_ACTIVE_LOW}")
    print(f"  Latch Low : {LATCH_ACTIVE_LOW}")
    print(f"  Reed Low  : {SENSOR_CLOSED_ACTIVE_LOW}")
    print("==========================================\n")

    # 1. HTTP sync first — loads which lockers are active + full user list.
    #    If unavailable, MQTT STARTUP will trigger Lambda to push users over MQTT.
    if API_BASE_URL:
        print("[MAIN] Startup HTTP sync...")
        http_sync(reason="startup")

    # 2. Build FIFO available-locker cache from loaded state.
    cache_initialize()

    # 3. Initialize CH423 boards and apply hardware state for active lockers.
    with locker_status_lock:
        active_lids = [lid for lid, s in locker_status_dict.items() if s.get("Active")]
    for lid in active_lids:
        get_board(lid)
        initialize_hardware_state(lid)   # latches always LOW on boot
        with locker_status_lock:
            if not locker_status_dict[lid].get("Door Shut", True):
                with open_door_lock:
                    open_door_lockers.add(lid)

    # 4. Launch background threads.
    threading.Thread(target=mqtt_maintainer_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop,       daemon=True).start()
    threading.Thread(target=open_door_monitor,    daemon=True).start()
    threading.Thread(target=cloud_sync_loop,      daemon=True).start()

    print("[MAIN] Waiting for MQTT connection (up to 30s)...")
    if not mqtt_ready.wait(timeout=30):
        print("[MAIN] MQTT not ready yet — continuing (maintainer will keep retrying)")

    print("[MAIN] Ready. Waiting for card swipes...\n")

    try:
        while True:
            try:
                check_locker_status_updates()
                wait_for_card_reader()
                raw = read_card_reader()
                if not raw:
                    continue
                card_id = validate_card(raw)
                if card_id is None:
                    continue
                handle_card_swipe(card_id)
            except Exception as exc:
                print(f"[MAIN] Loop error: {exc}")
                time.sleep(1)

    except KeyboardInterrupt:
        print("\n[MAIN] Shutting down...")

    finally:
        print("[MAIN] Securing all lockers before exit...")
        with locker_status_lock:
            all_active = [lid for lid, s in locker_status_dict.items() if s.get("Active")]
        for lid in all_active:
            try:
                _set_light(lid, False)
                lock_locker(lid)
            except Exception:
                pass
        print("[MAIN] Done.")


if __name__ == "__main__":
    main()
