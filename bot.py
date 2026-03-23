"""
Truck Updater Bot
=================
Pulls load details from QuickManage (OAuth2), truck GPS from Samsara,
and posts hourly location updates to a Telegram group.
"""

import os
import asyncio
import logging
import math
import time
import requests
from datetime import datetime, timezone
from typing import Optional
from collections import Counter

from dotenv import load_dotenv
load_dotenv()

import httpx
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ─── Config ───────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_GROUP_CHAT_ID = os.getenv("TELEGRAM_GROUP_CHAT_ID")

SAMSARA_API_TOKEN = os.getenv("SAMSARA_API_TOKEN")
SAMSARA_BASE_URL = "https://api.samsara.com"

QM_CLIENT_ID = os.getenv("QM_CLIENT_ID")
QM_CLIENT_SECRET = os.getenv("QM_CLIENT_SECRET")
QM_BASE_URL = "https://api.quickmanage.com"

UPDATE_INTERVAL_SECONDS = int(os.getenv("UPDATE_INTERVAL_SECONDS", "3600"))

_ACTIVE_STATUSES = {"dispatched", "in_transit"}

# ─── Truck Watchlist (only these trucks get hourly updates) ───────────
import json

WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "watchlist.json")

def _load_watchlist() -> set:
    """Load watchlist from disk."""
    try:
        with open(WATCHLIST_FILE, "r") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def _save_watchlist(trucks: set):
    """Save watchlist to disk."""
    with open(WATCHLIST_FILE, "w") as f:
        json.dump(sorted(trucks), f)

watched_trucks: set = _load_watchlist()

# ─── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# QUICKMANAGE — OAuth2 + Trip Search
# ═══════════════════════════════════════════════════════════════════════

_token = None
_token_expiry = 0


def _qm_get_token() -> Optional[str]:
    """Get QuickManage OAuth2 token with caching."""
    global _token, _token_expiry

    if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
        logger.warning("QM_CLIENT_ID or QM_CLIENT_SECRET not set")
        return None

    if _token and time.time() < _token_expiry - 60:
        return _token

    try:
        resp = requests.post(
            f"{QM_BASE_URL}/auth/token",
            json={"client_id": QM_CLIENT_ID, "client_secret": QM_CLIENT_SECRET},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        logger.info(f"QM auth: {resp.status_code}")

        if not resp.ok:
            resp = requests.post(
                f"{QM_BASE_URL}/auth/token",
                data={"client_id": QM_CLIENT_ID, "client_secret": QM_CLIENT_SECRET},
                timeout=10,
            )

        if not resp.ok:
            logger.error(f"QM auth failed: {resp.status_code} {resp.text[:200]}")
            return None

        data = resp.json()
        _token = (
            data.get("access_token") or
            data.get("token") or
            data.get("data", {}).get("access_token")
        )
        expires_in = data.get("expires_in", 3600)
        _token_expiry = time.time() + expires_in
        logger.info(f"QM token obtained (expires in {expires_in}s)")
        return _token

    except Exception as e:
        logger.error(f"QM auth error: {e}")
        return None


def _qm_headers() -> dict:
    token = _qm_get_token()
    if not token:
        return {}
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _qm_search_trips(page_size: int = 100) -> list:
    """Search QuickManage for trips."""
    hdrs = _qm_headers()
    if not hdrs:
        return []

    endpoints = [
        ("POST", f"{QM_BASE_URL}/x/trips/search", {
            "query": "", "page": 0, "page_size": page_size,
            "filters": [{"field": "status", "operator": "in", "value": ["in_transit", "dispatched"]}]
        }),
        ("POST", f"{QM_BASE_URL}/x/trips/search", {
            "query": "", "filters": [], "page": 0, "page_size": page_size
        }),
        ("GET", f"{QM_BASE_URL}/x/trips", None),
    ]

    for method, url, payload in endpoints:
        try:
            if method == "POST":
                resp = requests.post(url, json=payload, headers=hdrs, timeout=15)
            else:
                resp = requests.get(url, headers=hdrs, timeout=15)

            logger.info(f"QM {method} {url} → {resp.status_code}")

            if not resp.ok:
                continue

            data = resp.json()
            items = (
                data.get("data", {}).get("items") or
                data.get("data", {}).get("trips") or
                data.get("items") or
                data.get("trips") or
                (data.get("data") if isinstance(data.get("data"), list) else None) or
                []
            )
            if items:
                logger.info(f"QM: found {len(items)} trips")
                return items
        except Exception as e:
            logger.error(f"QM {method} {url} failed: {e}")

    return []


# ─── Geocoding (US Census + Nominatim fallback) ──────────────────────

_geocode_cache: dict = {}


def _geocode(address: str) -> Optional[tuple]:
    if not address:
        return None

    key = address.strip().lower()
    if key in _geocode_cache:
        return _geocode_cache[key]

    try:
        resp = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params={"address": address, "benchmark": "Public_AR_Current", "format": "json"},
            timeout=5,
        )
        if resp.ok:
            matches = resp.json().get("result", {}).get("addressMatches", [])
            if matches:
                coords = matches[0]["coordinates"]
                result = (float(coords["y"]), float(coords["x"]))
                _geocode_cache[key] = result
                return result
    except Exception:
        pass

    time.sleep(1.1)
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1, "countrycodes": "us"},
            headers={"User-Agent": "TruckUpdaterBot/1.0"},
            timeout=8,
        )
        if resp.ok and resp.text.strip():
            results = resp.json()
            if results:
                result = (float(results[0]["lat"]), float(results[0]["lon"]))
                _geocode_cache[key] = result
                return result
    except Exception as e:
        logger.warning(f"Geocode failed for '{address}': {e}")

    return None


def _stop_coords(stop: dict) -> Optional[tuple]:
    addr = stop.get("address") or {}
    line1 = addr.get("address_line_1", "").strip()
    city = addr.get("city", "").strip()
    state = addr.get("state", "").strip()
    zip_code = addr.get("zip_code", "").strip()
    if not city or not state:
        return None
    query = f"{line1}, {city}, {state} {zip_code}".strip(", ")
    return _geocode(query)


def _stop_city_state(stop: dict) -> str:
    addr = stop.get("address") or {}
    city = addr.get("city", "").strip()
    state = addr.get("state", "").strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"


# ─── Parse trips into loads ───────────────────────────────────────────

def get_active_loads() -> list:
    """Fetch active trips from QM, return load objects."""
    trips = _qm_search_trips()
    active = [t for t in trips if t.get("status", "").lower() in _ACTIVE_STATUSES]

    status_counts = Counter(t.get("status", "unknown") for t in trips)
    logger.info(f"QM: {len(trips)} total — statuses: {dict(status_counts)}")
    logger.info(f"QM: {len(active)} active (dispatched/in_transit)")

    loads = []
    for trip in active:
        stops = trip.get("stops") or []

        # Find truck number from stops → assigned_truck → number
        truck_number = None
        for stop in stops:
            truck = stop.get("assigned_truck") or {}
            num = str(truck.get("number", "")).strip()
            if num and truck.get("id") != "00000000-0000-0000-0000-000000000000":
                truck_number = num
                break

        if not truck_number:
            continue

        status = trip.get("status", "").lower()

        # Destination depends on status
        if status == "dispatched":
            dest_stop = next((s for s in stops if s.get("pickup")), None)
        else:
            passed_first = False
            dest_stop = None
            for s in stops:
                if s.get("pickup") and not passed_first:
                    passed_first = True
                    continue
                dest_stop = s
                break
            if not dest_stop:
                dest_stop = stops[-1] if stops else None

        dest_coords = _stop_coords(dest_stop) if dest_stop else None
        pickup_stop = next((s for s in stops if s.get("pickup")), stops[0] if stops else None)

        load = {
            "load_number": trip.get("ref_number") or trip.get("trip_num") or trip.get("id", "N/A"),
            "ref_number": trip.get("ref_number", ""),
            "truck_number": truck_number,
            "status": trip.get("status", ""),
            "pickup_location": _stop_city_state(pickup_stop) if pickup_stop else "Unknown",
            "destination_location": _stop_city_state(dest_stop) if dest_stop else "Unknown",
            "destination_lat": dest_coords[0] if dest_coords else None,
            "destination_lng": dest_coords[1] if dest_coords else None,
            "delivery_time": dest_stop.get("appointment_date", "") if dest_stop else "",
        }

        loads.append(load)
        logger.info(
            f"  Load {load['load_number']} | Truck {truck_number} | "
            f"{load['pickup_location']} → {load['destination_location']} [{status}]"
        )

    return loads


# ═══════════════════════════════════════════════════════════════════════
# SAMSARA — Truck GPS + Fuel
# ═══════════════════════════════════════════════════════════════════════

async def get_samsara_vehicles(client: httpx.AsyncClient) -> list:
    """Fetch vehicles using /fleet/vehicles/locations (same as FleetFuel)."""
    headers = {"Authorization": f"Bearer {SAMSARA_API_TOKEN}"}
    resp = await client.get(
        f"{SAMSARA_BASE_URL}/fleet/vehicles/locations",
        headers=headers,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    logger.info(f"Samsara: {len(data)} vehicles fetched")
    return data


async def get_samsara_fuel_levels(client: httpx.AsyncClient) -> dict:
    """Fetch fuel levels via /fleet/vehicles/stats/feed → returns {vehicle_id: fuel_pct}."""
    headers = {"Authorization": f"Bearer {SAMSARA_API_TOKEN}"}
    try:
        resp = await client.get(
            f"{SAMSARA_BASE_URL}/fleet/vehicles/stats/feed",
            headers=headers,
            params={"types": "fuelPercents"},
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])

        fuel_map = {}
        for v in data:
            vid = v.get("id")
            if not vid:
                continue
            fuel_events = v.get("fuelPercents", [])
            if fuel_events:
                latest = max(fuel_events, key=lambda x: x.get("time", ""))
                val = latest.get("value")
                if val is not None:
                    fval = float(val)
                    fuel_map[vid] = round(fval * 100, 1) if fval <= 1.0 else round(fval, 1)

        logger.info(f"Samsara: fuel data for {len(fuel_map)} vehicles")
        return fuel_map
    except Exception as e:
        logger.warning(f"Samsara fuel fetch failed: {e}")
        return {}


def find_truck_in_vehicles(vehicles: list, truck_number: str, fuel_map: dict = None) -> Optional[dict]:
    """Find a truck in the Samsara vehicle list. Uses v.location.latitude structure."""
    for v in vehicles:
        v_name = v.get("name", "").strip()
        if v_name.lower() == truck_number.lower() or truck_number.lower() in v_name.lower():
            loc = v.get("location", {})
            lat = loc.get("latitude")
            lng = loc.get("longitude")

            if lat is None or lng is None:
                logger.warning(f"Truck {truck_number} ({v_name}): no GPS coords")
                return None

            address = loc.get("reverseGeo", {}).get("formattedLocation", "Unknown")
            speed = float(loc.get("speed", 0) or 0)

            # Get fuel level from fuel_map by vehicle id
            vid = v.get("id")
            fuel_pct = fuel_map.get(vid) if fuel_map and vid else None

            return {
                "lat": float(lat),
                "lng": float(lng),
                "address": address,
                "speed_mph": round(speed, 1),
                "fuel_pct": fuel_pct,
            }

    return None


# ═══════════════════════════════════════════════════════════════════════
# DISTANCE + STATUS
# ═══════════════════════════════════════════════════════════════════════

def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return 0.0
    R = 3958.8
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(d_lon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def estimate_road_miles(straight_miles: float) -> int:
    return round(straight_miles * 1.3)


def determine_status(speed_mph: float, miles_left: int, qm_status: str) -> str:
    qm_lower = qm_status.lower()
    if "delivered" in qm_lower:
        return "✅ Delivered"
    elif "dispatched" in qm_lower:
        return "📦 Dispatched — heading to pickup"
    elif miles_left <= 50:
        return "📍 Almost There"
    elif speed_mph < 3:
        return "🅿️ Stopped"
    elif speed_mph < 15:
        return "🐢 Slow Traffic"
    else:
        return "🚛 Rolling"


# ═══════════════════════════════════════════════════════════════════════
# FORMAT MESSAGE
# ═══════════════════════════════════════════════════════════════════════

def _format_delivery_time(raw: str) -> str:
    """Format QM appointment_date into readable string."""
    if not raw:
        return "TBD"
    try:
        from datetime import datetime
        # Try common formats from QuickManage
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw.replace("+00:00", "Z").rstrip("Z"), fmt.rstrip("Z").rstrip("%z"))
                return dt.strftime("%m/%d/%Y %I:%M %p")
            except ValueError:
                continue
        return raw  # return as-is if no format matches
    except Exception:
        return raw


def format_update(load: dict, location: dict, miles_left: int, status: str) -> str:
    delivery = _format_delivery_time(load.get("delivery_time", ""))
    fuel = location.get("fuel_pct")
    fuel_str = f"{fuel}%" if fuel is not None else "N/A"
    return (
        f"<b>Update of the load # {load['load_number']} ✅\n"
        f"Truck: {load['truck_number']} 🚛\n"
        f"The truck is rolling to: 📌 {load['destination_location']}\n"
        f"Current location: 📍 {location['address']}\n"
        f"Miles left: 🚩 {miles_left}\n"
        f"Delivery time: 🕐 {delivery}\n"
        f"Fuel level: ⛽️ {fuel_str}\n"
        f"We will keep you updated.</b>"
    )


# ═══════════════════════════════════════════════════════════════════════
# MAIN UPDATE CYCLE
# ═══════════════════════════════════════════════════════════════════════

async def send_all_updates(bot: Bot):
    logger.info("⏰ Starting update cycle...")

    if not watched_trucks:
        logger.info("Watchlist is empty — no updates to send. Use /add <truck#> to add trucks.")
        return

    try:
        loads = get_active_loads()
        logger.info(f"Got {len(loads)} active loads with trucks")
    except Exception as e:
        logger.error(f"QuickManage error: {e}")
        return

    if not loads:
        logger.info("No active loads to update")
        return

    # Filter to only watched trucks
    loads = [l for l in loads if l["truck_number"] in watched_trucks]
    logger.info(f"Filtered to {len(loads)} loads matching watchlist: {sorted(watched_trucks)}")

    if not loads:
        logger.info("No watched trucks have active loads")
        return

    async with httpx.AsyncClient(timeout=30) as client:
        # Fetch ALL Samsara vehicles ONCE
        try:
            vehicles = await get_samsara_vehicles(client)
            logger.info(f"Samsara: fetched {len(vehicles)} vehicles")
        except Exception as e:
            logger.error(f"Samsara error: {e}")
            return

        # Fetch fuel levels ONCE
        fuel_map = await get_samsara_fuel_levels(client)

        update_count = 0
        skipped = 0
        for load in loads:
            truck_num = load["truck_number"]
            try:
                location = find_truck_in_vehicles(vehicles, truck_num, fuel_map)
                if not location:
                    skipped += 1
                    continue

                if load.get("destination_lat") and load.get("destination_lng"):
                    straight = haversine_miles(
                        location["lat"], location["lng"],
                        load["destination_lat"], load["destination_lng"]
                    )
                    miles_left = estimate_road_miles(straight)
                else:
                    miles_left = 0

                status = determine_status(location["speed_mph"], miles_left, load["status"])
                msg = format_update(load, location, miles_left, status)

                await bot.send_message(
                    chat_id=TELEGRAM_GROUP_CHAT_ID,
                    text=msg,
                    parse_mode="HTML"
                )
                update_count += 1
                logger.info(f"✅ load {load['load_number']} / truck {truck_num}")
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error load {load['load_number']}: {e}")

        logger.info(f"Done. Sent {update_count}/{len(loads)} updates. Skipped {skipped} (no GPS).")


# ═══════════════════════════════════════════════════════════════════════
# SCHEDULER + COMMANDS
# ═══════════════════════════════════════════════════════════════════════

async def scheduled_update(context: ContextTypes.DEFAULT_TYPE):
    await send_all_updates(context.bot)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚛 <b>Truck Updater Bot</b>\n\n"
        "Hourly updates for watched trucks only.\n\n"
        "<b>Watchlist:</b>\n"
        "/add 8161 0470 — Add trucks\n"
        "/remove 8161 — Remove truck\n"
        "/list — Show watched trucks\n"
        "/clear — Clear all trucks\n\n"
        "<b>Updates:</b>\n"
        "/update — Send updates now\n"
        "/load <ref#> — Update specific load\n"
        "/status — Bot connection status",
        parse_mode="HTML"
    )


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add trucks to watchlist. Usage: /add 8161 0470 3042"""
    global watched_trucks
    if not context.args:
        await update.message.reply_text("Usage: /add <truck#> <truck#> ...")
        return

    added = []
    for t in context.args:
        t = t.strip()
        if t and t not in watched_trucks:
            watched_trucks.add(t)
            added.append(t)

    _save_watchlist(watched_trucks)

    if added:
        await update.message.reply_text(f"✅ Added: {', '.join(added)}\n📋 Watchlist: {', '.join(sorted(watched_trucks))}")
    else:
        await update.message.reply_text(f"Already in watchlist.\n📋 Watchlist: {', '.join(sorted(watched_trucks))}")


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove trucks from watchlist. Usage: /remove 8161 0470"""
    global watched_trucks
    if not context.args:
        await update.message.reply_text("Usage: /remove <truck#> <truck#> ...")
        return

    removed = []
    for t in context.args:
        t = t.strip()
        if t in watched_trucks:
            watched_trucks.discard(t)
            removed.append(t)

    _save_watchlist(watched_trucks)

    if removed:
        await update.message.reply_text(f"❌ Removed: {', '.join(removed)}\n📋 Watchlist: {', '.join(sorted(watched_trucks)) or 'empty'}")
    else:
        await update.message.reply_text(f"Not in watchlist.\n📋 Watchlist: {', '.join(sorted(watched_trucks)) or 'empty'}")


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current watchlist."""
    if watched_trucks:
        await update.message.reply_text(f"📋 <b>Watched trucks ({len(watched_trucks)}):</b>\n{', '.join(sorted(watched_trucks))}", parse_mode="HTML")
    else:
        await update.message.reply_text("📋 Watchlist is empty. Use /add <truck#> to add trucks.")


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear all trucks from watchlist."""
    global watched_trucks
    watched_trucks = set()
    _save_watchlist(watched_trucks)
    await update.message.reply_text("🗑 Watchlist cleared. No trucks will receive updates.")


async def cmd_update_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Sending updates...")
    await send_all_updates(context.bot)
    await update.message.reply_text("✅ Done!")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    interval_min = UPDATE_INTERVAL_SECONDS // 60
    qm_token = _qm_get_token()
    await update.message.reply_text(
        f"🤖 <b>Bot Status</b>\n\n"
        f"⏰ Updates every {interval_min} min\n"
        f"📡 Samsara: {'✅' if SAMSARA_API_TOKEN else '❌'}\n"
        f"📦 QuickManage: {'✅ Token OK' if qm_token else '❌ No token'}\n"
        f"💬 Group: {TELEGRAM_GROUP_CHAT_ID}",
        parse_mode="HTML"
    )


async def cmd_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /load <trip_number>")
        return

    target = context.args[0]
    await update.message.reply_text(f"🔍 Looking up load {target}...")

    try:
        loads = get_active_loads()
        load = next(
            (l for l in loads if str(l["load_number"]) == target or str(l.get("ref_number")) == target),
            None
        )
        if not load:
            await update.message.reply_text(f"❌ Load {target} not found")
            return

        async with httpx.AsyncClient(timeout=30) as client:
            vehicles = await get_samsara_vehicles(client)
            fuel_map = await get_samsara_fuel_levels(client)
            location = find_truck_in_vehicles(vehicles, load["truck_number"], fuel_map)
            if not location:
                await update.message.reply_text(f"❌ No GPS for truck {load['truck_number']}")
                return

            if load.get("destination_lat") and load.get("destination_lng"):
                straight = haversine_miles(
                    location["lat"], location["lng"],
                    load["destination_lat"], load["destination_lng"]
                )
                miles_left = estimate_road_miles(straight)
            else:
                miles_left = 0

            status = determine_status(location["speed_mph"], miles_left, load["status"])
            msg = format_update(load, location, miles_left, status)

            await context.bot.send_message(chat_id=TELEGRAM_GROUP_CHAT_ID, text=msg, parse_mode="HTML")
            await update.message.reply_text(f"✅ Update sent for load {target}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not set")
    if not TELEGRAM_GROUP_CHAT_ID:
        raise ValueError("TELEGRAM_GROUP_CHAT_ID not set")

    logger.info("🚛 Truck Updater Bot starting...")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("update", cmd_update_now))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("load", cmd_load))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("remove", cmd_remove))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("clear", cmd_clear))

    app.job_queue.run_repeating(
        scheduled_update,
        interval=UPDATE_INTERVAL_SECONDS,
        first=10,
        name="hourly_updates"
    )

    logger.info(f"⏰ Updates every {UPDATE_INTERVAL_SECONDS // 60} min")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import asyncio
    # Fix for Python 3.14+ event loop
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    main()
