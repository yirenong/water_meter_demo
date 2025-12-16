#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BACnet + OPC-UA Water Collector (Daily Snapshot)

- Uses your water_meters_combined_full_sorted.json mapping.
- BACnet points are read via Wacnet /multi-objects using device_id.object_id.
- OPC points (if any) are read via OPC-UA (single shared session per run).
  A point is treated as OPC if its point-level "href" in the mapping starts
  with "ns=" or "opc.tcp://".

SAFE TIMING:
- Energy script runs at HH:00:00.
- This script runs at HH:00:10 to avoid OPC-UA session collision.

USAGE:
  # Continuous daemon
  python bacnet_water_database.py

  # One-time run to (re)write today's snapshot immediately
  python bacnet_water_database.py --once
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from opcua import Client as OPCUAClient


# --------------------- CONFIG ---------------------

WACNET_BASE  = os.environ.get("WACNET_BASE", "http://localhost:47800/api/v1/bacnet")
MAPPING_JSON = os.environ.get("MAPPING_JSON", "water_meters_combined_full_sorted.json")
DB_PATH      = os.environ.get("DB_PATH", "water_meters.sqlite")

PROPERTIES_RUN = ["present-value"]

OPC_ENDPOINT = os.environ.get("OPC_ENDPOINT", "opc.tcp://172.30.153.138:49320")
OPC_USER     = os.environ.get("OPC_USER", "OPC-UA-USER")
OPC_PASS     = os.environ.get("OPC_PASS", "Metasys-OPC-UA-client")

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))

_shutdown = False


# --------------------- LOGGING ---------------------

def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)


# --------------------- OPC-UA ---------------------

def opc_read_many(nodeids: List[str]) -> Dict[str, float]:
    """
    Read many OPC-UA nodes in a single session.
    Returns {nodeid: value_float}.
    """
    out: Dict[str, float] = {}

    if not nodeids:
        return out

    try:
        client = OPCUAClient(OPC_ENDPOINT)
        client.set_user(OPC_USER)
        client.set_password(OPC_PASS)
        client.connect()

        for nid in nodeids:
            try:
                node = client.get_node(nid)
                val = node.get_value()
                out[nid] = float(val)
            except Exception as e:
                log(f"[OPC ERROR] Failed reading {nid}: {e}")

        client.disconnect()

    except Exception as e:
        log(f"[OPC ERROR] Connect failed: {e}")

    return out


# --------------------- HTTP SESSION ---------------------

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_SESSION = build_session()


# --------------------- DB ---------------------

def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_daily_pv(
          meter_name    TEXT,
          ts            TEXT,
          present_value REAL,
          PRIMARY KEY(meter_name, ts)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_daily(
          meter_name  TEXT,
          ts          TEXT,
          consumption REAL,
          PRIMARY KEY(meter_name, ts)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS block_daily(
          block       TEXT,
          ts          TEXT,
          consumption REAL,
          PRIMARY KEY(block, ts)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_last_snapshot(
          meter_name TEXT PRIMARY KEY,
          last_pv    REAL,
          last_ts    TEXT
        )
    """)

    conn.commit()


def upsert_meter(conn: sqlite3.Connection, table: str, meter: str, ts: str, value: float) -> None:
    field = "present_value" if table == "meter_daily_pv" else "consumption"
    conn.execute(f"""
        INSERT INTO {table}(meter_name, ts, {field})
        VALUES (?, ?, ?)
        ON CONFLICT(meter_name, ts) DO UPDATE SET {field} = excluded.{field}
    """, (meter, ts, value))


def upsert_block(conn: sqlite3.Connection, block: str, ts: str, value: float) -> None:
    conn.execute("""
        INSERT INTO block_daily(block, ts, consumption)
        VALUES (?, ?, ?)
        ON CONFLICT(block, ts) DO UPDATE SET consumption = excluded.consumption
    """, (block, ts, value))


def set_last_snapshot(conn: sqlite3.Connection, meter: str, pv: float, ts: str) -> None:
    conn.execute("""
        INSERT INTO meter_last_snapshot(meter_name, last_pv, last_ts)
        VALUES (?, ?, ?)
        ON CONFLICT(meter_name)
        DO UPDATE SET last_pv = excluded.last_pv, last_ts = excluded.last_ts
    """, (meter, pv, ts))


def clean_legacy_points_rows(conn: sqlite3.Connection) -> None:
    """
    Remove any legacy rows where meter_name == 'points'
    from all relevant tables.
    Called in --once startup so the bad row is wiped.
    """
    bad_name = "points"
    total_deleted = 0

    for table in ("meter_daily_pv", "meter_daily", "meter_last_snapshot"):
        cur = conn.execute(f"DELETE FROM {table} WHERE meter_name = ?", (bad_name,))
        total_deleted += cur.rowcount

    conn.commit()

    if total_deleted > 0:
        log(f"[CLEAN] Removed {total_deleted} legacy rows with meter_name='points'.")


# --------------------- HELPERS ---------------------

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def midnight_ts() -> str:
    """Today's midnight as ISO string."""
    return datetime.now().strftime("%Y-%m-%dT00:00:00")


# --------------------- MAPPING ---------------------

def load_meter_points(path: str) -> Dict[str, dict]:
    """
    Parse mapping JSON.

    Returns dict:
      key = gid
        - for BACnet: 'device_id.object_id'
        - for OPC   : 'opc::<opc_node>'
      value = {
        "meter": <meter_name>,
        "block": <block_letter>,
        # OPC only:
        "opc_node": <nodeid string>
      }
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    mapping: Dict[str, dict] = {}

    for block_key, lvls in data.items():
        block = block_key.replace("Block ", "").strip()

        for _, meters_or_levels in lvls.items():
            if isinstance(meters_or_levels, dict) and any(
                isinstance(v, dict) and "meter_info" in v for v in meters_or_levels.values()
            ):
                level_dict = meters_or_levels
                for meter_name, meter_obj in level_dict.items():
                    _add_meter_points(mapping, block, meter_name, meter_obj)
            else:
                if not isinstance(meters_or_levels, dict):
                    continue
                for _, meters in meters_or_levels.items():
                    if not isinstance(meters, dict):
                        continue
                    for meter_name, meter_obj in meters.items():
                        _add_meter_points(mapping, block, meter_name, meter_obj)

    return mapping


def _add_meter_points(mapping: Dict[str, dict], block: str,
                      meter_name: str, meter_obj: dict) -> None:
    points = meter_obj.get("points", [])
    if not points:
        return

    for p in points:
        obj_name = p.get("object_name", "") or ""
        if "Volume_1" not in obj_name:
            continue

        device_id = str(p.get("device_id", "")).strip()
        object_id = str(p.get("object_id", "")).strip()
        href      = str(p.get("href", "") or "").strip()

        # OPC detection
        if href.startswith("ns=") or href.startswith("opc.tcp://"):
            gid = f"opc::{href}"
            mapping[gid] = {
                "meter": meter_name,
                "block": block,
                "opc_node": href,
            }
        else:
            if not device_id or not object_id:
                continue
            gid = f"{device_id}.{object_id}"
            mapping[gid] = {
                "meter": meter_name,
                "block": block,
            }

        break  # one Volume_1 per meter


# --------------------- FETCH ---------------------

def fetch_multi(gid: str, props: List[str]) -> dict:
    params = []
    for p in props:
        params.append(("properties", p))
    params.append(("global-object-ids", gid))

    r = _SESSION.get(f"{WACNET_BASE}/multi-objects", params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_all_present_values(mapping: Dict[str, dict]) -> Dict[str, dict]:
    """
    Unified BACnet + OPC fetch.

    Returns:
      { meter_name: { "block": <block>, "present_value": <float> } }
    """
    results: Dict[str, dict] = {}

    # OPC nodes prep
    opc_nodes: Dict[str, dict] = {}
    for meta in mapping.values():
        opc_node = meta.get("opc_node")
        if opc_node:
            opc_nodes[opc_node] = meta

    opc_values: Dict[str, float] = {}
    if opc_nodes:
        log(f"[OPC] Reading {len(opc_nodes)} nodes in one session...")
        opc_values = opc_read_many(list(opc_nodes.keys()))

    # Fill results
    for gid, meta in mapping.items():
        meter = meta["meter"]
        block = meta["block"]
        opc_node = meta.get("opc_node")

        if opc_node:
            pv = _to_float(opc_values.get(opc_node))
            if pv is None:
                log(f"[WARN] OPC invalid PV for meter={meter} node={opc_node}")
                continue

            results[meter] = {"block": block, "present_value": pv}
            continue

        # BACnet
        try:
            payload = fetch_multi(gid, PROPERTIES_RUN)
            pv_raw = payload.get(gid, {}).get("present-value")
            pv = _to_float(pv_raw)
            if pv is None:
                log(f"[WARN] BACnet invalid PV for meter={meter} gid={gid} raw={pv_raw}")
                continue

            results[meter] = {"block": block, "present_value": pv}

        except Exception as e:
            log(f"[BACNET ERROR] meter={meter} gid={gid} err={e}")

    return results


# --------------------- DAILY WRITE ---------------------

def write_daily(conn: sqlite3.Connection, readings: Dict[str, dict], ts: str) -> None:
    block_totals: Dict[str, float] = {}

    for meter, r in readings.items():
        pv = float(r["present_value"])
        block = r["block"]

        upsert_meter(conn, "meter_daily_pv", meter, ts, pv)
        upsert_meter(conn, "meter_daily", meter, ts, pv)
        set_last_snapshot(conn, meter, pv, ts)

        block_totals[block] = block_totals.get(block, 0.0) + pv

    for block, total in block_totals.items():
        upsert_block(conn, block, ts, total)

    conn.commit()
    log(f"[WRITE] Saved {len(readings)} meters; {len(block_totals)} blocks.")


# --------------------- SAFE SCHEDULING ---------------------

def wait_until_safe_window() -> datetime:
    """
    Energy script runs at HH:00:00.
    This script runs at HH:00:10 to avoid OPC-UA session collision.
    """
    now = datetime.now()
    safe = now.replace(minute=0, second=10, microsecond=0)

    if now >= safe:
        safe += timedelta(hours=1)

    wait_s = (safe - now).total_seconds()
    log(f"[SCHED] Waiting {wait_s:.1f} seconds for safe OPC-UA window (HH:00:10)...")
    time.sleep(wait_s)

    return safe


# --------------------- MAIN MODES ---------------------

def collect_once() -> None:
    """
    One-off collection for *today's* midnight timestamp.
    Also wipes legacy meter_name == 'points' rows first.
    """
    mapping = load_meter_points(MAPPING_JSON)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # cleanup old bad row(s)
    clean_legacy_points_rows(conn)

    ts = midnight_ts()
    log(f"[ONCE] Collecting single snapshot for {ts}...")
    readings = fetch_all_present_values(mapping)

    if readings:
        write_daily(conn, readings, ts)
    else:
        log("[ONCE] No readings collected.")

    conn.close()
    log("[ONCE] Done.")


def handle_signal(signum, _frame):
    global _shutdown
    _shutdown = True
    log(f"[SIGNAL] Received {signum}; will exit after current cycle.")


def main_loop() -> None:
    global _shutdown

    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except Exception:
        pass

    mapping = load_meter_points(MAPPING_JSON)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    log("[START] Water collector with SAFE OPC timing enabled.")

    while not _shutdown:
        safe_run_time = wait_until_safe_window()

        ts = safe_run_time.strftime("%Y-%m-%dT00:00:00")
        log(f"[COLLECT] Starting daily snapshot for {ts}")

        readings = fetch_all_present_values(mapping)
        if readings:
            write_daily(conn, readings, ts)
        else:
            log("[WARN] No readings.")

    conn.close()
    log("[STOP] Water collector stopped.")


# --------------------- ENTRY ---------------------

if __name__ == "__main__":
    import sys

    if "--once" in sys.argv:
        collect_once()
    else:
        main_loop()
