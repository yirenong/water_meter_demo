#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Water Meter REST API (FastAPI) — DAILY schema + monthly/yearly (meter/block/level)

Backed by your daily collector:
  meter_daily_pv(meter_name TEXT, ts TEXT, present_value REAL, PRIMARY KEY(meter_name, ts))
  meter_daily(meter_name TEXT, ts TEXT, consumption REAL, PRIMARY KEY(meter_name, ts))
  block_daily(block TEXT, ts TEXT, consumption REAL, PRIMARY KEY(block, ts))
  meter_last_snapshot(meter_name TEXT PRIMARY KEY, last_pv REAL, last_ts TEXT)

Meta tables:
  site_meta(
    id INTEGER PRIMARY KEY CHECK (id = 1),
    population INTEGER,
    area REAL,
    pop_updated_at TEXT,
    area_updated_at TEXT
  )
  block_meta(
    block TEXT PRIMARY KEY,
    population INTEGER,
    area REAL,
    pop_updated_at TEXT,
    area_updated_at TEXT
  )

Notes
- Daily-only storage; timestamps are “YYYY-MM-DDT00:00:00” (local midnight).
- Site population & area are reported as the SUM of all blocks when any blocks exist;
  otherwise we fall back to the standalone site_meta row.

Run:
  pip install fastapi uvicorn pydantic
  python -m uvicorn water_api:app --host 0.0.0.0 --port 8080 --reload
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, Depends, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict

DB_PATH = os.environ.get("DB_PATH", "water_meters.sqlite")

# ---------- FastAPI app ----------

app = FastAPI(title="Water Meter API (Daily)", version="2.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- DB helpers ----------

def _connect() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=503, detail=f"Database not found: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_db():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()

def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None

# ---------- Time helpers (daily only) ----------

def _iso_midnight(date_str: str) -> str:
    return f"{date_str}T00:00:00"

def _parse_iso_ts(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        if len(s) == 10:  # YYYY-MM-DD
            return f"{s}T00:00:00"
        return datetime.fromisoformat(s).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {s}")

def default_daily_range() -> Tuple[str, str]:
    now = datetime.now()
    end = _iso_midnight(now.strftime("%Y-%m-%d"))
    start = _iso_midnight((now - timedelta(days=30)).strftime("%Y-%m-%d"))
    return start, end

def ensure_daily_range(start: Optional[str], end: Optional[str]) -> Tuple[str, str]:
    if not start and not end:
        return default_daily_range()
    s = _parse_iso_ts(start) if start else None
    e = _parse_iso_ts(end) if end else None
    if not s and e:
        e_dt = datetime.fromisoformat(e)
        s = _iso_midnight((e_dt - timedelta(days=30)).strftime("%Y-%m-%d"))
    if s and not e:
        e_dt = datetime.fromisoformat(s) + timedelta(days=30)
        e = _iso_midnight(e_dt.strftime("%Y-%m-%d"))
    return s, e

# ---------- NEW: Month/Year helpers ----------

def _parse_month(s: Optional[str]) -> Optional[datetime]:
    """Accepts 'YYYY-MM' -> datetime at first day midnight."""
    if not s:
        return None
    try:
        y, m = map(int, s.split("-", 1))
        return datetime(year=y, month=m, day=1)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid month (YYYY-MM): {s}")

def _parse_year(s: Optional[str]) -> Optional[datetime]:
    """Accepts 'YYYY' -> datetime at Jan-01 midnight."""
    if not s:
        return None
    try:
        y = int(s)
        return datetime(year=y, month=1, day=1)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid year (YYYY): {s}")

def _month_bounds(start: Optional[str], end: Optional[str]) -> Tuple[str, str]:
    """
    Returns [start_midnight, end_midnight) as ISO strings for monthly queries.
    Defaults: last 12 whole months ending at the current month start.
    """
    now = datetime.now()
    default_end_dt = datetime(year=now.year, month=now.month, day=1)
    # go back 12 months
    y, m = default_end_dt.year, default_end_dt.month
    m -= 12
    while m <= 0:
        m += 12
        y -= 1
    default_start_dt = datetime(year=y, month=m, day=1)

    sdt = _parse_month(start) if start else default_start_dt
    edt = _parse_month(end) if end else default_end_dt

    if sdt >= edt:
        raise HTTPException(status_code=400, detail="start must be before end for monthly range")

    return sdt.strftime("%Y-%m-%dT00:00:00"), edt.strftime("%Y-%m-%dT00:00:00")

def _year_bounds(start: Optional[str], end: Optional[str]) -> Tuple[str, str]:
    """
    Returns [start_midnight, end_midnight) as ISO strings for yearly queries.
    Defaults: last 5 whole years ending at current year start.
    """
    now = datetime.now()
    default_end_dt = datetime(year=now.year, month=1, day=1)
    default_start_dt = datetime(year=now.year - 5, month=1, day=1)

    sdt = _parse_year(start) if start else default_start_dt
    edt = _parse_year(end) if end else default_end_dt

    if sdt >= edt:
        raise HTTPException(status_code=400, detail="start must be before end for yearly range")

    return sdt.strftime("%Y-%m-%dT00:00:00"), edt.strftime("%Y-%m-%dT00:00:00")

def _month_bucket_ts(ym: str) -> str:
    # ym = 'YYYY-MM'
    return f"{ym}-01T00:00:00"

def _year_bucket_ts(y: str) -> str:
    # y = 'YYYY'
    return f"{y}-01-01T00:00:00"

# ---------- Level helpers (for block level rollups from meter_name) ----------

def _level_label(n: int) -> str:
    return f"Level {n}"

def _parse_level_str(level: str) -> int:
    """
    Accepts 'Level 7', 'L7', '7' (case-insensitive) -> 7
    """
    s = level.strip().upper()
    m = re.search(r'(\d+)', s)
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid level: {level}")
    return int(m.group(1))

def _extract_block_level_from_tag(tag: str) -> tuple[str, int] | None:
    """
    Meter tag patterns:
      - Block A (ADM/AUD): 'A-1-ADM-L7-WM-1' or 'A-2-AUD-L3-WM-...'
      - Generic blocks (B..Z): 'E-L7-WM-1'
    """
    m = re.match(r'^A-\d+-(ADM|AUD)-L(\d+)-', tag, flags=re.IGNORECASE)
    if m:
        return ('A', int(m.group(2)))
    m = re.match(r'^([B-Z])-L(\d+)-', tag, flags=re.IGNORECASE)
    if m:
        return (m.group(1).upper(), int(m.group(2)))
    return None

def _like_patterns_for_block_level(block: str, level_num: int) -> list[str]:
    if block.upper() == 'A':
        return [f"A-%-ADM-L{level_num}-%", f"A-%-AUD-L{level_num}-%"]
    else:
        return [f"{block.upper()}-L{level_num}-%"]

# ---------- Models ----------

class Point(BaseModel):
    ts: str = Field(..., description="ISO 8601 local timestamp (midnight)")
    consumption: float = 0.0  # m3

class SeriesResponse(BaseModel):
    name: str
    unit: str = "m3"
    start: str
    end: str
    count: int
    data: List[Point]

class PVPoint(BaseModel):
    ts: str
    present_value: float  # m3

class PVSeriesResponse(BaseModel):
    name: str
    unit: str = "m3"
    start: str
    end: str
    count: int
    data: List[PVPoint]

class ListResponse(BaseModel):
    items: List[str]
    count: int

class LatestCumulative(BaseModel):
    meter_name: str
    last_value: float
    last_ts: str

class DateRangeResponse(BaseModel):
    dataset: str          # 'consumption' or 'pv'
    scope: str            # 'meter' or 'block'
    name: Optional[str] = None
    table: str
    min_ts: Optional[str]
    max_ts: Optional[str]
    row_count: int

# ---- Site & Blocks meta models ----

class BlockMetaItem(BaseModel):
    block: str
    population: Optional[int] = None
    area: Optional[float] = None
    pop_updated_at: Optional[str] = None
    area_updated_at: Optional[str] = None

class SiteMeta(BaseModel):
    population: Optional[int] = None
    area: Optional[float] = None
    pop_updated_at: Optional[str] = None
    area_updated_at: Optional[str] = None
    units: Dict[str, str] = {"area": "km2"}

class SiteMetaFull(SiteMeta):
    blocks: List[BlockMetaItem] = []

class UpdatePopulation(BaseModel):
    population: int = Field(..., ge=0, description="Total population (integer, >= 0)")

class UpdateArea(BaseModel):
    area: float = Field(..., ge=0, description="Area in km² (>= 0)")

class BlockUpdateItem(BaseModel):
    block: str = Field(..., description="Block code, e.g. 'A'")
    population: Optional[int] = Field(None, ge=0)
    area: Optional[float] = Field(None, ge=0)

    # allow extra fields (e.g. pop_updated_at/area_updated_at) in incoming JSON
    model_config = ConfigDict(extra="ignore")


class UpdateSitePayload(BaseModel):
    population: Optional[int] = Field(None, ge=0)
    area: Optional[float] = Field(None, ge=0)
    blocks: Optional[List[BlockUpdateItem]] = None

    # ignore extra fields so you can POST the same JSON back
    model_config = ConfigDict(extra="ignore")


# ---------- Site/Block meta helpers ----------

def _ensure_site_meta_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS site_meta(
            id INTEGER PRIMARY KEY CHECK (id = 1),
            population INTEGER,
            area REAL,
            pop_updated_at TEXT,
            area_updated_at TEXT
        )
    """)
    db.execute("INSERT OR IGNORE INTO site_meta(id) VALUES (1)")
    db.commit()

def _ensure_block_meta_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS block_meta(
            block TEXT PRIMARY KEY,
            population INTEGER,
            area REAL,
            pop_updated_at TEXT,
            area_updated_at TEXT
        )
    """)
    db.commit()

def _read_site_meta(db: sqlite3.Connection) -> Dict[str, Any]:
    row = db.execute("""
        SELECT population, area, pop_updated_at, area_updated_at
        FROM site_meta WHERE id = 1
    """).fetchone()
    return {
        "population": None if row["population"] is None else int(row["population"]),
        "area": None if row["area"] is None else float(row["area"]),
        "pop_updated_at": row["pop_updated_at"],
        "area_updated_at": row["area_updated_at"],
        "units": {"area": "km2"},
    }

def _read_blocks_meta(db: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not _table_exists(db, "block_meta"):
        return []
    rows = db.execute("""
        SELECT block, population, area, pop_updated_at, area_updated_at
        FROM block_meta
        ORDER BY block
    """).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "block": r["block"],
            "population": None if r["population"] is None else int(r["population"]),
            "area": None if r["area"] is None else float(r["area"]),
            "pop_updated_at": r["pop_updated_at"],
            "area_updated_at": r["area_updated_at"],
        })
    return out

def _sanitize_block_code(b: str) -> str:
    if not b or not isinstance(b, str):
        raise HTTPException(status_code=400, detail="Invalid block")
    code = b.strip().upper()
    # Accept single letter A-Z (adjust if your site uses other codes)
    if not re.fullmatch(r"[A-Z]", code):
        raise HTTPException(status_code=400, detail=f"Invalid block code: {b}")
    return code

def _upsert_block_meta_row(db: sqlite3.Connection, block: str,
                           population: Optional[int], area: Optional[float],
                           ts: str) -> None:
    db.execute("INSERT OR IGNORE INTO block_meta(block) VALUES (?)", (block,))
    if population is not None:
        db.execute("""
          UPDATE block_meta
             SET population = ?,
                 pop_updated_at = ?
           WHERE block = ?
        """, (population, ts, block))
    if area is not None:
        db.execute("""
          UPDATE block_meta
             SET area = ?,
                 area_updated_at = ?
           WHERE block = ?
        """, (area, ts, block))

def _sum_blocks(db: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    if not _table_exists(db, "block_meta"):
        return None
    row = db.execute("""
        SELECT
            COUNT(1)                       AS n_blocks,
            SUM(COALESCE(population, 0))   AS pop_sum,
            SUM(COALESCE(area, 0.0))       AS area_sum,
            MAX(pop_updated_at)            AS pop_updated_at,
            MAX(area_updated_at)           AS area_updated_at
        FROM block_meta
    """).fetchone()
    if not row or int(row["n_blocks"] or 0) == 0:
        return None
    return {
        "population": int(row["pop_sum"] or 0),
        "area": float(row["area_sum"] or 0.0),
        "pop_updated_at": row["pop_updated_at"],
        "area_updated_at": row["area_updated_at"],
        "n_blocks": int(row["n_blocks"]),
    }

def _write_site_totals_from_blocks(db: sqlite3.Connection) -> None:
    """Recompute site_meta totals from block_meta (if any blocks exist)."""
    totals = _sum_blocks(db)
    if not totals:
        return
    db.execute("""
        INSERT INTO site_meta(id, population, area, pop_updated_at, area_updated_at)
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            population     = excluded.population,
            area           = excluded.area,
            pop_updated_at = excluded.pop_updated_at,
            area_updated_at= excluded.area_updated_at
    """, (totals["population"], totals["area"],
          totals["pop_updated_at"], totals["area_updated_at"]))
    db.commit()

# ---------- Core endpoints (daily-only) ----------

@app.get("/health")
def health(db: sqlite3.Connection = Depends(get_db)) -> Dict[str, Any]:
    db.execute("SELECT 1").fetchone()
    return {"status": "ok", "db_path": DB_PATH}

@app.get("/meters", response_model=ListResponse, summary="List known meters")
def list_meters(
    limit: int = Query(1000, ge=1, le=10000),
    db: sqlite3.Connection = Depends(get_db),
):
    if not (_table_exists(db, "meter_daily") or _table_exists(db, "meter_daily_pv")):
        return {"items": [], "count": 0}
    rows = db.execute("""
      SELECT meter_name FROM meter_daily
      UNION
      SELECT meter_name FROM meter_daily_pv
      ORDER BY meter_name
      LIMIT ?
    """, (limit,)).fetchall()
    items = [r["meter_name"] for r in rows]
    return {"items": items, "count": len(items)}

@app.get("/blocks", response_model=ListResponse, summary="List blocks (from block_daily or meter tags)")
def list_blocks(db: sqlite3.Connection = Depends(get_db)):
    items: List[str] = []
    if _table_exists(db, "block_daily"):
        rows = db.execute("SELECT DISTINCT block FROM block_daily ORDER BY block").fetchall()
        items = [r["block"] for r in rows]
    if not items and _table_exists(db, "meter_daily"):
        rows = db.execute("""
            SELECT DISTINCT substr(meter_name,1,1) AS b
            FROM meter_daily
            WHERE substr(meter_name,1,1) GLOB '[A-Z]'
            ORDER BY b
        """).fetchall()
        items = [r["b"] for r in rows]
    return {"items": items, "count": len(items)}

@app.get("/meters/{meter_name}/series", response_model=SeriesResponse, summary="Meter daily consumption series")
def meter_series_daily(
    meter_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fill_missing: bool = Query(False, description="Fill missing days with 0"),
    db: sqlite3.Connection = Depends(get_db),
):
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "meter_daily"):
        return {"name": meter_name, "unit": "m3", "start": start_ts, "end": end_ts, "count": 0, "data": []}
    rows = db.execute("""
      SELECT ts, COALESCE(consumption, 0.0) AS consumption
      FROM meter_daily
      WHERE meter_name=? AND ts >= ? AND ts < ?
      ORDER BY ts
    """, (meter_name, start_ts, end_ts)).fetchall()

    if fill_missing:
        cur = datetime.fromisoformat(start_ts)
        end_dt = datetime.fromisoformat(end_ts)
        by_ts = {r["ts"]: float(r["consumption"]) for r in rows}
        data: List[Point] = []
        while cur < end_dt:
            iso = f"{cur.date()}T00:00:00"
            data.append(Point(ts=iso, consumption=by_ts.get(iso, 0.0)))
            cur += timedelta(days=1)
    else:
        data = [Point(ts=r["ts"], consumption=float(r["consumption"])) for r in rows]

    return {
        "name": meter_name,
        "unit": "m3",
        "start": start_ts,
        "end": end_ts,
        "count": len(data),
        "data": data,
    }

@app.get("/blocks/{block}/series", response_model=SeriesResponse, summary="Block daily consumption series")
def block_series_daily(
    block: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fill_missing: bool = Query(False, description="Fill missing days with 0"),
    db: sqlite3.Connection = Depends(get_db),
):
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "block_daily"):
        return {"name": block.upper(), "unit": "m3", "start": start_ts, "end": end_ts, "count": 0, "data": []}
    rows = db.execute("""
      SELECT ts, COALESCE(consumption, 0.0) AS consumption
      FROM block_daily
      WHERE block=? AND ts >= ? AND ts < ?
      ORDER BY ts
    """, (block.upper(), start_ts, end_ts)).fetchall()

    if fill_missing:
        cur = datetime.fromisoformat(start_ts)
        end_dt = datetime.fromisoformat(end_ts)
        by_ts = {r["ts"]: float(r["consumption"]) for r in rows}
        data: List[Point] = []
        while cur < end_dt:
            iso = f"{cur.date()}T00:00:00"
            data.append(Point(ts=iso, consumption=by_ts.get(iso, 0.0)))
            cur += timedelta(days=1)
    else:
        data = [Point(ts=r["ts"], consumption=float(r["consumption"])) for r in rows]

    return {
        "name": block.upper(),
        "unit": "m3",
        "start": start_ts,
        "end": end_ts,
        "count": len(data),
        "data": data,
    }

@app.get("/blocks/{block}/levels", response_model=ListResponse, summary="List levels available for a block")
def list_levels_for_block(
    block: str,
    db: sqlite3.Connection = Depends(get_db),
):
    if not (_table_exists(db, "meter_daily") or _table_exists(db, "meter_daily_pv")):
        return {"items": [], "count": 0}
    rows = db.execute("""
      SELECT meter_name FROM meter_daily
      UNION
      SELECT meter_name FROM meter_daily_pv
    """).fetchall()
    levels: set[int] = set()
    for r in rows:
        tag = r["meter_name"]
        bl_lvl = _extract_block_level_from_tag(tag or "")
        if not bl_lvl:
            continue
        bl, lv = bl_lvl
        if bl == block.upper():
            levels.add(lv)
    items = [_level_label(n) for n in sorted(levels)]
    return {"items": items, "count": len(items)}

@app.get("/blocks/{block}/levels/{level}/series", response_model=SeriesResponse,
         summary="Daily series for a block level (sum of all meters on that level)")
def level_series_daily(
    block: str,
    level: str,  # 'Level 7', 'L7', or '7'
    start: Optional[str] = None,
    end: Optional[str] = None,
    fill_missing: bool = Query(False, description="Fill missing days with 0"),
    db: sqlite3.Connection = Depends(get_db),
):
    block = block.upper()
    level_num = _parse_level_str(level)
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "meter_daily"):
        return {"name": f"{block} - Level {level_num}", "unit": "m3", "start": start_ts, "end": end_ts, "count": 0, "data": []}

    patterns = _like_patterns_for_block_level(block, level_num)
    where_or = " OR ".join(["meter_name LIKE ?"] * len(patterns))
    params = [start_ts, end_ts] + patterns

    rows = db.execute(f"""
      SELECT ts, SUM(COALESCE(consumption, 0.0)) AS consumption
      FROM meter_daily
      WHERE ts >= ? AND ts < ? AND ({where_or})
      GROUP BY ts
      ORDER BY ts
    """, params).fetchall()

    if fill_missing:
        cur = datetime.fromisoformat(start_ts)
        end_dt = datetime.fromisoformat(end_ts)
        by_ts = {r["ts"]: float(r["consumption"]) for r in rows}
        data: List[Point] = []
        while cur < end_dt:
            iso = f"{cur.date()}T00:00:00"
            data.append(Point(ts=iso, consumption=by_ts.get(iso, 0.0)))
            cur += timedelta(days=1)
    else:
        data = [Point(ts=r["ts"], consumption=float(r["consumption"])) for r in rows]

    return {
        "name": f"{block} - Level {level_num}",
        "unit": "m3",
        "start": start_ts,
        "end": end_ts,
        "count": len(data),
        "data": data,
    }

# ---------- Daily PV snapshots ----------

@app.get("/meters/{meter_name}/daily-pv", response_model=PVSeriesResponse,
         summary="Daily midnight present_value snapshots for a meter")
def meter_daily_pv_series(
    meter_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "meter_daily_pv"):
        return {"name": meter_name, "unit": "m3", "start": start_ts, "end": end_ts, "count": 0, "data": []}
    rows = db.execute("""
      SELECT ts, present_value
      FROM meter_daily_pv
      WHERE meter_name = ? AND ts >= ? AND ts < ?
      ORDER BY ts
    """, (meter_name, start_ts, end_ts)).fetchall()
    data = [PVPoint(ts=r["ts"], present_value=float(r["present_value"])) for r in rows]
    return {
        "name": meter_name,
        "unit": "m3",
        "start": start_ts,
        "end": end_ts,
        "count": len(data),
        "data": data,
    }

# ---------- Date range discovery (daily only) ----------

@app.get("/meta/date-range", response_model=DateRangeResponse,
         summary="Discover available date range for a dataset/scope (daily only)")
def meta_date_range(
    dataset: str = Query("consumption", pattern="^(consumption|pv)$"),
    scope: str = Query("meter", pattern="^(meter|block)$"),
    name: Optional[str] = Query(None, description="meter_name when scope=meter; block when scope=block"),
    db: sqlite3.Connection = Depends(get_db),
):
    if dataset == "pv":
        if scope != "meter":
            raise HTTPException(status_code=400, detail="PV snapshots only support scope=meter")
        table = "meter_daily_pv"
        if not _table_exists(db, table):
            return {"dataset": dataset, "scope": scope, "name": name, "table": table, "min_ts": None, "max_ts": None, "row_count": 0}
        where = "meter_name = ?" if name else "1=1"
        params = (name,) if name else ()
    else:
        # consumption
        table = "meter_daily" if scope == "meter" else "block_daily"
        if not _table_exists(db, table):
            return {"dataset": dataset, "scope": scope, "name": name, "table": table, "min_ts": None, "max_ts": None, "row_count": 0}
        if scope == "meter":
            where = "meter_name = ?" if name else "1=1"
            params = (name,) if name else ()
        else:
            where = "block = ?" if name else "1=1"
            params = (name.upper(),) if name else ()

    row = db.execute(f"""
      SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts, COUNT(1) AS row_count
      FROM {table}
      WHERE {where}
    """, params).fetchone()

    return {
        "dataset": dataset,
        "scope": scope,
        "name": name,
        "table": table,
        "min_ts": row["min_ts"],
        "max_ts": row["max_ts"],
        "row_count": int(row["row_count"] or 0),
    }

# ---------- Summaries ----------

@app.get("/summary/top-meters", summary="Top N meters by daily consumption")
def top_meters(
    date: str = Query(..., description="YYYY-MM-DD"),
    n: int = Query(10, ge=1, le=1000),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "meter_daily"):
        return {"date": date, "count": 0, "items": []}
    ts_mid = _iso_midnight(date)
    rows = db.execute(""" 
      SELECT meter_name, COALESCE(consumption, 0.0) AS consumption
      FROM meter_daily
      WHERE ts = ?
      ORDER BY consumption DESC
      LIMIT ?
    """, (ts_mid, n)).fetchall()
    return {"date": date, "count": len(rows), "items": [dict(r) for r in rows]}

@app.get("/summary/block", summary="Block totals for a day")
def block_totals(
    date: str = Query(..., description="YYYY-MM-DD"),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "block_daily"):
        return {"date": date, "items": []}
    ts_mid = _iso_midnight(date)
    rows = db.execute("""
      SELECT block, COALESCE(consumption, 0.0) AS consumption
      FROM block_daily
      WHERE ts = ?
      ORDER BY block
    """, (ts_mid,)).fetchall()
    return {"date": date, "items": [dict(r) for r in rows]}

@app.get("/summary/meter", summary="Meter totals for a day (optional block filter)")
def meter_totals_for_day(
    date: str = Query(..., description="YYYY-MM-DD"),
    block: Optional[str] = Query(None, description="Block letter e.g. A"),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "meter_daily"):
        return {"date": date, "items": []}
    ts_mid = _iso_midnight(date)
    if block:
        rows = db.execute("""
          SELECT meter_name, COALESCE(consumption, 0.0) AS consumption
          FROM meter_daily
          WHERE ts = ? AND substr(meter_name,1,1) = ?
          ORDER BY consumption DESC
        """, (ts_mid, block.upper())).fetchall()
    else:
        rows = db.execute("""
          SELECT meter_name, COALESCE(consumption, 0.0) AS consumption
          FROM meter_daily
          WHERE ts = ?
          ORDER BY consumption DESC
        """, (ts_mid,)).fetchall()
    return {"date": date, "items": [dict(r) for r in rows]}

# ---------- Latest cumulative (uses daily tables only) ----------

@app.get("/meters/{meter_name}/latest-cumulative", response_model=LatestCumulative, summary="Latest cumulative reading")
def meter_latest_cumulative(
    meter_name: str,
    db: sqlite3.Connection = Depends(get_db),
):
    # 1) Preferred: 'meter_last_snapshot'
    if _table_exists(db, "meter_last_snapshot"):
        row = db.execute(
            "SELECT last_pv AS v, last_ts AS t FROM meter_last_snapshot WHERE meter_name=?",
            (meter_name,),
        ).fetchone()
        if row and row["v"] is not None:
            return {"meter_name": meter_name, "last_value": float(row["v"]), "last_ts": row["t"]}

    # 2) Fallback: latest midnight PV in 'meter_daily_pv'
    if _table_exists(db, "meter_daily_pv"):
        row = db.execute(
            "SELECT ts AS t, present_value AS v FROM meter_daily_pv WHERE meter_name=? ORDER BY ts DESC LIMIT 1",
            (meter_name,),
        ).fetchone()
        if row and row["v"] is not None:
            return {"meter_name": meter_name, "last_value": float(row["v"]), "last_ts": row["t"]}

    raise HTTPException(status_code=404, detail="meter not found in meter_last_snapshot or meter_daily_pv")

# ---------- CSV exports (daily only) ----------

@app.get("/export/meter.csv", summary="Export meter daily series as CSV")
def export_meter_csv(
    meter_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "meter_daily"):
        return Response(content="ts,consumption_m3\n", media_type="text/csv")
    rows = db.execute("""
      SELECT ts, COALESCE(consumption, 0.0) AS consumption
      FROM meter_daily
      WHERE meter_name=? AND ts >= ? AND ts < ?
      ORDER BY ts
    """, (meter_name, start_ts, end_ts)).fetchall()
    csv = "ts,consumption_m3\n" + "\n".join(f'{r["ts"]},{float(r["consumption"])}' for r in rows)
    return Response(content=csv, media_type="text/csv")

@app.get("/export/block.csv", summary="Export block daily series as CSV")
def export_block_csv(
    block: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "block_daily"):
        return Response(content="ts,consumption_m3\n", media_type="text/csv")
    rows = db.execute("""
      SELECT ts, COALESCE(consumption, 0.0) AS consumption
      FROM block_daily
      WHERE block=? AND ts >= ? AND ts < ?
      ORDER BY ts
    """, (block.upper(), start_ts, end_ts)).fetchall()
    csv = "ts,consumption_m3\n" + "\n".join(f'{r["ts"]},{float(r["consumption"])}' for r in rows)
    return Response(content=csv, media_type="text/csv")

@app.get("/export/meter_pv.csv", summary="Export daily PV snapshots as CSV")
def export_meter_pv_csv(
    meter_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    start_ts, end_ts = ensure_daily_range(start, end)
    if not _table_exists(db, "meter_daily_pv"):
        return Response(content="ts,present_value_m3\n", media_type="text/csv")
    rows = db.execute("""
      SELECT ts, present_value
      FROM meter_daily_pv
      WHERE meter_name=? AND ts >= ? AND ts < ?
      ORDER BY ts
    """, (meter_name, start_ts, end_ts)).fetchall()
    csv = "ts,present_value_m3\n" + "\n".join(f'{r["ts"]},{float(r["present_value"])}' for r in rows)
    return Response(content=csv, media_type="text/csv")

# ---------- Site & Blocks meta ----------

@app.get("/meta/site", response_model=SiteMetaFull, summary="Get site totals (sum of blocks) and block list")
def get_site_meta(db: sqlite3.Connection = Depends(get_db)):
    _ensure_site_meta_table(db)
    _ensure_block_meta_table(db)

    blocks = _read_blocks_meta(db)
    totals = _sum_blocks(db)

    if totals:
        site = {
            "population": totals["population"],
            "area": totals["area"],
            "pop_updated_at": totals["pop_updated_at"],
            "area_updated_at": totals["area_updated_at"],
            "units": {"area": "km2"},
        }
    else:
        site = _read_site_meta(db)

    return {**site, "blocks": blocks}

@app.put("/meta/site", response_model=SiteMetaFull, summary="Upsert site and/or blocks; site totals derive from blocks when provided")
def upsert_site_and_blocks(payload: UpdateSitePayload, db: sqlite3.Connection = Depends(get_db)):
    """
    Rules:
      - If 'blocks' provided: upsert those blocks, then recompute site totals from block_meta
        (top-level population/area in the payload are ignored in this case).
      - If 'blocks' NOT provided: top-level population/area (if provided) update site_meta directly.
    """
    _ensure_site_meta_table(db)
    _ensure_block_meta_table(db)
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if payload.blocks:
        for blk in payload.blocks:
            code = _sanitize_block_code(blk.block)
            _upsert_block_meta_row(db, code, blk.population, blk.area, ts)
        db.commit()
        _write_site_totals_from_blocks(db)
    else:
        if payload.population is not None:
            db.execute("""
                INSERT INTO site_meta(id, population, pop_updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    population = excluded.population,
                    pop_updated_at = excluded.pop_updated_at
            """, (payload.population, ts))
        if payload.area is not None:
            db.execute("""
                INSERT INTO site_meta(id, area, area_updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    area = excluded.area,
                    area_updated_at = excluded.area_updated_at
            """, (payload.area, ts))
        db.commit()

    blocks = _read_blocks_meta(db)
    totals = _sum_blocks(db)
    if totals:
        site = {
            "population": totals["population"],
            "area": totals["area"],
            "pop_updated_at": totals["pop_updated_at"],
            "area_updated_at": totals["area_updated_at"],
            "units": {"area": "km2"},
        }
    else:
        site = _read_site_meta(db)
    return {**site, "blocks": blocks}

@app.put("/meta/site/population", response_model=SiteMetaFull, summary="(Back-compat) Update site population only")
def put_population(payload: UpdatePopulation, db: sqlite3.Connection = Depends(get_db)):
    _ensure_site_meta_table(db)
    _ensure_block_meta_table(db)
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute("""
        INSERT INTO site_meta(id, population, pop_updated_at)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            population = excluded.population,
            pop_updated_at = excluded.pop_updated_at
    """, (payload.population, ts))
    db.commit()

    blocks = _read_blocks_meta(db)
    totals = _sum_blocks(db)
    if totals:
        site = {
            "population": totals["population"],
            "area": totals["area"],
            "pop_updated_at": totals["pop_updated_at"],
            "area_updated_at": totals["area_updated_at"],
            "units": {"area": "km2"},
        }
    else:
        site = _read_site_meta(db)
    return {**site, "blocks": blocks}

@app.put("/meta/site/area", response_model=SiteMetaFull, summary="(Back-compat) Update site area only")
def put_area(payload: UpdateArea, db: sqlite3.Connection = Depends(get_db)):
    _ensure_site_meta_table(db)
    _ensure_block_meta_table(db)
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute("""
        INSERT INTO site_meta(id, area, area_updated_at)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            area = excluded.area,
            area_updated_at = excluded.area_updated_at
    """, (payload.area, ts))
    db.commit()

    blocks = _read_blocks_meta(db)
    totals = _sum_blocks(db)
    if totals:
        site = {
            "population": totals["population"],
            "area": totals["area"],
            "pop_updated_at": totals["pop_updated_at"],
            "area_updated_at": totals["area_updated_at"],
            "units": {"area": "km2"},
        }
    else:
        site = _read_site_meta(db)
    return {**site, "blocks": blocks}

# ---------- NEW: Monthly & Yearly (meter/block) ----------

@app.get("/meters/{meter_name}/series-monthly", response_model=SeriesResponse,
         summary="Meter monthly consumption series")
def meter_series_monthly(
    meter_name: str,
    start: Optional[str] = Query(None, description="Start month YYYY-MM (inclusive)"),
    end: Optional[str] = Query(None, description="End month YYYY-MM (exclusive)"),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "meter_daily"):
        s, e = _month_bounds(start, end)
        return {"name": meter_name, "unit": "m3", "start": s, "end": e, "count": 0, "data": []}

    start_ts, end_ts = _month_bounds(start, end)
    rows = db.execute("""
      SELECT substr(ts,1,7) AS ym, SUM(COALESCE(consumption, 0.0)) AS total
      FROM meter_daily
      WHERE meter_name = ? AND ts >= ? AND ts < ?
      GROUP BY ym
      ORDER BY ym
    """, (meter_name, start_ts, end_ts)).fetchall()

    data = [Point(ts=_month_bucket_ts(r["ym"]), consumption=float(r["total"] or 0.0)) for r in rows]
    return {"name": meter_name, "unit": "m3", "start": start_ts, "end": end_ts, "count": len(data), "data": data}

@app.get("/blocks/{block}/series-monthly", response_model=SeriesResponse,
         summary="Block monthly consumption series")
def block_series_monthly(
    block: str,
    start: Optional[str] = Query(None, description="Start month YYYY-MM (inclusive)"),
    end: Optional[str] = Query(None, description="End month YYYY-MM (exclusive)"),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "block_daily"):
        s, e = _month_bounds(start, end)
        return {"name": block.upper(), "unit": "m3", "start": s, "end": e, "count": 0, "data": []}

    start_ts, end_ts = _month_bounds(start, end)
    rows = db.execute("""
      SELECT substr(ts,1,7) AS ym, SUM(COALESCE(consumption, 0.0)) AS total
      FROM block_daily
      WHERE block = ? AND ts >= ? AND ts < ?
      GROUP BY ym
      ORDER BY ym
    """, (block.upper(), start_ts, end_ts)).fetchall()

    data = [Point(ts=_month_bucket_ts(r["ym"]), consumption=float(r["total"] or 0.0)) for r in rows]
    return {"name": block.upper(), "unit": "m3", "start": start_ts, "end": end_ts, "count": len(data), "data": data}

@app.get("/meters/{meter_name}/series-yearly", response_model=SeriesResponse,
         summary="Meter yearly consumption series")
def meter_series_yearly(
    meter_name: str,
    start: Optional[str] = Query(None, description="Start year YYYY (inclusive)"),
    end: Optional[str] = Query(None, description="End year YYYY (exclusive)"),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "meter_daily"):
        s, e = _year_bounds(start, end)
        return {"name": meter_name, "unit": "m3", "start": s, "end": e, "count": 0, "data": []}

    start_ts, end_ts = _year_bounds(start, end)
    rows = db.execute("""
      SELECT substr(ts,1,4) AS y, SUM(COALESCE(consumption, 0.0)) AS total
      FROM meter_daily
      WHERE meter_name = ? AND ts >= ? AND ts < ?
      GROUP BY y
      ORDER BY y
    """, (meter_name, start_ts, end_ts)).fetchall()

    data = [Point(ts=_year_bucket_ts(r["y"]), consumption=float(r["total"] or 0.0)) for r in rows]
    return {"name": meter_name, "unit": "m3", "start": start_ts, "end": end_ts, "count": len(data), "data": data}

@app.get("/blocks/{block}/series-yearly", response_model=SeriesResponse,
         summary="Block yearly consumption series")
def block_series_yearly(
    block: str,
    start: Optional[str] = Query(None, description="Start year YYYY (inclusive)"),
    end: Optional[str] = Query(None, description="End year YYYY (exclusive)"),
    db: sqlite3.Connection = Depends(get_db),
):
    if not _table_exists(db, "block_daily"):
        s, e = _year_bounds(start, end)
        return {"name": block.upper(), "unit": "m3", "start": s, "end": e, "count": 0, "data": []}

    start_ts, end_ts = _year_bounds(start, end)
    rows = db.execute("""
      SELECT substr(ts,1,4) AS y, SUM(COALESCE(consumption, 0.0)) AS total
      FROM block_daily
      WHERE block = ? AND ts >= ? AND ts < ?
      GROUP BY y
      ORDER BY y
    """, (block.upper(), start_ts, end_ts)).fetchall()

    data = [Point(ts=_year_bucket_ts(r["y"]), consumption=float(r["total"] or 0.0)) for r in rows]
    return {"name": block.upper(), "unit": "m3", "start": start_ts, "end": end_ts, "count": len(data), "data": data}

# ---------- NEW: Monthly & Yearly by Level (sum of meters on that level) ----------

@app.get("/blocks/{block}/levels/{level}/series-monthly", response_model=SeriesResponse,
         summary="Monthly series for a block level (sum of meters on that level)")
def level_series_monthly(
    block: str,
    level: str,  # 'Level 7', 'L7', or '7'
    start: Optional[str] = Query(None, description="Start month YYYY-MM (inclusive)"),
    end: Optional[str] = Query(None, description="End month YYYY-MM (exclusive)"),
    db: sqlite3.Connection = Depends(get_db),
):
    block = block.upper()
    level_num = _parse_level_str(level)
    if not _table_exists(db, "meter_daily"):
        s, e = _month_bounds(start, end)
        return {"name": f"{block} - Level {level_num}", "unit": "m3", "start": s, "end": e, "count": 0, "data": []}

    start_ts, end_ts = _month_bounds(start, end)
    patterns = _like_patterns_for_block_level(block, level_num)
    where_or = " OR ".join(["meter_name LIKE ?"] * len(patterns))
    params = [start_ts, end_ts] + patterns

    rows = db.execute(f"""
      SELECT substr(ts,1,7) AS ym, SUM(COALESCE(consumption, 0.0)) AS total
      FROM meter_daily
      WHERE ts >= ? AND ts < ? AND ({where_or})
      GROUP BY ym
      ORDER BY ym
    """, params).fetchall()

    data = [Point(ts=_month_bucket_ts(r["ym"]), consumption=float(r["total"] or 0.0)) for r in rows]
    return {
        "name": f"{block} - Level {level_num}",
        "unit": "m3",
        "start": start_ts,
        "end": end_ts,
        "count": len(data),
        "data": data,
    }

@app.get("/blocks/{block}/levels/{level}/series-yearly", response_model=SeriesResponse,
         summary="Yearly series for a block level (sum of meters on that level)")
def level_series_yearly(
    block: str,
    level: str,  # 'Level 7', 'L7', or '7'
    start: Optional[str] = Query(None, description="Start year YYYY (inclusive)"),
    end: Optional[str] = Query(None, description="End year YYYY (exclusive)"),
    db: sqlite3.Connection = Depends(get_db),
):
    block = block.upper()
    level_num = _parse_level_str(level)
    if not _table_exists(db, "meter_daily"):
        s, e = _year_bounds(start, end)
        return {"name": f"{block} - Level {level_num}", "unit": "m3", "start": s, "end": e, "count": 0, "data": []}

    start_ts, end_ts = _year_bounds(start, end)
    patterns = _like_patterns_for_block_level(block, level_num)
    where_or = " OR ".join(["meter_name LIKE ?"] * len(patterns))
    params = [start_ts, end_ts] + patterns

    rows = db.execute(f"""
      SELECT substr(ts,1,4) AS y, SUM(COALESCE(consumption, 0.0)) AS total
      FROM meter_daily
      WHERE ts >= ? AND ts < ? AND ({where_or})
      GROUP BY y
      ORDER BY y
    """, params).fetchall()

    data = [Point(ts=_year_bucket_ts(r["y"]), consumption=float(r["total"] or 0.0)) for r in rows]
    return {
        "name": f"{block} - Level {level_num}",
        "unit": "m3",
        "start": start_ts,
        "end": end_ts,
        "count": len(data),
        "data": data,
    }
