#!/usr/bin/env python3
"""
matter_devices.py
-----------------
Lists all Matter devices in Home Assistant with Thread network info
and HA device registry details.

All data is fetched over a single WebSocket connection — no REST calls.

Requirements:
    pip install websockets

Usage:
    python matter_devices.py

Output mode: set OUTPUT to "table", "json", or "debug" below.
"""

import asyncio
import csv
import io
import json
import re
import sys
from typing import Any


# ── Configuration ─────────────────────────────────────────────────────────────

HA_URL = "http://192.168.178.53:8123"
HA_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJmYTI2NDA1ZTA2NGI0YjI0YTk0OTgzZmVjNDkzMGRhZSIsImlhdCI6MTc3NzU1OTk1NywiZXhwIjoyMDkyOTE5OTU3fQ.E2dm2LhcCTqhbZhLM417hHR9rVfVaArjDGD9_rK9nKA"


# Output mode: "table" | "csv" | "json" | "debug"
OUTPUT = "csv"
CSV_FILE = "matter_devices.csv"          # Path for CSV output e.g. "matter_devices.csv". Empty = print to screen.


# ── WebSocket session ─────────────────────────────────────────────────────────

class HAWebSocket:
    """Persistent authenticated HA WebSocket session."""

    def __init__(self, ws_url: str, token: str, debug: bool = False):
        self.ws_url = ws_url
        self.token = token
        self.debug = debug
        self._ws = None
        self._msg_id = 0

    async def __aenter__(self):
        import websockets
        self._ws = await websockets.connect(f"{self.ws_url}/api/websocket")

        hello = json.loads(await self._ws.recv())
        if hello.get("type") != "auth_required":
            raise RuntimeError(f"Unexpected greeting: {hello}")

        await self._ws.send(json.dumps({"type": "auth", "access_token": self.token}))
        auth_result = json.loads(await self._ws.recv())
        if auth_result.get("type") != "auth_ok":
            raise RuntimeError(
                f"WebSocket auth failed: {auth_result.get('message', auth_result)}"
            )

        if self.debug:
            print(f"[WS] connected to {self.ws_url}", file=sys.stderr)
        return self

    async def __aexit__(self, *_):
        if self._ws:
            await self._ws.close()

    async def call(self, payload: dict) -> Any:
        """Send one command, return result or raise on error."""
        self._msg_id += 1
        payload["id"] = self._msg_id

        if self.debug:
            print(f"[WS] →  {json.dumps(payload)}", file=sys.stderr)

        await self._ws.send(json.dumps(payload))

        while True:
            raw = await self._ws.recv()
            msg = json.loads(raw)
            if self.debug:
                print(f"[WS] ←  {raw[:400]}", file=sys.stderr)
            if msg.get("id") == self._msg_id:
                if not msg.get("success", True):
                    err = msg.get("error", {})
                    raise RuntimeError(
                        f"Command '{payload.get('type')}' failed: "
                        f"{err.get('code')} – {err.get('message')}"
                    )
                return msg.get("result")

    async def try_call(self, payload: dict, default=None) -> Any:
        """Like call(), but returns default on failure instead of raising."""
        try:
            return await self.call(payload)
        except Exception as e:
            print(f"  ⚠  {e}", file=sys.stderr)
            return default


# ── helpers ───────────────────────────────────────────────────────────────────

def parse_matter_unique_id(unique_id: str) -> tuple[int | None, str | None]:
    """
    Parse (node_id, fabric_id) from an HA Matter unique_id string.

    Known formats:
      "deviceid_<FABRIC_HEX>-<NODE_HEX>-MatterNodeDevice"  ← most common in current HA
      "serial_<HEX>"                                        ← serial-only, no node ID
      "<FABRIC_HEX>-<NODE_HEX>"                            ← older format
      "<DECIMAL>"                                           ← bare node ID

    Returns (node_id, fabric_id); either may be None if not parseable.
    """
    if not unique_id:
        return None, None

    # deviceid_<fabric16>-<node16>-MatterNodeDevice
    m = re.match(
        r"^deviceid_([0-9A-Fa-f]+)-([0-9A-Fa-f]+)-MatterNodeDevice$", unique_id
    )
    if m:
        fabric_hex, node_hex = m.group(1), m.group(2)
        try:
            return int(node_hex, 16), fabric_hex.upper()
        except ValueError:
            return None, fabric_hex.upper()

    # serial_<hex> — no node ID extractable
    if unique_id.lower().startswith("serial_"):
        return None, None

    # <fabric_hex>-<node_hex>  (older format, take last segment as node)
    m = re.search(r"-([0-9A-Fa-f]{4,16})$", unique_id)
    if m:
        try:
            return int(m.group(1), 16), None
        except ValueError:
            pass

    # Pure decimal node ID
    if unique_id.isdigit():
        return int(unique_id), None

    return None, None


def normalise_thread_datasets(raw: Any) -> list[dict]:
    """
    `thread/list_datasets` can return different shapes depending on HA version:
      • list of dicts  (ideal)
      • dict like {"datasets": [...]}
      • list of dataset-ID strings (older builds)
    Always return a list of dicts (possibly empty).
    """
    if raw is None:
        return []

    # Unwrap {"datasets": [...]} wrapper
    if isinstance(raw, dict):
        raw = raw.get("datasets") or list(raw.values())

    if not isinstance(raw, list):
        return []

    # Filter out non-dict items (e.g. plain strings)
    dicts = [item for item in raw if isinstance(item, dict)]
    if len(dicts) < len(raw):
        skipped = len(raw) - len(dicts)
        print(
            f"  ⚠  thread/list_datasets: skipped {skipped} non-dict item(s) "
            f"(raw type was {type(raw[0]).__name__}). "
            f"Thread cross-referencing may be incomplete.",
            file=sys.stderr,
        )
    return dicts


# ── data collection ───────────────────────────────────────────────────────────

async def collect(ha: HAWebSocket) -> tuple[list, dict, list]:
    """Fetch everything in one WebSocket session."""

    # 1. HA device registry — filter to Matter devices
    print("  • device registry…", file=sys.stderr)
    all_devices: list[dict] = await ha.call({"type": "config/device_registry/list"}) or []

    matter_devices = [
        d for d in all_devices
        if any(
            isinstance(ident, (list, tuple)) and len(ident) >= 1 and ident[0] == "matter"
            for ident in d.get("identifiers", [])
        )
    ]
    print(f"    → {len(matter_devices)} Matter device(s) in registry", file=sys.stderr)

    # 2. Matter node diagnostics — the command requires device_id (HA device registry ID),
    #    not node_id. This covers all devices including serial_ ones.
    device_diagnostics: dict[str, dict] = {}  # ha_device_id → diagnostics

    print(f"  • matter/node_diagnostics for {len(matter_devices)} device(s)…", file=sys.stderr)
    ok = failed = 0
    for dev in matter_devices:
        ha_device_id = dev["id"]
        diag = await ha.try_call(
            {"type": "matter/node_diagnostics", "device_id": ha_device_id},
            default={}
        )
        device_diagnostics[ha_device_id] = diag or {}
        if diag:
            ok += 1
        else:
            failed += 1
    print(f"    → {ok} succeeded, {failed} empty/failed", file=sys.stderr)

    # 3. Thread datasets
    print("  • thread datasets…", file=sys.stderr)
    thread_raw = await ha.try_call({"type": "thread/list_datasets"}, default=[])
    thread_datasets = normalise_thread_datasets(thread_raw)
    print(f"    → {len(thread_datasets)} dataset(s)", file=sys.stderr)

    return matter_devices, device_diagnostics, thread_datasets


# ── data assembly ─────────────────────────────────────────────────────────────

NODE_TYPE_LABELS = {
    "routing_end_device": "Router-ED",
    "sleepy_end_device":  "Sleepy-ED",
    "router":             "Router",
    "end_device":         "End Device",
}

def extract_diag_info(diag: dict) -> dict:
    """
    Map matter/node_diagnostics response fields.

    HA returns a flat structure (not nested):
      node_id, network_type, node_type, network_name,
      ip_adresses (sic — typo in HA), mac_address, available,
      active_fabrics: [{fabric_id, vendor_id, fabric_index,
                        fabric_label, vendor_name}, ...]

    Channel / PAN ID are NOT in diagnostics — sourced from thread dataset.
    """
    node_type_raw = diag.get("node_type") or ""
    return {
        "node_id_diag":      diag.get("node_id"),
        "network_type":      diag.get("network_type") or "—",
        "thread_role":       NODE_TYPE_LABELS.get(node_type_raw, node_type_raw) or "—",
        "thread_role_raw":   node_type_raw,
        "thread_network_name": diag.get("network_name"),
        "mac_address":       diag.get("mac_address") or "—",
        "available":         diag.get("available"),
        # Note: HA has a typo — "ip_adresses" with one 'd'
        "ipv6_addresses":    diag.get("ip_adresses") or [],
        "active_fabrics":    diag.get("active_fabrics") or [],
    }


def build_rows(
    matter_devices: list[dict],
    device_diagnostics: dict[str, dict],
    thread_datasets: list[dict],
) -> list[dict]:

    # Thread datasets indexed by extended PAN ID
    ds_by_ext_pan: dict[str, dict] = {}
    for ds in thread_datasets:
        epid = ds.get("extended_pan_id") or ds.get("ext_pan_id")
        if epid:
            key = str(epid).lower().replace("0x", "").lstrip("0") or "0"
            ds_by_ext_pan[key] = ds

    # Use the single preferred Thread dataset as fallback for all Thread devices
    preferred_ds = next((d for d in thread_datasets if d.get("preferred")), None) or                    (thread_datasets[0] if thread_datasets else {})

    rows = []
    for dev in matter_devices:
        # Get Matter unique_id from HA identifiers
        matter_uid = None
        for ident in dev.get("identifiers", []):
            if isinstance(ident, (list, tuple)) and len(ident) == 2 and ident[0] == "matter":
                matter_uid = str(ident[1])
                break

        # Parse fabric_id from unique_id (node_id comes from diagnostics)
        _, fabric_id_from_uid = parse_matter_unique_id(matter_uid) if matter_uid else (None, None)

        # Diagnostics keyed by HA device ID
        diag = device_diagnostics.get(dev["id"], {})
        net = extract_diag_info(diag)

        # node_id: prefer diagnostics (covers serial_ devices too), fall back to uid parse
        node_id_uid, _ = parse_matter_unique_id(matter_uid) if matter_uid else (None, None)
        node_id = net["node_id_diag"] if net["node_id_diag"] is not None else node_id_uid

        # For Thread devices, fill channel/PAN from the preferred dataset
        is_thread = (net["network_type"] == "thread")
        thread_channel  = preferred_ds.get("channel")  if is_thread else None
        thread_pan_id   = preferred_ds.get("pan_id")   if is_thread else None
        thread_ext_pan  = preferred_ds.get("extended_pan_id") if is_thread else None

        # HA fabric (vendor_id 4939) from active_fabrics
        ha_fabric = next(
            (f for f in net["active_fabrics"] if f.get("vendor_id") == 4939), {}
        )
        fabric_label = ha_fabric.get("fabric_label") or "—"

        rows.append({
            # HA device registry
            "name":         dev.get("name_by_user") or dev.get("name") or "(unknown)",
            "ha_device_id": dev.get("id", "—"),
            "area_id":      dev.get("area_id") or "—",
            "manufacturer": dev.get("manufacturer") or "—",
            "model":        dev.get("model") or "—",
            "sw_version":   dev.get("sw_version") or "—",
            "hw_version":   dev.get("hw_version") or "—",
            # Matter identifiers
            "matter_node_id":   node_id if node_id is not None else "—",
            "matter_unique_id": matter_uid or "—",
            "fabric_id":        fabric_id_from_uid or "—",
            "fabric_label":     fabric_label,
            # Network
            "network_type":  net["network_type"],
            "available":     net["available"],
            "mac_address":   net["mac_address"],
            "ipv6_addresses": net["ipv6_addresses"],
            # Thread
            "thread_role":         net["thread_role"] if is_thread else "—",
            "thread_network_name": net["thread_network_name"] or (preferred_ds.get("network_name") if is_thread else None) or "—",
            "thread_channel":      thread_channel or "—",
            "thread_pan_id":       thread_pan_id or "—",
            "thread_ext_pan_id":   thread_ext_pan or "—",
            # Raw diagnostics (for debug mode)
            "_diag": diag,
        })

    rows.sort(key=lambda r: r["name"].lower())
    return rows


# ── output ────────────────────────────────────────────────────────────────────

def print_table(rows: list[dict]) -> None:
    if not rows:
        print("No Matter devices found.")
        return

    W = dict(name=28, mfr=16, model=20, nid=6, role=14, net=18, ch=4, ipv6=40)

    def t(v: Any, w: int) -> str:
        s = str(v)
        return s if len(s) <= w else s[:w - 1] + "…"

    hdr = (
        f"{'Device':<{W['name']}}  {'Manufacturer':<{W['mfr']}}  {'Model':<{W['model']}}  "
        f"{'NodeID':>{W['nid']}}  {'Thread Role':<{W['role']}}  {'Network':<{W['net']}}  "
        f"{'Ch':>{W['ch']}}  {'IPv6 (first)':<{W['ipv6']}}"
    )
    sep = "─" * len(hdr)
    print(f"\n{sep}\n{hdr}\n{sep}")

    for r in rows:
        ipv6 = r["ipv6_addresses"][0] if r["ipv6_addresses"] else "—"
        print(
            f"{t(r['name'], W['name']):<{W['name']}}  {t(r['manufacturer'], W['mfr']):<{W['mfr']}}  "
            f"{t(r['model'], W['model']):<{W['model']}}  {str(r['matter_node_id']):>{W['nid']}}  "
            f"{t(r['thread_role'], W['role']):<{W['role']}}  {t(r['thread_network_name'], W['net']):<{W['net']}}  "
            f"{str(r['thread_channel']):>{W['ch']}}  {t(ipv6, W['ipv6']):<{W['ipv6']}}"
        )

    print(f"{sep}\n\n{len(rows)} Matter device(s)\n")

    for r in rows:
        avail = {True: "yes", False: "NO", None: "—"}.get(r["available"], "—")
        print(f"┌─ {r['name']}  [{r['network_type'].upper()}]  available: {avail}")
        print(f"│  HA device ID    : {r['ha_device_id']}")
        print(f"│  Area            : {r['area_id']}")
        print(f"│  Manufacturer    : {r['manufacturer']}  │  Model: {r['model']}")
        print(f"│  SW version      : {r['sw_version']}  │  HW: {r['hw_version']}")
        print(f"│  Matter unique ID: {r['matter_unique_id']}")
        print(f"│  Matter node ID  : {r['matter_node_id']}")
        print(f"│  Fabric ID       : {r['fabric_id']}  │  Label: {r['fabric_label']}")
        print(f"│  MAC address     : {r['mac_address']}")
        if r["network_type"] == "thread":
            print(f"│  Thread role     : {r['thread_role']}")
            print(f"│  Thread network  : {r['thread_network_name']}  │  channel: {r['thread_channel']}")
            print(f"│  PAN ID          : {r['thread_pan_id']}  │  ext: {r['thread_ext_pan_id']}")
        for i, addr in enumerate(r["ipv6_addresses"] or ["—"]):
            label = "IPv6 addresses  " if i == 0 else "                "
            print(f"│  {label}  : {addr}")
        print("└")


def print_json(rows: list[dict]) -> None:
    export = [{k: v for k, v in r.items() if k != "_diag"} for r in rows]
    print(json.dumps(export, indent=2))


def print_csv(rows: list[dict]) -> None:
    """Write rows as CSV. Respects CSV_FILE: empty = stdout, otherwise write to file."""
    fields = [
        ("name",                "Name"),
        ("ha_device_id",        "HA Device ID"),
        ("area_id",             "Area"),
        ("manufacturer",        "Manufacturer"),
        ("model",               "Model"),
        ("sw_version",          "SW Version"),
        ("hw_version",          "HW Version"),
        ("matter_node_id",      "Matter Node ID"),
        ("matter_unique_id",    "Matter Unique ID"),
        ("fabric_id",           "Fabric ID"),
        ("fabric_label",        "Fabric Label"),
        ("network_type",        "Network Type"),
        ("available",           "Available"),
        ("mac_address",         "MAC Address"),
        ("thread_role",         "Thread Role"),
        ("thread_network_name", "Thread Network"),
        ("thread_channel",      "Thread Channel"),
        ("thread_pan_id",       "Thread PAN ID"),
        ("thread_ext_pan_id",   "Thread Ext PAN ID"),
        ("ipv6_addresses",      "IPv6 Addresses"),
    ]
    keys    = [f[0] for f in fields]
    headers = [f[1] for f in fields]

    def cell(row: dict, key: str) -> str:
        v = row.get(key)
        if isinstance(v, list):
            return "; ".join(str(x) for x in v)
        if isinstance(v, bool):
            return "yes" if v else "no"
        return "" if v is None else str(v)

    buf = io.StringIO()
    writer = csv.writer(buf, dialect="excel")
    writer.writerow(headers)
    for row in rows:
        writer.writerow([cell(row, k) for k in keys])

    csv_text = buf.getvalue()

    if CSV_FILE:
        with open(CSV_FILE, "w", encoding="utf-8-sig", newline="") as f:
            f.write(csv_text)
        print(f"CSV written to {CSV_FILE}  ({len(rows)} rows)", file=sys.stderr)
    else:
        sys.stdout.write(csv_text)


def print_debug(rows: list[dict], thread_raw: Any = None) -> None:
    """Dump raw diagnostics + thread dataset raw so you can see actual key names."""
    if thread_raw is not None:
        print("\n" + "=" * 60)
        print("RAW thread/list_datasets response:")
        print(json.dumps(thread_raw, indent=2, default=str))

    for r in rows:
        print(f"\n{'=' * 60}")
        print(f"Device : {r['name']}")
        print(f"Unique : {r['matter_unique_id']}  →  parsed node_id={r['matter_node_id']}")
        print("Raw diagnostics:")
        print(json.dumps(r["_diag"], indent=2, default=str))


# ── main ──────────────────────────────────────────────────────────────────────

async def run() -> None:
    ws_url = HA_URL.rstrip("/").replace("https://", "wss://").replace("http://", "ws://")
    debug  = (OUTPUT == "debug")

    print("Connecting to Home Assistant…", file=sys.stderr)
    async with HAWebSocket(ws_url, HA_TOKEN, debug=debug) as ha:
        matter_devices, device_diagnostics, thread_datasets = await collect(ha)

        # Keep raw thread result for debug output
        thread_raw = await ha.try_call({"type": "thread/list_datasets"}, default=[]) \
            if debug else None

    rows = build_rows(matter_devices, device_diagnostics, thread_datasets)

    if OUTPUT == "debug":
        print_debug(rows, thread_raw)
    elif OUTPUT == "json":
        print_json(rows)
    elif OUTPUT == "csv":
        print_csv(rows)
    else:
        print_table(rows)


if __name__ == "__main__":
    asyncio.run(run())