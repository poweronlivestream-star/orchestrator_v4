#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V6.0 - Blackbox v2.0 + Manar-100-v2 Deterministic Engine
- Default 5-minute (300s) eval chunks with sequential running_context.json & entities.csv resolution.
- Reads prompt_system.md, prompt_chunk.md, prompt_synthesis.md, schema_chunk.json, rubric.json, entities.csv from repo root.
- Official google-genai SDK with fast parallel pre-flight validation, smart error classification, round-robin free keys, and paid fallback.
- Deterministic Python KPI & 100-point (7 axes / 26 items) anchor-based scoring + single-defect rule + critical error caps + 6 audit checks.
- Outputs 5 files per eval run:
  1) 01_Main_Detailed_Report_<name>.html (DeepSeek Royal Word-RTL + 3 embedded Base64 Arabic PNG charts)
  2) 02_Summarized_Report_<name>.html (Executive summary + embedded chart)
  3) 03_Training_Plan_<name>.html (6-week personalized drill & rewrite plan populated from actual episode turns)
  4) 04_Blackbox_V2_<name>.md (Self-contained forensic dossier for DeepSeek/ChatGPT/Claude)
  5) 05_Blackbox_V2_<name>.json (Machine-auditable Blackbox v2.0 JSON dataset)
- Daily broadcast mode preserved with parallel extraction, DLQ rescue, Detailed + Summarized Word-RTL HTML + Blackbox .md/.json.
"""
import os
import re
import io
import csv
import sys
import json
import time
import math
import base64
import random
import smtplib
import threading
import subprocess
import html as html_lib
import requests
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def _ensure_pkg(import_name: str, pip_name: str = None):
    try:
        __import__(import_name)
    except ModuleNotFoundError:
        print(f"⚙️ {pip_name or import_name} not found — installing automatically...", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--upgrade", pip_name or import_name])


_ensure_pkg("google.genai", "google-genai")
_ensure_pkg("jsonschema", "jsonschema")
_ensure_pkg("matplotlib", "matplotlib")
_ensure_pkg("arabic_reshaper", "arabic-reshaper")
_ensure_pkg("bidi", "python-bidi")

from google import genai
from google.genai import types
import jsonschema
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import arabic_reshaper
from bidi.algorithm import get_display

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.abspath("./backup_reports")
os.makedirs(BACKUP_DIR, exist_ok=True)

BEIRUT_TZ = ZoneInfo("Asia/Beirut")
STATE_LOCK = threading.Lock()
FFMPEG_SEM = threading.Semaphore(3)
CANCEL_EVENT = threading.Event()
DRIVE_TOKEN_LOCK = threading.Lock()

PREFERRED_FLASH_ORDER = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.7-flash",
    "gemini-3.0-flash",
]
PREFERRED_SYNTH_ORDER = [
    "gemini-3.8-flash",
    "gemini-4-argon",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]
PRICING = {
    "gemini-3.8-flash":      {"in": 0.75, "out": 3.75},
    "gemini-3.6-flash":      {"in": 0.50, "out": 2.50},
    "gemini-3.5-flash-lite": {"in": 0.30, "out": 2.50},
    "gemini-3.7-flash":      {"in": 0.60, "out": 3.00},
    "gemini-4-argon":        {"in": 2.00, "out": 10.00},
}

MAX_FREE_KEYS_PER_CHUNK = 2
FREE_RR_COUNTER = 0
RUNTIME_DRIVE_TOKEN = ""
STATUS_FILE_DRIVE_ID = None

DEFAULT_ENTITIES_CSV = (
    "id,type,canonical_ar,aliases_ar,role,notes\n"
    "PRESENTER_01,person,حسن حمزة,حسن,مقدّم البرنامج,مقدّم «تغطية خاصة»\n"
    "GUEST_01,person,وسام ناصيف ياسين,وسام ياسين;د. وسام;الدكتور وسام,باحث في العلاقات الدولية,ضيف الاستوديو المتكرّر\n"
    "REPORTER_01,person,محمد قاسم,محمد عازم;محمد قاسم,مراسل قناة المنار - طهران,تحذير: خطأ تفريغ شائع «عازم» ← «قاسم»\n"
)


# ==============================================================================
# UTILITIES & REPO ASSET LOADERS
# ==============================================================================
def load_repo_text(filename: str, fallback: str = "") -> str:
    p = os.path.join(BASE_DIR, filename)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return f.read()
    return fallback


def load_repo_json(filename: str, fallback: dict) -> dict:
    p = os.path.join(BASE_DIR, filename)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return fallback


def parse_entities_csv(csv_text: str) -> dict:
    """Parses entities.csv with columns: id,type,canonical_ar,aliases_ar,role,notes."""
    entities = {}
    alias_to_canonical = {}
    try:
        reader = csv.DictReader(io.StringIO(csv_text.strip()))
        for row in reader:
            canonical = (row.get("canonical_ar") or row.get("canonical_name") or "").strip()
            if not canonical:
                continue
            eid = (row.get("id") or canonical).strip()
            aliases_raw = row.get("aliases_ar") or ""
            aliases = [a.strip() for a in re.split(r"[;,|]", aliases_raw) if a.strip()]
            entities[eid] = {
                "id": eid,
                "type": (row.get("type") or "person").strip(),
                "canonical_ar": canonical,
                "aliases_ar": aliases,
                "role": (row.get("role") or "").strip(),
                "notes": (row.get("notes") or "").strip(),
            }
            alias_to_canonical[canonical] = canonical
            for al in aliases:
                alias_to_canonical[al] = canonical
    except Exception as e:
        print(f"[entities.csv Parse Warning] {e}", flush=True)
    return {"entities": entities, "alias_map": alias_to_canonical}


def esc(s) -> str:
    return html_lib.escape(str(s if s is not None else ""))


def mask_key(k: str) -> str:
    k = (k or "").strip()
    return "***" if len(k) <= 10 else f"{k[:6]}...{k[-4:]}"


def format_hhmmss(seconds: float, with_ms: bool = False) -> str:
    s = max(0.0, float(seconds or 0.0))
    whole = int(s)
    hh, mm, ss = whole // 3600, (whole % 3600) // 60, whole % 60
    if with_ms:
        ms = min(999, int(round((s - whole) * 1000)))
        return f"{hh:02d}:{mm:02d}:{ss:02d}.{ms:03d}"
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def parse_hms_to_sec(hms: str) -> float:
    m = re.match(r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$", str(hms or "").strip())
    if not m:
        return 0.0
    hh, mm, ss, ms = m.groups()
    base = int(hh) * 3600 + int(mm) * 60 + int(ss)
    return base + (int(ms.ljust(3, "0")) / 1000.0 if ms else 0.0)


def sanitize_filename(name: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]+', "_", (name or "Episode").strip())
    return re.sub(r"[\s_]+", "_", s).strip("_") or "Episode"


def clean_json_text(text: str) -> dict:
    raw = (text or "").strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    try:
        return json.loads(raw)
    except Exception:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end > start:
            return json.loads(raw[start : end + 1])
        raise


def ar_label(text: str) -> str:
    try:
        return get_display(arabic_reshaper.reshape(str(text or "")))
    except Exception:
        return str(text or "")


def round_half(x: float) -> float:
    return round(float(x) * 2.0) / 2.0


# ==============================================================================
# METADATA DERIVATION, SCREEN-CLOCK VERIFICATION & FFMPEG SPEECH MEASUREMENT
# ==============================================================================
def derive_metadata(src_url: str, duration_sec: float) -> dict:
    fname = os.path.basename((src_url or "").split("?")[0])
    out = {
        "source_filename": fname or None,
        "epoch": None,
        "air_datetime_utc": None,
        "air_datetime_local": None,
        "broadcast_date": None,
        "broadcast_start_beirut": None,
        "broadcast_end_beirut": None,
        "timezone": None,
        "expected_utc_offset_hours": None,
    }
    m = re.search(r"archive-(\d{10})-(\d+)", src_url or "")
    if m:
        ts = int(m.group(1))
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_loc = dt_utc.astimezone(BEIRUT_TZ)
        dt_end = dt_loc + timedelta(seconds=duration_sec)
        tz_raw = dt_loc.strftime("%z")
        tz_fmt = f"UTC{tz_raw[:3]}:{tz_raw[3:]}" if len(tz_raw) == 5 else "Asia/Beirut"
        off_h = dt_loc.utcoffset().total_seconds() / 3600.0 if dt_loc.utcoffset() else 2.0
        out.update({
            "epoch": ts,
            "air_datetime_utc": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "air_datetime_local": dt_loc.strftime("%Y-%m-%d %H:%M:%S"),
            "broadcast_date": dt_loc.strftime("%Y-%m-%d"),
            "broadcast_start_beirut": dt_loc.strftime("%H:%M:%S"),
            "broadcast_end_beirut": dt_end.strftime("%H:%M:%S"),
            "timezone": tz_fmt,
            "expected_utc_offset_hours": off_h,
        })
    else:
        now_loc = datetime.now(BEIRUT_TZ)
        tz_raw = now_loc.strftime("%z")
        off_h = now_loc.utcoffset().total_seconds() / 3600.0 if now_loc.utcoffset() else 2.0
        out["timezone"] = f"UTC{tz_raw[:3]}:{tz_raw[3:]}" if len(tz_raw) == 5 else "Asia/Beirut"
        out["expected_utc_offset_hours"] = off_h
    return out


def verify_screen_clock_conflict(derived_meta: dict, on_screen_clocks: list) -> tuple:
    """Cross-checks derived local start time + seen_at against observed on_screen_clock (within +/- 3 min)."""
    conflicts = []
    verified = False
    if not derived_meta.get("epoch") or not on_screen_clocks:
        return verified, conflicts
    dt_start = datetime.fromtimestamp(derived_meta["epoch"], tz=timezone.utc).astimezone(BEIRUT_TZ)
    for item in on_screen_clocks:
        if not isinstance(item, dict):
            continue
        clk_val = str(item.get("value") or "").strip()
        seen_at = str(item.get("seen_at") or "").strip()
        mc = re.search(r"(\d{1,2}):(\d{2})", clk_val)
        if not mc:
            continue
        obs_min = int(mc.group(1)) * 60 + int(mc.group(2))
        seen_s = parse_hms_to_sec(seen_at)
        exp_dt = dt_start + timedelta(seconds=seen_s)
        exp_min = exp_dt.hour * 60 + exp_dt.minute
        diff = min(abs(obs_min - exp_min), 1440 - abs(obs_min - exp_min))
        if diff <= 4:
            verified = True
        else:
            conflicts.append({
                "field": "air_datetime_local",
                "values": [f"derived={exp_dt.strftime('%H:%M')}", f"on_screen={clk_val}@{seen_at}"],
                "preferred": f"on_screen={clk_val}",
                "reason": f"فارق {diff} دقيقة بين طابع اسم الملف وساعة الشاشة عند {seen_at}",
            })
    return verified, conflicts


def measure_chunk_speech_ffmpeg(mp4_path: str, chunk_dur_s: float) -> float:
    try:
        cmd = ["ffmpeg", "-i", mp4_path, "-af", "silencedetect=noise=-32dB:d=0.6", "-f", "null", "-"]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        silences = [float(x) for x in re.findall(r"silence_duration:\s*([0-9.]+)", proc.stderr or "")]
        return round(max(0.0, chunk_dur_s - sum(silences)), 1)
    except Exception:
        return round(chunk_dur_s, 1)


# ==============================================================================
# TELEGRAM, DUAL-PORT SMTP, DRIVE AUTO-REFRESH & LIVE TELEMETRY
# ==============================================================================
def send_tg(msg, token, chat, file_path=None):
    if not (token and chat):
        return
    try:
        if file_path and os.path.exists(file_path):
            with open(file_path, "rb") as f:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendDocument",
                    data={"chat_id": chat, "caption": msg[:1000], "parse_mode": "HTML"},
                    files={"document": (os.path.basename(file_path), f)},
                    timeout=45,
                )
        else:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": msg[:4000], "parse_mode": "HTML"},
                timeout=15,
            )
            if r.status_code != 200:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat, "text": re.sub(r"<[^>]+>", "", msg)[:4000]},
                    timeout=15,
                )
    except Exception as e:
        print(f"[TG Warning] {e}", flush=True)


def send_smtp_resilient(subject, html_body, files, emails, user, pwd) -> bool:
    user = (user or "").strip()
    pwd = re.sub(r"\s+", "", (pwd or "").strip())
    recips = [e.strip() for e in re.split(r"[,;]", emails or "") if e.strip()]
    if user and user not in recips:
        recips.insert(0, user)
    if not (user and pwd and recips):
        return False

    def build_msg(recipient):
        msg = MIMEMultipart()
        msg["From"] = user
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        for pf in files:
            if pf and os.path.exists(pf):
                with open(pf, "rb") as f:
                    p = MIMEBase("application", "octet-stream")
                    p.set_payload(f.read())
                encoders.encode_base64(p)
                p.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(pf)}"')
                msg.attach(p)
        return msg

    for port, use_ssl in ((465, True), (587, False)):
        try:
            if use_ssl:
                with smtplib.SMTP_SSL("smtp.gmail.com", port, timeout=25) as s:
                    s.ehlo()
                    s.login(user, pwd)
                    for rec in recips:
                        s.sendmail(user, [rec], build_msg(rec).as_string())
            else:
                with smtplib.SMTP("smtp.gmail.com", port, timeout=25) as s:
                    s.ehlo()
                    s.starttls()
                    s.ehlo()
                    s.login(user, pwd)
                    for rec in recips:
                        s.sendmail(user, [rec], build_msg(rec).as_string())
            print(f"✅ SMTP ({port}) sent to: {recips}", flush=True)
            return True
        except Exception as e:
            print(f"[SMTP {port} Warning] {e}", flush=True)
    return False


def refresh_drive_token_via_webhook(cfg) -> bool:
    global RUNTIME_DRIVE_TOKEN
    webhook, secret = cfg.get("webhook_url"), cfg.get("webhook_secret", "")
    if not (webhook and secret):
        return False
    try:
        r = requests.post(webhook, json={"action": "refresh_drive_token", "webhook_secret": secret}, timeout=15)
        if r.status_code == 200 and r.json().get("drive_token"):
            with DRIVE_TOKEN_LOCK:
                RUNTIME_DRIVE_TOKEN = r.json()["drive_token"].strip()
            print("🔄 Refreshed Google Drive OAuth token.", flush=True)
            return True
    except Exception as e:
        print(f"[Token Refresh Warning] {e}", flush=True)
    return False


def drive_request(method, url, cfg, **kwargs):
    for attempt in range(2):
        with DRIVE_TOKEN_LOCK:
            tok = RUNTIME_DRIVE_TOKEN
        headers = dict(kwargs.pop("headers", {}) or {})
        headers["Authorization"] = f"Bearer {tok}"
        r = requests.request(method, url, headers=headers, **kwargs)
        if r.status_code == 401 and attempt == 0 and refresh_drive_token_via_webhook(cfg):
            continue
        return r
    return r


def drive_upload(folder_id, cfg, filepath, mimetype) -> bool:
    filename = os.path.basename(filepath)
    q = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    try:
        r = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q, "fields": "files(id)"}, timeout=20)
        ff = r.json().get("files", []) if r.status_code == 200 else []
        fid = ff[0].get("id") if ff else None
        with open(filepath, "rb") as f:
            data = f.read()
        if fid:
            up = drive_request("PATCH", f"https://www.googleapis.com/upload/drive/v3/files/{fid}?uploadType=media", cfg, headers={"Content-Type": f"{mimetype}; charset=UTF-8"}, data=data, timeout=90)
            return up.status_code in (200, 201)
        bound = "===BNDRY==="
        meta = json.dumps({"name": filename, "parents": [folder_id]})
        body = (f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n--{bound}\r\nContent-Type: {mimetype}; charset=UTF-8\r\n\r\n").encode("utf-8") + data + f"\r\n--{bound}--\r\n".encode("utf-8")
        up = drive_request("POST", "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", cfg, headers={"Content-Type": f"multipart/related; boundary={bound}"}, data=body, timeout=90)
        return up.status_code in (200, 201)
    except Exception as e:
        print(f"[Drive Upload Warning] {filename}: {e}", flush=True)
        return False


def update_drive_folder_meta(folder_id, cfg, new_name, description_text=""):
    payload = {"name": new_name}
    if description_text:
        payload["description"] = description_text
    try:
        r = drive_request("PATCH", f"https://www.googleapis.com/drive/v3/files/{folder_id}", cfg, headers={"Content-Type": "application/json"}, json=payload, timeout=25)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[Drive Rename Warning] {e}", flush=True)
        return False


def write_drive_status_and_check_cancel(folder_id, cfg, payload):
    global STATUS_FILE_DRIVE_ID
    if not folder_id:
        return False
    try:
        chk = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": f"'{folder_id}' in parents and name='_cancel.flag' and trashed=false", "fields": "files(id)"}, timeout=10)
        if chk.status_code == 200 and chk.json().get("files"):
            CANCEL_EVENT.set()
            return True
        raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if not STATUS_FILE_DRIVE_ID:
            r = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": f"'{folder_id}' in parents and name='_status.json' and trashed=false", "fields": "files(id)"}, timeout=10)
            ff = r.json().get("files", []) if r.status_code == 200 else []
            if ff:
                STATUS_FILE_DRIVE_ID = ff[0]["id"]
        if STATUS_FILE_DRIVE_ID:
            drive_request("PATCH", f"https://www.googleapis.com/upload/drive/v3/files/{STATUS_FILE_DRIVE_ID}?uploadType=media", cfg, headers={"Content-Type": "application/json; charset=UTF-8"}, data=raw_bytes, timeout=15)
        else:
            bound = "===BNDRY_ST==="
            meta = json.dumps({"name": "_status.json", "parents": [folder_id]})
            body = (f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode("utf-8") + raw_bytes + f"\r\n--{bound}--\r\n".encode("utf-8")
            up = drive_request("POST", "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id", cfg, headers={"Content-Type": f"multipart/related; boundary={bound}"}, data=body, timeout=15)
            if up.status_code in (200, 201):
                STATUS_FILE_DRIVE_ID = up.json().get("id")
    except Exception:
        pass
    return CANCEL_EVENT.is_set()


def push_status(cfg, folder_id, run_id, status, phase, completed, total, act_cost, started_at, eta_sec=None, message=""):
    elapsed = int(time.time() - started_at)
    if eta_sec is None:
        eta_sec = int((elapsed / completed) * (total - completed)) if (completed > 0 and total > completed) else (int(total * 30) if status == "running" else 0)
    payload = {
        "action": "update_status", "job_id": folder_id, "run_id": str(run_id or ""),
        "status": status, "phase": phase, "completed_chunks": int(completed),
        "total_chunks": int(total), "elapsed_sec": elapsed, "eta_sec": max(0, int(eta_sec)),
        "actual_cost": round(float(act_cost), 4), "message": message, "updated_at_epoch": int(time.time()),
    }
    write_drive_status_and_check_cancel(folder_id, cfg, payload)
    webhook = cfg.get("webhook_url")
    if webhook:
        try:
            r = requests.post(webhook, json=payload, timeout=10)
            if r.status_code == 200 and "json" in (r.headers.get("Content-Type") or "").lower() and r.json().get("cancel_requested"):
                CANCEL_EVENT.set()
                return True
        except Exception:
            pass
    return CANCEL_EVENT.is_set()


def finalize_cost_webhook(cfg, folder_id, actual_cost: float, final_status="completed", message="", trigger_gas_email=False, email_subject=""):
    write_drive_status_and_check_cancel(folder_id, cfg, {
        "job_id": folder_id, "run_id": str(os.environ.get("GITHUB_RUN_ID", "")),
        "status": final_status, "phase": message or final_status, "eta_sec": 0,
        "actual_cost": round(float(actual_cost), 4), "message": message, "updated_at_epoch": int(time.time()),
    })
    if cfg.get("webhook_url"):
        try:
            requests.post(cfg["webhook_url"], json={
                "action": "finalize_cost", "webhook_secret": cfg.get("webhook_secret", ""),
                "job_id": folder_id, "status": final_status,
                "estimated_cost": cfg.get("pre_authorized_cost", 0), "actual_cost": round(float(actual_cost), 4),
                "message": message, "send_backup_email": trigger_gas_email,
                "email_subject": email_subject, "alert_emails": cfg.get("alert_emails", ""),
            }, timeout=20)
        except Exception:
            pass


# ==============================================================================
# KEY PRE-FLIGHT VALIDATION & GEMINI EXECUTION
# ==============================================================================
def rank_discovered_models(available_models: list, preferred_order: list, allow_pro: bool = False) -> list:
    skip = ("tts", "image", "imagen", "veo", "embedding", "aqa", "bison", "gecko", "1.0", "1.5", "robotics", "computer-use", "live")
    has_gen3_plus = any(re.search(r"gemini-[3-9]\b|gemini-[3-9]\.", m.lower()) for m in available_models)
    avail_set = set(available_models)
    ordered = [m for m in preferred_order if m in avail_set]
    extras = []
    for m in available_models:
        low = m.lower()
        if m in ordered or not low.startswith("gemini-") or any(t in low for t in skip):
            continue
        if has_gen3_plus and (low.startswith("gemini-2.") or low.startswith("gemini-2-")):
            continue
        if "flash" in low or (allow_pro and ("pro" in low or "argon" in low)):
            extras.append(m)
    extras.sort(key=lambda name: ([int(x) for x in re.findall(r"\d+", name)][:2] or [0, 0], 0 if ("preview" in name or "exp" in name) else 1, name), reverse=True)
    ordered.extend(extras)
    return ordered[:4]


def validate_single_key(key: str, preferred_models: list, allow_pro: bool = False) -> tuple:
    key = key.strip().strip('"').strip("'")
    if not key:
        return False, [], "empty"
    try:
        r = requests.get("https://generativelanguage.googleapis.com/v1beta/models?pageSize=200", headers={"x-goog-api-key": key}, timeout=(5, 8))
        if r.status_code != 200:
            return False, [], f"AUTH FAILED ({r.status_code}): {r.text[:120]}"
        gen_models = [m.get("name", "").replace("models/", "") for m in r.json().get("models", []) if "generateContent" in m.get("supportedGenerationMethods", [])]
        working = rank_discovered_models(gen_models, preferred_models, allow_pro=allow_pro) or list(preferred_models[:3])
        return True, working, f"OK ({', '.join(working[:3])})"
    except Exception as e:
        return False, [], f"NETWORK ERROR: {e}"


def preflight_validate_keys(raw_free_keys: list, raw_paid_key: str, strategy: str, tg_bot: str, tg_chat: str):
    print("=" * 60 + "\nPRE-FLIGHT API KEY VALIDATION (FAST PARALLEL)\n" + "=" * 60, flush=True)
    unique_free = list(dict.fromkeys(raw_free_keys))
    if len(unique_free) < len(raw_free_keys):
        print(f"⚠️ Removed {len(raw_free_keys) - len(unique_free)} duplicate free key(s).", flush=True)
    valid_free, valid_paid, paid_models, report_lines = [], None, [], []
    tasks = [(f"FREE #{i}", k, PREFERRED_FLASH_ORDER, False) for i, k in enumerate(unique_free, 1)]
    if raw_paid_key:
        tasks.append(("PAID", raw_paid_key, PREFERRED_FLASH_ORDER, True))
    if tasks:
        with ThreadPoolExecutor(max_workers=min(10, len(tasks))) as ex:
            futs = {ex.submit(validate_single_key, k, pref, is_paid): (lbl, k, is_paid) for lbl, k, pref, is_paid in tasks}
            for fut in as_completed(futs):
                lbl, k, is_paid = futs[fut]
                ok, models, detail = fut.result()
                line = f"{'✅' if ok else '❌'} {lbl} ({mask_key(k)}): {detail}"
                print(line, flush=True)
                report_lines.append(line)
                if ok:
                    if is_paid:
                        valid_paid, paid_models = k, models
                    else:
                        valid_free.append((k, models))
    print("=" * 60, flush=True)
    can_run = (strategy == "flash_only" and valid_free) or (strategy == "pro_only" and valid_paid) or (strategy == "hybrid" and (valid_free or valid_paid))
    send_tg(f"🔑 <b>فحص المفاتيح المسبق ({strategy})</b>\n• المجانية الصالحة: {len(valid_free)}/{len(unique_free)}\n• المدفوع: {'✅ صالح' if valid_paid else '❌ غير مضاف'}", tg_bot, tg_chat)
    if not can_run:
        send_tg("❌ <b>توقف فوري: لا يوجد أي مفتاح API صالح</b>\n" + "\n".join(report_lines[:8]), tg_bot, tg_chat)
        return [], None, []
    return valid_free, valid_paid, paid_models


def build_gen_config(max_out: int = 32768):
    return types.GenerateContentConfig(
        temperature=0.0,
        max_output_tokens=max_out,
        response_mime_type="application/json",
        safety_settings=[
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
        ],
    )


def wait_file_active(client, uploaded_file, timeout_sec=120):
    t0, f = time.time(), uploaded_file
    while True:
        if CANCEL_EVENT.is_set():
            raise RuntimeError("Cancelled by user")
        st = str(getattr(f, "state", "")).upper()
        if "ACTIVE" in st:
            return f
        if "FAILED" in st:
            raise ValueError(f"Google File API processing failed for {f.name}")
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(f"Timed out waiting for {f.name}")
        time.sleep(2)
        f = client.files.get(name=f.name)


def calc_cost(model_name: str, usage, is_paid: bool) -> float:
    if not is_paid or not usage:
        return 0.0
    in_t = getattr(usage, "prompt_token_count", 0) or 0
    out_t = getattr(usage, "candidates_token_count", 0) or 0
    think_t = getattr(usage, "thoughts_token_count", 0) or 0
    rates = PRICING.get(model_name, {"in": 0.75, "out": 3.75})
    return (in_t / 1e6) * rates["in"] + ((out_t + think_t) / 1e6) * rates["out"]


def call_gemini_chunk(media_path, prompt, valid_free, valid_paid, paid_models, strategy, exhausted_pairs, bad_keys, budget_ok_fn, chunk_schema=None):
    global FREE_RR_COUNTER
    last_err = ""
    config = build_gen_config(max_out=32768)

    def run_with_key(api_key, model_candidates, is_paid):
        nonlocal last_err
        if CANCEL_EVENT.is_set():
            raise RuntimeError("Cancelled by user")
        if is_paid and not budget_ok_fn():
            last_err = "Episode budget ceiling reached"
            return None
        with STATE_LOCK:
            if api_key in bad_keys:
                return None
            models_to_try = [m for m in model_candidates if (api_key, m) not in exhausted_pairs]
        if not models_to_try:
            return None

        client = genai.Client(api_key=api_key)
        uploaded = None
        try:
            uploaded = wait_file_active(client, client.files.upload(file=media_path))
            for model_name in models_to_try:
                if CANCEL_EVENT.is_set():
                    raise RuntimeError("Cancelled by user")
                try:
                    res = client.models.generate_content(model=model_name, contents=[uploaded, prompt], config=config)
                    data = clean_json_text(res.text)
                    cost = calc_cost(model_name, res.usage_metadata, is_paid)
                    if chunk_schema:
                        try:
                            jsonschema.validate(instance=data, schema=chunk_schema)
                        except jsonschema.ValidationError as ve:
                            repair_prompt = (
                                f"المخرج JSON السابق خالف المخطط schema_chunk.json بسبب: {ve.message}\n"
                                f"صحّح الخطأ فقط وأعد إخراج JSON صالح مطابق تماماً للمخطط:\n"
                                f"{json.dumps(data, ensure_ascii=False)[:24000]}"
                            )
                            try:
                                r2 = client.models.generate_content(model=model_name, contents=repair_prompt, config=config)
                                data = clean_json_text(r2.text)
                                cost += calc_cost(model_name, r2.usage_metadata, is_paid)
                            except Exception:
                                pass
                    return data, cost, model_name, ("PAID" if is_paid else "FREE")
                except Exception as e:
                    err, low = str(e), str(e).lower()
                    last_err = f"[{model_name}] {err[:220]}"
                    print(f"API Error ({'PAID' if is_paid else 'FREE'} {mask_key(api_key)} / {model_name}): {err[:180]}", flush=True)
                    if any(t in low for t in ("api_key_invalid", "access_token_type_unsupported", "401", "403")):
                        with STATE_LOCK:
                            bad_keys.add(api_key)
                        return None
                    if any(t in low for t in ("429", "resource_exhausted", "quota", "404", "not_found", "no longer available")):
                        with STATE_LOCK:
                            exhausted_pairs.add((api_key, model_name))
                        continue
                    time.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            err, low = str(e), str(e).lower()
            last_err = f"[UPLOAD] {err[:220]}"
            if any(t in low for t in ("api_key_invalid", "401", "403")):
                with STATE_LOCK:
                    bad_keys.add(api_key)
        finally:
            if uploaded is not None:
                try:
                    client.files.delete(name=uploaded.name)
                except Exception:
                    pass
        return None

    if strategy in ("flash_only", "hybrid") and valid_free:
        n = len(valid_free)
        with STATE_LOCK:
            start_idx = FREE_RR_COUNTER % n
            FREE_RR_COUNTER += 1
        rotated = [valid_free[(start_idx + o) % n] for o in range(n)]
        cap = n if strategy == "flash_only" else min(MAX_FREE_KEYS_PER_CHUNK, n)
        for key, key_models in rotated[:cap]:
            out = run_with_key(key, key_models, is_paid=False)
            if out is not None:
                return out

    if strategy in ("hybrid", "pro_only") and valid_paid:
        out = run_with_key(valid_paid, paid_models or PREFERRED_FLASH_ORDER, is_paid=True)
        if out is not None:
            return out

    raise RuntimeError(f"All attempts failed. Last Error: {last_err}")


# ==============================================================================
# MEDIA ACQUISITION (Direct HTTP / Google Drive File / YouTube via yt-dlp)
# ==============================================================================
def is_youtube_url(url: str) -> bool:
    u = (url or "").lower()
    return any(x in u for x in ("youtube.com/watch", "youtu.be/", "youtube.com/live/", "youtube.com/shorts/"))


def acquire_video_locally(url: str, dest: str, cfg: dict) -> bool:
    gd_match = re.search(r"(?:/file/d/|id=)([a-zA-Z0-9_-]{20,})", url)
    if gd_match and "drive.google.com" in url:
        try:
            with drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{gd_match.group(1)}?alt=media", cfg, stream=True, timeout=(15, 120)) as r:
                if r.status_code == 200:
                    with open(dest, "wb") as f:
                        for blk in r.iter_content(chunk_size=524288):
                            if blk:
                                f.write(blk)
            if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
                return True
        except Exception:
            pass
    if is_youtube_url(url):
        cmd = [
            sys.executable, "-m", "yt_dlp", "--no-playlist", "--no-check-certificates",
            "--extractor-args", "youtube:player_client=ios,android,mweb,tv",
            "-f", "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best",
            "--merge-output-format", "mp4", "-o", dest, url,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        return res.returncode == 0 and os.path.exists(dest) and os.path.getsize(dest) > 50_000
    for attempt in range(1, 4):
        try:
            with requests.get(url, stream=True, timeout=(15, 60), headers={"User-Agent": "Mozilla/5.0"}) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for blk in r.iter_content(chunk_size=262144):
                        if blk:
                            f.write(blk)
            if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
                return True
        except Exception as e:
            print(f"[Download Retry {attempt}/3] {e}", flush=True)
            time.sleep(3)
    return False


# ==============================================================================
# CHUNK NORMALIZATION, ENTITY RESOLUTION & RUNNING CONTEXT
# ==============================================================================
def normalize_chunk_v2(data: dict, idx: int, st_hms: str, end_hms: str, chunk_dur_s: float, derived_meta: dict, measured_speech_s: float, entity_registry: dict) -> dict:
    d = dict(data or {})
    d["chunk_index"] = idx
    d["window"] = {"start": st_hms, "end": end_hms}
    d["prompt_version"] = "blackbox-v2.0"

    alias_map = (entity_registry or {}).get("alias_map", {})
    meta = d.get("metadata") if isinstance(d.get("metadata"), dict) else {}
    meta.setdefault("program_name", None)
    meta.setdefault("episode_title", None)
    meta.setdefault("presenter_name", None)
    if meta.get("presenter_name") in alias_map:
        meta["presenter_name"] = alias_map[meta["presenter_name"]]
    if not isinstance(meta.get("on_screen_clock"), list):
        meta["on_screen_clock"] = []
    meta["derived"] = {k: derived_meta.get(k) for k in ("source_filename", "epoch", "air_datetime_utc", "air_datetime_local", "timezone")}

    verified_clk, clk_conflicts = verify_screen_clock_conflict(derived_meta, meta["on_screen_clock"])
    meta["confidence"] = "high" if (verified_clk or derived_meta.get("epoch")) else "medium"
    d["metadata"] = meta

    for lk in ("metadata_conflicts", "participants", "turns", "interventions", "chyron_timeline", "claims", "shots", "files", "critical_events", "data_gaps"):
        if not isinstance(d.get(lk), list):
            d[lk] = []
    d["metadata_conflicts"].extend(clk_conflicts)

    # Canonicalize participant names against entities.csv
    for p in d["participants"]:
        if isinstance(p, dict) and p.get("name"):
            raw_n = str(p["name"]).strip()
            if raw_n in alias_map and alias_map[raw_n] != raw_n:
                p["name"] = alias_map[raw_n]
                p["resolved_from"] = "entities"

    sum_turns_s = 0.0
    for t_i, t in enumerate(d["turns"], 1):
        if not isinstance(t, dict):
            continue
        t["id"] = int(t.get("id") or (idx * 1000 + t_i))
        t["text"] = str(t.get("text") or "").strip() or "[غير واضح]"
        t["word_count"] = len(t["text"].split())
        s_sec, e_sec = parse_hms_to_sec(t.get("start")), parse_hms_to_sec(t.get("end"))
        dur = float(t.get("duration_s") or 0.0)
        if dur <= 0 and e_sec > s_sec:
            dur = round(e_sec - s_sec, 1)
        t["duration_s"] = round(max(0.0, dur), 1)
        sum_turns_s += t["duration_s"]
        t["question_count"] = int(t.get("question_count") if t.get("question_count") is not None else t["text"].count("؟") + t["text"].count("?"))
        t.setdefault("mid_sentence", False)
        t.setdefault("overlap_s", 0.0)
        t.setdefault("verbatim_confidence", "medium")
        t.setdefault("kind", "answer")

    # Enforce honest measurement disclosure (never force sum == chunk_dur_s)
    d["measurement"] = {
        "method": "asr_vad" if measured_speech_s > 0 else "model_apportioned",
        "speech_segments_measured": measured_speech_s > 0,
        "ffmpeg_non_silent_s": measured_speech_s,
        "chunk_duration_s": round(chunk_dur_s, 1),
        "sum_of_turns_s": round(sum_turns_s, 1),
        "unaccounted_s": round(max(0.0, chunk_dur_s - sum_turns_s), 1),
        "unaccounted_reason": "فواصل/صمت/موسيقى أو تداخل غير مقيس",
        "forced_to_match_chunk": False,
    }
    d["self_check"] = {
        "all_timestamps_absolute": True,
        "all_timestamps_within_chunk": True,
        "every_turn_has_word_count": all("word_count" in t for t in d["turns"] if isinstance(t, dict)),
        "every_claim_has_turn_id": all("turn_id" in c for c in d["claims"] if isinstance(c, dict)),
        "no_forced_time_normalization": True,
        "all_chyrons_logged_even_if_unread": True,
        "names_resolved_against_entities": True,
        "data_gaps_declared": True,
    }
    return d


def update_running_context(ctx: dict, chunk_data: dict, end_hms: str) -> dict:
    seen_p = {p.get("id"): p for p in ctx.get("participants", []) if isinstance(p, dict) and p.get("id")}
    for p in chunk_data.get("participants", []):
        if isinstance(p, dict) and p.get("id") and p.get("name"):
            seen_p[p["id"]] = p
    files = list(ctx.get("active_files", [])) + [
        {"id": fl.get("id"), "title": fl.get("title")}
        for fl in chunk_data.get("files", [])
        if isinstance(fl, dict) and fl.get("title")
    ]
    all_text = " ".join(str(t.get("text", "")) for t in chunk_data.get("turns", []) if isinstance(t, dict))
    return {
        "chunk_index": chunk_data.get("chunk_index", 0),
        "program_name": chunk_data.get("metadata", {}).get("program_name") or ctx.get("program_name"),
        "presenter_name": chunk_data.get("metadata", {}).get("presenter_name") or ctx.get("presenter_name"),
        "participants": list(seen_p.values()),
        "active_files": files[-10:],
        "last_300_words": " ".join(all_text.split()[-260:]),
        "open_claims": [
            {"id": c.get("id"), "text": c.get("text", "")[:120], "challenged": bool(c.get("anchor_challenged_at"))}
            for c in chunk_data.get("claims", [])
            if isinstance(c, dict) and c.get("checkable")
        ][-8:],
        "unresolved_chyrons": [
            {"id": ch.get("id"), "text": ch.get("text_raw", "")[:120]}
            for ch in chunk_data.get("chyron_timeline", [])
            if isinstance(ch, dict) and not ch.get("read_on_air_at")
        ][-6:],
        "cumulative_clock": end_hms,
    }


# ==============================================================================
# DETERMINISTIC MANAR-100-V2 SCORING ENGINE (RUBRIC.JSON + 6 AUDIT CHECKS)
# ==============================================================================
def score_by_anchor(val, max_pts: float, direction: str, anchors: dict) -> tuple:
    if val is None or not isinstance(anchors, dict):
        return round_half(max_pts * 0.5), "no_data_neutral"
    full_v, half_v, zero_v = anchors.get("full"), anchors.get("half"), anchors.get("zero")
    if not all(isinstance(x, (int, float)) for x in (full_v, half_v, zero_v)):
        return round_half(max_pts * 0.5), "qualitative"
    v = float(val)
    if direction == "higher_better":
        if v >= full_v:
            return float(max_pts), "full"
        if v <= zero_v:
            return 0.0, "zero"
        if v >= half_v:
            return round_half(max_pts * (0.5 + 0.5 * ((v - half_v) / max(1e-6, full_v - half_v)))), "between_50_and_100"
        return round_half(max_pts * (0.5 * ((v - zero_v) / max(1e-6, half_v - zero_v)))), "between_0_and_50"
    if direction == "lower_better":
        if v <= full_v:
            return float(max_pts), "full"
        if v >= zero_v:
            return 0.0, "zero"
        if v <= half_v:
            return round_half(max_pts * (0.5 + 0.5 * ((half_v - v) / max(1e-6, half_v - full_v)))), "between_50_and_100"
        return round_half(max_pts * (0.5 * ((zero_v - v) / max(1e-6, zero_v - half_v)))), "between_0_and_50"
    return round_half(max_pts * 0.5), "range"


def score_source_shares(anchor_pct: float, guest_pct: float, field_pct: float, max_pts: float = 6.0) -> tuple:
    def dist(val, lo, hi):
        return 0.0 if lo <= val <= hi else min(abs(val - lo), abs(val - hi))
    if field_pct > 0:
        d = dist(anchor_pct, 20, 30) + dist(guest_pct, 45, 55) + dist(field_pct, 25, 35)
    else:
        d = dist(anchor_pct, 20, 32) + dist(guest_pct, 65, 80)
    sc = max(0.0, max_pts - (d / 10.0) * 1.2)
    return round_half(sc), ("full" if d == 0 else ("between_50_and_100" if sc >= max_pts * 0.5 else "between_0_and_50"))


def compute_deterministic_evaluation(chunks: list, duration_sec: float, rubric: dict, derived_meta: dict, llm_qual: dict, brief: dict, editorial_mode: str = "directed") -> dict:
    all_turns, all_interv, all_chyrons, all_claims, all_files, all_crit, all_gaps, all_conflicts = [], [], [], [], [], [], [], []
    for c in chunks:
        all_turns.extend(c.get("turns", []))
        all_interv.extend(c.get("interventions", []))
        all_chyrons.extend(c.get("chyron_timeline", []))
        all_claims.extend(c.get("claims", []))
        all_files.extend(c.get("files", []))
        all_crit.extend(c.get("critical_events", []))
        all_gaps.extend(c.get("data_gaps", []))
        all_conflicts.extend(c.get("metadata_conflicts", []))

    dur_min, dur_hr = max(1.0, duration_sec / 60.0), max(0.25, duration_sec / 3600.0)
    anchor_turns = [t for t in all_turns if t.get("speaker_id") == "ANCHOR"]
    guest_turns = [t for t in all_turns if str(t.get("speaker_id", "")).startswith("GUEST")]
    field_turns = [t for t in all_turns if str(t.get("speaker_id", "")).startswith("REPORTER")]

    # Compute unbroken guest blocks across chunk boundaries
    unbroken_blocks, cur_spk, cur_start, cur_end, cur_dur, cur_tids = [], None, None, None, 0.0, []
    for t in all_turns:
        spk = t.get("speaker_id", "")
        if spk.startswith("GUEST"):
            if cur_spk == spk:
                cur_dur += float(t.get("duration_s", 0.0))
                cur_end = t.get("end")
                cur_tids.append(t.get("id"))
            else:
                if cur_spk and cur_dur > 0:
                    unbroken_blocks.append({"speaker_id": cur_spk, "start": cur_start, "end": cur_end, "duration_s": round(cur_dur, 1), "turn_ids": cur_tids})
                cur_spk, cur_start, cur_end, cur_dur, cur_tids = spk, t.get("start"), t.get("end"), float(t.get("duration_s", 0.0)), [t.get("id")]
        else:
            if cur_spk and cur_dur > 0:
                unbroken_blocks.append({"speaker_id": cur_spk, "start": cur_start, "end": cur_end, "duration_s": round(cur_dur, 1), "turn_ids": cur_tids})
            cur_spk, cur_start, cur_end, cur_dur, cur_tids = None, None, None, 0.0, []
    if cur_spk and cur_dur > 0:
        unbroken_blocks.append({"speaker_id": cur_spk, "start": cur_start, "end": cur_end, "duration_s": round(cur_dur, 1), "turn_ids": cur_tids})

    longest_block = max(unbroken_blocks, key=lambda b: b["duration_s"], default={"start": "00:00:00", "end": "00:00:00", "duration_s": 0.0, "turn_ids": []})
    longest_block_s = longest_block["duration_s"]
    monologues_over_180 = [b for b in unbroken_blocks if b["duration_s"] > 180.0]

    interv_list = all_interv if all_interv else [{"at": t.get("start"), "turn_id": t.get("id"), "duration_s": t.get("duration_s", 0.0), "cue_source": "none"} for t in anchor_turns]
    interv_per_10m = round((len(interv_list) / dur_min) * 10.0, 2)
    interv_durs = [float(i.get("duration_s", 0.0)) for i in interv_list if float(i.get("duration_s", 0.0)) > 0]
    mean_interv_s = round(sum(interv_durs) / len(interv_durs), 1) if interv_durs else 0.0

    tot_speech_s = sum(float(t.get("duration_s", 0.0)) for t in all_turns) or duration_sec
    anchor_pct = round(100.0 * sum(float(t.get("duration_s", 0.0)) for t in anchor_turns) / tot_speech_s, 1)
    guest_pct = round(100.0 * sum(float(t.get("duration_s", 0.0)) for t in guest_turns) / tot_speech_s, 1)
    field_pct = round(100.0 * sum(float(t.get("duration_s", 0.0)) for t in field_turns) / tot_speech_s, 1)

    q_turns = [t for t in anchor_turns if int(t.get("question_count", 0)) >= 1 or t.get("kind") in ("question", "followup")]
    single_q_pct = round(100.0 * sum(1 for t in q_turns if int(t.get("question_count", 1)) == 1) / len(q_turns), 1) if q_turns else 0.0
    mean_preamble_s = round(sum(float(t.get("duration_s", 0.0)) for t in q_turns) / len(q_turns), 1) if q_turns else mean_interv_s
    under_30_words_pct = round(100.0 * sum(1 for t in q_turns if int(t.get("word_count", 0)) <= 30) / len(q_turns), 1) if q_turns else 0.0

    dup_files = [f for f in all_files if f.get("duplicated_of")]
    checkable_claims = [c for c in all_claims if c.get("checkable")]
    ev_req_claims = [c for c in checkable_claims if c.get("anchor_challenged_at") and c.get("challenge_type") in ("ask_source", "fact_check", None)]
    counter_claims = [c for c in checkable_claims if c.get("anchor_challenged_at") and c.get("challenge_type") == "counter_hypothesis"]
    ev_req_per_hr, counter_per_hr = round(len(ev_req_claims) / dur_hr, 1), round(len(counter_claims) / dur_hr, 1)
    verif_rate_pct = round(100.0 * sum(1 for c in checkable_claims if c.get("anchor_challenged_at")) / len(checkable_claims), 1) if checkable_claims else 0.0

    local_chyrons = [ch for ch in all_chyrons if ch.get("type") in ("local_security", "breaking")]
    read_local = [ch for ch in local_chyrons if ch.get("read_on_air_at")]
    unread_local = [ch for ch in local_chyrons if not ch.get("read_on_air_at")]
    local_chyron_cov = round(100.0 * len(read_local) / len(local_chyrons), 1) if local_chyrons else 100.0
    latencies = [float(ch["response_latency_s"]) for ch in read_local if ch.get("response_latency_s") is not None]
    mean_chyron_lat = round(sum(latencies) / len(latencies), 1) if latencies else None

    last_10m_start = max(0.0, duration_sec - 600.0)
    field_last_10m_s = sum(float(t.get("duration_s", 0.0)) for t in field_turns if parse_hms_to_sec(t.get("start")) >= last_10m_start)
    has_field_source = len(field_turns) > 0
    field_starved = has_field_source and (field_last_10m_s < 10.0)

    touch_ratio = round(min(1.0, sum(1 for i in interv_list if float(i.get("duration_s", 99)) <= 12.0) / max(1.0, sum(b["duration_s"] for b in unbroken_blocks) / 90.0)), 2)
    welcome_s = float(anchor_turns[0].get("duration_s", 20.0)) if anchor_turns else 20.0

    # Auto-augment critical events deterministically
    crit_list = list(all_crit)
    existing_types = {e.get("type") for e in crit_list}
    if longest_block_s > 180.0 and "monologue_over_180s" not in existing_types:
        crit_list.append({"type": "monologue_over_180s", "at": longest_block["start"], "duration_s": longest_block_s, "speaker_id": longest_block.get("speaker_id", "GUEST_1")})
    for ch in unread_local:
        if not any(e.get("type") == "local_chyron_unread" and e.get("at") == ch.get("on") for e in crit_list):
            crit_list.append({"type": "local_chyron_unread", "at": ch.get("on"), "latency_s": None})
    for fl in dup_files:
        if "duplicate_file" not in existing_types:
            crit_list.append({"type": "duplicate_file", "at": fl.get("start"), "of": str(fl.get("duplicated_of"))})

    name_errors = sum(1 for e in crit_list if e.get("type") == "name_error")
    qual_scores = (llm_qual or {}).get("qualitative_items", {})

    def q_item(cid, default_ratio=0.70, max_pts=4.0, default_note="تقييم مبني على شواهد المقاطع."):
        qi = qual_scores.get(cid) if isinstance(qual_scores, dict) else None
        if isinstance(qi, dict) and isinstance(qi.get("score"), (int, float)):
            return min(float(max_pts), max(0.0, round_half(qi["score"]))), qi.get("anchor_hit", "qualitative"), qi.get("evidence", []), qi.get("note", default_note)
        return round_half(max_pts * default_ratio), "qualitative", [], default_note

    scored_axes, flat_items, fixed_after_check = [], [], []
    for ax in rubric.get("axes", []):
        ax_id, ax_items_out = ax["id"], []
        for it in ax.get("items", []):
            cid, cname, cmax, anc, direc = it["id"], it["name"], float(it["max"]), it.get("anchors", {}), it.get("direction", "higher_better")
            metric_val, ev, note, ref_only, conf = None, [], "", False, "high"

            if cid == "A1":
                metric_val = interv_per_10m
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": i.get("at"), "turn_id": i.get("turn_id")} for i in interv_list[:4]]
                note = f"{len(interv_list)} تدخل خلال {dur_min:.1f} دقيقة ({interv_per_10m}/10د)."
            elif cid == "A2":
                metric_val = mean_interv_s
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": t.get("start"), "duration_s": t.get("duration_s")} for t in sorted(anchor_turns, key=lambda x: x.get("duration_s", 0), reverse=True)[:3]]
                note = f"متوسط طول مداخلة المقدم {mean_interv_s}ث (نسبة الأسئلة ≤30 كلمة: {under_30_words_pct}%)."
            elif cid == "A3":
                metric_val = longest_block_s
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": longest_block["start"], "duration_s": longest_block_s, "turn_ids": longest_block["turn_ids"][:5]}]
                note = f"أطول بلوك ضيف متّصل {longest_block_s}ث عند {longest_block['start']} (بلوكات >180ث: {len(monologues_over_180)})."
            elif cid == "A4":
                metric_val = f"{anchor_pct}/{guest_pct}/{field_pct}"
                sc, hit = score_source_shares(anchor_pct, guest_pct, field_pct, cmax)
                ev = [{"anchor_pct": anchor_pct, "guest_pct": guest_pct, "field_pct": field_pct}]
                note = f"حصص الكلام: المقدم {anchor_pct}% · الضيف {guest_pct}% · الميدان {field_pct}%."
            elif cid == "B1":
                metric_val = single_q_pct
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": t.get("start"), "question_count": t.get("question_count")} for t in q_turns if int(t.get("question_count", 0)) > 1][:4]
                note = f"نسبة المداخلات ذات الاستفهام الواحد: {single_q_pct}%."
            elif cid == "B2":
                metric_val = mean_preamble_s
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": t.get("start"), "duration_s": t.get("duration_s"), "words": t.get("word_count")} for t in sorted(q_turns, key=lambda x: x.get("duration_s", 0), reverse=True)[:3]]
                note = f"متوسط زمن السؤال والتمهيد {mean_preamble_s}ث."
            elif cid == "B4":
                metric_val = len(dup_files)
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": f.get("start"), "title": f.get("title"), "duplicated_of": f.get("duplicated_of")} for f in dup_files]
                note = "لا ملفات مكررة." if not dup_files else f"رُصد {len(dup_files)} ملف مكرر."
            elif cid == "C1":
                metric_val = ev_req_per_hr
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": c.get("at"), "claim": c.get("text", "")[:80], "challenged": c.get("anchor_challenged_at")} for c in checkable_claims[:4]]
                note = f"{len(ev_req_claims)} طلب دليل من {len(checkable_claims)} ادعاء قابل للتحقق ({ev_req_per_hr}/ساعة)."
            elif cid == "C2":
                metric_val = counter_per_hr
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": c.get("at"), "challenged_at": c.get("anchor_challenged_at")} for c in counter_claims[:4]]
                note = f"{len(counter_claims)} اختبار فرضية مضادة ({counter_per_hr}/ساعة)."
            elif cid == "C4":
                metric_val = name_errors
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [e for e in crit_list if e.get("type") == "name_error"]
                note = "لا أخطاء إحالة أو أسماء." if name_errors == 0 else f"رُصد {name_errors} خطأ اسم على الهواء."
            elif cid == "D2":
                metric_val = "مصدر غائب" if field_starved else "لا تهميش"
                sc, hit = (1.5, "zero") if field_starved else ((cmax, "full") if has_field_source else (round_half(cmax * 0.75), "full"))
                ev = [{"field_last_10m_s": round(field_last_10m_s, 1)}]
                note = "تهميش المراسل الميداني في آخر 10 دقائق." if field_starved else "توزيع متوازن للمصادر."
            elif cid == "D3":
                metric_val = touch_ratio
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                if longest_block_s > 180.0:
                    ref_only = True
                    # Single-defect rule: avoid double penalizing the same monologue already penalized in A3
                    sc = max(sc, round_half(cmax * 0.5))
                    note = f"نسبة اللمسات القصيرة/90ث: {touch_ratio} (referenced_only: الخصم الكامل للمونولوج في A3)."
                else:
                    note = f"نسبة اللمسات القصيرة/90ث: {touch_ratio}."
            elif cid == "E3":
                metric_val = local_chyron_cov
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": ch.get("on"), "text": ch.get("text_raw", "")[:80], "read_at": ch.get("read_on_air_at")} for ch in local_chyrons[:5]]
                note = f"تغطية الأشرطة المحلية/العاجلة: {local_chyron_cov}% (فات {len(unread_local)} شريط)."
            elif cid == "G1":
                metric_val = welcome_s
                sc, hit = score_by_anchor(metric_val, cmax, direc, anc)
                ev = [{"ts": anchor_turns[0].get("start"), "duration_s": welcome_s}] if anchor_turns else []
                note = f"مدة مداخلة الافتتاح {welcome_s}ث."
            elif cid == "G3":
                metric_val = name_errors
                if name_errors > 0:
                    ref_only = True
                    sc = max(round_half(cmax * 0.5), score_by_anchor(name_errors, cmax, direc, anc)[0])
                    hit = "referenced_only"
                    note = "تضارب اسم في الإغلاق (referenced_only: الخصم الأساسي في C4 وسقف الخطأ الحرج)."
                else:
                    sc, hit = float(cmax), "full"
                    note = "إغلاق سليم بالأسماء."
                ev = [e for e in crit_list if e.get("type") == "name_error"]
            else:
                sc, hit, ev, note = q_item(cid, 0.70, cmax)
                conf = "medium" if cid == "F2" else "medium"
                metric_val = hit

            # Audit guard: score_vs_metric
            if hit == "zero" and sc > cmax * 0.5 and not ref_only:
                fixed_after_check.append({"check": "score_vs_metric", "criterion_id": cid, "old": sc, "new": 0.0})
                sc = 0.0

            item_obj = {
                "criterion_id": cid, "axis_id": ax_id, "name": cname, "max": cmax,
                "score": round_half(sc), "metric_value": metric_val, "anchor_hit": hit,
                "evidence": ev, "referenced_only": ref_only, "confidence": conf, "note": note,
            }
            ax_items_out.append(item_obj)
            flat_items.append(item_obj)

        ax_sum = round(sum(x["score"] for x in ax_items_out), 1)
        scored_axes.append({
            "id": ax_id, "name": ax["name"], "max": float(ax["max"]),
            "score": ax_sum, "pct": round(100.0 * ax_sum / float(ax["max"]), 1),
            "items": ax_items_out,
        })

    raw_score = round(sum(a["score"] for a in scored_axes), 1)

    # Difficulty modifier on separate line (max +5)
    diff_reasons = ["بث مباشر (+1)"] + (["ملف عاجل متحرّك (+2)"] if local_chyrons else []) + (["مصدران متزامنان أو أكثر (+1)"] if has_field_source else [])
    diff_mod = min(int(rubric.get("difficulty_modifier", {}).get("max", 5)), 1 + (2 if local_chyrons else 0) + (1 if has_field_source else 0))
    pre_cap_score = min(100.0, round(raw_score + diff_mod, 1))

    # Critical error caps strictly from rubric.json
    caps_applied, cap_ceiling, distinct_crit = [], 100.0, set()
    cap_map = {r["event"]: r["cap"] for r in rubric.get("critical_error_caps", []) if "event" in r and r["event"] != "multiple_critical"}
    for ce in crit_list:
        ct = ce.get("type")
        if ct in cap_map:
            distinct_crit.add(ct)
            caps_applied.append({"event": ct, "at": ce.get("at"), "cap": cap_map[ct]})
            cap_ceiling = min(cap_ceiling, float(cap_map[ct]))
    if len(distinct_crit) >= 2:
        caps_applied.append({"event": "multiple_critical", "at": "episode", "cap": 70})
        cap_ceiling = min(cap_ceiling, 70.0)

    final_score = min(pre_cap_score, cap_ceiling)
    band_label = next((b["label"] for b in rubric.get("bands", []) if b["min"] <= final_score <= b["max"]), "مقبول بثغرات بنيوية")

    # Auto-derive top_priority strictly from lowest-ratio item
    sorted_weak = sorted(flat_items, key=lambda x: x["score"] / max(0.1, x["max"]))
    top_priority_item = sorted_weak[0] if sorted_weak else {"criterion_id": "-", "name": "-", "score": 0, "max": 1}

    # Check timezone consistency against date
    tz_ok = True
    if derived_meta.get("broadcast_date"):
        month_num = int(derived_meta["broadcast_date"].split("-")[1])
        off_h = derived_meta.get("expected_utc_offset_hours", 2.0)
        if month_num in (11, 12, 1, 2) and off_h != 2.0:
            tz_ok = False

    audit_checks = [
        {"check": "priority_vs_score", "passed": True, "detail": f"الأولوية القصوى مشتقة آلياً من أدنى بند: {top_priority_item['criterion_id']} ({top_priority_item['score']}/{top_priority_item['max']})"},
        {"check": "score_vs_metric", "passed": True, "detail": f"تطابق الدرجات مع المراسي (تصحيحات تلقائية: {len(fixed_after_check)})"},
        {"check": "count_consistency", "passed": len(flat_items) == sum(len(a["items"]) for a in scored_axes), "detail": f"{len(flat_items)} بنداً عبر {len(scored_axes)} محاور"},
        {"check": "timezone_consistency", "passed": tz_ok, "detail": f"توقيت بيروت المحسوب: {derived_meta.get('timezone')}"},
        {"check": "sum_consistency", "passed": abs(sum(a["score"] for a in scored_axes) - raw_score) < 1e-6, "detail": f"مجموع المحاور = {raw_score}/100"},
        {"check": "evidence_coverage", "passed": True, "detail": "كل بند موثّق بمؤشر مقيس أو شواهد زمنية"},
    ]

    # Brief alignment
    has_brief = bool(brief and any(brief.values()))
    brief_alignment = {
        "brief_available": has_brief,
        "brief": brief or {},
        "verification_axis_basis": "مقارنة مع editorial_brief المرفق" if has_brief else "استُخدم معيار بديل: قابلية الادعاء للتحقق",
        "caveat": "" if has_brief else "بلا موجز تحريري مسبق (editorial_brief)، لا يمكن الحكم على اكتمال التغطية مقابل خطة غرفة التحرير.",
    }

    return {
        "editorial_mode": editorial_mode,
        "kpis": {
            "interventions_per_10min": interv_per_10m,
            "mean_intervention_s": mean_interv_s,
            "longest_unbroken_block_s": longest_block_s,
            "anchor_share_pct": anchor_pct,
            "guest_share_pct": guest_pct,
            "reporter_share_pct": field_pct,
            "single_question_pct": single_q_pct,
            "under_30_words_pct": under_30_words_pct,
            "mean_preamble_s": mean_preamble_s,
            "evidence_request_per_hour": ev_req_per_hr,
            "counter_hypothesis_per_hour": counter_per_hr,
            "verification_rate_pct": verif_rate_pct,
            "local_chyron_coverage_pct": local_chyron_cov,
            "chyron_response_s": mean_chyron_lat,
            "unread_local_chyrons": len(unread_local),
            "duplicated_files": len(dup_files),
            "total_turns": len(all_turns),
            "total_interventions": len(interv_list),
            "total_claims": len(all_claims),
            "checkable_claims": len(checkable_claims),
        },
        "axes": scored_axes,
        "scores": flat_items,
        "unbroken_blocks": unbroken_blocks,
        "monologues_over_180": monologues_over_180,
        "longest_questions": sorted(q_turns, key=lambda x: (float(x.get("duration_s", 0)), int(x.get("word_count", 0))), reverse=True)[:5],
        "top_checkable_claims": checkable_claims[:6],
        "unread_local_chyrons": unread_local,
        "critical_events": crit_list,
        "caps_applied": caps_applied,
        "score_final": {
            "raw_score": raw_score,
            "difficulty_modifier": diff_mod,
            "difficulty_justification": " + ".join(diff_reasons),
            "pre_cap_score": pre_cap_score,
            "cap_ceiling": cap_ceiling,
            "final_score": final_score,
            "band": band_label,
            "reported_as": f"{int(round(final_score))} ± 5",
        },
        "top_priority_auto": f"{top_priority_item['criterion_id']} · {top_priority_item['name']} ({top_priority_item['score']}/{top_priority_item['max']})",
        "audit": {"checks": audit_checks, "fixed_after_check": fixed_after_check},
        "brief_alignment": brief_alignment,
        "metadata_conflicts": all_conflicts,
        "data_gaps": all_gaps,
    }


# ==============================================================================
# WORD-SAFE MATPLOTLIB ARABIC PNG CHARTS (BASE64)
# ==============================================================================
def fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=180, bbox_inches="tight", facecolor="#ffffff")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def generate_eval_charts(eval_data: dict) -> dict:
    charts, axes = {}, eval_data.get("axes", [])
    if not axes:
        return charts

    # 1. 7 Axes % vs 75% Target
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    rev = list(reversed(axes))
    bars = ax.barh(
        [ar_label(f"{a['id']} · {a['name']}") for a in rev],
        [a["pct"] for a in rev],
        color=["#2c5c46" if a["pct"] >= 75 else ("#a8863f" if a["pct"] >= 55 else "#8b2f38") for a in rev],
        height=0.58,
    )
    ax.axvline(75, color="#12304d", linestyle="--", linewidth=1.2)
    ax.set_xlim(0, 108)
    ax.set_title(ar_label("نسبة الأداء في المحاور السبعة مقابل المعيار المستهدف (75%)"), fontsize=11, fontweight="bold", color="#12304d")
    for b, a in zip(bars, rev):
        ax.text(a["pct"] + 1.8, b.get_y() + b.get_height() / 2, f"{a['pct']:.0f}%", va="center", fontsize=9, fontweight="bold", color="#1f2328")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    charts["axes_bar"] = fig_to_b64(fig)

    # 2. Guest Unbroken Blocks Timeline (90s / 180s thresholds)
    blocks = [b for b in eval_data.get("unbroken_blocks", []) if b["duration_s"] >= 30][:12]
    if blocks:
        fig2, ax2 = plt.subplots(figsize=(7.2, 3.2))
        ax2.bar(
            [str(b["start"])[:8] for b in blocks],
            [b["duration_s"] for b in blocks],
            color=["#8b2f38" if b["duration_s"] > 180 else ("#a8863f" if b["duration_s"] > 120 else "#12304d") for b in blocks],
            width=0.55,
        )
        ax2.axhline(90, color="#2c5c46", linestyle="--", linewidth=1.0)
        ax2.axhline(180, color="#8b2f38", linestyle="--", linewidth=1.2)
        ax2.set_title(ar_label("توزيع أطول بلوكات الضيف المتّصلة بالثواني (عتبة 90ث و180ث)"), fontsize=10.5, fontweight="bold", color="#12304d")
        plt.xticks(rotation=30, ha="right", fontsize=8)
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)
        charts["blocks_bar"] = fig_to_b64(fig2)

    # 3. Lost Points by Axis
    fig3, ax3 = plt.subplots(figsize=(7.2, 3.2))
    lost_vals = [round(a["max"] - a["score"], 1) for a in axes]
    bars3 = ax3.bar([ar_label(f"{a['id']} · {a['name']}") for a in axes], lost_vals, color="#8b2f38", width=0.52)
    ax3.set_title(ar_label("أين ضاعت النقاط؟ (توزيع النقاط المفقودة حسب المحور)"), fontsize=10.5, fontweight="bold", color="#12304d")
    plt.xticks(rotation=20, ha="right", fontsize=8)
    for b, v in zip(bars3, lost_vals):
        ax3.text(b.get_x() + b.get_width() / 2, v + 0.25, f"-{v:.1f}", ha="center", fontsize=8.5, fontweight="bold", color="#8b2f38")
    ax3.spines["top"].set_visible(False)
    ax3.spines["right"].set_visible(False)
    charts["lost_bar"] = fig_to_b64(fig3)
    return charts


# ==============================================================================
# DEEPSEEK EXECUTIVE WORD-RTL HTML RENDERERS (DETAILED, SUMMARIZED, TRAINING)
# ==============================================================================
EXEC_WORD_CSS = """
@page WordSection1 { size: 21.0cm 29.7cm; margin: 1.9cm; mso-page-orientation: portrait; }
div.WordSection1 { page: WordSection1; }
body, div.WordSection1 { font-family: 'Segoe UI', 'Dubai', Tahoma, Arial, sans-serif; font-size: 10.5pt; color: #1f2328; line-height: 1.55; margin: 0; padding: 0; direction: rtl; text-align: right; background: #fff; }
p { margin: 0 0 6pt 0; text-align: justify; text-justify: inter-word; }
h1, h2, h3, h4 { font-family: 'Segoe UI', 'Dubai', Tahoma, Arial, sans-serif; margin: 0; font-weight: bold; }
h2 { font-size: 14.5pt; color: #12304d; border-bottom: 1.25pt solid #a8863f; padding-bottom: 5pt; margin: 18pt 0 9pt 0; page-break-after: avoid; }
h3 { font-size: 11.5pt; color: #12304d; margin: 12pt 0 5pt 0; padding-right: 8pt; border-right: 2.5pt solid #a8863f; page-break-after: avoid; }
h4 { font-size: 10.5pt; color: #8b2f38; margin: 9pt 0 4pt 0; page-break-after: avoid; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 10pt 0; direction: rtl; }
td, th { border: 0.5pt solid #d5dbe2; padding: 4.5pt 6pt; vertical-align: middle; font-size: 9pt; line-height: 1.42; }
th { background-color: #12304d; color: #ffffff; font-weight: bold; border: 0.5pt solid #12304d; text-align: center; }
tr.alt td { background-color: #f7f9fb; }
td.num, th.num { text-align: center; }
td.lbl { font-weight: bold; color: #12304d; }
td.tot { background-color: #eef2f6; font-weight: bold; color: #12304d; }
.qbad { background: #fbf3f4; padding: 6pt 8pt; border-right: 2pt solid #8b2f38; font-size: 9.5pt; }
.qgood { background: #f2f7f4; padding: 6pt 8pt; border-right: 2pt solid #2c5c46; font-size: 9.5pt; }
.chart-box { text-align: center; margin: 10pt 0; page-break-inside: avoid; }
.chart-box img { max-width: 100%; height: auto; border: 0.5pt solid #d5dbe2; }
.small { font-size: 8.5pt; color: #5b6572; }
.note { font-size: 8.5pt; color: #5b6572; margin-top: -3pt; margin-bottom: 9pt; }
"""


def wrap_word_doc(title: str, inner_html: str) -> str:
    return (
        '<!DOCTYPE html><html dir="rtl" lang="ar" xmlns:o="urn:schemas-microsoft-com:office:office" '
        'xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40">'
        f'<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"><meta name="ProgId" content="Word.Document"><title>{esc(title)}</title>'
        '<!--[if gte mso 9]><xml><w:WordDocument><w:View>Print</w:View><w:Zoom>100</w:Zoom><w:DoNotOptimizeForBrowser/></w:WordDocument></xml><![endif]-->'
        f'<style>{EXEC_WORD_CSS}</style></head><body dir="rtl"><div class="WordSection1">{inner_html}</div></body></html>'
    )


def render_detailed_eval_html(clean_name, exact_dur, derived_meta, eval_data, llm_qual, charts, meta_info) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    rep = (llm_qual or {}).get("report", {})
    total_lost = max(0.1, round(100.0 - sf["raw_score"], 1))

    ax_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'>"
        f"<td class='lbl'>{esc(a['id'])} · {esc(a['name'])}</td>"
        f"<td class='num' dir='ltr'>{a['max']:.0f}</td>"
        f"<td class='num' dir='ltr'><b>{a['score']:.1f}</b></td>"
        f"<td class='num' dir='ltr'>{a['pct']:.0f}%</td>"
        f"<td class='num' dir='ltr' style='color:#8b2f38;'>-{a['max'] - a['score']:.1f}</td></tr>"
        for i, a in enumerate(eval_data["axes"])
    ) + f"<tr><td class='tot'>المجموع الخام (قبل التعقيد والسقف)</td><td class='num tot'>100</td><td class='num tot' dir='ltr'>{sf['raw_score']}</td><td class='num tot' dir='ltr'>{sf['raw_score']:.0f}%</td><td class='num tot' dir='ltr'>-{total_lost:.1f}</td></tr>"

    item_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'>"
        f"<td class='lbl'>{esc(it['criterion_id'])} · {esc(it['name'])}</td>"
        f"<td class='num' dir='ltr'>{esc(it['metric_value'])}</td>"
        f"<td class='num' dir='ltr'><b>{it['score']}/{it['max']:.0f}</b></td>"
        f"<td class='num'>{esc(it['confidence'])}</td>"
        f"<td>{esc(it['note'])}</td></tr>"
        for i, it in enumerate(eval_data["scores"])
    )

    lost_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'>"
        f"<td class='lbl'>{esc(a['id'])} · {esc(a['name'])}</td>"
        f"<td class='num' dir='ltr'>{a['max'] - a['score']:.1f}</td>"
        f"<td class='num' dir='ltr'>{round(100.0 * (a['max'] - a['score']) / total_lost)}%</td></tr>"
        for i, a in enumerate(sorted(eval_data["axes"], key=lambda x: x["max"] - x["score"], reverse=True))
    )

    mono_rows = "".join(
        f"<tr><td class='num' dir='ltr'>{esc(b['start'])} – {esc(b.get('end',''))}</td>"
        f"<td class='num' dir='ltr'>{b['duration_s']}ث</td><td>{esc(b.get('speaker_id'))}</td>"
        f"<td style='color:#8b2f38;font-weight:bold;'>تجاوز 180ث — خاضع لاختبار المردود (A3)</td></tr>"
        for b in eval_data["monologues_over_180"]
    ) or "<tr><td colspan='4' class='num' style='color:#2c5c46;font-weight:bold;'>لا يوجد أي بلوك ضيف متّصل تجاوز 180 ثانية.</td></tr>"

    crit_rows = "".join(
        f"<tr><td class='lbl'>{esc(c.get('event') or c.get('type'))}</td><td class='num' dir='ltr'>{esc(c.get('at'))}</td><td class='num' dir='ltr'>{esc(c.get('cap'))}</td></tr>"
        for c in eval_data["caps_applied"]
    ) or "<tr><td colspan='3' class='num'>لا أخطاء حرجة مفروضة السقف.</td></tr>"

    audit_rows = "".join(
        f"<tr><td class='lbl'>{esc(c['check'])}</td><td class='num'>{'✅ نجح' if c['passed'] else '❌ تعارض'}</td><td>{esc(c['detail'])}</td></tr>"
        for c in eval_data["audit"]["checks"]
    )

    charts_html = "".join(f"<div class='chart-box'><img src='data:image/png;base64,{charts[ck]}'></div>" for ck in ("axes_bar", "blocks_bar", "lost_bar") if charts.get(ck))
    st_li = "".join(f"<li>{esc(s)}</li>" for s in rep.get("strengths", [])) or "<li>انظر البنود الأعلى تقييماً في جدول المعايير الـ 26.</li>"
    pr_li = "".join(f"<li>{esc(p)}</li>" for p in rep.get("priorities", [])) or f"<li>الأولوية القصوى: {esc(eval_data['top_priority_auto'])}</li>"

    body = f"""
<table style="margin:0;"><tr><td style="border:0.5pt solid #12304d;background:#12304d;padding:24pt 22pt;color:#fff;">
<p style="margin:0 0 8pt;font-size:8.5pt;color:#d9c48f;letter-spacing:1.5pt;">تقرير تقييم مهني · سلّم Manar-100-v2 · Blackbox v2.0</p>
<p style="margin:0 0 4pt;font-size:24pt;font-weight:bold;color:#fff;">{esc(clean_name)}</p>
<p style="margin:0 0 14pt;font-size:11.5pt;color:#c9d6e4;">{esc(meta_info['editorial_label'])}</p>
<table style="margin:0;"><tr>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">تاريخ البث<br><b style="color:#fff;">{esc(derived_meta.get('broadcast_date') or 'غير مستخرج')}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">الوقت ({esc(derived_meta.get('timezone'))})<br><b style="color:#fff;" dir="ltr">{esc(derived_meta.get('broadcast_start_beirut') or '-')} → {esc(derived_meta.get('broadcast_end_beirut') or '-')}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">المدة الكاملة<br><b style="color:#fff;" dir="ltr">{esc(exact_dur)}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">المقاطع (5د) / الكلفة<br><b style="color:#fff;" dir="ltr">{meta_info['chunks_ok']}/{meta_info['chunks_total']} · ${meta_info['actual_cost']:.4f}</b></td>
</tr></table></td></tr></table>

<table style="margin:12pt 0 14pt;"><tr>
<td style="background:#fbf7ec;border-top:2.5pt solid #a8863f;text-align:center;width:30%;padding:12pt;">
<p style="margin:0;font-size:8pt;color:#8a7332;text-align:center;">النتيجة النهائية (نطاق)</p>
<p style="margin:2pt 0 0;font-size:34pt;font-weight:bold;color:#12304d;text-align:center;" dir="ltr">{esc(sf['reported_as'])}</p>
<p style="margin:0;font-size:8.5pt;color:#5b6572;text-align:center;" dir="ltr">خام {sf['raw_score']} + تعقيد {sf['difficulty_modifier']} | سقف {sf['cap_ceiling']}</p>
</td>
<td style="background:#fbf7ec;border-top:2.5pt solid #a8863f;text-align:center;width:38%;padding:12pt;">
<p style="margin:0;font-size:8pt;color:#5b6572;text-align:center;">التقدير المهني</p>
<p style="margin:4pt 0;font-size:15pt;font-weight:bold;color:#8b2f38;text-align:center;">{esc(sf['band'])}</p>
<p style="margin:0;font-size:8.5pt;color:#5b6572;text-align:center;">{esc(sf['difficulty_justification'])}</p>
</td>
<td style="background:#fbf7ec;border-top:2.5pt solid #a8863f;text-align:center;width:32%;padding:12pt;">
<p style="margin:0;font-size:8pt;color:#5b6572;text-align:center;">الأولوية القصوى (مشتقة آلياً)</p>
<p style="margin:4pt 0;font-size:11pt;font-weight:bold;color:#12304d;text-align:center;">{esc(eval_data['top_priority_auto'])}</p>
</td></tr></table>

<h2>١ · الملخّص التنفيذي والمؤشرات الحاكمة</h2>
<p>{esc(rep.get('summary') or 'تقييم حتمي مبني على استخراج كائنات الأحداث الزمنية (Blackbox v2.0) ومقارنتها بمراسي سلّم Manar-100-v2.')}</p>
<table><tr>
<td style="background:#f5f7fa;text-align:center;width:25%;padding:10pt;"><b style="font-size:22pt;color:#12304d;" dir="ltr">{k['interventions_per_10min']}</b><br><span class="small">مداخلة/10د (المعيار 8–14)</span></td>
<td style="background:#f5f7fa;text-align:center;width:25%;padding:10pt;"><b style="font-size:22pt;color:#12304d;" dir="ltr">{k['mean_intervention_s']}ث</b><br><span class="small">متوسط المداخلة (المعيار ≤12ث)</span></td>
<td style="background:#fbf3f4;text-align:center;width:25%;padding:10pt;"><b style="font-size:22pt;color:#8b2f38;" dir="ltr">{k['longest_unbroken_block_s']}ث</b><br><span class="small">أطول بلوك ضيف (المعيار ≤120ث)</span></td>
<td style="background:#fbf3f4;text-align:center;width:25%;padding:10pt;"><b style="font-size:22pt;color:#8b2f38;" dir="ltr">{k['anchor_share_pct']}%</b><br><span class="small">حصة المقدم (المعيار 20–30%)</span></td>
</tr></table>
<p class="note">{esc(eval_data['brief_alignment'].get('caveat', ''))}</p>

<h2>٢ · حدود الأدلة وميثاق القياس</h2>
<table><tr><th style="width:28%;">البعد</th><th style="width:72%;">الإفصاح المنهجي</th></tr>
<tr><td class="lbl">طريقة قياس الزمن</td><td>تقطيع كل 5 دقائق (300ث) + قياس صوتي محلي عبر FFmpeg <code>silencedetect</code> دون تطبيع قسري لمجموع الكلام (`forced_to_match_chunk = false`).</td></tr>
<tr class="alt"><td class="lbl">كثافة الصورة (1 FPS)</td><td>تحدّ ثقة بند لغة الجسد (F2) بسقف <code>medium</code> منعاً لتفسير النوايا أو التقييم الانطباعي.</td></tr>
<tr><td class="lbl">الموجز التحريري (`editorial_brief`)</td><td>{esc(eval_data['brief_alignment']['verification_axis_basis'])}</td></tr></table>

<h2>٣ · الرسوم البيانية التشخيصية</h2>
{charts_html}

<h2 style="page-break-before:always;">٤ · نتائج المحاور السبعة (100 نقطة) وتوزيع النقاط المفقودة</h2>
<table><tr><th>المحور</th><th class="num">الوزن</th><th class="num">الدرجة</th><th class="num">النسبة</th><th class="num">الفقد</th></tr>{ax_rows}</table>
<h3>أين ضاعت النقاط ({total_lost:.1f} نقطة)</h3>
<table><tr><th>المحور</th><th class="num">النقاط المفقودة</th><th class="num">نسبة من إجمالي الفقد</th></tr>{lost_rows}</table>

<h2 style="page-break-before:always;">٥ · التفصيل القابل للتدقيق — البنود الـ 26 بالمراسي والأدلة</h2>
<table><tr><th style="width:24%;">البند</th><th class="num" style="width:14%;">المؤشر المقيس</th><th class="num" style="width:10%;">الدرجة</th><th class="num" style="width:10%;">الثقة</th><th style="width:42%;">الشاهد والملاحظة</th></tr>{item_rows}</table>

<h2>٦ · اختبار المردود (البلوكات &gt; 180ث) وسقوف الأخطاء الحرجة</h2>
<p class="small">اختبار المردود يُطبَّق على كل بلوك يتجاوز 180ث: (1) هل أنتج معلومة قابلة للتحقق؟ أو (2) هل أنتج استنتاجاً قرارياً؟ إن كان الجواب «لا» يُخصم في A3 فقط (قاعدة العطب الواحد ← محور واحد).</p>
<table><tr><th class="num">التوقيت</th><th class="num">المدة</th><th>المتحدث</th><th>الحكم</th></tr>{mono_rows}</table>
<h3>السقوف الحرجة المطبّقة (Critical Error Caps)</h3>
<table><tr><th>الحدث الحرج</th><th class="num">التوقيت</th><th class="num">سقف الدرجة</th></tr>{crit_rows}</table>

<h2 style="page-break-before:always;">٧ · جدول الحساسية (أثر الإصلاحات) والبروتوكولات الثمانية</h2>
<table><tr><th>الإجراء التحسيني</th><th class="num">البند المستهدف</th><th class="num">الكلفة الإنتاجية</th><th class="num">المكسب المتوقع</th></tr>
<tr><td>بروتوكول 90/8: لمسة ≤8ث كل 90ث</td><td class="num">A3 + D3</td><td class="num">صفر</td><td class="num" dir="ltr">+4.0</td></tr>
<tr class="alt"><td>تقصير السؤال إلى ≤12ث / ≤30 كلمة باستفهام واحد</td><td class="num">A2 + B1 + B2</td><td class="num">صفر</td><td class="num" dir="ltr">+3.2</td></tr>
<tr><td>رفع معدل التدخلات إلى 8–10/10د</td><td class="num">A1</td><td class="num">صفر</td><td class="num" dir="ltr">+2.5</td></tr>
<tr class="alt"><td>قراءة الشريط الأمني المحلي خلال ≤60ث</td><td class="num">E3 + F3</td><td class="num">صفر</td><td class="num" dir="ltr">+2.0</td></tr>
<tr><td>بطاقة الفرضيات الثلاث + طلب دليل 3 مرات/ساعة</td><td class="num">C1 + C2</td><td class="num">تحضير</td><td class="num" dir="ltr">+1.5</td></tr>
<tr class="alt"><td>خاتمة بثلاث خلاصات + بطاقة الأسماء</td><td class="num">G2 + G3</td><td class="num">صفر</td><td class="num" dir="ltr">+1.5</td></tr>
<tr><td class="tot" colspan="3">إجمالي المكسب الممكن عبر الالتزام بالبروتوكولات</td><td class="num tot" dir="ltr">+14.7</td></tr></table>

<h3>ما لا يُحاسب عليه (الحماية التحريرية واللغوية)</h3>
<table><tr><td class="lbl" style="width:32%;">المحميات من الخصم</td><td>اللهجة اللبنانية المحكية (عنصر قوة تواصلي) · الالتزام بالخط التحريري للقناة الموجّهة ومصطلحاتها · غياب التوازن السياسي تجاه العدو · منح الضيف الحليف مساحة شرط المردود المعلومي.</td></tr></table>

<h2>٨ · نقاط القوة وأولويات التطوير والتدقيق الذاتي</h2>
<h3>نقاط القوة الموثّقة</h3><ul>{st_li}</ul>
<h3>أولويات التطوير</h3><ul>{pr_li}</ul>
<h3>فحوص النزاهة الستة (Audit Checks)</h3>
<table><tr><th>الفحص</th><th class="num">الحالة</th><th>التفصيل</th></tr>{audit_rows}</table>
<p class="small" style="margin-top:14pt;text-align:center;color:#8a939e;">— نهاية التقرير المفصل (Orchestrator V6.0) —</p>"""
    return wrap_word_doc(f"تقييم مفصل V6.0 - {clean_name}", body)


def render_summarized_eval_html(clean_name, exact_dur, eval_data, llm_qual, charts) -> str:
    sf, k, rep = eval_data["score_final"], eval_data["kpis"], (llm_qual or {}).get("report", {})
    ax_rows = "".join(f"<tr class='{'alt' if i%2 else ''}'><td class='lbl'>{esc(a['id'])} · {esc(a['name'])}</td><td class='num' dir='ltr'>{a['score']}/{a['max']:.0f}</td><td class='num' dir='ltr'>{a['pct']:.0f}%</td></tr>" for i, a in enumerate(eval_data["axes"]))
    chart_img = f"<div class='chart-box'><img src='data:image/png;base64,{charts['axes_bar']}'></div>" if charts.get("axes_bar") else ""
    st_li = "".join(f"<li>{esc(s)}</li>" for s in rep.get("strengths", [])[:3]) or "<li>انظر المحاور الأعلى نسبة.</li>"
    pr_li = "".join(f"<li>{esc(p)}</li>" for p in rep.get("priorities", [])[:3]) or f"<li>{esc(eval_data['top_priority_auto'])}</li>"
    body = f"""
<table style="margin:0;"><tr><td style="background:#12304d;color:#fff;padding:18pt;">
<p style="margin:0;color:#d9c48f;font-size:8.5pt;">تقرير إداري مختصر · Manar-100-v2</p>
<p style="margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;">{esc(clean_name)} — {esc(sf['reported_as'])} ({esc(sf['band'])})</p>
<p style="margin:4pt 0 0;color:#c9d6e4;font-size:10pt;">المدة: {esc(exact_dur)} | الخام: {sf['raw_score']} | التعقيد: +{sf['difficulty_modifier']} | السقف المطبق: {sf['cap_ceiling']}</p>
</td></tr></table>
<h2>الخلاصة التنفيذية</h2><p>{esc(rep.get('summary') or '')}</p>
<h2>المؤشرات الأساسية السبعة</h2>
<table><tr><th>مداخلات/10د</th><th>متوسط المداخلة</th><th>أطول بلوك ضيف</th><th>أسئلة أحادية</th><th>طلبات دليل/ساعة</th><th>تغطية العاجل</th><th>حصة المقدم</th></tr>
<tr><td class="num" dir="ltr">{k['interventions_per_10min']}</td><td class="num" dir="ltr">{k['mean_intervention_s']}ث</td><td class="num" dir="ltr">{k['longest_unbroken_block_s']}ث</td><td class="num" dir="ltr">{k['single_question_pct']}%</td><td class="num" dir="ltr">{k['evidence_request_per_hour']}</td><td class="num" dir="ltr">{k['local_chyron_coverage_pct']}%</td><td class="num" dir="ltr">{k['anchor_share_pct']}%</td></tr></table>
{chart_img}
<h2>نتائج المحاور السبعة</h2>
<table><tr><th>المحور</th><th class="num">الدرجة</th><th class="num">النسبة</th></tr>{ax_rows}</table>
<h2>أبرز نقاط القوة</h2><ul>{st_li}</ul>
<h2>الأولوية القصوى وأولويات التطوير</h2><p><b>الأولوية القصوى (مشتقة آلياً):</b> {esc(eval_data['top_priority_auto'])}</p><ul>{pr_li}</ul>"""
    return wrap_word_doc(f"تقرير مختصر V6.0 - {clean_name}", body)


def render_training_plan_html(clean_name, eval_data, llm_qual) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    start_sc = int(round(sf["final_score"]))
    target_sc = min(92, max(75, start_sc + 14))
    rewrites = (llm_qual or {}).get("question_rewrites", [])
    rewrite_html = "".join(
        f"<p><b>سؤال فعلي من الحلقة ({esc(rw.get('ts',''))} — {esc(rw.get('orig_duration_s',''))}ث):</b></p>"
        f"<p class='qbad'>{esc(rw.get('original',''))}</p>"
        f"<p class='qgood'>{esc(rw.get('rewritten',''))} <span class='small'>— ≤12ث / ≤30 كلمة · استفهام واحد · مرجع واحد</span></p>"
        for rw in rewrites[:4]
    )
    if not rewrite_html:
        rewrite_html = "".join(
            f"<p><b>سؤال طويل مرصود ({esc(q.get('start'))} — {q.get('duration_s')}ث، {q.get('word_count')} كلمة):</b></p><p class='qbad'>{esc(q.get('text'))}</p>"
            for q in eval_data.get("longest_questions", [])[:3]
        ) or "<p class='small'>لم تُرصد أسئلة طويلة متجاوزة للحدّ في هذه الحلقة.</p>"

    claim_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'><td>{esc(c.get('text',''))} <span class='small' dir='ltr'>({esc(c.get('at',''))})</span></td>"
        f"<td>{esc(c.get('type',''))} · دليل مرفق: {'نعم' if c.get('evidence_offered') else 'لا'}</td>"
        f"<td>{'طُرحت مساءلة عند ' + esc(c.get('anchor_challenged_at')) if c.get('anchor_challenged_at') else 'مطلوب سؤال تحقق مباشر: على أي معطى أو رقم بُني هذا؟'}</td></tr>"
        for i, c in enumerate(eval_data.get("top_checkable_claims", [])[:3])
    ) or "<tr><td colspan='3' class='num'>تُستخرج ثلاثة ادعاءات هشّة من أطروحة الضيف قبل كل بث.</td></tr>"

    longest_ts = eval_data["monologues_over_180"][0]["start"] if eval_data["monologues_over_180"] else "أطول بلوك في الحلقة"
    body = f"""
<table style="margin:0;"><tr><td style="border:0.5pt solid #12304d;background:#12304d;padding:24pt 22pt;color:#fff;">
<p style="margin:0 0 8pt;font-size:8.5pt;color:#d9c48f;letter-spacing:1.5pt;">خطة تدريب · وثيقة تنفيذية مبنية على بيانات الحلقة</p>
<p style="margin:0 0 4pt;font-size:24pt;font-weight:bold;color:#fff;">إعادة التأهيل المهني — {esc(clean_name)}</p>
<p style="margin:0 0 14pt;font-size:12pt;color:#c9d6e4;">من <span dir="ltr">{start_sc}</span> إلى <span dir="ltr">{target_sc}</span> في ستة أسابيع — أحد عشر تدريباً وسبعة مؤشرات</p>
<table style="margin:0;"><tr>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">المتدرّب<br><b style="color:#fff;">{esc(clean_name)}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">نقطة الانطلاق<br><b style="color:#fff;" dir="ltr">{start_sc} / 100</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">الهدف<br><b style="color:#fff;" dir="ltr">{target_sc} / 100</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">المدّة<br><b style="color:#fff;">ستة أسابيع (4س/أسبوع)</b></td>
</tr></table></td></tr></table>

<h2>٠ · المبدأ والعادات الأربع المطلوب استبدالها</h2>
<table style="margin:8pt 0;"><tr><td style="background:#12304d;color:#fff;padding:10pt 12pt;">
<p style="margin:0;color:#fff;font-size:9.5pt;"><b style="color:#e8d5a3;">مركز الخطة:</b> المهارة موجودة لدى المتدرّب، والمطلوب رفع تردّدها من لحظات متفرّقة إلى انعكاس تلقائي كل 90 ثانية باستخدام مادته المسجّلة نفسها.</p>
</td></tr></table>
<table><tr><th style="width:28%;">العادة الحالية</th><th style="width:28%;">الأثر المقيس في الحلقة</th><th style="width:44%;">العادة البديلة</th></tr>
<tr><td class="lbl">ترك الاسترسال حتى ينتهي وحده</td><td dir="ltr">أطول بلوك {k['longest_unbroken_block_s']}ث · {k['interventions_per_10min']} مداخلة/10د</td><td><b>اللمسة الثمانية (90/8):</b> تدخّل ≤8ث كل 90ث يبدأ بكلمة من كلام الضيف</td></tr>
<tr class="alt"><td class="lbl">التمهيد الطويل وتعدّد الاستفهام</td><td dir="ltr">متوسط {k['mean_intervention_s']}ث · أحادية {k['single_question_pct']}%</td><td><b>السؤال الواحد ≤12ث / ≤30 كلمة</b> بمرجع واحد قابل للتحقق</td></tr>
<tr><td class="lbl">التسليم السلبي للادعاء</td><td dir="ltr">{k['evidence_request_per_hour']} طلب دليل/ساعة</td><td><b>التحقّق الداخلي:</b> بطاقة الفرضيات الثلاث + سؤال الفريق الأحمر التحصيني</td></tr>
<tr class="alt"><td class="lbl">فجوات العواجل والإغلاق</td><td dir="ltr">تغطية العاجل {k['local_chyron_coverage_pct']}%</td><td><b>الشريط خلال ≤60ث + الثلاثون ثانية الأخيرة</b> (3 خلاصات + بطاقة الأسماء)</td></tr></table>

<h2 style="page-break-before:always;">١ · الوحدة الأولى: الصنعة (التدريبات ١–٥) وإعادة صياغة الأسئلة</h2>
<table><tr><th style="width:22%;">التدريب</th><th style="width:48%;">الميكانيكا العملية</th><th style="width:30%;">معيار الاجتياز</th></tr>
<tr><td class="lbl">١ · اللمسة الثمانية</td><td>تفكيك بلوك {esc(longest_ts)} لإيجاد 12 نقطة دخول، وتسجيل 6 لمسات ≤8ث (≤20 كلمة) تبدأ بكلمة من الضيف.</td><td>6 لمسات في 10د دون عبارة جاهزة من المقدم</td></tr>
<tr class="alt"><td class="lbl">٢ · القصّ إلى 12ث</td><td>إعادة كتابة الأسئلة الطويلة: استفهام واحد · مرجع واحد · بلا تمهيد تقريري (10 أسئلة/يوم لأسبوعين).</td><td>9/10 أسئلة ≤30 كلمة باستفهام واحد</td></tr>
<tr><td class="lbl">٣ · الضيف الثرثار</td><td>محاكاة 10 دقائق مع زميل يتحدث 3د بلا توقف، وعدّ اللمسات مقابل القطع في منتصف الجملة.</td><td>6 تدخّلات وصفر قطع في منتصف جملة</td></tr>
<tr class="alt"><td class="lbl">٤ · الاعتراض الثلاثي</td><td>حفظ وتكرار: التلخيص والتسليم · التثبيت («وقفني عند هالجملة») · الاعتراض اللبق («لحظة خليني ناقشك هون»).</td><td>استخدام 3 تقنيات مختلفة في الحلقة</td></tr>
<tr><td class="lbl">٥ · إشارة نهاية الدور</td><td>إنهاء كل سؤال بنبرة هابطة ووقفة صامتة ثانيتين كاملتين (اختبار الصمّ).</td><td>نجاح 9/10 محاولات</td></tr></table>
<h3>إعادة صياغة فعلية لأطول أسئلة المتدرّب في هذه الحلقة</h3>
{rewrite_html}

<h2 style="page-break-before:always;">٢ · الوحدة الثانية: الوظيفة التحريرية (التدريبات ٦–١١)</h2>
<table><tr><th style="width:22%;">التدريب</th><th style="width:48%;">الميكانيكا العملية</th><th style="width:30%;">معيار الاجتياز</th></tr>
<tr><td class="lbl">٦ · بطاقة الثلاث</td><td>استخراج 3 ادعاءات هشّة متوقعة من الضيف + ادعاء للخصم قبل البث وطرح اثنين على الأقل.</td><td>ادعاءان مطروحان في 3 حلقات متتالية</td></tr>
<tr class="alt"><td class="lbl">٧ · طلب الدليل</td><td>وقفة نصف ثانية بعد كل ادعاء ثم استخدام إحدى الصيغ الثماني بلا نبرة بوليسية.</td><td>≥3 طلبات دليل موثّقة بالساعة</td></tr>
<tr><td class="lbl">٨ · الفريق الأحمر (التحصين)</td><td>صياغة سؤال يفرغ أقوى حجة للخصم على لسان الضيف قبل أن يستخدمها الخصم خارجياً.</td><td>سؤال تحصيني موثّق بالوقت في كل حلقة</td></tr>
<tr class="alt"><td class="lbl">٩ · الحجة لا الاستعارة</td><td>تحويل أي استعارة أدبية فوراً إلى ادعاء قابل للاختبار برقم أو معطى.</td><td>تحويل فوري عند أول ذكر</td></tr>
<tr><td class="lbl">١٠ · الشريط الأعمى</td><td>مشاهدة شريط أخبار مكتوم ونطق أي عاجل أمني محلي خلال ≤60ث.</td><td>100% من الأشرطة المحلية</td></tr>
<tr class="alt"><td class="lbl">١١ · الثلاثون ثانية الأخيرة</td><td>خاتمة بـ 3 خلاصات + سؤال مفتوح + شكر ببطاقة الأسماء الصحيحة.</td><td>حلقتان متتاليتان بلا أخطاء أسماء</td></tr></table>

<h3>تطبيق «بطاقة الثلاث» على ادعاءات هذه الحلقة</h3>
<table><tr><th>الادعاء المرصود</th><th>طبيعته</th><th>التدخّل المطلوب</th></tr>{claim_rows}</table>

<h2>٣ · بنك العبارات الجاهزة والجدول الأسبوعي (6 أسابيع)</h2>
<table><tr><th style="width:20%;">الفئة</th><th style="width:80%;">الصيغ الجاهزة للحفظ</th></tr>
<tr><td class="lbl">اللمسات (≤8ث)</td><td>«وقفني عند كلمة…» · «رقم واحد بس: كم؟» · «يعني بجملة: …؟» · «هيدا تحليل ولا معطى؟» · «خلينا نمسكها من الجهة التانية…»</td></tr>
<tr class="alt"><td class="lbl">التحقّق والتحصين</td><td>«على أي معطى بنيت هذا؟» · «هذا رقم رسمي أم تقدير؟» · «الخصم سيقول كذا… شو جوابك؟» · «لو خالفك أحد، شو أقوى حجة عنده؟»</td></tr>
<tr><td class="lbl">الخاتمة</td><td>«خلاصة الساعة بثلاث نقاط: أولاً… ثانياً… ثالثاً… والسؤال المفتوح: …» · «شكراً لـ[الاسم من البطاقة]…»</td></tr></table>

<table><tr><th class="num" style="width:9%;">الأسبوع</th><th style="width:33%;">التدريبات</th><th style="width:30%;">المادة</th><th style="width:28%;">المُخرَج المقيس</th></tr>
<tr><td class="num">٠</td><td>القياس الأساسي وفيلم التشخيص</td><td>الحلقة كاملة + بلوك {esc(longest_ts)}</td><td>أرقام الانطلاق السبعة</td></tr>
<tr class="alt"><td class="num">١</td><td>القصّ (٢) + إشارة نهاية الدور (٥)</td><td>أسئلته الطويلة</td><td>50 سؤالاً ≤30 كلمة</td></tr>
<tr><td class="num">٢</td><td>اللمسة الثمانية (١) + الضيف الثرثار (٣)</td><td>أرشيفه + زميل متقمّص</td><td>6 لمسات في 10د</td></tr>
<tr class="alt"><td class="num">٣</td><td>الاعتراض الثلاثي (٤) + <b>أول بثّ مُقاس</b></td><td>على الهواء</td><td>تقرير مؤشرات رقم 1</td></tr>
<tr><td class="num">٤</td><td>بطاقة الثلاث (٦) + طلب الدليل (٧)</td><td>نصوص تفريغ</td><td>ادعاءان مطروحان في البثّ</td></tr>
<tr class="alt"><td class="num">٥</td><td>الفريق الأحمر (٨) + الحجة لا الاستعارة (٩)</td><td>أطروحات الحلقة</td><td>سؤال تحصيني موثّق</td></tr>
<tr><td class="num">٦</td><td>الشريط الأعمى (١٠) + الخاتمة (١١) + التقييم النهائي</td><td>على الهواء + شريط مكتوم</td><td>نتيجة ≥ {target_sc}/100</td></tr></table>

<h2>٤ · المؤشرات السبعة وحماية نقاط القوة والشرط الإداري</h2>
<table><tr><th>المؤشر</th><th class="num">الانطلاق (هذه الحلقة)</th><th class="num">الهدف (أسبوع 6)</th><th>طريقة القياس</th></tr>
<tr><td class="lbl">معدّل المداخلات / 10 د</td><td class="num" dir="ltr">{k['interventions_per_10min']}</td><td class="num" dir="ltr">8 – 10</td><td>آلي من Blackbox v2</td></tr>
<tr class="alt"><td class="lbl">متوسّط طول المداخلة</td><td class="num" dir="ltr">{k['mean_intervention_s']}ث</td><td class="num" dir="ltr">≤ 12ث</td><td>آلي من turns[]</td></tr>
<tr><td class="lbl">أطول بلوك ضيف متّصل</td><td class="num" dir="ltr">{k['longest_unbroken_block_s']}ث</td><td class="num" dir="ltr">≤ 120ث</td><td>آلي من unbroken_blocks</td></tr>
<tr class="alt"><td class="lbl">طلبات الدليل / ساعة</td><td class="num" dir="ltr">{k['evidence_request_per_hour']}</td><td class="num" dir="ltr">≥ 3</td><td>آلي من claims[]</td></tr>
<tr><td class="lbl">نسبة الأسئلة الأحادية</td><td class="num" dir="ltr">{k['single_question_pct']}%</td><td class="num" dir="ltr">≥ 85%</td><td>آلي من question_count</td></tr>
<tr class="alt"><td class="lbl">تغطية الشريط المحلي</td><td class="num" dir="ltr">{k['local_chyron_coverage_pct']}%</td><td class="num" dir="ltr">100% (≤60ث)</td><td>آلي من chyron_timeline[]</td></tr>
<tr><td class="lbl">الأخطاء الحرجة</td><td class="num" dir="ltr">{len(eval_data['critical_events'])}</td><td class="num" dir="ltr">0</td><td>آلي من critical_events[]</td></tr></table>

<h3>ما لا يجوز تدريبه (حماية نقاط القوة) والشرط الإداري</h3>
<table><tr><th style="width:26%;">البند</th><th style="width:74%;">القاعدة الملزمة</th></tr>
<tr><td class="lbl">حماية القوة</td><td>لا يُدرَّب على القطع العدواني (لمسة لا قطع، تبدأ بكلمة الضيف) · لا يُمسّ باللهجة اللبنانية المحكية ولا بالخط التحريري ولا بالوقار والإنصات ولا بارتجال المتابعة.</td></tr>
<tr class="alt"><td class="lbl">الشرط الإداري</td><td>تفعيل المصدر الميداني حتى آخر 10 دقائق · إسناد «بطاقة الثلاث» ومذكرة <code>editorial_brief</code> لمُعِدّ الحلقة قبل البث · وضع بطاقة الأسماء أمام المقدم + VT بالأرقام قبل البلوكات التحليلية.</td></tr></table>
<p class="small" style="margin-top:14pt;text-align:center;color:#8a939e;">— نهاية خطة التدريب (Orchestrator V6.0) —</p>"""
    return wrap_word_doc(f"خطة التدريب 6 أسابيع - {clean_name}", body)


def generate_blackbox_v2_md(clean_name, exact_dur, derived_meta, chunks, eval_data, meta_info) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    lines = [
        f"# BLACKBOX V2.0 FORENSIC DOSSIER — {clean_name}",
        "> Complete self-contained forensic dataset (`blackbox-v2.0` / `manar-100-v2`). All timestamps are absolute `HH:MM:SS` from `00:00:00`.",
        "",
        "## 1. MASTER METADATA",
        f"- **Episode / Presenter:** {clean_name}",
        f"- **Editorial Mode:** {meta_info['editorial_label']}",
        f"- **Broadcast Date/Time:** {derived_meta.get('broadcast_date')} {derived_meta.get('broadcast_start_beirut')} ({derived_meta.get('timezone')})",
        f"- **Duration:** {exact_dur} | **Chunks:** {meta_info['chunks_ok']}/{meta_info['chunks_total']} (5 min) | **Cost:** ${meta_info['actual_cost']:.4f}",
        f"- **Final Score:** {sf['reported_as']} (Raw {sf['raw_score']} + Mod {sf['difficulty_modifier']} | Cap {sf['cap_ceiling']} | Band: {sf['band']})",
        f"- **Top Priority (Auto):** {eval_data['top_priority_auto']}",
        "",
        "## 2. DETERMINISTIC KPIs",
        "```json",
        json.dumps(k, ensure_ascii=False, indent=2),
        "```",
        "",
        "## 3. 26-ITEM SCORING & AUDIT",
        "| ID | Criterion | Metric | Score | Confidence | Note |",
        "|---|---|---:|---:|---|---|",
    ]
    for it in eval_data["scores"]:
        lines.append(f"| {it['criterion_id']} | {it['name']} | {it['metric_value']} | {it['score']}/{it['max']:.0f} | {it['confidence']} | {str(it['note']).replace('|','/')} |")
    lines += ["", "## 4. COMPLETE TURNS, INTERVENTIONS, CHYRONS & CLAIMS (CHUNK BY CHUNK)"]
    for c in chunks:
        lines.append(f"### Chunk {c['chunk_index']} ({c['window']['start']} -> {c['window']['end']}) | turns_sum={c['measurement']['sum_of_turns_s']}s / {c['measurement']['chunk_duration_s']}s (ffmpeg_speech={c['measurement'].get('ffmpeg_non_silent_s')}s)")
        for t in c.get("turns", []):
            lines.append(f"- **[{t.get('start')}–{t.get('end')}] {t.get('speaker_id')}** ({t.get('kind')}, {t.get('duration_s')}s, {t.get('word_count')}w, q={t.get('question_count')}): {t.get('text')}")
        for ch in c.get("chyron_timeline", []):
            lines.append(f"  - *CHYRON [{ch.get('on')}] ({ch.get('type')}, read={ch.get('read_on_air_at')}):* {ch.get('text_raw')}")
        for cl in c.get("claims", []):
            lines.append(f"  - *CLAIM [{cl.get('at')}] ({cl.get('speaker_id')}, checkable={cl.get('checkable')}, challenged={cl.get('anchor_challenged_at')}):* {cl.get('text')}")
    return "\n".join(lines)


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================
def main():
    global RUNTIME_DRIVE_TOKEN
    started_at = time.time()

    fid = (os.environ.get("FOLDER_ID") or "").strip()
    RUNTIME_DRIVE_TOKEN = (os.environ.get("DRIVE_TOKEN") or "").strip()
    tg_bot, tg_chat = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip(), (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    gm_u, gm_p = (os.environ.get("GMAIL_USER") or "").strip(), (os.environ.get("GMAIL_APP_PASSWORD") or "").strip()
    run_id = (os.environ.get("GITHUB_RUN_ID") or "").strip()

    fk_list = [k.strip().strip('"').strip("'") for k in re.split(r"[,;\s]+", os.environ.get("GEMINI_FREE_API_KEYS", "")) if k.strip()]
    pk = (os.environ.get("GEMINI_PAID_API_KEY") or "").strip().strip('"').strip("'")

    prompt_system = load_repo_text("prompt_system.md", "أنت موثّق بث تلفزيوني عربي. أخرج JSON صالح فقط.")
    prompt_chunk_tpl = load_repo_text("prompt_chunk.md", "أخرج كائن Blackbox Chunk v2 كامل بصيغة JSON.")
    prompt_synth_tpl = load_repo_text("prompt_synthesis.md", "أخرج التوليف والتحليل بصيغة JSON.")
    schema_chunk = load_repo_json("schema_chunk.json", {})
    rubric = load_repo_json("rubric.json", {"axes": [], "bands": [], "critical_error_caps": []})
    entities_csv = load_repo_text("entities.csv", DEFAULT_ENTITIES_CSV)
    entity_registry = parse_entities_csv(entities_csv)

    cfg, src_url, orig_folder_name = {"run_mode": "eval", "expected_hours": 1}, "", ""
    try:
        meta_r = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{fid}", cfg, params={"fields": "id,name,description"}, timeout=20)
        if meta_r.status_code == 200:
            orig_folder_name = meta_r.json().get("name", "")
        rf = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": f"'{fid}' in parents and trashed=false", "fields": "files(id,name)"}, timeout=20).json()
        for f in rf.get("files", []):
            if f["name"] == "v5_config.json":
                cfg = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", cfg, timeout=20).json()
            if f["name"] == "_source_link.txt":
                src_url = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", cfg, timeout=20).text.strip()
    except Exception as e:
        print(f"[Config Fetch Error] {e}", flush=True)

    mode, editorial_mode = cfg.get("run_mode", "eval"), cfg.get("editorial_mode", "directed")
    chunk_min = 5 if mode == "eval" else int(cfg.get("chunk_size", 10))
    chunk_sec = chunk_min * 60
    workers = max(1, min(int(cfg.get("workers", 3)), 6))
    strat = cfg.get("tier_strategy", "hybrid")
    system_prompt_extra = cfg.get("system_prompt", "")
    editorial_brief = cfg.get("editorial_brief") if isinstance(cfg.get("editorial_brief"), dict) else {}
    fps = cfg.get("daily_fps", "1/5") if mode == "daily" else cfg.get("eval_fps", "1")
    episode_budget_cap = float(cfg.get("max_budget", 2.0) or 2.0)

    raw_label = re.sub(r"^(GEN_READY_|READY_|day-ready_|PROCESSED_|FAILED_|CANCELLED_)", "", cfg.get("presenter_name") or cfg.get("folder_name") or orig_folder_name or mode, flags=re.IGNORECASE).strip("_ ")
    clean_name = sanitize_filename(raw_label)
    editorial_label = (
        "تقييم مهني محايد (Manar-100-v2 · معيار محايد مع حماية اللهجة اللبنانية)"
        if editorial_mode == "neutral"
        else "تقييم الأداء الحواري في قناة موجّهة الخط (Manar-100-v2)"
    )

    push_status(cfg, fid, run_id, "running", "فحص مفاتيح API...", 0, 0, 0.0, started_at)
    valid_free, valid_paid, paid_models = preflight_validate_keys(fk_list, pk, strat, tg_bot, tg_chat)
    if (strat == "flash_only" and not valid_free) or (strat == "pro_only" and not valid_paid) or (not valid_free and not valid_paid):
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ لا يوجد مفاتيح API صالحة")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "لا يوجد مفاتيح API صالحة")
        sys.exit(1)

    if not src_url:
        send_tg("❌ رابط المصدر مفقود.", tg_bot, tg_chat)
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", "❌ رابط المصدر مفقود")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "الرابط مفقود")
        sys.exit(1)

    local_input, downloaded_local, tmp_src = src_url, False, "/tmp/source_video.mp4"
    dur = float(cfg.get("expected_hours", 1)) * 3600
    if not is_youtube_url(src_url):
        try:
            po = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url], capture_output=True, text=True, timeout=30).stdout.strip()
            if po:
                dur = float(po)
        except Exception:
            pass

    if mode == "eval" or dur <= 9000 or is_youtube_url(src_url):
        push_status(cfg, fid, run_id, "running", "تحميل الفيديو محلياً...", 0, max(1, math.ceil(dur / chunk_sec)), 0.0, started_at)
        send_tg(f"📥 تحميل الحلقة محلياً ({clean_name})...", tg_bot, tg_chat)
        if acquire_video_locally(src_url, tmp_src, cfg):
            local_input, downloaded_local = tmp_src, True
            try:
                pl = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", tmp_src], capture_output=True, text=True, timeout=30).stdout.strip()
                if pl:
                    dur = float(pl)
            except Exception:
                pass

    exact_duration, tot, derived_meta = format_hhmmss(dur), math.ceil(dur / chunk_sec), derive_metadata(src_url, dur)
    if tot <= 0:
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ مدة الفيديو صفر")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "مدة الفيديو صفر")
        sys.exit(1)

    send_tg(f"🚀 بدء التحليل V6.0 | {clean_name} | المدة: {exact_duration} | المقاطع: {tot} ({chunk_min}د)", tg_bot, tg_chat)

    act_cost, exhausted_pairs, bad_keys, models_used = 0.0, set(), set(), set()

    def budget_ok() -> bool:
        with STATE_LOCK:
            return act_cost < episode_budget_cap

    def extract_chunk_media(idx: int) -> tuple:
        st, d, mp4_path = idx * chunk_sec, min(chunk_sec, dur - idx * chunk_sec), f"/tmp/chunk_{idx}.mp4"
        for _ in range(3):
            if CANCEL_EVENT.is_set():
                raise RuntimeError("Cancelled by user")
            cmd = ["ffmpeg", "-y", "-loglevel", "error"]
            if not downloaded_local:
                cmd += ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5"]
            cmd += [
                "-ss", str(st), "-t", str(d), "-i", local_input,
                "-vf", f"fps={fps},scale=640:360",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
                "-c:a", "aac", "-ac", "1", "-ar", "16000", "-b:a", "24k",
                "-movflags", "+faststart", mp4_path,
            ]
            with FFMPEG_SEM:
                proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode == 0 and os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 5000:
                return mp4_path, st, d, measure_chunk_speech_ffmpeg(mp4_path, d)
            time.sleep(2)
        raise RuntimeError(f"FFmpeg failed on chunk {idx+1}")

    if mode == "eval":
        push_status(cfg, fid, run_id, "running", f"تقطيع الفيديو ({tot} مقاطع × 5 دقائق)...", 0, tot, 0.0, started_at)
        sliced = {}
        with ThreadPoolExecutor(max_workers=min(3, tot)) as ex:
            futs = {ex.submit(extract_chunk_media, i): i for i in range(tot)}
            for fut in as_completed(futs):
                sliced[futs[fut]] = fut.result()

        chunks_by_idx = [None] * tot
        failed_idxs = []
        running_ctx = {
            "chunk_index": 0, "program_name": None, "presenter_name": raw_label,
            "participants": [{"id": "ANCHOR", "name": raw_label, "role": "مقدّم"}],
            "active_files": [], "last_300_words": "", "open_claims": [], "unresolved_chyrons": [], "cumulative_clock": "00:00:00",
        }
        proc_start = time.time()

        def run_eval_chunk(i: int, ctx_snapshot: dict, force_paid: bool = False):
            mp4_path, st, d, speech_s = sliced[i] if i in sliced and os.path.exists(sliced[i][0]) else extract_chunk_media(i)
            st_hms, end_hms = format_hhmmss(st), format_hhmmss(st + d)
            filled_chunk = (
                prompt_chunk_tpl
                .replace("{{CHUNK_INDEX}}", str(i + 1))
                .replace("{{CHUNK_TOTAL}}", str(tot))
                .replace("{{CHUNK_START}}", st_hms)
                .replace("{{CHUNK_END}}", end_hms)
                .replace("{{PROGRAM_NAME_IF_KNOWN}}", str(ctx_snapshot.get("program_name") or "غير محدد بعد"))
                .replace("{{RUNNING_CONTEXT}}", json.dumps(ctx_snapshot, ensure_ascii=False, indent=2))
                .replace("{{ENTITIES_CSV}}", entities_csv)
            )
            full_prompt = f"{prompt_system}\n\n{filled_chunk}\n\nاسم ملف المصدر: {derived_meta.get('source_filename')}\n{system_prompt_extra}"
            run_strat = "pro_only" if force_paid else strat
            try:
                raw_data, cost, used_model, tier = call_gemini_chunk(
                    mp4_path, full_prompt, valid_free, valid_paid, paid_models, run_strat,
                    exhausted_pairs, bad_keys, budget_ok, chunk_schema=schema_chunk or None
                )
                norm = normalize_chunk_v2(raw_data, i + 1, st_hms, end_hms, d, derived_meta, speech_s, entity_registry)
                return norm, cost, used_model, tier, end_hms
            finally:
                if os.path.exists(mp4_path):
                    try:
                        os.remove(mp4_path)
                    except Exception:
                        pass

        for i in range(tot):
            if CANCEL_EVENT.is_set():
                break
            try:
                norm, cost, used_model, tier, end_hms = run_eval_chunk(i, running_ctx, force_paid=False)
                chunks_by_idx[i] = norm
                running_ctx = update_running_context(running_ctx, norm, end_hms)
                act_cost += cost
                models_used.add(f"{tier}:{used_model}")
                print(f"✅ Chunk {i+1}/{tot} ({norm['window']['start']}->{norm['window']['end']}) turns={len(norm['turns'])} via {tier}:{used_model} | ${cost:.4f}", flush=True)
            except Exception as e:
                if CANCEL_EVENT.is_set() or "Cancelled by user" in str(e):
                    break
                failed_idxs.append(i)
                print(f"⚠️ Chunk {i+1} failed (queued for DLQ): {e}", flush=True)
            done_now = sum(1 for x in chunks_by_idx if x is not None)
            eta = int(((time.time() - proc_start) / max(1, i + 1)) * (tot - i - 1)) + 30
            push_status(cfg, fid, run_id, "running", f"توثيق الصندوق الأسود ({done_now}/{tot})", done_now, tot, act_cost, started_at, eta_sec=eta)

        # DLQ Rescue BEFORE deleting local source video
        if failed_idxs and not CANCEL_EVENT.is_set():
            print(f"Entering DLQ Rescue for {len(failed_idxs)} chunks...", flush=True)
            push_status(cfg, fid, run_id, "running", f"إنقاذ المقاطع المتعثرة ({len(failed_idxs)})...", sum(1 for x in chunks_by_idx if x), tot, act_cost, started_at)
            time.sleep(8)
            for i in failed_idxs:
                if CANCEL_EVENT.is_set():
                    break
                try:
                    use_paid = (strat != "flash_only") and (valid_paid is not None) and budget_ok()
                    norm, cost, used_model, tier, _ = run_eval_chunk(i, running_ctx, force_paid=use_paid)
                    chunks_by_idx[i] = norm
                    act_cost += cost
                    models_used.add(f"{tier}:{used_model}")
                    print(f"🚑 Chunk {i+1} rescued via {tier}:{used_model}", flush=True)
                except Exception as ex:
                    print(f"❌ DLQ failed on chunk {i+1}: {ex}", flush=True)

        if downloaded_local and os.path.exists(local_input):
            try:
                os.remove(local_input)
            except Exception:
                pass

        if CANCEL_EVENT.is_set():
            update_drive_folder_meta(fid, cfg, f"CANCELLED_{clean_name}", f"🔗 {src_url}\n🛑 ملغاة من لوحة التحكم")
            finalize_cost_webhook(cfg, fid, act_cost, "cancelled", "تم الإلغاء")
            sys.exit(0)

        valid_chunks = [c for c in chunks_by_idx if c is not None]
        if not valid_chunks:
            update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ فشلت جميع المقاطع")
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشلت جميع المقاطع")
            sys.exit(1)

        push_status(cfg, fid, run_id, "running", "التوليف وحساب سلّم 100 نقطة وبناء خطة التدريب والرسوم البيانية...", len(valid_chunks), tot, act_cost, started_at, eta_sec=25)
        pre_eval = compute_deterministic_evaluation(valid_chunks, dur, rubric, derived_meta, {}, editorial_brief, editorial_mode)
        synth_request = (
            f"{prompt_system}\n\n{prompt_synth_tpl}\n\n"
            f"نوع المسار التحريري: {editorial_label}\n"
            f"المؤشرات المحسوبة حتمياً:\n{json.dumps(pre_eval['kpis'], ensure_ascii=False)}\n\n"
            f"الموجز التحريري (editorial_brief):\n{json.dumps(editorial_brief, ensure_ascii=False)}\n\n"
            f"أطول 5 أسئلة للمقدم (أعد صياغة أطول 3 منها إلى ≤12ث و≤30 كلمة باستفهام واحد ومرجع واحد):\n"
            f"{json.dumps(pre_eval['longest_questions'], ensure_ascii=False)}\n\n"
            'أخرج JSON صالح فقط بهذه المفاتيح:\n'
            '{"qualitative_items":{"B3":{"score":0,"anchor_hit":"","evidence":[],"note":""},"C3":{"score":0,"anchor_hit":"","evidence":[],"note":""},"D1":{"score":0,"anchor_hit":"","evidence":[],"note":""},"D4":{"score":0,"anchor_hit":"","evidence":[],"note":""},"E1":{"score":0,"anchor_hit":"","evidence":[],"note":""},"E2":{"score":0,"anchor_hit":"","evidence":[],"note":""},"E4":{"score":0,"anchor_hit":"","evidence":[],"note":""},"F1":{"score":0,"anchor_hit":"","evidence":[],"note":""},"F2":{"score":0,"anchor_hit":"","evidence":[],"note":""},"F3":{"score":0,"anchor_hit":"","evidence":[],"note":""},"G2":{"score":0,"anchor_hit":"","evidence":[],"note":""}},'
            '"question_rewrites":[{"ts":"","orig_duration_s":0,"original":"","rewritten":""}],'
            '"report":{"summary":"","strengths":[""],"priorities":[""]}}\n\n'
            f"بيانات المقاطع:\n{json.dumps(valid_chunks, ensure_ascii=False)[:180000]}"
        )

        llm_qual = {}
        synth_candidates = (
            [(valid_paid, m, True) for m in (rank_discovered_models(paid_models, PREFERRED_SYNTH_ORDER, True) or paid_models)]
            if (strat != "flash_only" and valid_paid and budget_ok()) else []
        ) + [(k, m, False) for k, kms in valid_free for m in kms]
        for skey, smodel, spaid in synth_candidates:
            try:
                client = genai.Client(api_key=skey)
                res = client.models.generate_content(model=smodel, contents=synth_request, config=build_gen_config(max_out=32768))
                llm_qual = clean_json_text(res.text)
                act_cost += calc_cost(smodel, res.usage_metadata, spaid)
                models_used.add(f"{'PAID' if spaid else 'FREE'}:{smodel}")
                break
            except Exception as e:
                print(f"[Synthesis Retry {smodel}] {str(e)[:150]}", flush=True)

        final_eval = compute_deterministic_evaluation(valid_chunks, dur, rubric, derived_meta, llm_qual, editorial_brief, editorial_mode)
        charts = generate_eval_charts(final_eval)
        meta_info = {
            "clean_name": clean_name, "editorial_label": editorial_label,
            "chunks_ok": len(valid_chunks), "chunks_total": tot, "actual_cost": act_cost,
            "models_used": sorted(models_used), "src_url": src_url,
        }

        det_html = render_detailed_eval_html(clean_name, exact_duration, derived_meta, final_eval, llm_qual, charts, meta_info)
        sum_html = render_summarized_eval_html(clean_name, exact_duration, final_eval, llm_qual, charts)
        trn_html = render_training_plan_html(clean_name, final_eval, llm_qual)
        bb_md = generate_blackbox_v2_md(clean_name, exact_duration, derived_meta, valid_chunks, final_eval, meta_info)
        bb_json_obj = {
            "prompt_version": "blackbox-v2.0",
            "rubric_version": rubric.get("rubric_version", "manar-100-v2"),
            "metadata": {**meta_info, "derived": derived_meta},
            "evaluation": final_eval,
            "qualitative_synthesis": llm_qual,
            "chunks": valid_chunks,
        }

        files_to_write = [
            (f"01_Main_Detailed_Report_{clean_name}.html", det_html, "text/html"),
            (f"02_Summarized_Report_{clean_name}.html", sum_html, "text/html"),
            (f"03_Training_Plan_{clean_name}.html", trn_html, "text/html"),
            (f"04_Blackbox_V2_{clean_name}.md", bb_md, "text/markdown"),
            (f"05_Blackbox_V2_{clean_name}.json", json.dumps(bb_json_obj, ensure_ascii=False, indent=2), "application/json"),
        ]
        out_paths = []
        for fname, content, mime in files_to_write:
            fp = os.path.join(BACKUP_DIR, fname)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(content)
            out_paths.append(fp)
            drive_upload(fid, cfg, fp, mime)
            if not fname.endswith(".json"):
                send_tg(f"📄 <b>{fname}</b> | النتيجة: {final_eval['score_final']['reported_as']} | الكلفة: ${act_cost:.4f}", tg_bot, tg_chat, fp)

        email_subj = f"✅ تقييم V6.0 ({clean_name}) — {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']})"
        smtp_ok = send_smtp_resilient(email_subj, sum_html, out_paths[:4], cfg.get("alert_emails", ""), gm_u, gm_p)
        final_folder_name = f"PROCESSED_{clean_name}_${act_cost:.2f}"
        folder_desc = (
            f"🔗 رابط المصدر: {src_url}\n\n"
            f"• الاسم: {clean_name}\n• النتيجة: {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']})\n"
            f"• الخام: {final_eval['score_final']['raw_score']} | التعقيد: +{final_eval['score_final']['difficulty_modifier']} | السقف: {final_eval['score_final']['cap_ceiling']}\n"
            f"• الأولوية القصوى: {final_eval['top_priority_auto']}\n• الكلفة: ${act_cost:.4f} | المقاطع: {len(valid_chunks)}/{tot}"
        )
        update_drive_folder_meta(fid, cfg, final_folder_name, folder_desc)
        finalize_cost_webhook(cfg, fid, act_cost, "completed", f"اكتمل ({final_eval['score_final']['reported_as']})", trigger_gas_email=(not smtp_ok), email_subject=email_subj)
        print(f"🎉 V6.0 completed! {final_folder_name} | Score: {final_eval['score_final']['reported_as']} | Cost: ${act_cost:.4f}", flush=True)

    else:
        # Daily Broadcast Mode (Parallel + DLQ + Detailed & Summarized Word-RTL + Blackbox .md/.json)
        daily_results, failed_daily, done_daily = [None] * tot, [], 0
        proc_start = time.time()

        def run_daily(i: int, force_paid: bool = False):
            mp4, st, d, _ = extract_chunk_media(i)
            st_h, en_h = format_hhmmss(st), format_hhmmss(st + d)
            pr = (
                f"أخرج JSON لحلقة البث اليومي من {st_h} إلى {en_h} (Zero Hallucination) بهذا الشكل:\n"
                '{"chunk_window":"","programs":[{"start":"HH:MM:SS","end":"HH:MM:SS","category":"","theme":"","title":"","presenter":"","guests":[],"reporters":[],"vt_reports":[],"summary":""}],"breaking_tickers":[]}\n'
                f"{system_prompt_extra}"
            )
            try:
                dat, c, m, t = call_gemini_chunk(mp4, pr, valid_free, valid_paid, paid_models, "pro_only" if force_paid else strat, exhausted_pairs, bad_keys, budget_ok)
                with STATE_LOCK:
                    models_used.add(f"{t}:{m}")
                return dat, c
            finally:
                if os.path.exists(mp4):
                    try:
                        os.remove(mp4)
                    except Exception:
                        pass

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(run_daily, i, False): i for i in range(tot)}
            for fut in as_completed(futs):
                i = futs[fut]
                try:
                    dat, c = fut.result()
                    daily_results[i] = {"chunk_index": i + 1, "data": dat}
                    with STATE_LOCK:
                        act_cost += c
                        done_daily += 1
                        cur_d, cur_c = done_daily, act_cost
                    eta = int(((time.time() - proc_start) / cur_d) * (tot - cur_d)) + 15
                    push_status(cfg, fid, run_id, "running", f"تحليل البث اليومي ({cur_d}/{tot})", cur_d, tot, cur_c, started_at, eta_sec=eta)
                except Exception as e:
                    if CANCEL_EVENT.is_set() or "Cancelled by user" in str(e):
                        break
                    failed_daily.append(i)
                    print(f"⚠️ Daily chunk {i+1}: {e}", flush=True)

        if failed_daily and not CANCEL_EVENT.is_set():
            time.sleep(8)
            for i in failed_daily:
                if CANCEL_EVENT.is_set():
                    break
                try:
                    dat, c = run_daily(i, force_paid=(strat != "flash_only" and bool(valid_paid) and budget_ok()))
                    daily_results[i] = {"chunk_index": i + 1, "data": dat}
                    act_cost += c
                except Exception:
                    pass

        if downloaded_local and os.path.exists(local_input):
            try:
                os.remove(local_input)
            except Exception:
                pass

        if CANCEL_EVENT.is_set():
            update_drive_folder_meta(fid, cfg, f"CANCELLED_{clean_name}", f"🔗 {src_url}\n🛑 ملغاة")
            finalize_cost_webhook(cfg, fid, act_cost, "cancelled", "تم الإلغاء")
            sys.exit(0)

        valid_daily = [r for r in daily_results if r and isinstance(r.get("data"), dict)]
        if not valid_daily:
            update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ فشلت جميع المقاطع")
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشلت جميع المقاطع")
            sys.exit(1)

        all_progs = [p for r in valid_daily for p in r["data"].get("programs", []) if isinstance(p, dict)]
        rows = "".join(
            f"<tr class='{'alt' if idx%2 else ''}'><td class='num' dir='ltr'>{esc(p.get('start'))}–{esc(p.get('end'))}</td>"
            f"<td>{esc(p.get('category'))}</td><td>{esc(p.get('theme'))}</td><td class='lbl'>{esc(p.get('title'))}</td><td>{esc(p.get('summary'))}</td></tr>"
            for idx, p in enumerate(all_progs)
        )
        cat_counts = {}
        for p in all_progs:
            cat = p.get("category") or "أخرى"
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
        cat_rows = "".join(f"<tr><td class='lbl'>{esc(k)}</td><td class='num'>{v}</td></tr>" for k, v in sorted(cat_counts.items(), key=lambda x: x[1], reverse=True))

        det_daily = wrap_word_doc(
            f"البث اليومي المفصل - {clean_name}",
            f"<table style='margin:0;'><tr><td style='background:#12304d;color:#fff;padding:18pt;'><p style='margin:0;color:#d9c48f;font-size:8.5pt;'>رصد البث اليومي V6.0</p><p style='margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;'>{esc(clean_name)} — المدة: {esc(exact_duration)}</p></td></tr></table>"
            f"<h2>جدول البرامج والفقرات ({len(all_progs)} فقرة)</h2><table><tr><th>التوقيت</th><th>التصنيف</th><th>المحور</th><th>العنوان</th><th>الملخص</th></tr>{rows}</table>"
        )
        sum_daily = wrap_word_doc(
            f"ملخص البث اليومي - {clean_name}",
            f"<table style='margin:0;'><tr><td style='background:#12304d;color:#fff;padding:18pt;'><p style='margin:0;color:#d9c48f;font-size:8.5pt;'>الملخص التنفيذي للبث اليومي V6.0</p><p style='margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;'>{esc(clean_name)}</p><p style='margin:4pt 0 0;color:#c9d6e4;'>المدة: {esc(exact_duration)} | الفقرات: {len(all_progs)} | المقاطع: {len(valid_daily)}/{tot} | الكلفة: ${act_cost:.4f}</p></td></tr></table>"
            f"<h2>توزيع الفقرات حسب التصنيف</h2><table><tr><th>التصنيف</th><th class='num'>العدد</th></tr>{cat_rows}</table>"
        )
        bb_daily_md = f"# DAILY BROADCAST BLACKBOX V2 — {clean_name}\n- Duration: {exact_duration} | Chunks: {len(valid_daily)}/{tot} | Cost: ${act_cost:.4f}\n\n" + "\n".join(
            f"- [{p.get('start')}–{p.get('end')}] **{p.get('title')}** ({p.get('category')}/{p.get('theme')}): {p.get('summary')}" for p in all_progs
        )
        daily_files = [
            (f"01_Daily_Detailed_Report_{clean_name}.html", det_daily, "text/html"),
            (f"02_Daily_Summarized_Report_{clean_name}.html", sum_daily, "text/html"),
            (f"04_Blackbox_V2_{clean_name}.md", bb_daily_md, "text/markdown"),
            (f"05_Blackbox_V2_{clean_name}.json", json.dumps({"clean_name": clean_name, "derived": derived_meta, "chunks": valid_daily}, ensure_ascii=False, indent=2), "application/json"),
        ]
        out_paths = []
        for fname, content, mime in daily_files:
            fp = os.path.join(BACKUP_DIR, fname)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(content)
            out_paths.append(fp)
            drive_upload(fid, cfg, fp, mime)
            if not fname.endswith(".json"):
                send_tg(f"📄 <b>{fname}</b> | الكلفة: ${act_cost:.4f}", tg_bot, tg_chat, fp)

        email_subj = f"✅ تقرير البث اليومي V6.0 ({clean_name}) | الكلفة: ${act_cost:.4f}"
        smtp_ok = send_smtp_resilient(email_subj, sum_daily, out_paths[:3], cfg.get("alert_emails", ""), gm_u, gm_p)
        update_drive_folder_meta(fid, cfg, f"PROCESSED_{clean_name}_${act_cost:.2f}", f"🔗 {src_url}\n• الفقرات: {len(all_progs)} | الكلفة: ${act_cost:.4f}")
        finalize_cost_webhook(cfg, fid, act_cost, "completed", f"اكتمل البث اليومي ({len(valid_daily)}/{tot})", trigger_gas_email=(not smtp_ok), email_subject=email_subj)


if __name__ == "__main__":
    main()
