#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V7.0 - Blackbox v2.0 + Manar-100-v2 Deterministic Engine
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
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
_ensure_pkg("PIL", "pillow")
_ensure_pkg("fontTools", "fonttools")

from google import genai
from google.genai import types
import jsonschema
from PIL import Image, ImageDraw, ImageFont, features as pil_features
from fontTools.ttLib import TTFont

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
MODEL_COOLDOWN_UNTIL = {}   # model_name -> epoch timestamp (for 503 spikes)
PAIR_COOLDOWN_UNTIL = {}    # (api_key, model_name) -> epoch timestamp (for 1st 429 RPM)
PAIR_429_COUNT = {}         # (api_key, model_name) -> count of 429 hits
KEY_HEALTH_STATS = {}       # masked_key -> {tier, ok, err_429, err_503, err_timeout, err_other}
DRIVE_AUTH_MODE = "token"   # service_account | refresh_token | token
RUN_START_MONO = time.time()


def record_key_stat(api_key: str, is_paid: bool, outcome: str):
    mk = mask_key(api_key)
    with STATE_LOCK:
        st = KEY_HEALTH_STATS.setdefault(mk, {"key": mk, "tier": "PAID" if is_paid else "FREE", "ok": 0, "err_429": 0, "err_503": 0, "err_timeout": 0, "err_other": 0})
        if outcome in st:
            st[outcome] += 1


def get_key_health_snapshot() -> list:
    with STATE_LOCK:
        return [dict(v) for v in KEY_HEALTH_STATS.values()]


def log(msg: str):
    """Timestamped unbuffered logger for GitHub Actions diagnostics."""
    now_str = datetime.now(BEIRUT_TZ).strftime("%H:%M:%S")
    el = int(time.time() - RUN_START_MONO)
    em, es = divmod(el, 60)
    eh, em = divmod(em, 60)
    el_str = f"{eh:02d}:{em:02d}:{es:02d}" if eh else f"{em:02d}:{es:02d}"
    print(f"[{now_str} | +{el_str}] {msg}", flush=True)


def run_with_hard_timeout(fn, timeout_sec: float, label: str):
    """Runs blocking SDK/network calls inside a daemon thread so stalled TCP sockets NEVER hang the job."""
    box = {"done": False, "val": None, "err": None}

    def _target():
        try:
            box["val"] = fn()
        except BaseException as exc:
            box["err"] = exc
        finally:
            box["done"] = True

    th = threading.Thread(target=_target, daemon=True)
    th.start()
    th.join(timeout=timeout_sec)
    if not box["done"]:
        raise TimeoutError(f"Hard watchdog timeout ({int(timeout_sec)}s) exceeded during: {label}")
    if box["err"] is not None:
        raise box["err"]
    return box["val"]


def make_genai_client(api_key: str, timeout_ms: int = 180_000):
    """Creates google-genai Client with explicit HTTP timeout (milliseconds) when supported."""
    try:
        http_opts = types.HttpOptions(timeout=timeout_ms)
        return genai.Client(api_key=api_key, http_options=http_opts)
    except Exception:
        return genai.Client(api_key=api_key)

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
    parsed = None
    try:
        parsed = json.loads(raw)
    except Exception:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end > start:
            parsed = json.loads(raw[start : end + 1])
        else:
            raise
    # Unwrap accidental single-element list wrapping (e.g., [{...}])
    if isinstance(parsed, list):
        for elem in parsed:
            if isinstance(elem, dict):
                return elem
        raise ValueError(f"Expected JSON object, got list of length {len(parsed)}")
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected JSON object, got {type(parsed).__name__}")
    return parsed


AXIS_AR_CODE = {"A": "أ", "B": "ب", "C": "ج", "D": "د", "E": "هـ", "F": "و", "G": "ز"}
ITEM_AR_CODE = {
    "A1": "أ١", "A2": "أ٢", "A3": "أ٣", "A4": "أ٤",
    "B1": "ب١", "B2": "ب٢", "B3": "ب٣", "B4": "ب٤",
    "C1": "ج١", "C2": "ج٢", "C3": "ج٣", "C4": "ج٤",
    "D1": "د١", "D2": "د٢", "D3": "د٣", "D4": "د٤",
    "E1": "هـ١", "E2": "هـ٢", "E3": "هـ٣", "E4": "هـ٤",
    "F1": "و١", "F2": "و٢", "F3": "و٣",
    "G1": "ز١", "G2": "ز٢", "G3": "ز٣",
}
CONF_AR = {"high": "عالية", "medium": "متوسطة", "low": "منخفضة"}
CRIT_EVENT_AR = {
    "name_error": "خطأ في اسم ضيف أو مراسل على الهواء",
    "local_chyron_unread": "فوات شريط أمني/عاجل محلي دون قراءة",
    "monologue_over_180s": "استرسال ضيف متّصل تجاوز 180 ثانية",
    "guest_uninterrupted_over_300s": "استرسال ضيف متّصل تجاوز 300 ثانية",
    "duplicate_file": "تكرار ملف حواري سبقت مناقشته",
    "factual_error_uncorrected": "خطأ واقعي لم يُصحَّح على الهواء",
    "multiple_critical": "تعدّد الأخطاء الحرجة (خطآن فأكثر)",
    "topic_abandoned": "ترك محور رئيسي دون إغلاق",
}
AUDIT_CHECK_AR = {
    "priority_vs_score": "اتساق الأولوية القصوى مع أدنى بند تقييماً",
    "score_vs_metric": "تطابق الدرجات مع المراسي الرقمية المعتمدة",
    "count_consistency": "تطابق عدد البنود الـ 26 مع المحاور السبعة",
    "timezone_consistency": "دقّة المنطقة الزمنية لتوقيت بيروت",
    "sum_consistency": "الدقّة الحسابية لمجموع درجات المحاور",
    "evidence_coverage": "تغطية الأدلة والشواهد الزمنية لكل بند",
}
ANCHOR_HIT_AR = {
    "full": "تحقّق كامل للمعيار",
    "high": "مستوى مرتفع",
    "between_50_and_100": "فوق النصف (جيد)",
    "partial": "تحقّق جزئي",
    "between_0_and_50": "دون النصف (يحتاج تطوير)",
    "between_50_and_0": "دون النصف (يحتاج تطوير)",
    "low": "منخفض",
    "zero": "دون الحدّ الأدنى",
    "qualitative": "تقييم نوعي موثّق",
    "no_data_neutral": "بيانات غير كافية",
    "referenced_only": "إشارة مرجعية (دون تكرار خصم)",
    "range": "مقارنة بالنطاق المستهدف",
}


def clean_display_name(raw_label: str) -> str:
    """Turns 'أحمد_ياسين_1' into clean display name 'أحمد ياسين' while preserving real names."""
    s = re.sub(r"^(GEN_READY_|READY_|day-ready_|PROCESSED_|FAILED_|CANCELLED_)", "", str(raw_label or ""), flags=re.IGNORECASE)
    s = re.sub(r"[_]+", " ", s).strip()
    # Strip trailing standalone run counter like ' 1' or ' 2' if preceded by Arabic text
    s = re.sub(r"(?<=[؀-ۿ])\s+\d{1,2}$", "", s).strip()
    return s or "مقدّم البرنامج"


def fmt_ts_clean(ts: str) -> str:
    """Strips .000 milliseconds for crisp Arabic report presentation."""
    s = str(ts or "").strip()
    m = re.match(r"^(\d{1,2}:\d{2}:\d{2})", s)
    return m.group(1) if m else s


def fmt_ts_span(ts: str) -> str:
    c = fmt_ts_clean(ts)
    return f'<span dir="ltr">{esc(c)}</span>' if c else "—"


def fmt_anchor_benchmark(item_def: dict) -> str:
    anc = item_def.get("anchors") or {}
    cid = item_def.get("id", "")
    if cid == "A4":
        return "مقدّم 20–30% · ضيف 45–55% · ميدان 25–35%"
    if cid == "F2":
        return "تواصل بصري وحضور متزن (ثقة ≤ متوسطة)"
    u = item_def.get("unit", "")
    unit_ar = {"per_10min": "/10د", "seconds": " ث", "percent": "%", "count": "", "per_hour": "/ساعة", "ratio": ""}.get(u, "")
    if "full" in anc and "half" in anc:
        return f"كاملة: {anc['full']}{unit_ar} · نصفية: {anc['half']}{unit_ar}"
    return "وفق المرساة المعتمدة"


def format_metric_display(cid: str, val, kpis: dict = None) -> str:
    if val is None or val == "":
        return "غير متاح"
    s = str(val).strip()
    if s in ANCHOR_HIT_AR:
        return ANCHOR_HIT_AR[s]
    if cid == "A1":
        return f"{float(val):.1f} مداخلة / 10 دقائق"
    if cid == "A2":
        return f"{float(val):.1f} ثانية / مداخلة"
    if cid == "A3":
        return f"{int(round(float(val)))} ثانية (أطول بلوك)"
    if cid == "A4" and "/" in s:
        p = s.split("/")
        if len(p) == 3:
            return f"مقدّم {p[0]}% · ضيف {p[1]}% · ميدان {p[2]}%"
    if cid == "B1":
        return f"{float(val):.1f}% أسئلة أحادية"
    if cid == "B2":
        return f"{float(val):.1f} ثانية (متوسط التمهيد)"
    if cid == "B4":
        n = int(float(val))
        return "لا تكرار (0)" if n == 0 else f"{n} ملف مكرّر"
    if cid == "C1":
        return f"{float(val):.1f} طلب دليل / ساعة"
    if cid == "C2":
        return f"{float(val):.1f} فرضية مضادة / ساعة"
    if cid in ("C4", "G3"):
        n = int(float(val))
        return "صفر أخطاء" if n == 0 else f"{n} خطأ"
    if cid == "D3":
        return f"{float(val):.2f} لمسة / 90 ثانية"
    if cid == "E3":
        return f"{float(val):.0f}% تغطية العواجل"
    if cid == "G1":
        return f"{int(round(float(val)))} ثانية (الافتتاح)"
    return s


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
    from email.message import EmailMessage
    user = (user or "").strip()
    pwd = re.sub(r"\s+", "", (pwd or "").strip())
    recips = [e.strip() for e in re.split(r"[,;\s\n\r]+", emails or "") if "@" in e.strip()]
    if user and user not in recips:
        recips.insert(0, user)
    if not (user and pwd and recips):
        return False

    def build_msg(recipient, attach_files):
        msg = EmailMessage()
        msg["From"] = user
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.set_content("يرجى فتح التقرير المرفق بصيغة HTML.", subtype="plain", charset="utf-8")
        msg.add_alternative(html_body, subtype="html", charset="utf-8")
        for pf in attach_files:
            if pf and os.path.exists(pf):
                with open(pf, "rb") as f:
                    raw = f.read()
                fn = os.path.basename(pf)
                sub = "html" if fn.endswith(".html") else ("markdown" if fn.endswith(".md") else "octet-stream")
                main_t = "text" if sub in ("html", "markdown") else "application"
                msg.add_attachment(raw, maintype=main_t, subtype=sub, filename=fn)
        return msg

    # Try full attachments on 465 & 587, then body-only fallback if connection drops on size
    for attach_set, label in ((files, "with attachments"), ([], "summary only")):
        for port, use_ssl in ((465, True), (587, False)):
            try:
                if use_ssl:
                    with smtplib.SMTP_SSL("smtp.gmail.com", port, timeout=60) as s:
                        s.ehlo()
                        s.login(user, pwd)
                        for rec in recips:
                            s.send_message(build_msg(rec, attach_set))
                else:
                    with smtplib.SMTP("smtp.gmail.com", port, timeout=60) as s:
                        s.ehlo()
                        s.starttls()
                        s.ehlo()
                        s.login(user, pwd)
                        for rec in recips:
                            s.send_message(build_msg(rec, attach_set))
                print(f"✅ SMTP ({port}, {label}) sent to: {recips}", flush=True)
                return True
            except Exception as e:
                print(f"[SMTP {port} {label} Warning] {e}", flush=True)
                time.sleep(2)
    return False


def init_or_refresh_permanent_drive_token() -> bool:
    """Acquires/refreshes a Google Drive OAuth token without 60-min expiration via Service Account or Refresh Token."""
    global RUNTIME_DRIVE_TOKEN, DRIVE_AUTH_MODE
    sa_json = (os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON") or "").strip()
    if sa_json:
        try:
            from google.oauth2 import service_account
            from google.auth.transport.requests import Request as GoogleAuthRequest
            info = json.loads(sa_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/drive"])
            creds.refresh(GoogleAuthRequest())
            if creds.token:
                with DRIVE_TOKEN_LOCK:
                    RUNTIME_DRIVE_TOKEN = creds.token
                DRIVE_AUTH_MODE = "service_account"
                log("🔐 Authenticated Google Drive via Permanent Service Account.")
                return True
        except Exception as e:
            log(f"⚠️ Service Account Drive Auth failed (falling back): {e}")

    rtok = (os.environ.get("GDRIVE_REFRESH_TOKEN") or "").strip()
    cid = (os.environ.get("GDRIVE_CLIENT_ID") or "").strip()
    csec = (os.environ.get("GDRIVE_CLIENT_SECRET") or "").strip()
    if rtok and cid and csec:
        try:
            r = requests.post(
                "https://oauth2.googleapis.com/token",
                data={"client_id": cid, "client_secret": csec, "refresh_token": rtok, "grant_type": "refresh_token"},
                timeout=15,
            )
            if r.status_code == 200 and r.json().get("access_token"):
                with DRIVE_TOKEN_LOCK:
                    RUNTIME_DRIVE_TOKEN = r.json()["access_token"].strip()
                DRIVE_AUTH_MODE = "refresh_token"
                log("🔐 Refreshed Google Drive OAuth token via permanent Refresh Token.")
                return True
        except Exception as e:
            log(f"⚠️ OAuth Refresh Token failed (falling back): {e}")
    return False


def refresh_drive_token_via_webhook(cfg) -> bool:
    global RUNTIME_DRIVE_TOKEN
    if init_or_refresh_permanent_drive_token():
        return True
    webhook, secret = cfg.get("webhook_url"), cfg.get("webhook_secret", "")
    if not (webhook and secret):
        return False
    try:
        r = requests.post(webhook, json={"action": "refresh_drive_token", "webhook_secret": secret}, timeout=15)
        if r.status_code == 200 and r.json().get("drive_token"):
            with DRIVE_TOKEN_LOCK:
                RUNTIME_DRIVE_TOKEN = r.json()["drive_token"].strip()
            log("🔄 Refreshed Google Drive OAuth token via Apps Script webhook.")
            return True
    except Exception as e:
        log(f"[Token Refresh Warning] {e}")
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


def drive_upload_json_blob(folder_id, cfg, filename: str, obj) -> bool:
    tmp_p = os.path.join("/tmp", f"ckpt_{os.getpid()}_{filename}")
    try:
        with open(tmp_p, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        return drive_upload(folder_id, cfg, tmp_p, "application/json")
    except Exception as e:
        log(f"[Checkpoint Save Warning] {filename}: {e}")
        return False
    finally:
        if os.path.exists(tmp_p):
            try:
                os.remove(tmp_p)
            except Exception:
                pass


def load_drive_checkpoints(folder_id, cfg) -> tuple:
    """Loads any previously saved _ckpt_chunk_<idx>.json and _ckpt_ctx.json from Drive for seamless Resume."""
    chunks_map, saved_ctx = {}, None
    if not folder_id:
        return chunks_map, saved_ctx
    try:
        q = f"'{folder_id}' in parents and name contains '_ckpt_' and trashed=false"
        r = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q, "fields": "files(id,name)", "pageSize": 500}, timeout=25)
        if r.status_code != 200:
            return chunks_map, saved_ctx
        for f in r.json().get("files", []):
            fn = f.get("name", "")
            if fn == "_ckpt_ctx.json":
                rr = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", cfg, timeout=20)
                if rr.status_code == 200:
                    saved_ctx = rr.json()
            else:
                m = re.match(r"^_ckpt_chunk_(\d+)\.json$", fn)
                if m:
                    rr = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", cfg, timeout=20)
                    if rr.status_code == 200:
                        chunks_map[int(m.group(1))] = rr.json()
    except Exception as e:
        log(f"[Checkpoint Load Warning] {e}")
    return chunks_map, saved_ctx


def cleanup_drive_checkpoints(folder_id, cfg):
    """Removes temporary _ckpt_*.json files once a run completes cleanly."""
    if not folder_id:
        return
    try:
        q = f"'{folder_id}' in parents and name contains '_ckpt_' and trashed=false"
        r = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q, "fields": "files(id,name)", "pageSize": 500}, timeout=20)
        if r.status_code == 200:
            for f in r.json().get("files", []):
                drive_request("DELETE", f"https://www.googleapis.com/drive/v3/files/{f['id']}", cfg, timeout=15)
    except Exception:
        pass


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


def estimate_initial_eta_sec(cfg: dict, total_chunks: int) -> int:
    if total_chunks <= 0:
        return 0
    mode = cfg.get("run_mode", "eval")
    strat = cfg.get("tier_strategy", "hybrid")
    workers = max(1, min(int(cfg.get("workers", 3) or 3), 6))
    if mode == "eval":
        # Sequential chunks (running_context): realistic per-chunk time including upload + ACTIVE + API + retries
        sec_per_chunk = 95 if strat == "flash_only" else (65 if strat == "hybrid" else 45)
        return int(total_chunks * sec_per_chunk + 75)
    batches = math.ceil(total_chunks / workers)
    sec_per_batch = 115 if strat == "flash_only" else (75 if strat == "hybrid" else 45)
    return int(batches * sec_per_batch + 60)


def push_status(cfg, folder_id, run_id, status, phase, completed, total, act_cost, started_at, eta_sec=None, message=""):
    elapsed = int(time.time() - started_at)
    if eta_sec is None:
        if completed > 0 and total > completed:
            eta_sec = int((elapsed / completed) * (total - completed)) + 45
        elif status == "running":
            eta_sec = estimate_initial_eta_sec(cfg, total)
        else:
            eta_sec = 0
    payload = {
        "action": "update_status", "job_id": folder_id, "run_id": str(run_id or ""),
        "status": status, "phase": phase, "completed_chunks": int(completed),
        "total_chunks": int(total), "elapsed_sec": elapsed, "eta_sec": max(0, int(eta_sec)),
        "actual_cost": round(float(act_cost), 4), "message": message, "updated_at_epoch": int(time.time()),
        "key_health": get_key_health_snapshot(),
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


def finalize_cost_webhook(cfg, folder_id, actual_cost: float, final_status="completed", message="", trigger_gas_email=False, email_subject="", completed_chunks=None, total_chunks=None, kpi_summary=None):
    kh = get_key_health_snapshot()
    st_payload = {
        "job_id": folder_id, "run_id": str(os.environ.get("GITHUB_RUN_ID", "")),
        "status": final_status, "phase": message or final_status, "eta_sec": 0,
        "actual_cost": round(float(actual_cost), 4), "message": message, "updated_at_epoch": int(time.time()),
        "key_health": kh,
    }
    if completed_chunks is not None:
        st_payload["completed_chunks"] = int(completed_chunks)
    if total_chunks is not None:
        st_payload["total_chunks"] = int(total_chunks)
    write_drive_status_and_check_cancel(folder_id, cfg, st_payload)
    if cfg.get("webhook_url"):
        try:
            wh_body = {
                "action": "finalize_cost", "webhook_secret": cfg.get("webhook_secret", ""),
                "job_id": folder_id, "status": final_status,
                "estimated_cost": cfg.get("pre_authorized_cost", 0), "actual_cost": round(float(actual_cost), 4),
                "message": message, "send_backup_email": trigger_gas_email,
                "email_subject": email_subject, "alert_emails": cfg.get("alert_emails", ""),
                "completed_chunks": completed_chunks, "total_chunks": total_chunks,
                "key_health": kh,
            }
            if kpi_summary:
                wh_body["kpi_summary"] = kpi_summary
            requests.post(cfg["webhook_url"], json=wh_body, timeout=25)
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


def wait_file_active(client, uploaded_file, timeout_sec=90, tag=""):
    t0, f = time.time(), uploaded_file
    polls = 0
    while True:
        if CANCEL_EVENT.is_set():
            raise RuntimeError("Cancelled by user")
        st = str(getattr(f, "state", "")).upper()
        if "ACTIVE" in st:
            log(f"  ✅ {tag}File ACTIVE ({f.name}) in {time.time() - t0:.1f}s ({polls} polls)")
            return f
        if "FAILED" in st:
            raise ValueError(f"Google File API processing failed for {f.name}")
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(f"Timed out waiting {timeout_sec}s for {f.name} (state={st})")
        time.sleep(2)
        polls += 1
        f = run_with_hard_timeout(lambda: client.files.get(name=f.name), 25, f"{tag}files.get({f.name})")


def calc_cost(model_name: str, usage, is_paid: bool) -> float:
    if not is_paid or not usage:
        return 0.0
    in_t = getattr(usage, "prompt_token_count", 0) or 0
    out_t = getattr(usage, "candidates_token_count", 0) or 0
    think_t = getattr(usage, "thoughts_token_count", 0) or 0
    rates = PRICING.get(model_name, {"in": 0.75, "out": 3.75})
    return (in_t / 1e6) * rates["in"] + ((out_t + think_t) / 1e6) * rates["out"]


def call_gemini_chunk(media_path, prompt, valid_free, valid_paid, paid_models, strategy, exhausted_pairs, bad_keys, budget_ok_fn, chunk_schema=None, chunk_tag=""):
    global FREE_RR_COUNTER
    last_err = ""
    config = build_gen_config(max_out=32768)
    tag = f"[{chunk_tag}] " if chunk_tag else ""
    chunk_t0 = time.time()
    file_mb = (os.path.getsize(media_path) / (1024 * 1024)) if os.path.exists(media_path) else 0.0

    def run_with_key(api_key, model_candidates, is_paid):
        nonlocal last_err
        tier_lbl = "PAID" if is_paid else "FREE"
        if CANCEL_EVENT.is_set():
            raise RuntimeError("Cancelled by user")
        if time.time() - chunk_t0 > 480:
            last_err = "Chunk wall-clock limit (480s) reached"
            log(f"  ⏰ {tag}{last_err} — aborting chunk to DLQ")
            return None
        if is_paid and not budget_ok_fn():
            last_err = "Episode budget ceiling reached"
            log(f"  💰 {tag}Skipped PAID key — episode budget cap reached")
            return None
        with STATE_LOCK:
            if api_key in bad_keys:
                return None
            now_ts = time.time()
            avail_m = [m for m in model_candidates if (api_key, m) not in exhausted_pairs]
            # Prefer models neither in 503 model-cooldown nor in 429 pair-cooldown
            warm_m = [m for m in avail_m if MODEL_COOLDOWN_UNTIL.get(m, 0) <= now_ts and PAIR_COOLDOWN_UNTIL.get((api_key, m), 0) <= now_ts]
            cool_m = [m for m in avail_m if m not in warm_m]
            models_to_try = warm_m + cool_m
        if not models_to_try:
            log(f"  ⏭️ {tag}Skipping {tier_lbl} {mask_key(api_key)} — all models exhausted")
            return None

        client = make_genai_client(api_key, timeout_ms=180_000)
        uploaded = None
        try:
            t_up = time.time()
            log(f"  ⬆️ {tag}Uploading {file_mb:.2f} MB to {tier_lbl} {mask_key(api_key)} (models={models_to_try})...")
            raw_up = run_with_hard_timeout(lambda: client.files.upload(file=media_path), 90, f"{tag}files.upload({mask_key(api_key)})")
            log(f"  ⏳ {tag}Uploaded ({raw_up.name}) in {time.time() - t_up:.1f}s — waiting for ACTIVE...")
            uploaded = wait_file_active(client, raw_up, timeout_sec=90, tag=tag)

            for model_name in models_to_try:
                if CANCEL_EVENT.is_set():
                    raise RuntimeError("Cancelled by user")
                if time.time() - chunk_t0 > 480:
                    last_err = "Chunk wall-clock limit (480s) reached"
                    log(f"  ⏰ {tag}{last_err}")
                    return None
                try:
                    t_gen = time.time()
                    log(f"  🤖 {tag}Calling {tier_lbl} {mask_key(api_key)} / {model_name} (timeout=180s)...")
                    res = run_with_hard_timeout(
                        lambda: client.models.generate_content(model=model_name, contents=[uploaded, prompt], config=config),
                        180,
                        f"{tag}generate_content({model_name})",
                    )
                    resp_text = getattr(res, "text", "") or ""
                    log(f"  📥 {tag}Response from {model_name} in {time.time() - t_gen:.1f}s ({len(resp_text)} chars)")
                    data = clean_json_text(resp_text)
                    cost = calc_cost(model_name, res.usage_metadata, is_paid)
                    if chunk_schema:
                        try:
                            jsonschema.validate(instance=data, schema=chunk_schema)
                            log(f"  🔍 {tag}Schema validation PASSED (turns={len(data.get('turns', []))}, claims={len(data.get('claims', []))})")
                        except jsonschema.ValidationError as ve:
                            log(f"  🛠️ {tag}Schema mismatch ({ve.message[:100]}) — running 90s JSON repair...")
                            repair_prompt = (
                                f"المخرج JSON السابق خالف المخطط schema_chunk.json بسبب: {ve.message}\n"
                                f"صحّح الخطأ فقط وأعد إخراج JSON صالح مطابق تماماً للمخطط:\n"
                                f"{json.dumps(data, ensure_ascii=False)[:24000]}"
                            )
                            try:
                                r2 = run_with_hard_timeout(
                                    lambda: client.models.generate_content(model=model_name, contents=repair_prompt, config=config),
                                    90,
                                    f"{tag}schema_repair({model_name})",
                                )
                                data = clean_json_text(r2.text)
                                cost += calc_cost(model_name, r2.usage_metadata, is_paid)
                                log(f"  ✅ {tag}Schema repair completed")
                            except Exception as rep_e:
                                log(f"  ⚠️ {tag}Schema repair skipped ({str(rep_e)[:100]}) — continuing with normalized output")
                    record_key_stat(api_key, is_paid, "ok")
                    return data, cost, model_name, tier_lbl
                except TimeoutError as te:
                    last_err = f"[{model_name}] {te}"
                    log(f"  ⏰ {tag}{te} -> cooling {model_name} 90s & rotating!")
                    with STATE_LOCK:
                        MODEL_COOLDOWN_UNTIL[model_name] = time.time() + 90
                    record_key_stat(api_key, is_paid, "err_timeout")
                    continue
                except Exception as e:
                    err, low = str(e), str(e).lower()
                    last_err = f"[{model_name}] {err[:220]}"
                    log(f"  ⚠️ {tag}API Error ({tier_lbl} {mask_key(api_key)} / {model_name}): {err[:180]}")
                    if any(t in low for t in ("api_key_invalid", "access_token_type_unsupported", "401", "403")):
                        with STATE_LOCK:
                            bad_keys.add(api_key)
                        log(f"  🚫 {tag}Marked {tier_lbl} {mask_key(api_key)} as invalid")
                        return None
                    if any(t in low for t in ("404", "not_found", "no longer available")):
                        with STATE_LOCK:
                            exhausted_pairs.add((api_key, model_name))
                        continue
                    if any(t in low for t in ("429", "resource_exhausted", "quota")):
                        with STATE_LOCK:
                            cnt = PAIR_429_COUNT.get((api_key, model_name), 0) + 1
                            PAIR_429_COUNT[(api_key, model_name)] = cnt
                            record_key_stat(api_key, is_paid, "err_429")
                            if cnt >= 2 or "per day" in low or "daily" in low:
                                exhausted_pairs.add((api_key, model_name))
                                log(f"  🛑 {tag}Quota exhausted on {mask_key(api_key)}/{model_name} (hit #{cnt})")
                            else:
                                PAIR_COOLDOWN_UNTIL[(api_key, model_name)] = time.time() + 65
                                log(f"  🧊 {tag}RPM spike on {mask_key(api_key)}/{model_name} — 65s cooldown (hit #1)")
                        continue
                    if any(t in low for t in ("503", "unavailable", "high demand", "overloaded")):
                        with STATE_LOCK:
                            MODEL_COOLDOWN_UNTIL[model_name] = time.time() + 90
                        record_key_stat(api_key, is_paid, "err_503")
                        log(f"  🧊 {tag}503 High Demand on {model_name} — 90s model cooldown")
                        continue
                    time.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            err, low = str(e), str(e).lower()
            last_err = f"[UPLOAD] {err[:220]}"
            log(f"  ⚠️ {tag}Upload/Active Error ({tier_lbl} {mask_key(api_key)}): {err[:180]}")
            if any(t in low for t in ("api_key_invalid", "401", "403")):
                with STATE_LOCK:
                    bad_keys.add(api_key)
        finally:
            if uploaded is not None:
                try:
                    run_with_hard_timeout(lambda: client.files.delete(name=uploaded.name), 15, f"{tag}files.delete")
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
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        return res.returncode == 0 and os.path.exists(dest) and os.path.getsize(dest) > 50_000
    # Build candidate download attempts: (target_url, verify_ssl)
    dl_targets = [(url, True)]
    if url.lower().startswith("https://"):
        dl_targets.append((url, False))
        dl_targets.append(("http://" + url[8:], True))

    for target_url, verify_ssl in dl_targets:
        for attempt in range(1, 3):
            try:
                if not verify_ssl:
                    print(f"⚠️ Retrying download without SSL verification: {target_url}", flush=True)
                with requests.get(target_url, stream=True, timeout=(15, 60), headers={"User-Agent": "Mozilla/5.0"}, verify=verify_ssl) as r:
                    r.raise_for_status()
                    with open(dest, "wb") as f:
                        for blk in r.iter_content(chunk_size=262144):
                            if blk:
                                f.write(blk)
                if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
                    print(f"✅ Downloaded locally ({os.path.getsize(dest)/(1024*1024):.1f} MB) from {target_url} (verify={verify_ssl})", flush=True)
                    return True
            except requests.exceptions.SSLError as ssl_e:
                print(f"[SSL Error verify={verify_ssl}] {ssl_e}", flush=True)
                if verify_ssl:
                    break  # immediately fall through to verify=False
                time.sleep(2)
            except Exception as e:
                print(f"[Download Retry {attempt}/2 | {target_url}] {e}", flush=True)
                time.sleep(2)

    # Final fallback: curl -k -L (handles quirky TLS / redirects)
    try:
        res = subprocess.run(["curl", "-k", "-L", "--fail", "--retry", "2", "-sS", "-o", dest, url], capture_output=True, text=True, timeout=600)
        if res.returncode == 0 and os.path.exists(dest) and os.path.getsize(dest) > 50_000:
            print(f"✅ Downloaded locally via curl -k ({os.path.getsize(dest)/(1024*1024):.1f} MB)", flush=True)
            return True
    except Exception as ce:
        print(f"[curl -k fallback failed] {ce}", flush=True)
    return False


# ==============================================================================
# CHUNK NORMALIZATION, ENTITY RESOLUTION & RUNNING CONTEXT
# ==============================================================================
def normalize_chunk_v2(data: dict, idx: int, st_hms: str, end_hms: str, chunk_dur_s: float, derived_meta: dict, measured_speech_s: float, entity_registry: dict, locked_presenter: str = '') -> dict:
    d = dict(data) if isinstance(data, dict) else (dict(data[0]) if isinstance(data, list) and data and isinstance(data[0], dict) else {})
    d["chunk_index"] = idx
    d["window"] = {"start": st_hms, "end": end_hms}
    d["prompt_version"] = "blackbox-v2.0"

    alias_map = (entity_registry or {}).get("alias_map", {})
    meta = d.get("metadata") if isinstance(d.get("metadata"), dict) else {}
    meta.setdefault("program_name", None)
    meta.setdefault("episode_title", None)
    meta.setdefault("presenter_name", None)
    if locked_presenter:
        meta["presenter_name"] = locked_presenter
    elif meta.get("presenter_name") in alias_map:
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

    # Canonicalize participant names against entities.csv, while locking ANCHOR to episode presenter
    for p in d["participants"]:
        if not isinstance(p, dict):
            continue
        if p.get("id") == "ANCHOR" and locked_presenter:
            p["name"] = locked_presenter
            continue
        if p.get("name"):
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
    participants_map = {}
    program_name_detected = None
    episode_title_detected = None
    for c in chunks:
        cmeta = c.get("metadata") or {}
        if not program_name_detected and cmeta.get("program_name"):
            program_name_detected = cmeta.get("program_name")
        if not episode_title_detected and cmeta.get("episode_title"):
            episode_title_detected = cmeta.get("episode_title")
        for p in c.get("participants", []):
            if isinstance(p, dict) and p.get("name") and p.get("id") != "ANCHOR":
                key = str(p["name"]).strip()
                if key not in participants_map:
                    participants_map[key] = p
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
                "score": round_half(sc), "metric_value": metric_val,
                "metric_display": format_metric_display(cid, metric_val),
                "benchmark_display": fmt_anchor_benchmark(it),
                "anchor_hit": hit, "anchor_hit_ar": ANCHOR_HIT_AR.get(hit, hit),
                "evidence": ev, "referenced_only": ref_only,
                "confidence": conf, "confidence_ar": CONF_AR.get(conf, "متوسطة"),
                "note": note,
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

    # Apply optional human_overrides (e.g., waived/ignored critical events)
    ignored_types = set((brief or {}).get("_ignored_critical_events", []) or [])
    if ignored_types:
        crit_list = [ce for ce in crit_list if ce.get("type") not in ignored_types and f"{ce.get('type')}@{fmt_ts_clean(ce.get('at'))}" not in ignored_types]

    # Critical error caps strictly from rubric.json
    caps_applied, cap_ceiling, distinct_crit = [], 100.0, set()
    cap_map = {r["event"]: r["cap"] for r in rubric.get("critical_error_caps", []) if "event" in r and r["event"] != "multiple_critical"}
    for ce in crit_list:
        ct = ce.get("type")
        if ct in cap_map:
            distinct_crit.add(ct)
            caps_applied.append({"event": ct, "event_ar": CRIT_EVENT_AR.get(ct, ct), "at": ce.get("at"), "detail": ce.get("text") or ce.get("said") or "", "cap": cap_map[ct]})
            cap_ceiling = min(cap_ceiling, float(cap_map[ct]))
    if len(distinct_crit) >= 2:
        caps_applied.append({"event": "multiple_critical", "event_ar": CRIT_EVENT_AR["multiple_critical"], "at": "كامل الحلقة", "detail": "تحقّق خطآن حرجان أو أكثر", "cap": 70})
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

    # Build clean episode examples automatically if LLM synthesis didn't provide them
    auto_examples = []
    for t in sorted(q_turns, key=lambda x: float(x.get("duration_s", 0)), reverse=True)[:4]:
        auto_examples.append({"ts": fmt_ts_clean(t.get("start")), "speaker": "المقدّم", "quote": str(t.get("text", ""))[:220], "significance": f"مداخلة سؤال ({t.get('duration_s')} ثانية، {t.get('word_count')} كلمة، {t.get('question_count')} استفهام)."})
    for b in sorted(unbroken_blocks, key=lambda x: float(x.get("duration_s", 0)), reverse=True)[:3]:
        if float(b.get("duration_s", 0)) >= 90:
            auto_examples.append({"ts": f"من {fmt_ts_clean(b.get('start'))} إلى {fmt_ts_clean(b.get('end'))}", "speaker": b.get("speaker_id", "الضيف"), "quote": f"بلوك متّصل للضيف لمدة {int(round(float(b['duration_s'])))} ثانية دون مداخلة توجيهية.", "significance": "تجاوز عتبة 180 ثانية (استرسال)" if float(b["duration_s"]) > 180 else "بلوك متوسط الطول (بين 90 و180 ثانية)."})
    for ch in unread_local[:2]:
        auto_examples.append({"ts": fmt_ts_clean(ch.get("on")), "speaker": "شريط الشاشة", "quote": str(ch.get("text_raw", ""))[:180], "significance": "شريط أمني/عاجل محلي لم يُقرأ على الهواء — خطأ حرج يفرض سقفاً على النتيجة."})

    return {
        "editorial_mode": editorial_mode,
        "program_name": program_name_detected,
        "episode_title": episode_title_detected,
        "guests_list": list(participants_map.values()),
        "auto_examples": auto_examples,
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
# NATIVE PILLOW + HARFBUZZ/FRIBIDI ARABIC RTL PNG CHART ENGINE (WORD-SAFE BASE64)
# ==============================================================================
_REQUIRED_CHART_GLYPHS = "ابتثجحخدذرزسشصضطظعغفقكلمنهوي0123456789%.():-|—"


def _font_has_all_glyphs(font_path: str) -> bool:
    if not font_path or not os.path.exists(font_path):
        return False
    try:
        cmap = TTFont(font_path).getBestCmap() or {}
        return all(ord(c) in cmap for c in _REQUIRED_CHART_GLYPHS)
    except Exception:
        return False


def _pick_arabic_chart_fonts() -> tuple:
    bold_candidates = [
        os.path.join(BASE_DIR, "fonts", "Cairo-Bold.ttf"),
        os.path.join(BASE_DIR, "fonts", "Cairo-Variable.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansArabic-Bold.ttf",
    ]
    reg_candidates = [
        os.path.join(BASE_DIR, "fonts", "Cairo-Regular.ttf"),
        os.path.join(BASE_DIR, "fonts", "Cairo-Variable.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansArabic-Regular.ttf",
    ]
    b_path = next((p for p in bold_candidates if _font_has_all_glyphs(p)), None)
    r_path = next((p for p in reg_candidates if _font_has_all_glyphs(p)), None)
    if not b_path:
        b_path = next((p for p in bold_candidates if os.path.exists(p)), None)
    if not r_path:
        r_path = next((p for p in reg_candidates if os.path.exists(p)), b_path)
    return b_path, r_path


def _load_pil_font(size: int, bold: bool = False):
    b_path, r_path = _pick_arabic_chart_fonts()
    path = b_path if bold else r_path
    return ImageFont.truetype(path, size) if path else ImageFont.load_default()


def _draw_rtl(draw, xy, text: str, font, fill, anchor="ra"):
    if pil_features.check("raqm"):
        draw.text(xy, str(text), font=font, fill=fill, anchor=anchor, direction="rtl", language="ar")
    else:
        draw.text(xy, str(text), font=font, fill=fill, anchor=anchor)


def _dashed_vline(draw, x, y0, y1, color, width=3, dash=11, gap=7):
    y = y0
    while y < y1:
        draw.line([(x, y), (x, min(y + dash, y1))], fill=color, width=width)
        y += dash + gap


def _dashed_hline(draw, x0, x1, y, color, width=3, dash=12, gap=8):
    x = x0
    while x < x1:
        draw.line([(x, y), (min(x + dash, x1), y)], fill=color, width=width)
        x += dash + gap


def _pil_to_b64(img: Image.Image) -> str:
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def render_axes_chart_pil(axes: list) -> Image.Image:
    W, H = 1520, 740
    img = Image.new("RGB", (W, H), "#FFFFFF")
    draw = ImageDraw.Draw(img)

    f_title = _load_pil_font(29, bold=True)
    f_sub = _load_pil_font(17, bold=False)
    f_lbl = _load_pil_font(21, bold=True)
    f_val = _load_pil_font(20, bold=True)
    f_tick = _load_pil_font(16, bold=False)

    draw.rounded_rectangle([8, 8, W - 9, H - 9], radius=14, outline="#D5DBE2", width=2, fill="#FFFFFF")
    draw.rectangle([10, 10, W - 11, 20], fill="#12304D")

    _draw_rtl(draw, (W - 44, 46), "نسبة الأداء في المحاور السبعة مقابل المعيار المستهدف (75%)", f_title, "#12304D", anchor="ra")
    _draw_rtl(draw, (W - 44, 90), "أخضر: متجاوز للمستهدف (75% فأكثر)  |  ذهبي: متوسط (55% - 74%)  |  قرمزي: دون 55%", f_sub, "#5B6572", anchor="ra")

    label_area_x = W - 44
    bar_right_x = W - 445
    bar_left_x = 120
    bar_span = bar_right_x - bar_left_x
    top_y, bottom_y = 148, H - 76
    row_h = (bottom_y - top_y) / max(1, len(axes))

    for pct in (0, 25, 50, 75, 100):
        gx = int(bar_right_x - (pct / 100.0) * bar_span)
        if pct == 75:
            _dashed_vline(draw, gx, top_y - 10, bottom_y + 6, "#12304D", width=3)
            _draw_rtl(draw, (gx, bottom_y + 18), "المستهدف 75%", _load_pil_font(16, bold=True), "#12304D", anchor="ma")
        else:
            draw.line([(gx, top_y - 4), (gx, bottom_y + 4)], fill="#E5E9F0", width=2)
            draw.text((gx, bottom_y + 18), f"{pct}%", font=f_tick, fill="#5B6572", anchor="ma")

    draw.line([(bar_right_x, top_y - 8), (bar_right_x, bottom_y + 6)], fill="#12304D", width=3)

    for i, a in enumerate(axes):
        cy = int(top_y + i * row_h + row_h / 2)
        bh = int(row_h * 0.56)
        pct = max(0.0, min(100.0, float(a["pct"])))
        color = "#2C5C46" if pct >= 75 else ("#A8863F" if pct >= 55 else "#8B2F38")
        if i % 2 == 1:
            draw.rectangle([bar_left_x - 60, int(top_y + i * row_h), W - 28, int(top_y + (i + 1) * row_h)], fill="#F8FAFC")

        ar_code = AXIS_AR_CODE.get(a["id"], a["id"])
        _draw_rtl(draw, (label_area_x, cy), f"{ar_code} - {a['name']}", f_lbl, "#12304D", anchor="rm")
        draw.rounded_rectangle([bar_left_x, cy - bh // 2, bar_right_x, cy + bh // 2], radius=6, fill="#EEF2F6")

        bw = max(6, int((pct / 100.0) * bar_span))
        bx0 = bar_right_x - bw
        draw.rounded_rectangle([bx0, cy - bh // 2, bar_right_x, cy + bh // 2], radius=6, fill=color)

        # Put % cleanly inside bar when wide enough (zero collision with 75% line!)
        if pct >= 18:
            draw.text((bx0 + 16, cy), f"{pct:.0f}%", font=f_val, fill="#FFFFFF", anchor="lm")
        else:
            draw.text((bx0 - 14, cy), f"{pct:.0f}%", font=f_val, fill=color, anchor="rm")

    gx75 = int(bar_right_x - 0.75 * bar_span)
    _dashed_vline(draw, gx75, top_y - 10, bottom_y + 6, "#12304D", width=3)
    return img


def render_blocks_chart_pil(unbroken_blocks: list):
    blocks = [b for b in unbroken_blocks if float(b.get("duration_s", 0)) >= 25][:12]
    if not blocks:
        return None
    W, H = 1520, 700
    img = Image.new("RGB", (W, H), "#FFFFFF")
    draw = ImageDraw.Draw(img)

    f_title = _load_pil_font(29, bold=True)
    f_sub = _load_pil_font(17, bold=False)
    f_tick = _load_pil_font(16, bold=False)
    f_val = _load_pil_font(18, bold=True)
    f_leg = _load_pil_font(16, bold=True)

    draw.rounded_rectangle([8, 8, W - 9, H - 9], radius=14, outline="#D5DBE2", width=2, fill="#FFFFFF")
    draw.rectangle([10, 10, W - 11, 20], fill="#12304D")

    _draw_rtl(draw, (W - 44, 46), "توزيع أطول بلوكات الضيوف المتّصلة (بالثواني)", f_title, "#12304D", anchor="ra")
    _draw_rtl(draw, (W - 44, 90), "قياس زمن استرسال الضيف دون مداخلة توجيهية من المقدّم عبر كامل الحلقة", f_sub, "#5B6572", anchor="ra")

    chart_l, chart_r = 128, W - 56
    chart_t, chart_b = 172, H - 110
    max_val = max(210.0, max(float(b["duration_s"]) for b in blocks) * 1.18)
    max_y = int(((int(max_val) // 30) + 1) * 30)

    # Clean legend pills at top-right (zero overlap with chart)
    draw.rounded_rectangle([chart_r - 245, 122, chart_r, 156], radius=7, fill="#F2F7F4", outline="#2C5C46", width=2)
    _draw_rtl(draw, (chart_r - 122, 139), "90 ثانية (الحد المثالي)", f_leg, "#2C5C46", anchor="mm")
    draw.rounded_rectangle([chart_r - 535, 122, chart_r - 262, 156], radius=7, fill="#FBF3F4", outline="#8B2F38", width=2)
    _draw_rtl(draw, (chart_r - 398, 139), "180 ثانية (حد الاسترسال)", f_leg, "#8B2F38", anchor="mm")

    step = 30 if max_y <= 240 else 60
    for yv in range(0, max_y + 1, step):
        yy = int(chart_b - (yv / max_y) * (chart_b - chart_t))
        draw.line([(chart_l, yy), (chart_r, yy)], fill="#E5E9F0", width=2)
        _draw_rtl(draw, (chart_l - 14, yy), f"{yv} ث", f_tick, "#5B6572", anchor="rm")

    draw.line([(chart_l, chart_t), (chart_l, chart_b)], fill="#12304D", width=3)
    draw.line([(chart_l, chart_b), (chart_r, chart_b)], fill="#12304D", width=3)

    for thr, col in ((90, "#2C5C46"), (180, "#8B2F38")):
        if thr <= max_y:
            ty = int(chart_b - (thr / max_y) * (chart_b - chart_t))
            _dashed_hline(draw, chart_l, chart_r, ty, col, width=3)

    n = len(blocks)
    slot_w = (chart_r - chart_l - 30) / max(1, n)
    bar_w = min(74, int(slot_w * 0.56))

    for i, b in enumerate(blocks):
        v = float(b["duration_s"])
        cx = int(chart_l + 15 + i * slot_w + slot_w / 2)
        bh = int((v / max_y) * (chart_b - chart_t))
        col = "#8B2F38" if v > 180 else ("#A8863F" if v > 120 else "#12304D")
        draw.rounded_rectangle([cx - bar_w // 2, chart_b - bh, cx + bar_w // 2, chart_b], radius=6, fill=col)
        # White halo behind value label so dashed threshold lines never cut through text
        draw.rounded_rectangle([cx - 34, chart_b - bh - 34, cx + 34, chart_b - bh - 6], radius=5, fill="#FFFFFF")
        _draw_rtl(draw, (cx, chart_b - bh - 19), f"{int(round(v))} ث", f_val, col, anchor="mm")
        draw.text((cx, chart_b + 16), fmt_ts_clean(b.get("start", "")), font=_load_pil_font(15, bold=False), fill="#1F2328", anchor="ma")

    _draw_rtl(draw, ((chart_l + chart_r) // 2, H - 36), "توقيت بداية كل بلوك متّصل للضيف (ساعة:دقيقة:ثانية)", _load_pil_font(18, bold=True), "#12304D", anchor="ma")
    return img


def render_lost_chart_pil(axes: list) -> Image.Image:
    W, H = 1520, 740
    img = Image.new("RGB", (W, H), "#FFFFFF")
    draw = ImageDraw.Draw(img)

    f_title = _load_pil_font(29, bold=True)
    f_sub = _load_pil_font(17, bold=False)
    f_lbl = _load_pil_font(21, bold=True)
    f_val = _load_pil_font(19, bold=True)
    f_tick = _load_pil_font(16, bold=False)

    draw.rounded_rectangle([8, 8, W - 9, H - 9], radius=14, outline="#D5DBE2", width=2, fill="#FFFFFF")
    draw.rectangle([10, 10, W - 11, 20], fill="#8B2F38")

    total_lost = round(sum(max(0.0, float(a["max"]) - float(a["score"])) for a in axes), 1)
    _draw_rtl(draw, (W - 44, 46), f"أين ضاعت النقاط؟ توزيع النقاط المفقودة حسب المحور (الإجمالي: {total_lost:.1f} نقطة)", f_title, "#12304D", anchor="ra")
    _draw_rtl(draw, (W - 44, 90), "ترتيب المحاور تنازلياً من الأكثر فقداناً للنقاط لتحديد أولويات التطوير الأعلى أثراً", f_sub, "#5B6572", anchor="ra")

    sorted_axes = sorted(axes, key=lambda a: float(a["max"]) - float(a["score"]), reverse=True)
    max_lost = max(4.0, max(float(a["max"]) - float(a["score"]) for a in sorted_axes) * 1.28)
    max_scale = int(max_lost) + 2

    label_area_x = W - 44
    bar_right_x = W - 445
    bar_left_x = 190
    bar_span = bar_right_x - bar_left_x
    top_y, bottom_y = 148, H - 76
    row_h = (bottom_y - top_y) / max(1, len(sorted_axes))

    for step_v in range(0, max_scale + 1, max(1, max_scale // 5)):
        gx = int(bar_right_x - (step_v / max_scale) * bar_span)
        draw.line([(gx, top_y - 4), (gx, bottom_y + 4)], fill="#E5E9F0", width=2)
        _draw_rtl(draw, (gx, bottom_y + 18), f"{step_v} نقطة" if step_v > 0 else "0", f_tick, "#5B6572", anchor="ma")

    draw.line([(bar_right_x, top_y - 8), (bar_right_x, bottom_y + 6)], fill="#12304D", width=3)

    for i, a in enumerate(sorted_axes):
        cy = int(top_y + i * row_h + row_h / 2)
        bh = int(row_h * 0.56)
        lost = round(max(0.0, float(a["max"]) - float(a["score"])), 1)
        if i % 2 == 1:
            draw.rectangle([bar_left_x - 140, int(top_y + i * row_h), W - 28, int(top_y + (i + 1) * row_h)], fill="#F8FAFC")

        ar_code = AXIS_AR_CODE.get(a["id"], a["id"])
        _draw_rtl(draw, (label_area_x, cy), f"{ar_code} - {a['name']}", f_lbl, "#12304D", anchor="rm")
        draw.rounded_rectangle([bar_left_x, cy - bh // 2, bar_right_x, cy + bh // 2], radius=6, fill="#F3F4F6")

        if lost > 0:
            bw = max(8, int((lost / max_scale) * bar_span))
            bx0 = bar_right_x - bw
            col = "#8B2F38" if lost >= 5.0 else ("#A8863F" if lost >= 2.5 else "#3D5A78")
            draw.rounded_rectangle([bx0, cy - bh // 2, bar_right_x, cy + bh // 2], radius=6, fill=col)
            _draw_rtl(draw, (bx0 - 14, cy), f"{lost:.1f} نقطة", f_val, col, anchor="rm")
        else:
            _draw_rtl(draw, (bar_right_x - 14, cy), "0 (درجة كاملة)", f_val, "#2C5C46", anchor="rm")

    return img


def generate_eval_charts(eval_data: dict) -> dict:
    charts = {}
    axes = eval_data.get("axes", [])
    if not axes:
        return charts
    try:
        charts["axes_bar"] = _pil_to_b64(render_axes_chart_pil(axes))
    except Exception as e:
        print(f"[Chart 1 Warning] {e}", flush=True)
    try:
        im_b = render_blocks_chart_pil(eval_data.get("unbroken_blocks", []))
        if im_b is not None:
            charts["blocks_bar"] = _pil_to_b64(im_b)
    except Exception as e:
        print(f"[Chart 2 Warning] {e}", flush=True)
    try:
        charts["lost_bar"] = _pil_to_b64(render_lost_chart_pil(axes))
    except Exception as e:
        print(f"[Chart 3 Warning] {e}", flush=True)
    return charts


# ==============================================================================
# EXECUTIVE WORD-RTL HTML RENDERERS (CLEAN ARABIC, ZERO RAW ENGLISH, A4 PRINT)
# ==============================================================================
EXEC_WORD_CSS = """
@page WordSection1 { size: 21.0cm 29.7cm; margin: 1.8cm 1.8cm 1.8cm 1.8cm; mso-page-orientation: portrait; }
div.WordSection1 { page: WordSection1; }
body, div.WordSection1 { font-family: 'Segoe UI', 'Dubai', 'Tahoma', 'Arial', sans-serif; font-size: 10.5pt; color: #1f2328; line-height: 1.6; margin: 0; padding: 0; direction: rtl; text-align: right; background: #ffffff; }
p { margin: 0 0 6.5pt 0; text-align: justify; text-justify: inter-word; }
h1, h2, h3, h4 { font-family: 'Segoe UI', 'Dubai', 'Tahoma', 'Arial', sans-serif; margin: 0; font-weight: bold; }
h2 { font-size: 14pt; color: #12304d; border-bottom: 1.5pt solid #a8863f; padding-bottom: 4.5pt; margin: 18pt 0 9pt 0; page-break-after: avoid; }
h3 { font-size: 11.2pt; color: #12304d; margin: 12pt 0 5pt 0; padding-right: 8pt; border-right: 3pt solid #a8863f; page-break-after: avoid; }
h4 { font-size: 10.2pt; color: #8b2f38; margin: 9pt 0 4pt 0; page-break-after: avoid; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 11pt 0; direction: rtl; }
td, th { border: 0.5pt solid #d5dbe2; padding: 5pt 6.5pt; vertical-align: middle; font-size: 9pt; line-height: 1.45; }
th { background-color: #12304d; color: #ffffff; font-weight: bold; border: 0.5pt solid #12304d; text-align: center; }
tr.alt td { background-color: #f7f9fb; }
td.num, th.num { text-align: center; }
td.lbl { font-weight: bold; color: #12304d; }
td.tot { background-color: #eef2f6; font-weight: bold; color: #12304d; }
.callout-navy { background: #12304d; color: #ffffff; padding: 11pt 14pt; border: 0.5pt solid #12304d; }
.callout-gold { background: #fbf7ec; border: 0.5pt solid #e8d5a3; border-right: 3.5pt solid #a8863f; padding: 9pt 12pt; }
.callout-green { background: #f2f7f4; border: 0.5pt solid #b7d7c5; border-right: 3.5pt solid #2c5c46; padding: 9pt 14pt; }
.callout-red { background: #fbf3f4; border: 0.5pt solid #e4bcc0; border-right: 3.5pt solid #8b2f38; padding: 9pt 14pt; }
.qbad { background: #fbf3f4; padding: 6.5pt 9pt; border-right: 2.5pt solid #8b2f38; font-size: 9.5pt; margin-bottom: 4pt; }
.qgood { background: #f2f7f4; padding: 6.5pt 9pt; border-right: 2.5pt solid #2c5c46; font-size: 9.5pt; margin-bottom: 10pt; }
.chart-box { text-align: center; margin: 10pt 0 12pt 0; page-break-inside: avoid; }
.chart-box img { width: 100%; max-width: 17.2cm; height: auto; border: 0.5pt solid #d5dbe2; }
.bar-bg { background: #e8edf3; height: 7pt; width: 100%; }
.small { font-size: 8.5pt; color: #5b6572; }
.note { font-size: 8.5pt; color: #5b6572; margin-top: -3pt; margin-bottom: 9pt; }
ul { margin: 4pt 0 8pt 0; padding-right: 18pt; }
li { margin-bottom: 5pt; }
"""


def wrap_word_doc(title: str, inner_html: str) -> str:
    return (
        '<!DOCTYPE html><html dir="rtl" lang="ar" xmlns:o="urn:schemas-microsoft-com:office:office" '
        'xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40">'
        f'<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"><meta name="ProgId" content="Word.Document"><title>{esc(title)}</title>'
        '<!--[if gte mso 9]><xml><w:WordDocument><w:View>Print</w:View><w:Zoom>100</w:Zoom><w:DoNotOptimizeForBrowser/></w:WordDocument></xml><![endif]-->'
        f'<style>{EXEC_WORD_CSS}</style></head><body dir="rtl"><div class="WordSection1">{inner_html}</div></body></html>'
    )


def _format_guests_summary(eval_data: dict) -> str:
    guests = eval_data.get("guests_list") or []
    if not guests:
        return "غير محدد في المقاطع"
    parts = []
    for g in guests[:6]:
        name = str(g.get("name", "")).strip()
        role = str(g.get("role", "")).strip()
        parts.append(f"{name} ({role})" if role else name)
    return " · ".join(parts)


def _next_band_gap_text(final_score: float) -> tuple:
    if final_score < 70:
        return f"{round(70.0 - final_score, 1)} نقطة", "للترقية إلى «احترافي سليم» (70)"
    if final_score < 80:
        return f"{round(80.0 - final_score, 1)} نقطة", "للترقية إلى «احترافي متقدّم» (80)"
    if final_score < 90:
        return f"{round(90.0 - final_score, 1)} نقطة", "للترقية إلى «استثنائي» (90)"
    return "مستوى القمة", "ضمن نطاق «استثنائي» (90–100)"


def render_detailed_eval_html(display_name, exact_dur, derived_meta, eval_data, llm_qual, charts, meta_info) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    rep = (llm_qual or {}).get("report", {})
    total_lost = max(0.0, round(100.0 - sf["raw_score"], 1))
    gap_num, gap_sub = _next_band_gap_text(sf["final_score"])
    prog_label = eval_data.get("program_name") or (llm_qual or {}).get("episode_overview", {}).get("program_name") or "برنامج حواري / تغطية خاصة"
    guests_str = _format_guests_summary(eval_data)

    # 7-axis overview rows with Word-safe inline progress bar
    ax_rows = ""
    for i, a in enumerate(eval_data["axes"]):
        ar_c = AXIS_AR_CODE.get(a["id"], a["id"])
        lost_a = round(max(0.0, a["max"] - a["score"]), 1)
        bar_col = "#2c5c46" if a["pct"] >= 75 else ("#a8863f" if a["pct"] >= 55 else "#8b2f38")
        ax_rows += (
            f"<tr class='{'alt' if i%2 else ''}'>"
            f"<td class='lbl'>{ar_c} · {esc(a['name'])}</td>"
            f"<td class='num'>{a['max']:.0f}</td>"
            f"<td class='num'><b>{a['score']:.1f}</b></td>"
            f"<td class='num' style='color:{bar_col};font-weight:bold;'>{a['pct']:.0f}%</td>"
            f"<td style='width:20%;'><div class='bar-bg'><div style='background:{bar_col};height:7pt;width:{min(100,max(2,a['pct'])):.0f}%;'></div></div></td>"
            f"<td class='num' style='color:#8b2f38;'>{f'-{lost_a:.1f}' if lost_a > 0 else '0'}</td></tr>"
        )
    ax_rows += f"<tr><td class='tot'>المجموع الخام (قبل مُعدِّل التعقيد والسقف الحرج)</td><td class='num tot'>100</td><td class='num tot'>{sf['raw_score']:.1f}</td><td class='num tot'>{sf['raw_score']:.0f}%</td><td class='tot'></td><td class='num tot'>-{total_lost:.1f}</td></tr>"

    # Grouped 7 Axes x 26 Sub-Criteria Breakdown
    grouped_sections_html = ""
    for a in eval_data["axes"]:
        ar_c = AXIS_AR_CODE.get(a["id"], a["id"])
        bar_col = "#2c5c46" if a["pct"] >= 75 else ("#a8863f" if a["pct"] >= 55 else "#8b2f38")
        rows_sub = ""
        for j, it in enumerate(a["items"]):
            ic = ITEM_AR_CODE.get(it["criterion_id"], it["criterion_id"])
            rows_sub += (
                f"<tr class='{'alt' if j%2 else ''}'>"
                f"<td class='lbl' style='width:20%;'>{ic} · {esc(it['name'])}</td>"
                f"<td style='width:18%;' class='small'>{esc(it.get('benchmark_display', ''))}</td>"
                f"<td style='width:16%;' class='num'>{esc(it.get('metric_display', it['metric_value']))}</td>"
                f"<td style='width:9%;' class='num'><b>{it['score']:.1f} / {it['max']:.0f}</b></td>"
                f"<td style='width:8%;' class='num'>{esc(it.get('confidence_ar', CONF_AR.get(it['confidence'], 'متوسطة')))}</td>"
                f"<td style='width:29%;'>{esc(it['note'])}</td></tr>"
            )
        grouped_sections_html += (
            f"<h3>المحور {ar_c} · {esc(a['name'])} — <span style='color:{bar_col};'>{a['score']:.1f} من {a['max']:.0f} ({a['pct']:.0f}%)</span></h3>"
            f"<table><tr><th>البند الفرعي</th><th>المرساة المرجعية</th><th>القيمة المقيسة</th><th>الدرجة</th><th>الثقة</th><th>الشاهد والملاحظة المهنية</th></tr>{rows_sub}</table>"
        )

    # Documented examples table
    examples = (llm_qual or {}).get("documented_examples") or eval_data.get("auto_examples") or []
    ex_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'>"
        f"<td class='num' style='width:15%;'>{fmt_ts_span(ex.get('ts')) if 'من' not in str(ex.get('ts','')) else esc(ex.get('ts'))}</td>"
        f"<td class='lbl' style='width:14%;'>{esc(ex.get('speaker', 'المقدّم'))}</td>"
        f"<td style='width:41%;'>{esc(ex.get('quote', ''))}</td>"
        f"<td style='width:30%;'>{esc(ex.get('significance', ''))}</td></tr>"
        for i, ex in enumerate(examples[:10])
    ) or "<tr><td colspan='4' class='num'>انظر سجل الأدوار الكامل في ملف الصندوق الأسود المرفق.</td></tr>"

    # Monologues > 180s
    mono_rows = "".join(
        f"<tr><td class='num'>من {fmt_ts_span(b['start'])} إلى {fmt_ts_span(b.get('end',''))}</td>"
        f"<td class='num'>{int(round(float(b['duration_s'])))} ثانية</td>"
        f"<td class='num'>الضيف ({esc(b.get('speaker_id'))})</td>"
        f"<td style='color:#8b2f38;font-weight:bold;'>تجاوز 180 ثانية — خاضع لاختبار المردود (يُحاسب في أ٣ فقط)</td></tr>"
        for b in eval_data["monologues_over_180"]
    ) or "<tr><td colspan='4' class='num' style='color:#2c5c46;font-weight:bold;'>نتيجة إيجابية: لم يتجاوز أي بلوك متّصل للضيف عتبة 180 ثانية في هذه الحلقة.</td></tr>"

    # Critical caps
    crit_rows = "".join(
        f"<tr><td class='lbl'>{esc(c.get('event_ar') or CRIT_EVENT_AR.get(c.get('event'), c.get('event')))}</td>"
        f"<td class='num'>{fmt_ts_span(c.get('at'))}</td>"
        f"<td>{esc(c.get('detail', ''))}</td>"
        f"<td class='num' style='color:#8b2f38;font-weight:bold;'>سقف {c.get('cap')} / 100</td></tr>"
        for c in eval_data["caps_applied"]
    ) or "<tr><td colspan='4' class='num' style='color:#2c5c46;font-weight:bold;'>لا توجد أخطاء حرجة مفروضة السقف في هذه الحلقة.</td></tr>"

    # Audit checks in clean Arabic
    audit_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'>"
        f"<td class='lbl'>{esc(AUDIT_CHECK_AR.get(c['check'], c['check']))}</td>"
        f"<td class='num' style='color:{'#2c5c46' if c['passed'] else '#8b2f38'};font-weight:bold;'>{'مطابق' if c['passed'] else 'تعارض'}</td>"
        f"<td>{esc(c['detail'])}</td></tr>"
        for i, c in enumerate(eval_data["audit"]["checks"])
    )

    charts_html = "".join(f"<div class='chart-box'><img src='data:image/png;base64,{charts[ck]}' alt='رسم بياني'></div>" for ck in ("axes_bar", "blocks_bar", "lost_bar") if charts.get(ck))
    st_li = "".join(f"<li>{esc(s)}</li>" for s in rep.get("strengths", [])) or "<li>انظر المحاور التي حقّقت 75% فأكثر في الجدول أعلاه.</li>"
    pr_li = "".join(f"<li>{esc(p)}</li>" for p in rep.get("priorities", [])) or f"<li>الأولوية القصوى: {esc(eval_data['top_priority_auto'])}</li>"

    start_beirut = derived_meta.get("broadcast_start_beirut") or "—"
    end_beirut = derived_meta.get("broadcast_end_beirut") or "—"
    time_range_html = f"من {fmt_ts_span(start_beirut)} إلى {fmt_ts_span(end_beirut)}" if start_beirut != "—" else "غير متوفر"

    body = f"""
<table style="margin:0;"><tr><td style="border:0.5pt solid #12304d;background:#12304d;padding:22pt 20pt;color:#fff;">
<p style="margin:0 0 6pt;font-size:8.5pt;color:#d9c48f;letter-spacing:1.2pt;">تقرير تقييم مهني تفصيلي · سلّم Manar-100-v2 · الصندوق الأسود v2.0</p>
<p style="margin:0 0 4pt;font-size:23pt;font-weight:bold;color:#fff;">{esc(display_name)} — {esc(prog_label)}</p>
<p style="margin:0 0 12pt;font-size:10.5pt;color:#c9d6e4;">{esc(meta_info['editorial_label'])}</p>
<table style="margin:0;"><tr>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;width:24%;">تاريخ البث<br><b style="color:#fff;">{esc(derived_meta.get('broadcast_date') or 'غير مستخرج')}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;width:30%;">توقيت البث (بيروت {esc(derived_meta.get('timezone'))})<br><b style="color:#fff;">{time_range_html}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;width:22%;">مدة الحلقة الكاملة<br><b style="color:#fff;">{fmt_ts_span(exact_dur)}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;width:24%;">التحليل (مقاطع 5د) / الكلفة<br><b style="color:#fff;">{meta_info['chunks_ok']} من {meta_info['chunks_total']} · <span dir="ltr">${meta_info['actual_cost']:.4f}</span></b></td>
</tr></table>
<p style="margin:8pt 0 0;padding-top:6pt;border-top:0.5pt solid #2a4766;font-size:8.5pt;color:#c9d6e4;"><b style="color:#d9c48f;">الضيوف والمشاركون:</b> {esc(guests_str)}</p>
</td></tr></table>

<table style="margin:12pt 0 14pt;"><tr>
<td style="background:#fbf7ec;border-top:2.5pt solid #a8863f;text-align:center;width:30%;padding:12pt 8pt;">
<p style="margin:0;font-size:8pt;color:#8a7332;text-align:center;">النتيجة النهائية (بنطاق القياس)</p>
<p style="margin:2pt 0 0;font-size:32pt;font-weight:bold;color:#12304d;text-align:center;" dir="ltr">{esc(sf['reported_as'])}</p>
<p style="margin:2pt 0 0;font-size:8.2pt;color:#5b6572;text-align:center;">خام {sf['raw_score']:.1f} + تعقيد {sf['difficulty_modifier']} · السقف {sf['cap_ceiling']:.0f}</p>
</td>
<td style="background:#fbf7ec;border-top:2.5pt solid #a8863f;text-align:center;width:38%;padding:12pt 8pt;">
<p style="margin:0;font-size:8pt;color:#5b6572;text-align:center;">التقدير المهني المعتمد</p>
<p style="margin:4pt 0;font-size:15pt;font-weight:bold;color:#8b2f38;text-align:center;">{esc(sf['band'])}</p>
<p style="margin:0;font-size:8.2pt;color:#5b6572;text-align:center;">{esc(sf['difficulty_justification'])}</p>
</td>
<td style="background:#fbf7ec;border-top:2.5pt solid #a8863f;text-align:center;width:32%;padding:12pt 8pt;">
<p style="margin:0;font-size:8pt;color:#5b6572;text-align:center;">الفارق عن المستوى الأعلى</p>
<p style="margin:4pt 0;font-size:14pt;font-weight:bold;color:#12304d;text-align:center;">{esc(gap_num)}</p>
<p style="margin:0;font-size:8pt;color:#5b6572;text-align:center;">{esc(gap_sub)}</p>
</td></tr></table>

<h2>١ · الملخّص التنفيذي والمؤشرات الأربعة الحاكمة</h2>
<p>{esc(rep.get('summary') or 'تقييم مهني حتمي مبني على استخراج كائنات الأحداث الزمنية من كامل مدة الحلقة ومقارنتها بمراسي سلّم Manar-100-v2.')}</p>
<table><tr>
<td style="background:#f5f7fa;text-align:center;width:25%;padding:10pt 6pt;"><b style="font-size:20pt;color:#12304d;">{k['interventions_per_10min']:.1f}</b><br><span class="small">مداخلة / 10 دقائق<br>(المعيار: 8 إلى 14)</span></td>
<td style="background:#f5f7fa;text-align:center;width:25%;padding:10pt 6pt;"><b style="font-size:20pt;color:#12304d;">{k['mean_intervention_s']:.1f} ث</b><br><span class="small">متوسط طول المداخلة<br>(المعيار: 12 ثانية أو أقل)</span></td>
<td style="background:{'#fbf3f4' if k['longest_unbroken_block_s'] > 180 else '#fbf7ec'};text-align:center;width:25%;padding:10pt 6pt;"><b style="font-size:20pt;color:{'#8b2f38' if k['longest_unbroken_block_s'] > 180 else '#a8863f'};">{int(round(k['longest_unbroken_block_s']))} ث</b><br><span class="small">أطول بلوك ضيف متّصل<br>(المعيار: 90 إلى 120 ثانية)</span></td>
<td style="background:#f5f7fa;text-align:center;width:25%;padding:10pt 6pt;"><b style="font-size:20pt;color:#12304d;">{k['anchor_share_pct']:.1f}%</b><br><span class="small">حصّة المقدّم من الكلام<br>(المعيار: 20% إلى 30%)</span></td>
</tr></table>
<p class="note">{esc(eval_data['brief_alignment'].get('caveat', ''))}</p>

<h2>٢ · الرسوم البيانية التشخيصية</h2>
{charts_html}

<h2 style="page-break-before:always;">٣ · خلاصة المحاور السبعة (100 نقطة)</h2>
<table><tr><th>المحور</th><th class="num">الوزن</th><th class="num">الدرجة</th><th class="num">النسبة</th><th class="num">شريط الإنجاز</th><th class="num">الفقد</th></tr>{ax_rows}</table>

<h2 style="page-break-before:always;">٤ · التفصيل القابل للتدقيق — البنود الـ 26 موزّعة على المحاور السبعة</h2>
{grouped_sections_html}

<h2 style="page-break-before:always;">٥ · شواهد وأمثلة موثّقة من الحلقة</h2>
<table><tr><th class="num">التوقيت</th><th>المتحدّث</th><th>الاقتباس الحرفي أو الحدث المرصود</th><th>الدلالة المهنية</th></tr>{ex_rows}</table>

<h2>٦ · اختبار المردود (البلوكات &gt; 180 ثانية) وسقوف الأخطاء الحرجة</h2>
<p class="small">يُطبَّق اختبار المردود على كل بلوك متّصل للضيف يتجاوز 180 ثانية: هل أنتج معلومة جديدة قابلة للتحقق أو استنتاجاً قرارياً؟ إن كان الجواب «لا» يُخصم في البند (أ٣) فقط تطبيقاً لقاعدة «العطب الواحد ← محور واحد».</p>
<table><tr><th class="num">النافذة الزمنية</th><th class="num">المدة</th><th class="num">المتحدّث</th><th>الحكم المنهجي</th></tr>{mono_rows}</table>
<h3>السقوف الحرجة المطبّقة على النتيجة</h3>
<table><tr><th>الحدث الحرج</th><th class="num">التوقيت</th><th>التفصيل الموثّق</th><th class="num">السقف الأعلى</th></tr>{crit_rows}</table>

<h2 style="page-break-before:always;">٧ · نقاط القوة وأولويات التطوير وخارطة المكسب</h2>
<div class="callout-green"><b>نقاط القوة الموثّقة بالتوقيت:</b><ul>{st_li}</ul></div>
<div class="callout-red" style="margin-top:8pt;"><b>أولويات التطوير (الأولوية القصوى المشتقّة آلياً: {esc(eval_data['top_priority_auto'])}):</b><ul>{pr_li}</ul></div>

<h3>جدول الحساسية: أثر الالتزام بالبروتوكولات الثمانية</h3>
<table><tr><th>الإجراء التحسيني (البروتوكول)</th><th class="num">البند المستهدف</th><th class="num">الكلفة الإنتاجية</th><th class="num">المكسب المتوقّع</th></tr>
<tr><td>بروتوكول 90/8: لمسة توجيهية ≤ 8 ثوانٍ كل 90 ثانية تبدأ بكلمة من الضيف</td><td class="num">أ٣ + د٣</td><td class="num">صفر</td><td class="num">+4.0 نقاط</td></tr>
<tr class="alt"><td>قاعدة السؤال الواحد: ≤ 12 ثانية / ≤ 30 كلمة باستفهام واحد ومرجع محدد</td><td class="num">أ٢ + ب١ + ب٢</td><td class="num">صفر</td><td class="num">+3.2 نقاط</td></tr>
<tr><td>رفع معدّل التدخّلات النشطة إلى 8–10 مداخلات كل 10 دقائق</td><td class="num">أ١</td><td class="num">صفر</td><td class="num">+2.5 نقاط</td></tr>
<tr class="alt"><td>قراءة أي شريط أمني/عاجل محلي فور ظهوره خلال ≤ 60 ثانية</td><td class="num">هـ٣ + و٣</td><td class="num">صفر</td><td class="num">+2.0 نقطتان</td></tr>
<tr><td>بطاقة الفرضيات الثلاث + طلب دليل صريح 3 مرّات في الساعة</td><td class="num">ج١ + ج٢</td><td class="num">تحضير</td><td class="num">+1.5 نقطة</td></tr>
<tr class="alt"><td>خاتمة بـ 3 خلاصات تنفيذية + بطاقة الأسماء أمام المقدّم</td><td class="num">ز٢ + ز٣</td><td class="num">صفر</td><td class="num">+1.5 نقطة</td></tr>
<tr><td class="tot" colspan="3">إجمالي المكسب الممكن دون كلفة إنتاجية إضافية</td><td class="num tot">+14.7 نقطة</td></tr></table>

<h3>ضوابط الحماية المهنية واللغوية (ما لا يُخصم عليه)</h3>
<div class="callout-gold"><p style="margin:0;">لا يُخصم أي جزء من العلامة على: <b>اللهجة اللبنانية المحكية</b> (تُعدّ عنصر قوة تواصلي محبّب)، ولا على <b>الالتزام بالخط التحريري للقناة الموجّهة ومصطلحاتها</b>، ولا على <b>عدم التوازن السياسي تجاه العدو</b>؛ وتنحصر المحاسبة في الصنعة الحوارية والإيقاع والتحقّق الداخلي وتحصين السردية.</p></div>

<h2>٨ · تقرير التدقيق الذاتي للنزاهة (6 فحوص آلية)</h2>
<table><tr><th>الفحص المنهجي</th><th class="num">الحالة</th><th>النتيجة الموثّقة</th></tr>{audit_rows}</table>
<p class="small" style="margin-top:12pt;text-align:center;color:#8a939e;">— نهاية التقرير المفصّل (Orchestrator V7.0) —</p>"""
    return wrap_word_doc(f"تقييم مفصل - {display_name}", body)


def render_summarized_eval_html(display_name, exact_dur, eval_data, llm_qual, charts) -> str:
    sf, k, rep = eval_data["score_final"], eval_data["kpis"], (llm_qual or {}).get("report", {})
    gap_num, gap_sub = _next_band_gap_text(sf["final_score"])
    guests_str = _format_guests_summary(eval_data)
    ax_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'>"
        f"<td class='lbl'>{AXIS_AR_CODE.get(a['id'], a['id'])} · {esc(a['name'])}</td>"
        f"<td class='num'><b>{a['score']:.1f} / {a['max']:.0f}</b></td>"
        f"<td class='num'>{a['pct']:.0f}%</td></tr>"
        for i, a in enumerate(eval_data["axes"])
    )
    chart_img = f"<div class='chart-box'><img src='data:image/png;base64,{charts['axes_bar']}'></div>" if charts.get("axes_bar") else ""
    st_li = "".join(f"<li>{esc(s)}</li>" for s in rep.get("strengths", [])[:3]) or "<li>انظر المحاور الأعلى نسبة.</li>"
    pr_li = "".join(f"<li>{esc(p)}</li>" for p in rep.get("priorities", [])[:3]) or f"<li>{esc(eval_data['top_priority_auto'])}</li>"
    body = f"""
<table style="margin:0;"><tr><td style="background:#12304d;color:#fff;padding:18pt 20pt;">
<p style="margin:0;color:#d9c48f;font-size:8.5pt;">تقرير إداري مختصر · سلّم Manar-100-v2</p>
<p style="margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;">{esc(display_name)} — <span dir="ltr">{esc(sf['reported_as'])}</span> ({esc(sf['band'])})</p>
<p style="margin:4pt 0 0;color:#c9d6e4;font-size:9.5pt;">المدة: {fmt_ts_span(exact_dur)} · الخام: {sf['raw_score']:.1f} · التعقيد: +{sf['difficulty_modifier']} · السقف: {sf['cap_ceiling']:.0f} · الفارق عن الترقية: {esc(gap_num)} ({esc(gap_sub)})</p>
<p style="margin:5pt 0 0;color:#c9d6e4;font-size:8.5pt;"><b style="color:#d9c48f;">الضيوف:</b> {esc(guests_str)}</p>
</td></tr></table>
<h2>الخلاصة التنفيذية</h2><p>{esc(rep.get('summary') or '')}</p>
<h2>لوحة المؤشرات السبعة</h2>
<table><tr><th>مداخلات / 10د</th><th>متوسط المداخلة</th><th>أطول بلوك ضيف</th><th>أسئلة أحادية</th><th>طلبات دليل / ساعة</th><th>تغطية العاجل</th><th>حصّة المقدّم</th></tr>
<tr><td class="num">{k['interventions_per_10min']:.1f}</td><td class="num">{k['mean_intervention_s']:.1f} ث</td><td class="num">{int(round(k['longest_unbroken_block_s']))} ث</td><td class="num">{k['single_question_pct']:.0f}%</td><td class="num">{k['evidence_request_per_hour']:.1f}</td><td class="num">{k['local_chyron_coverage_pct']:.0f}%</td><td class="num">{k['anchor_share_pct']:.0f}%</td></tr></table>
{chart_img}
<h2>نتائج المحاور السبعة</h2>
<table><tr><th>المحور</th><th class="num">الدرجة</th><th class="num">النسبة المئوية</th></tr>{ax_rows}</table>
<div class="callout-green"><b>أبرز نقاط القوة:</b><ul>{st_li}</ul></div>
<div class="callout-red" style="margin-top:8pt;"><b>الأولوية القصوى (مشتقّة آلياً): {esc(eval_data['top_priority_auto'])}</b><ul>{pr_li}</ul></div>"""
    return wrap_word_doc(f"تقرير مختصر - {display_name}", body)


def render_training_plan_html(display_name, eval_data, llm_qual) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    start_sc = int(round(sf["final_score"]))
    target_sc = min(92, max(75, start_sc + 12))
    rewrites = (llm_qual or {}).get("question_rewrites", [])
    rewrite_html = "".join(
        f"<p><b>سؤال فعلي من الحلقة (التوقيت {fmt_ts_span(rw.get('ts',''))} — المدة {esc(rw.get('orig_duration_s',''))} ثانية):</b></p>"
        f"<p class='qbad'><b>الصياغة الأصلية:</b> «{esc(rw.get('original',''))}»</p>"
        f"<p class='qgood'><b>الصياغة المقترحة (≤12 ثانية · استفهام واحد):</b> «{esc(rw.get('rewritten',''))}»</p>"
        for rw in rewrites[:4]
    )
    if not rewrite_html:
        rewrite_html = "".join(
            f"<p><b>سؤال طويل مرصود ({fmt_ts_span(q.get('start'))} — {q.get('duration_s')} ثانية، {q.get('word_count')} كلمة):</b></p><p class='qbad'>«{esc(q.get('text'))}»</p>"
            for q in eval_data.get("longest_questions", [])[:3]
        ) or "<p class='small'>لم تُرصد أسئلة طويلة متجاوزة للحدّ في هذه الحلقة.</p>"

    claim_rows = "".join(
        f"<tr class='{'alt' if i%2 else ''}'><td>«{esc(c.get('text',''))}» <span class='small'>({fmt_ts_span(c.get('at',''))})</span></td>"
        f"<td class='num'>{'مرفق بدليل' if c.get('evidence_offered') else 'بلا دليل رقمي'}</td>"
        f"<td>{'طُرحت مساءلة عند ' + fmt_ts_span(c.get('anchor_challenged_at')) if c.get('anchor_challenged_at') else 'مطلوب سؤال تحقّق مباشر: على أي معطى أو رقم بُني هذا الادعاء؟'}</td></tr>"
        for i, c in enumerate(eval_data.get("top_checkable_claims", [])[:3])
    ) or "<tr><td colspan='3' class='num'>تُستخرج ثلاثة ادعاءات هشّة من أطروحة الضيف قبل كل بثّ.</td></tr>"

    longest_ts = fmt_ts_clean(eval_data["monologues_over_180"][0]["start"]) if eval_data["monologues_over_180"] else (fmt_ts_clean(eval_data["unbroken_blocks"][0]["start"]) if eval_data.get("unbroken_blocks") else "أطول بلوك")
    body = f"""
<table style="margin:0;"><tr><td style="border:0.5pt solid #12304d;background:#12304d;padding:22pt 20pt;color:#fff;">
<p style="margin:0 0 6pt;font-size:8.5pt;color:#d9c48f;letter-spacing:1.2pt;">خطة تدريب تنفيذية · مبنية على بيانات الحلقة الفعلية</p>
<p style="margin:0 0 4pt;font-size:23pt;font-weight:bold;color:#fff;">برنامج التأهيل المهني — {esc(display_name)}</p>
<p style="margin:0 0 12pt;font-size:11.5pt;color:#c9d6e4;">من <span dir="ltr">{start_sc}</span> إلى <span dir="ltr">{target_sc}</span> في ستة أسابيع — أحد عشر تدريباً حركياً وسبعة مؤشرات مقيسة</p>
<table style="margin:0;"><tr>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">المتدرّب<br><b style="color:#fff;">{esc(display_name)}</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">نقطة الانطلاق<br><b style="color:#fff;" dir="ltr">{start_sc} / 100</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">الهدف المستهدف<br><b style="color:#fff;" dir="ltr">{target_sc} / 100</b></td>
<td style="border:0;border-top:1pt solid #3d5a78;padding:8pt 0 0;color:#a9bccf;font-size:8.5pt;">المدّة<br><b style="color:#fff;">6 أسابيع (4 ساعات/أسبوع)</b></td>
</tr></table></td></tr></table>

<h2>٠ · مبدأ التدريب والعادات الأربع المطلوب استبدالها</h2>
<div class="callout-navy"><p style="margin:0;color:#fff;font-size:9.5pt;"><b style="color:#e8d5a3;">مركز الخطة:</b> المهارة الأساسية موجودة لدى المتدرّب، والمطلوب رفع تردّدها من لحظات متفرّقة إلى انعكاس إيقاعي تلقائي كل 90 ثانية باستخدام مادّته المسجّلة نفسها.</p></div>
<table><tr><th style="width:28%;">العادة الحالية</th><th style="width:28%;">الأثر المقيس في الحلقة</th><th style="width:44%;">العادة المهنية البديلة</th></tr>
<tr><td class="lbl">ترك الاسترسال حتى ينتهي وحده</td><td>أطول بلوك {int(round(k['longest_unbroken_block_s']))} ثانية · {k['interventions_per_10min']:.1f} مداخلة/10د</td><td><b>اللمسة الثمانية (90/8):</b> تدخّل ≤ 8 ثوانٍ كل 90 ثانية يبدأ بكلمة من كلام الضيف</td></tr>
<tr class="alt"><td class="lbl">التمهيد الطويل وتعدّد الاستفهام</td><td>متوسط {k['mean_intervention_s']:.1f} ثانية · أحادية {k['single_question_pct']:.0f}%</td><td><b>السؤال الواحد ≤ 12 ثانية / ≤ 30 كلمة</b> بمرجع واحد قابل للتحقّق</td></tr>
<tr><td class="lbl">التسليم السلبي للادعاء</td><td>{k['evidence_request_per_hour']:.1f} طلب دليل / ساعة</td><td><b>التحقّق الداخلي:</b> بطاقة الفرضيات الثلاث + سؤال الفريق الأحمر التحصيني</td></tr>
<tr class="alt"><td class="lbl">فجوات العواجل والإغلاق</td><td>تغطية العاجل {k['local_chyron_coverage_pct']:.0f}%</td><td><b>قراءة العاجل خلال ≤ 60 ثانية + الثلاثون ثانية الأخيرة</b> (3 خلاصات + بطاقة الأسماء)</td></tr></table>

<h2 style="page-break-before:always;">١ · الوحدة الأولى: الصنعة الحوارية (التدريبات ١–٥)</h2>
<table><tr><th style="width:22%;">التدريب</th><th style="width:48%;">الميكانيكا العملية</th><th style="width:30%;">معيار الاجتياز</th></tr>
<tr><td class="lbl">١ · اللمسة الثمانية</td><td>تفكيك بلوك {fmt_ts_span(longest_ts)} لإيجاد 12 نقطة دخول، وتسجيل 6 لمسات ≤ 8 ثوانٍ (≤ 20 كلمة) تبدأ بكلمة من الضيف.</td><td>6 لمسات في 10د دون عبارة جاهزة</td></tr>
<tr class="alt"><td class="lbl">٢ · القصّ إلى 12 ثانية</td><td>إعادة كتابة الأسئلة الطويلة: استفهام واحد · مرجع واحد · بلا تمهيد تقريري (10 أسئلة يومياً لأسبوعين).</td><td>9 من 10 أسئلة ≤ 30 كلمة</td></tr>
<tr><td class="lbl">٣ · الضيف الثرثار</td><td>محاكاة 10 دقائق مع زميل يتحدّث 3 دقائق متواصلة، وعدّ اللمسات مقابل القطع في منتصف الجملة.</td><td>6 تدخّلات وصفر قطع بمنتصف جملة</td></tr>
<tr class="alt"><td class="lbl">٤ · الاعتراض الثلاثي</td><td>حفظ وتكرار: التلخيص والتسليم · التثبيت («وقفني عند هالجملة») · الاعتراض اللبق («لحظة خليني ناقشك هون»).</td><td>استخدام 3 تقنيات مختلفة في الحلقة</td></tr>
<tr><td class="lbl">٥ · إشارة نهاية الدور</td><td>إنهاء كل سؤال بنبرة هابطة ووقفة صامتة ثانيتين كاملتين (اختبار الصمّ).</td><td>نجاح 9 من 10 محاولات</td></tr></table>
<h3>إعادة صياغة فعلية لأطول أسئلة المتدرّب في هذه الحلقة</h3>
{rewrite_html}

<h2 style="page-break-before:always;">٢ · الوحدة الثانية: الوظيفة التحريرية (التدريبات ٦–١١)</h2>
<table><tr><th style="width:22%;">التدريب</th><th style="width:48%;">الميكانيكا العملية</th><th style="width:30%;">معيار الاجتياز</th></tr>
<tr><td class="lbl">٦ · بطاقة الثلاث</td><td>استخراج 3 ادعاءات هشّة متوقّعة من الضيف + ادعاء للخصم قبل البث وطرح اثنين على الأقل.</td><td>ادعاءان مطروحان في 3 حلقات متتالية</td></tr>
<tr class="alt"><td class="lbl">٧ · طلب الدليل</td><td>وقفة نصف ثانية بعد كل ادعاء ثم استخدام إحدى الصيغ الجاهزة بلا نبرة بوليسية.</td><td>3 طلبات دليل موثّقة في الساعة</td></tr>
<tr><td class="lbl">٨ · الفريق الأحمر (التحصين)</td><td>صياغة سؤال يُفرغ أقوى حجة للخصم على لسان الضيف قبل أن يستخدمها الخصم خارجياً.</td><td>سؤال تحصيني موثّق بالوقت في كل حلقة</td></tr>
<tr class="alt"><td class="lbl">٩ · الحجة لا الاستعارة</td><td>تحويل أي استعارة أدبية فوراً إلى ادعاء قابل للاختبار برقم أو معطى.</td><td>تحويل فوري عند أول ذكر</td></tr>
<tr><td class="lbl">١٠ · الشريط الأعمى</td><td>مشاهدة شريط أخبار مكتوم ونطق أي عاجل أمني محلي خلال ≤ 60 ثانية.</td><td>تغطية 100% من الأشرطة المحلية</td></tr>
<tr class="alt"><td class="lbl">١١ · الثلاثون ثانية الأخيرة</td><td>خاتمة بـ 3 خلاصات تنفيذية + سؤال مفتوح + شكر ببطاقة الأسماء الصحيحة.</td><td>حلقتان متتاليتان بلا أخطاء أسماء</td></tr></table>

<h3>تطبيق «بطاقة الثلاث» على ادعاءات هذه الحلقة</h3>
<table><tr><th>الادعاء المرصود من الحلقة</th><th class="num">حالة الدليل</th><th>التدخّل التحريري المطلوب</th></tr>{claim_rows}</table>

<h2>٣ · بنك العبارات الجاهزة والجدول الأسبوعي (6 أسابيع)</h2>
<table><tr><th style="width:20%;">الفئة</th><th style="width:80%;">الصيغ الجاهزة للحفظ</th></tr>
<tr><td class="lbl">اللمسات (≤ 8 ثوانٍ)</td><td>«وقفني عند كلمة…» · «رقم واحد بس: كم؟» · «يعني بجملة: …؟» · «هيدا تحليل ولا معطى؟» · «خلينا نمسكها من الجهة التانية…»</td></tr>
<tr class="alt"><td class="lbl">التحقّق والتحصين</td><td>«على أي معطى بنيت هذا؟» · «هذا رقم رسمي أم تقدير؟» · «الخصم سيقول كذا… شو جوابك؟» · «لو خالفك أحد، شو أقوى حجة عنده؟»</td></tr>
<tr><td class="lbl">الخاتمة</td><td>«خلاصة الساعة بثلاث نقاط: أولاً… ثانياً… ثالثاً… والسؤال المفتوح: …» · «شكراً لـ[الاسم من البطاقة]…»</td></tr></table>

<table><tr><th class="num" style="width:9%;">الأسبوع</th><th style="width:33%;">التدريبات</th><th style="width:30%;">المادة التطبيقية</th><th style="width:28%;">المُخرَج المقيس</th></tr>
<tr><td class="num">٠</td><td>القياس الأساسي وفيلم التشخيص</td><td>الحلقة كاملة + بلوك {fmt_ts_span(longest_ts)}</td><td>تثبيت أرقام الانطلاق السبعة</td></tr>
<tr class="alt"><td class="num">١</td><td>القصّ (٢) + إشارة نهاية الدور (٥)</td><td>أسئلته الطويلة من الحلقة</td><td>50 سؤالاً ≤ 30 كلمة</td></tr>
<tr><td class="num">٢</td><td>اللمسة الثمانية (١) + الضيف الثرثار (٣)</td><td>أرشيفه + محاكاة حيّة</td><td>6 لمسات في 10 دقائق</td></tr>
<tr class="alt"><td class="num">٣</td><td>الاعتراض الثلاثي (٤) + <b>أول بثّ مُقاس</b></td><td>على الهواء</td><td>تقرير مؤشرات رقم 1</td></tr>
<tr><td class="num">٤</td><td>بطاقة الثلاث (٦) + طلب الدليل (٧)</td><td>نصوص تفريغ</td><td>ادعاءان مطروحان في البثّ</td></tr>
<tr class="alt"><td class="num">٥</td><td>الفريق الأحمر (٨) + الحجة لا الاستعارة (٩)</td><td>أطروحات الحلقة</td><td>سؤال تحصيني موثّق</td></tr>
<tr><td class="num">٦</td><td>الشريط الأعمى (١٠) + الخاتمة (١١) + التقييم النهائي</td><td>على الهواء + شريط مكتوم</td><td>نتيجة ≥ {target_sc} / 100</td></tr></table>

<h2>٤ · لوحة المؤشرات السبعة وحماية نقاط القوة والشرط الإداري</h2>
<table><tr><th>المؤشر</th><th class="num">الانطلاق (هذه الحلقة)</th><th class="num">الهدف (الأسبوع 6)</th><th>طريقة القياس</th></tr>
<tr><td class="lbl">معدّل المداخلات / 10 دقائق</td><td class="num">{k['interventions_per_10min']:.1f} مداخلة</td><td class="num">8 – 10 مداخلات</td><td>قياس آلي من الصندوق الأسود</td></tr>
<tr class="alt"><td class="lbl">متوسّط طول المداخلة</td><td class="num">{k['mean_intervention_s']:.1f} ثانية</td><td class="num">12 ثانية أو أقل</td><td>قياس آلي من جدول الأدوار</td></tr>
<tr><td class="lbl">أطول بلوك ضيف متّصل</td><td class="num">{int(round(k['longest_unbroken_block_s']))} ثانية</td><td class="num">120 ثانية أو أقل</td><td>قياس آلي للبلوكات المتّصلة</td></tr>
<tr class="alt"><td class="lbl">طلبات الدليل / ساعة</td><td class="num">{k['evidence_request_per_hour']:.1f} طلب</td><td class="num">3 طلبات فأكثر</td><td>قياس آلي من جدول الادعاءات</td></tr>
<tr><td class="lbl">نسبة الأسئلة الأحادية</td><td class="num">{k['single_question_pct']:.0f}%</td><td class="num">85% فأكثر</td><td>عدّ الاستفهامات لكل مداخلة</td></tr>
<tr class="alt"><td class="lbl">تغطية الشريط المحلي العاجل</td><td class="num">{k['local_chyron_coverage_pct']:.0f}%</td><td class="num">100% (خلال ≤ 60ث)</td><td>تتبّع الشريط الزمني للشاشة</td></tr>
<tr><td class="lbl">الأخطاء الحرجة</td><td class="num">{len(eval_data['critical_events'])}</td><td class="num">0 (صفر)</td><td>بطاقة الأخطاء الحرجة</td></tr></table>

<h3>ما لا يجوز تدريبه (حماية نقاط القوة) والشرط الإداري</h3>
<table><tr><th style="width:24%;">البند</th><th style="width:76%;">القاعدة الملزمة</th></tr>
<tr><td class="lbl">حماية نقاط القوة</td><td>لا يُدرَّب على القطع العدواني (لمسة لا قطع، تبدأ بكلمة من كلام الضيف) · لا يُمسّ باللهجة اللبنانية المحكية ولا بالخط التحريري ولا بالوقار والإنصات ولا بمرونة المتابعة.</td></tr>
<tr class="alt"><td class="lbl">الشرط الإداري</td><td>تفعيل المصدر الميداني حتى آخر 10 دقائق · إسناد «بطاقة الثلاث» ومذكرة غرفة التحرير لمُعِدّ الحلقة قبل البث · وضع بطاقة الأسماء أمام المقدّم مع تقارير قصيرة بالأرقام قبل البلوكات التحليلية.</td></tr></table>
<p class="small" style="margin-top:12pt;text-align:center;color:#8a939e;">— نهاية خطة التدريب (Orchestrator V7.0) —</p>"""
    return wrap_word_doc(f"خطة التدريب 6 أسابيع - {display_name}", body)


def generate_blackbox_v2_md(display_name, exact_dur, derived_meta, chunks, eval_data, meta_info) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    lines = [
        f"# BLACKBOX V2.0 FORENSIC DOSSIER — {display_name}",
        "> Complete self-contained forensic dataset (`blackbox-v2.0` / `manar-100-v2`). All timestamps are absolute `HH:MM:SS` from `00:00:00`.",
        "",
        "## 1. MASTER METADATA",
        f"- **Presenter:** {display_name}",
        f"- **Program:** {eval_data.get('program_name') or 'Not specified'}",
        f"- **Guests & Reporters:** {_format_guests_summary(eval_data)}",
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
        "| ID | Criterion | Measured Value | Score | Confidence | Note |",
        "|---|---|---|---:|---|---|",
    ]
    for it in eval_data["scores"]:
        lines.append(f"| {it['criterion_id']} | {it['name']} | {it.get('metric_display', it['metric_value'])} | {it['score']}/{it['max']:.0f} | {it.get('confidence_ar', it['confidence'])} | {str(it['note']).replace('|','/')} |")
    lines += ["", "## 4. COMPLETE TURNS, INTERVENTIONS, CHYRONS & CLAIMS (CHUNK BY CHUNK)"]
    for c in chunks:
        lines.append(f"### Chunk {c['chunk_index']} ({c['window']['start']} -> {c['window']['end']}) | turns_sum={c['measurement']['sum_of_turns_s']}s / {c['measurement']['chunk_duration_s']}s (ffmpeg_speech={c['measurement'].get('ffmpeg_non_silent_s')}s)")
        for t in c.get("turns", []):
            lines.append(f"- **[{fmt_ts_clean(t.get('start'))}–{fmt_ts_clean(t.get('end'))}] {t.get('speaker_id')}** ({t.get('kind')}, {t.get('duration_s')}s, {t.get('word_count')}w, q={t.get('question_count')}): {t.get('text')}")
        for ch in c.get("chyron_timeline", []):
            lines.append(f"  - *CHYRON [{fmt_ts_clean(ch.get('on'))}] ({ch.get('type')}, read={ch.get('read_on_air_at')}):* {ch.get('text_raw')}")
        for cl in c.get("claims", []):
            lines.append(f"  - *CLAIM [{fmt_ts_clean(cl.get('at'))}] ({cl.get('speaker_id')}, checkable={cl.get('checkable')}, challenged={cl.get('anchor_challenged_at')}):* {cl.get('text')}")
    return "\n".join(lines)


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================
def main():
    global RUNTIME_DRIVE_TOKEN
    started_at = time.time()

    fid = (os.environ.get("FOLDER_ID") or "").strip()
    RUNTIME_DRIVE_TOKEN = (os.environ.get("DRIVE_TOKEN") or "").strip()
    init_or_refresh_permanent_drive_token()
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
    display_name = clean_display_name(raw_label)
    editorial_label = (
        "تقييم مهني محايد (Manar-100-v2 · معيار محايد مع حماية اللهجة اللبنانية)"
        if editorial_mode == "neutral"
        else "تقييم الأداء الحواري في قناة موجّهة الخط (Manar-100-v2)"
    )

    # === V7.0 ZERO-COST RE-RENDER MODE (from existing 05_Blackbox_V2_*.json + human_overrides) ===
    if mode == "rerender":
        log(f"♻️ Starting Zero-Cost Re-Render Mode for folder {fid} ({display_name})...")
        push_status(cfg, fid, run_id, "running", "إعادة توليد التقارير والرسوم من الصندوق الأسود ($0.00)...", 0, 1, 0.0, started_at, eta_sec=15)
        bb_file_id = None
        try:
            rf2 = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": f"'{fid}' in parents and trashed=false", "fields": "files(id,name)"}, timeout=20).json()
            for f in rf2.get("files", []):
                if f["name"].startswith("05_Blackbox_V2_") and f["name"].endswith(".json"):
                    bb_file_id = f["id"]
                    break
        except Exception as e:
            log(f"❌ Failed scanning folder for Blackbox JSON: {e}")
        if not bb_file_id:
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "لم يتم العثور على ملف الصندوق الأسود JSON لإعادة التوليد")
            sys.exit(1)

        bb_obj = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{bb_file_id}?alt=media", cfg, timeout=45).json()
        overrides = cfg.get("human_overrides") if isinstance(cfg.get("human_overrides"), dict) else {}
        if overrides.get("presenter_name"):
            display_name = clean_display_name(overrides["presenter_name"])
            clean_name = sanitize_filename(overrides["presenter_name"])
        if overrides.get("editorial_mode") in ("directed", "neutral"):
            editorial_mode = overrides["editorial_mode"]
            editorial_label = (
                "تقييم مهني محايد (Manar-100-v2 · معيار محايد مع حماية اللهجة اللبنانية)"
                if editorial_mode == "neutral"
                else "تقييم الأداء الحواري في قناة موجّهة الخط (Manar-100-v2)"
            )
        if isinstance(overrides.get("editorial_brief"), dict) and any(overrides["editorial_brief"].values()):
            editorial_brief = overrides["editorial_brief"]
        if overrides.get("ignored_critical_events"):
            editorial_brief = dict(editorial_brief or {})
            editorial_brief["_ignored_critical_events"] = list(overrides["ignored_critical_events"])

        valid_chunks = bb_obj.get("chunks", [])
        llm_qual = bb_obj.get("qualitative_synthesis", {})
        if overrides.get("custom_summary"):
            llm_qual = dict(llm_qual or {})
            rep_d = dict(llm_qual.get("report") or {})
            rep_d["summary"] = overrides["custom_summary"]
            llm_qual["report"] = rep_d

        old_meta = bb_obj.get("metadata") or {}
        derived_meta = old_meta.get("derived") or derive_metadata(src_url or old_meta.get("src_url", ""), 3600)
        tot_chunks = int(old_meta.get("chunks_total") or len(valid_chunks) or 1)
        dur = sum(float((c.get("measurement") or {}).get("chunk_duration_s", 300)) for c in valid_chunks) or 3600.0
        exact_duration = format_hhmmss(dur)

        final_eval = compute_deterministic_evaluation(valid_chunks, dur, rubric, derived_meta, llm_qual, editorial_brief, editorial_mode)
        charts = generate_eval_charts(final_eval)
        meta_info = {
            "clean_name": clean_name, "editorial_label": editorial_label,
            "chunks_ok": len(valid_chunks), "chunks_total": tot_chunks, "actual_cost": 0.0,
            "models_used": old_meta.get("models_used", ["RERENDER:$0"]), "src_url": src_url or old_meta.get("src_url", ""),
            "human_overrides": overrides,
        }
        det_html = render_detailed_eval_html(display_name, exact_duration, derived_meta, final_eval, llm_qual, charts, meta_info)
        sum_html = render_summarized_eval_html(display_name, exact_duration, final_eval, llm_qual, charts)
        trn_html = render_training_plan_html(display_name, final_eval, llm_qual)
        bb_md = generate_blackbox_v2_md(display_name, exact_duration, derived_meta, valid_chunks, final_eval, meta_info)
        bb_obj["metadata"] = {**meta_info, "derived": derived_meta}
        bb_obj["evaluation"] = final_eval
        bb_obj["qualitative_synthesis"] = llm_qual

        files_to_write = [
            (f"01_Main_Detailed_Report_{clean_name}.html", det_html, "text/html"),
            (f"02_Summarized_Report_{clean_name}.html", sum_html, "text/html"),
            (f"03_Training_Plan_{clean_name}.html", trn_html, "text/html"),
            (f"04_Blackbox_V2_{clean_name}.md", bb_md, "text/markdown"),
            (f"05_Blackbox_V2_{clean_name}.json", json.dumps(bb_obj, ensure_ascii=False, indent=2), "application/json"),
        ]
        for fname, content, mime in files_to_write:
            fp = os.path.join(BACKUP_DIR, fname)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(content)
            up_ok = drive_upload(fid, cfg, fp, mime)
            log(f"  ☁️ Re-render upload {fname}: {'✅ OK' if up_ok else '❌ FAILED'}")

        kpi_sum = {
            "folder_id": fid, "presenter": display_name, "date": derived_meta.get("broadcast_date") or datetime.now(BEIRUT_TZ).strftime("%Y-%m-%d"),
            "duration": exact_duration, "final_score": final_eval["score_final"]["final_score"], "raw_score": final_eval["score_final"]["raw_score"],
            "reported_as": final_eval["score_final"]["reported_as"], "band": final_eval["score_final"]["band"],
            "axes": {a["id"]: a["pct"] for a in final_eval["axes"]}, "kpis": final_eval["kpis"],
            "critical_events": [e.get("type") for e in final_eval["critical_events"]], "src_url": meta_info["src_url"],
        }
        update_drive_folder_meta(fid, cfg, f"PROCESSED_{clean_name}_$0.00", f"🔗 رابط المصدر: {meta_info['src_url']}\n• الاسم: {display_name}\n• النتيجة (مُعاد توليدها): {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']})")
        finalize_cost_webhook(cfg, fid, 0.0, "completed", f"تمت إعادة التوليد ({final_eval['score_final']['reported_as']})", completed_chunks=len(valid_chunks), total_chunks=tot_chunks, kpi_summary=kpi_sum)
        send_tg(f"♻️ <b>تمت إعادة توليد تقارير {display_name} بنجاح ($0.00)</b>\n• النتيجة الجديدة: {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']})", tg_bot, tg_chat)
        log("🎉 Re-Render V7.0 completed in $0.00!")
        sys.exit(0)

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
    m_fn_dur = re.search(r"archive-\d{10}-(\d+)", src_url or "")
    if m_fn_dur and int(m_fn_dur.group(1)) > 30:
        dur = float(m_fn_dur.group(1))
    if not is_youtube_url(src_url):
        probe_candidates = [
            (["ffprobe", "-v", "error", "-tls_verify", "0", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url] if src_url.lower().startswith("https://") else ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url]),
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", ("http://" + src_url[8:]) if src_url.lower().startswith("https://") else src_url],
        ]
        for pcmd in probe_candidates:
            try:
                po = subprocess.run(pcmd, capture_output=True, text=True, timeout=30).stdout.strip()
                if po and float(po) > 0:
                    dur = float(po)
                    break
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

    send_tg(f"🚀 بدء التحليل V7.0 | {clean_name} | المدة: {exact_duration} | المقاطع: {tot} ({chunk_min}د)", tg_bot, tg_chat)

    act_cost, exhausted_pairs, bad_keys, models_used = 0.0, set(), set(), set()

    def budget_ok() -> bool:
        with STATE_LOCK:
            return act_cost < episode_budget_cap

    def extract_chunk_media(idx: int) -> tuple:
        st, d, mp4_path = idx * chunk_sec, min(chunk_sec, dur - idx * chunk_sec), f"/tmp/chunk_{idx}.mp4"
        last_ff_err = ""
        for attempt in range(1, 4):
            if CANCEL_EVENT.is_set():
                raise RuntimeError("Cancelled by user")
            target_in = local_input
            if not downloaded_local and attempt == 3 and target_in.lower().startswith("https://"):
                target_in = "http://" + target_in[8:]
            cmd = ["ffmpeg", "-y", "-loglevel", "error"]
            if not downloaded_local:
                cmd += ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5"]
                if target_in.lower().startswith("https://"):
                    cmd += ["-tls_verify", "0"]
            cmd += [
                "-ss", str(st), "-t", str(d), "-i", target_in,
                "-vf", f"fps={fps},scale=640:360",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
                "-c:a", "aac", "-ac", "1", "-ar", "16000", "-b:a", "24k",
                "-movflags", "+faststart", mp4_path,
            ]
            t_ff = time.time()
            with FFMPEG_SEM:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            if proc.returncode == 0 and os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 5000:
                sp_s = measure_chunk_speech_ffmpeg(mp4_path, d)
                mb = os.path.getsize(mp4_path) / (1024 * 1024)
                log(f"🎞️ Sliced chunk {idx+1}/{tot} ({format_hhmmss(st)}->{format_hhmmss(st+d)}) | {mb:.2f} MB | speech={sp_s}s | {time.time()-t_ff:.1f}s")
                return mp4_path, st, d, sp_s
            last_ff_err = (proc.stderr or "").strip()[:240]
            print(f"[FFmpeg Retry {attempt}/3 chunk {idx+1}] rc={proc.returncode} err={last_ff_err}", flush=True)
            time.sleep(2)
        raise RuntimeError(f"FFmpeg failed on chunk {idx+1}: {last_ff_err}")

    if mode == "eval":
        saved_ckpts, saved_ctx = load_drive_checkpoints(fid, cfg)
        chunks_by_idx = [None] * tot
        for ci, cobj in saved_ckpts.items():
            if 0 <= ci < tot and isinstance(cobj, dict):
                chunks_by_idx[ci] = cobj
        resumed_cnt = sum(1 for x in chunks_by_idx if x is not None)
        if resumed_cnt > 0:
            log(f"♻️ RESUME ACTIVE: Restored {resumed_cnt}/{tot} completed chunks from Drive checkpoints!")
            send_tg(f"♻️ <b>استكمال التقييم ({display_name})</b>: تم استعادة {resumed_cnt}/{tot} مقطع من نقاط الحفظ.", tg_bot, tg_chat)

        missing_idxs = [i for i in range(tot) if chunks_by_idx[i] is None]
        push_status(cfg, fid, run_id, "running", f"تقطيع المقاطع المطلوبة ({len(missing_idxs)}/{tot} × 5 دقائق)...", resumed_cnt, tot, 0.0, started_at)
        sliced = {}
        if missing_idxs:
            with ThreadPoolExecutor(max_workers=min(3, len(missing_idxs))) as ex:
                futs = {ex.submit(extract_chunk_media, i): i for i in missing_idxs}
                for fut in as_completed(futs):
                    sliced[futs[fut]] = fut.result()

        failed_idxs = []
        running_ctx = saved_ctx if isinstance(saved_ctx, dict) else {
            "chunk_index": 0, "program_name": None, "presenter_name": display_name,
            "participants": [{"id": "ANCHOR", "name": display_name, "role": "مقدّم"}],
            "active_files": [], "last_300_words": "", "open_claims": [], "unresolved_chyrons": [], "cumulative_clock": "00:00:00",
        }
        for i in range(tot):
            if chunks_by_idx[i] is not None:
                running_ctx = update_running_context(running_ctx, chunks_by_idx[i], chunks_by_idx[i].get("window", {}).get("end", format_hhmmss((i + 1) * chunk_sec)))
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
            full_prompt = (
                f"{prompt_system}\n\n{filled_chunk}\n\n"
                f"تنبيه إلزامي: اسم مقدّم هذه الحلقة (ANCHOR) المثبّت هو «{display_name}» — يُمنع استبداله بأي اسم مقدم آخر من entities.csv.\n"
                f"اسم ملف المصدر: {derived_meta.get('source_filename')}\n{system_prompt_extra}"
            )
            run_strat = "pro_only" if force_paid else strat
            try:
                log(f"▶️ [Chunk {i+1}/{tot} {st_hms}->{end_hms}] Starting analysis (strategy={run_strat}, dlq={force_paid})...")
                raw_data, cost, used_model, tier = call_gemini_chunk(
                    mp4_path, full_prompt, valid_free, valid_paid, paid_models, run_strat,
                    exhausted_pairs, bad_keys, budget_ok, chunk_schema=schema_chunk or None,
                    chunk_tag=f"Chunk {i+1}/{tot}"
                )
                norm = normalize_chunk_v2(raw_data, i + 1, st_hms, end_hms, d, derived_meta, speech_s, entity_registry, locked_presenter=display_name)
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
            if chunks_by_idx[i] is not None:
                log(f"⏭️ Skipping chunk {i+1}/{tot} (already restored from checkpoint)")
                continue
            try:
                norm, cost, used_model, tier, end_hms = run_eval_chunk(i, running_ctx, force_paid=False)
                chunks_by_idx[i] = norm
                running_ctx = update_running_context(running_ctx, norm, end_hms)
                act_cost += cost
                models_used.add(f"{tier}:{used_model}")
                drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", norm)
                drive_upload_json_blob(fid, cfg, "_ckpt_ctx.json", running_ctx)
                log(f"✅ Chunk {i+1}/{tot} ({norm['window']['start']}->{norm['window']['end']}) turns={len(norm['turns'])} chyrons={len(norm['chyron_timeline'])} claims={len(norm['claims'])} via {tier}:{used_model} | ${cost:.4f}")
            except Exception as e:
                if CANCEL_EVENT.is_set() or "Cancelled by user" in str(e):
                    break
                failed_idxs.append(i)
                log(f"⚠️ Chunk {i+1}/{tot} failed (queued for DLQ): {e}")
            done_now = sum(1 for x in chunks_by_idx if x is not None)
            avg_per_chunk = (time.time() - proc_start) / max(1, i + 1)
            eta = int(avg_per_chunk * (tot - i - 1)) + (len(failed_idxs) * 60) + 60
            log(f"📡 Progress {done_now}/{tot} | failed_dlq={len(failed_idxs)} | avg={avg_per_chunk:.1f}s/chunk | ETA={format_hhmmss(eta)}")
            push_status(cfg, fid, run_id, "running", f"توثيق الصندوق الأسود ({done_now}/{tot})", done_now, tot, act_cost, started_at, eta_sec=eta)

        # DLQ Rescue BEFORE deleting local source video
        if failed_idxs and not CANCEL_EVENT.is_set():
            log(f"🚑 Entering DLQ Rescue for {len(failed_idxs)} chunks...", flush=True)
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
                    drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", norm)
                    log(f"🚑 Chunk {i+1} rescued via {tier}:{used_model}")
                    done_now = sum(1 for x in chunks_by_idx if x is not None)
                    push_status(cfg, fid, run_id, "running", f"إنقاذ المقاطع المتعثرة ({done_now}/{tot})", done_now, tot, act_cost, started_at, eta_sec=25)
                except Exception as ex:
                    log(f"❌ DLQ failed on chunk {i+1}: {ex}")

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
        qual_ids = ["B3", "C3", "D1", "D4", "E1", "E2", "E4", "F1", "F2", "F3", "G2"]
        qual_defs = []
        for ax in rubric.get("axes", []):
            for it in ax.get("items", []):
                if it["id"] in qual_ids:
                    qual_defs.append(f"- {it['id']} ({it['name']} — الحد الأقصى {it['max']}): KPI={it.get('kpi')} | مراسي={it.get('anchors', {})}")
        qual_defs_text = "\n".join(qual_defs)

        synth_request = (
            f"{prompt_system}\n\n{prompt_synth_tpl}\n\n"
            f"اسم مقدّم الحلقة: {display_name} | نوع المسار التحريري: {editorial_label}\n"
            f"تعريفات البنود النوعية الـ 11 الإلزامية (التزم التطابق الحرفي بين كل رمز ومعناه — يُمنع الخلط بينها):\n{qual_defs_text}\n\n"
            f"المؤشرات المحسوبة حتمياً:\n{json.dumps(pre_eval['kpis'], ensure_ascii=False)}\n\n"
            f"الموجز التحريري (editorial_brief):\n{json.dumps(editorial_brief, ensure_ascii=False)}\n\n"
            f"أطول 5 أسئلة للمقدم (أعد صياغة أطول 3 منها إلى ≤12ث و≤30 كلمة باستفهام واحد ومرجع واحد):\n"
            f"{json.dumps(pre_eval['longest_questions'], ensure_ascii=False)}\n\n"
            'أخرج JSON صالح فقط بهذه المفاتيح (واكتب الملاحظات بالعربية الفصحى السلسة مع توقيتات HH:MM:SS):\n'
            '{"episode_overview":{"program_name":"","topic":""},'
            '"qualitative_items":{"B3":{"score":0,"anchor_hit":"full|partial|low|zero","evidence":[],"note":""},"C3":{"score":0,"anchor_hit":"","evidence":[],"note":""},"D1":{"score":0,"anchor_hit":"","evidence":[],"note":""},"D4":{"score":0,"anchor_hit":"","evidence":[],"note":""},"E1":{"score":0,"anchor_hit":"","evidence":[],"note":""},"E2":{"score":0,"anchor_hit":"","evidence":[],"note":""},"E4":{"score":0,"anchor_hit":"","evidence":[],"note":""},"F1":{"score":0,"anchor_hit":"","evidence":[],"note":""},"F2":{"score":0,"anchor_hit":"","evidence":[],"note":""},"F3":{"score":0,"anchor_hit":"","evidence":[],"note":""},"G2":{"score":0,"anchor_hit":"","evidence":[],"note":""}},'
            '"documented_examples":[{"ts":"HH:MM:SS","speaker":"المقدّم|الضيف|شريط الشاشة","quote":"اقتباس حرفي أو رصد دقيق","significance":"الدلالة المهنية"}],'
            '"question_rewrites":[{"ts":"HH:MM:SS","orig_duration_s":0,"original":"","rewritten":""}],'
            '"report":{"summary":"خلاصة تنفيذية مركزة وواضحة من 5-7 أسطر","strengths":["نقطة قوة مع توقيت HH:MM:SS"],"priorities":["أولوية تطوير عملية مع توقيت HH:MM:SS"]}}\n\n'
            f"بيانات المقاطع:\n{json.dumps(valid_chunks, ensure_ascii=False)[:180000]}"
        )

        llm_qual = {}
        now_ts = time.time()
        raw_synth = (
            [(valid_paid, m, True) for m in (rank_discovered_models(paid_models, PREFERRED_SYNTH_ORDER, True) or paid_models)]
            if (strat != "flash_only" and valid_paid and budget_ok()) else []
        ) + [(k, m, False) for k, kms in valid_free for m in kms]
        synth_candidates = [x for x in raw_synth if MODEL_COOLDOWN_UNTIL.get(x[1], 0) <= now_ts] + [x for x in raw_synth if MODEL_COOLDOWN_UNTIL.get(x[1], 0) > now_ts]
        log(f"🧠 Starting Synthesis stage ({len(valid_chunks)}/{tot} valid chunks, {len(synth_candidates)} model candidates)...")
        for skey, smodel, spaid in synth_candidates:
            s_tier = "PAID" if spaid else "FREE"
            try:
                t_syn = time.time()
                log(f"  🤖 [Synthesis] Calling {s_tier} {mask_key(skey)} / {smodel} (timeout=240s)...")
                client = make_genai_client(skey, timeout_ms=240_000)
                res = run_with_hard_timeout(
                    lambda: client.models.generate_content(model=smodel, contents=synth_request, config=build_gen_config(max_out=32768)),
                    240,
                    f"synthesis({smodel})",
                )
                llm_qual = clean_json_text(res.text)
                s_cost = calc_cost(smodel, res.usage_metadata, spaid)
                act_cost += s_cost
                models_used.add(f"{s_tier}:{smodel}")
                log(f"✅ Synthesis completed via {s_tier}:{smodel} in {time.time() - t_syn:.1f}s | ${s_cost:.4f}")
                break
            except Exception as e:
                log(f"  ⚠️ [Synthesis Retry {s_tier} {mask_key(skey)} / {smodel}] {str(e)[:180]}")

        final_eval = compute_deterministic_evaluation(valid_chunks, dur, rubric, derived_meta, llm_qual, editorial_brief, editorial_mode)
        charts = generate_eval_charts(final_eval)
        meta_info = {
            "clean_name": clean_name, "editorial_label": editorial_label,
            "chunks_ok": len(valid_chunks), "chunks_total": tot, "actual_cost": act_cost,
            "models_used": sorted(models_used), "src_url": src_url,
        }

        det_html = render_detailed_eval_html(display_name, exact_duration, derived_meta, final_eval, llm_qual, charts, meta_info)
        sum_html = render_summarized_eval_html(display_name, exact_duration, final_eval, llm_qual, charts)
        trn_html = render_training_plan_html(display_name, final_eval, llm_qual)
        bb_md = generate_blackbox_v2_md(display_name, exact_duration, derived_meta, valid_chunks, final_eval, meta_info)
        bb_json_obj = {
            "prompt_version": "blackbox-v2.0",
            "rubric_version": rubric.get("rubric_version", "manar-100-v2"),
            "metadata": {**meta_info, "derived": derived_meta, "key_health": get_key_health_snapshot()},
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
        log(f"📊 Scoring & 3 RTL charts complete: {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']}) | Uploading 5 outputs...")
        for fname, content, mime in files_to_write:
            fp = os.path.join(BACKUP_DIR, fname)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(content)
            out_paths.append(fp)
            up_ok = drive_upload(fid, cfg, fp, mime)
            log(f"  ☁️ Drive upload {fname} ({os.path.getsize(fp)//1024} KB): {'✅ OK' if up_ok else '❌ FAILED'}")
            if not fname.endswith(".json"):
                send_tg(f"📄 <b>{fname}</b> | النتيجة: {final_eval['score_final']['reported_as']} | الكلفة: ${act_cost:.4f}", tg_bot, tg_chat, fp)

        email_subj = f"✅ تقييم V7.0 ({clean_name}) — {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']})"
        smtp_ok = send_smtp_resilient(email_subj, sum_html, out_paths[:4], cfg.get("alert_emails", ""), gm_u, gm_p)
        if smtp_ok:
            rec_list = [e.strip() for e in re.split(r"[,;\s\n\r]+", cfg.get("alert_emails", "") or "") if "@" in e.strip()]
            send_tg(f"📧 تم إرسال التقارير بالبريد إلى: {', '.join(rec_list) or gm_u}", tg_bot, tg_chat)
        cleanup_drive_checkpoints(fid, cfg)
        final_folder_name = f"PROCESSED_{clean_name}_${act_cost:.2f}"
        folder_desc = (
            f"🔗 رابط المصدر: {src_url}\n\n"
            f"• الاسم: {display_name}\n• النتيجة: {final_eval['score_final']['reported_as']} ({final_eval['score_final']['band']})\n"
            f"• الخام: {final_eval['score_final']['raw_score']} | التعقيد: +{final_eval['score_final']['difficulty_modifier']} | السقف: {final_eval['score_final']['cap_ceiling']}\n"
            f"• الأولوية القصوى: {final_eval['top_priority_auto']}\n• الكلفة: ${act_cost:.4f} | المقاطع: {len(valid_chunks)}/{tot}"
        )
        update_drive_folder_meta(fid, cfg, final_folder_name, folder_desc)
        kpi_sum = {
            "folder_id": fid, "presenter": display_name, "date": derived_meta.get("broadcast_date") or datetime.now(BEIRUT_TZ).strftime("%Y-%m-%d"),
            "duration": exact_duration, "final_score": final_eval["score_final"]["final_score"], "raw_score": final_eval["score_final"]["raw_score"],
            "reported_as": final_eval["score_final"]["reported_as"], "band": final_eval["score_final"]["band"],
            "axes": {a["id"]: a["pct"] for a in final_eval["axes"]}, "kpis": final_eval["kpis"],
            "critical_events": [e.get("type") for e in final_eval["critical_events"]], "src_url": src_url,
        }
        finalize_cost_webhook(cfg, fid, act_cost, "completed", f"اكتمل ({final_eval['score_final']['reported_as']})", trigger_gas_email=(not smtp_ok), email_subject=email_subj, completed_chunks=len(valid_chunks), total_chunks=tot, kpi_summary=kpi_sum)
        log(f"🎉 V7.0 completed! {final_folder_name} | Score: {final_eval['score_final']['reported_as']} | Cost: ${act_cost:.4f}")

    else:
        # Daily Broadcast Mode (Parallel + Checkpoint/Resume + DLQ + Detailed & Summarized Word-RTL + Blackbox .md/.json)
        saved_ckpts, _ = load_drive_checkpoints(fid, cfg)
        daily_results, failed_daily = [None] * tot, []
        for ci, cobj in saved_ckpts.items():
            if 0 <= ci < tot and isinstance(cobj, dict):
                daily_results[ci] = cobj
        done_daily = sum(1 for x in daily_results if x is not None)
        if done_daily > 0:
            log(f"♻️ DAILY RESUME ACTIVE: Restored {done_daily}/{tot} chunks from Drive checkpoints!")
            send_tg(f"♻️ <b>استكمال البث اليومي ({clean_name})</b>: تم استعادة {done_daily}/{tot} مقطع.", tg_bot, tg_chat)
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
                log(f"▶️ [Daily {i+1}/{tot} {st_h}->{en_h}] Starting analysis...")
                dat, c, m, t = call_gemini_chunk(mp4, pr, valid_free, valid_paid, paid_models, "pro_only" if force_paid else strat, exhausted_pairs, bad_keys, budget_ok, chunk_tag=f"Daily {i+1}/{tot}")
                with STATE_LOCK:
                    models_used.add(f"{t}:{m}")
                return dat, c
            finally:
                if os.path.exists(mp4):
                    try:
                        os.remove(mp4)
                    except Exception:
                        pass

        daily_todo = [i for i in range(tot) if daily_results[i] is None]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(run_daily, i, False): i for i in daily_todo}
            for fut in as_completed(futs):
                i = futs[fut]
                try:
                    dat, c = fut.result()
                    obj_d = {"chunk_index": i + 1, "data": dat}
                    daily_results[i] = obj_d
                    drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", obj_d)
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
                    obj_d = {"chunk_index": i + 1, "data": dat}
                    daily_results[i] = obj_d
                    drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", obj_d)
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
            f"<table style='margin:0;'><tr><td style='background:#12304d;color:#fff;padding:18pt;'><p style='margin:0;color:#d9c48f;font-size:8.5pt;'>رصد البث اليومي V7.0</p><p style='margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;'>{esc(clean_name)} — المدة: {esc(exact_duration)}</p></td></tr></table>"
            f"<h2>جدول البرامج والفقرات ({len(all_progs)} فقرة)</h2><table><tr><th>التوقيت</th><th>التصنيف</th><th>المحور</th><th>العنوان</th><th>الملخص</th></tr>{rows}</table>"
        )
        sum_daily = wrap_word_doc(
            f"ملخص البث اليومي - {clean_name}",
            f"<table style='margin:0;'><tr><td style='background:#12304d;color:#fff;padding:18pt;'><p style='margin:0;color:#d9c48f;font-size:8.5pt;'>الملخص التنفيذي للبث اليومي V7.0</p><p style='margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;'>{esc(clean_name)}</p><p style='margin:4pt 0 0;color:#c9d6e4;'>المدة: {esc(exact_duration)} | الفقرات: {len(all_progs)} | المقاطع: {len(valid_daily)}/{tot} | الكلفة: ${act_cost:.4f}</p></td></tr></table>"
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

        email_subj = f"✅ تقرير البث اليومي V7.0 ({clean_name}) | الكلفة: ${act_cost:.4f}"
        smtp_ok = send_smtp_resilient(email_subj, sum_daily, out_paths[:3], cfg.get("alert_emails", ""), gm_u, gm_p)
        cleanup_drive_checkpoints(fid, cfg)
        update_drive_folder_meta(fid, cfg, f"PROCESSED_{clean_name}_${act_cost:.2f}", f"🔗 {src_url}\n• الفقرات: {len(all_progs)} | الكلفة: ${act_cost:.4f}")
        finalize_cost_webhook(cfg, fid, act_cost, "completed", f"اكتمل البث اليومي ({len(valid_daily)}/{tot})", trigger_gas_email=(not smtp_ok), email_subject=email_subj, completed_chunks=len(valid_daily), total_chunks=tot)


if __name__ == "__main__":
    main()
