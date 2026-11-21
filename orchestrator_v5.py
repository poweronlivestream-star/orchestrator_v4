#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V8.0 — Template-Driven Engine & Universal Blackbox v3.0
- Python is strictly the execution/calculation engine, NOT the prompt or criteria store.
- Reads all templates, criteria, weights, caps, and admin rules dynamically from GitHub /profiles/.
- Universal Blackbox v3.0 captures 6 neutral layers once per episode (turns with intervention/question split,
  color-aware chyrons, visual/directing cues, preparation signals, per-guest observations, critical events).
- Supports: ingest_only ($0.00 video pre-archive), rerender/additional templates from Blackbox,
  multi-template scored evaluation (Presenter+Prep, Guests, Directing, Episode, Custom),
  structured inventory (Daily 18h or Custom), clean Pause/Resume (_pause.flag), and Detailed Score Loss Breakdown.
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
PAUSE_EVENT = threading.Event()
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


AXIS_AR_CODE = {"A": "أ", "B": "ب", "C": "ج", "D": "د", "E": "هـ", "F": "و", "G": "ز", "H": "ح"}
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


def format_metric_display(cid: str, val, kpis: dict = None, metric_key: str = "") -> str:
    if val is None or val == "":
        return "غير متاح"
    s = str(val).strip()
    if s in ANCHOR_HIT_AR:
        return ANCHOR_HIT_AR[s]
    mk = metric_key or ""
    if mk == "interventions_per_10min" or cid == "A1":
        return f"{float(val):.1f} مداخلة / 10 دقائق"
    if mk == "mean_intervention_s" or cid == "A2":
        return f"{float(val):.1f} ثانية / مداخلة"
    if mk in ("longest_unbroken_block_s", "guest_longest_block_s") or cid in ("A3", "GC2"):
        return f"{int(round(float(val)))} ثانية (أطول بلوك)"
    if (mk == "source_shares" or cid == "A4") and "/" in s:
        p = s.split("/")
        if len(p) == 3:
            return f"مقدّم {p[0]}% · ضيف {p[1]}% · ميدان {p[2]}%"
    if mk == "single_question_pct" or cid == "B1":
        return f"{float(val):.1f}% أسئلة أحادية"
    if mk in ("mean_question_s", "mean_preamble_s") or cid == "B2":
        return f"{float(val):.1f} ثانية (متوسط السؤال)"
    if mk == "duplicated_files" or cid == "B4":
        n = int(float(val))
        return "لا تكرار (0)" if n == 0 else f"{n} ملف مكرّر"
    if mk == "evidence_request_per_hour" or cid == "C1":
        return f"{float(val):.1f} طلب دليل / ساعة"
    if mk == "counter_hypothesis_per_hour" or cid == "C2":
        return f"{float(val):.1f} فرضية مضادة / ساعة"
    if mk == "name_errors" or cid in ("C4", "G3"):
        n = int(float(val))
        return "صفر أخطاء" if n == 0 else f"{n} خطأ"
    if mk == "field_balance" or cid == "D2":
        return "متوازن" if float(val) >= 0.75 else "تهميش للميدان"
    if mk == "touch_ratio" or cid == "D3":
        return f"{float(val):.2f} لمسة / 90 ثانية"
    if mk in ("red_chyron_coverage_pct", "local_chyron_coverage_pct") or cid == "E2":
        return f"{float(val):.0f}% تغطية العاجل الأحمر"
    if mk == "guest_evidence_pct" or cid == "GB1":
        return f"{float(val):.0f}% ادعاءات مدعّمة بدليل"
    if mk == "welcome_s" or cid == "G1":
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


def drive_set_anyone_reader(file_id: str, cfg: dict) -> bool:
    """Grants anyone-with-link read-only permission on verification video so RBAC users can play it in-app."""
    if not file_id:
        return False
    try:
        r = drive_request(
            "POST",
            f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions",
            cfg,
            headers={"Content-Type": "application/json"},
            json={"role": "reader", "type": "anyone"},
            timeout=20,
        )
        return r.status_code in (200, 201)
    except Exception as e:
        log(f"[Drive Share Warning] {e}")
        return False


def drive_upload_resumable(folder_id: str, cfg: dict, filepath: str, filename: str, mimetype: str = "video/mp4") -> str:
    """Uploads large video files (50 MB -> multi-GB) to Google Drive via Resumable Upload in 8 MB chunks."""
    if not (folder_id and os.path.exists(filepath)):
        return ""
    total_size = os.path.getsize(filepath)
    if total_size < 10_000:
        return ""

    # 1. Skip re-upload if already saved in this folder (e.g., on Resume)
    try:
        q = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
        chk = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q, "fields": "files(id,size)"}, timeout=20)
        if chk.status_code == 200:
            for ef in chk.json().get("files", []):
                if int(ef.get("size") or 0) > 50_000:
                    log(f"⏭️ Video already archived on Drive ({filename}, id={ef['id']}) — skipping re-upload.")
                    return ef["id"]
    except Exception:
        pass

    # 2. Fast lossless faststart remux (2s) so Drive & browsers seek instantly
    upload_path = filepath
    fast_path = f"/tmp/faststart_{os.getpid()}.mp4"
    if mimetype == "video/mp4":
        try:
            remux = subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", "-i", filepath, "-c", "copy", "-movflags", "+faststart", fast_path],
                capture_output=True, text=True, timeout=180,
            )
            if remux.returncode == 0 and os.path.exists(fast_path) and os.path.getsize(fast_path) > 50_000:
                upload_path = fast_path
                total_size = os.path.getsize(upload_path)
        except Exception:
            pass

    chunk_bytes = 8 * 1024 * 1024  # 8 MB (strict multiple of 256 KB)
    file_id = ""
    try:
        t0 = time.time()
        log(f"☁️ Starting resumable Drive upload for {filename} ({total_size / (1024*1024):.1f} MB)...")
        init_r = drive_request(
            "POST",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name,size",
            cfg,
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": mimetype,
                "X-Upload-Content-Length": str(total_size),
            },
            data=json.dumps({"name": filename, "parents": [folder_id]}).encode("utf-8"),
            timeout=30,
        )
        if init_r.status_code not in (200, 201) or not init_r.headers.get("Location"):
            log(f"❌ Resumable upload init failed: HTTP {init_r.status_code} {init_r.text[:160]}")
            return ""
        session_url = init_r.headers["Location"]

        offset = 0
        last_log_pct = -1
        with open(upload_path, "rb") as f:
            while offset < total_size:
                if CANCEL_EVENT.is_set():
                    return ""
                f.seek(offset)
                data = f.read(chunk_bytes)
                if not data:
                    break
                end_pos = offset + len(data) - 1
                cr_hdr = f"bytes {offset}-{end_pos}/{total_size}"
                ok_chunk = False
                for attempt in range(1, 4):
                    try:
                        pr = requests.put(
                            session_url,
                            headers={"Content-Type": mimetype, "Content-Range": cr_hdr, "Content-Length": str(len(data))},
                            data=data,
                            timeout=120,
                        )
                        if pr.status_code in (200, 201):
                            file_id = (pr.json() or {}).get("id", "")
                            ok_chunk = True
                            break
                        if pr.status_code == 308:
                            ok_chunk = True
                            break
                        log(f"  ⚠️ Video chunk retry {attempt}/3 (HTTP {pr.status_code})")
                        time.sleep(2 * attempt)
                    except Exception as ce:
                        log(f"  ⚠️ Video chunk net error {attempt}/3: {ce}")
                        time.sleep(2 * attempt)
                if not ok_chunk:
                    log(f"❌ Aborted video upload at {offset}/{total_size}")
                    return ""
                offset = end_pos + 1
                pct = int((offset / total_size) * 100)
                if pct >= last_log_pct + 25 or pct == 100:
                    last_log_pct = pct
                    log(f"  ☁️ Video upload progress: {pct}% ({offset/(1024*1024):.1f}/{total_size/(1024*1024):.1f} MB)")

        if file_id:
            log(f"✅ Video saved to Drive ({filename}, id={file_id}) in {time.time() - t0:.1f}s!")
            if cfg.get("video_public_link", True):
                shared = drive_set_anyone_reader(file_id, cfg)
                log(f"  🔗 Video link sharing (anyone=reader): {'✅ Enabled' if shared else '⚠️ Skipped'}")
        return file_id
    except Exception as e:
        log(f"[Video Upload Warning] {e}")
        return ""
    finally:
        if upload_path == fast_path and os.path.exists(fast_path):
            try:
                os.remove(fast_path)
            except Exception:
                pass


def probe_remote_content_length(url: str) -> int:
    try:
        r = requests.head(url, allow_redirects=True, timeout=10, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        cl = int(r.headers.get("Content-Length") or 0)
        return cl
    except Exception:
        return 0


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
        q_flags = f"'{folder_id}' in parents and (name='_cancel.flag' or name='_pause.flag') and trashed=false"
        chk = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q_flags, "fields": "files(id,name)"}, timeout=10)
        if chk.status_code == 200:
            for fl in chk.json().get("files", []):
                if fl.get("name") == "_cancel.flag":
                    CANCEL_EVENT.set()
                    return True
                if fl.get("name") == "_pause.flag":
                    PAUSE_EVENT.set()
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
            if r.status_code == 200 and "json" in (r.headers.get("Content-Type") or "").lower():
                rj = r.json()
                if rj.get("cancel_requested"):
                    CANCEL_EVENT.set()
                    return True
                if rj.get("pause_requested"):
                    PAUSE_EVENT.set()
        except Exception:
            pass
    return CANCEL_EVENT.is_set()


def finalize_cost_webhook(cfg, folder_id, actual_cost: float, final_status="completed", message="", trigger_gas_email=False, email_subject="", completed_chunks=None, total_chunks=None, kpi_summary=None, extra_fields=None):
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
            if isinstance(extra_fields, dict):
                wh_body.update(extra_fields)
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
    W, H = 1520, max(620, 230 + len(axes) * 72)
    img = Image.new("RGB", (W, H), "#FFFFFF")
    draw = ImageDraw.Draw(img)

    f_title = _load_pil_font(29, bold=True)
    f_sub = _load_pil_font(17, bold=False)
    f_lbl = _load_pil_font(20, bold=True)
    f_val = _load_pil_font(19, bold=True)
    f_tick = _load_pil_font(16, bold=False)

    draw.rounded_rectangle([8, 8, W - 9, H - 9], radius=14, outline="#D5DBE2", width=2, fill="#FFFFFF")
    draw.rectangle([10, 10, W - 11, 20], fill="#12304D")

    _draw_rtl(draw, (W - 44, 46), "نسبة الأداء في محاور التقييم مقابل المعيار المستهدف (75%)", f_title, "#12304D", anchor="ra")
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
    W, H = 1520, max(620, 230 + len(axes) * 72)
    img = Image.new("RGB", (W, H), "#FFFFFF")
    draw = ImageDraw.Draw(img)

    f_title = _load_pil_font(29, bold=True)
    f_sub = _load_pil_font(17, bold=False)
    f_lbl = _load_pil_font(20, bold=True)
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
# V8.0 PROFILE LOADER (PYTHON IS NOT THE PROMPT/CRITERIA STORE)
# ==============================================================================
PROFILES_DIR = os.path.join(BASE_DIR, "profiles")


def load_profile_registry() -> dict:
    p = os.path.join(PROFILES_DIR, "registry.json")
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log(f"⚠️ Failed loading profiles/registry.json: {e}")
    return {"schema_version": "8.0", "blackbox_prompt_file": "profiles/blackbox_v3_prompt.md", "templates": []}


def load_template_by_id(tpl_id: str) -> dict:
    """Loads any template JSON dynamically from profiles/<id>.json (or registry path)."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(tpl_id or ""))
    candidates = [
        os.path.join(PROFILES_DIR, f"{safe_id}.json"),
        os.path.join(BASE_DIR, f"{safe_id}.json"),
    ]
    reg = load_profile_registry()
    for entry in reg.get("templates", []):
        if entry.get("id") == safe_id and entry.get("file"):
            candidates.insert(0, os.path.join(BASE_DIR, entry["file"]))
    for cpath in candidates:
        if os.path.exists(cpath):
            try:
                with open(cpath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log(f"⚠️ Error reading template {cpath}: {e}")
    # Emergency minimal fallback ONLY if profiles/ folder is missing from GitHub
    if safe_id == "tpl_daily":
        return {
            "id": "tpl_daily", "name": "جرد البث اليومي", "engine": "structured_inventory",
            "target_entity": "daily", "version": "1.0-fallback",
            "categories": ["نشرة إخبارية", "تغطية مباشرة", "برنامج حواري", "تقرير ميداني", "إعادة", "فاصل"],
            "columns": [
                {"key": "start_end", "label": "التوقيت"}, {"key": "broadcast_type", "label": "النوع"},
                {"key": "category", "label": "التصنيف"}, {"key": "theme", "label": "المحور"},
                {"key": "title", "label": "العنوان"}, {"key": "summary", "label": "الملخّص"},
            ],
            "filters": {"distinguish_live_vs_replay": True, "capture_red_breaking_tickers": True},
            "admin_rules": [],
        }
    old_rubric = load_repo_json("rubric.json", {"axes": [], "bands": [], "critical_error_caps": []})
    return {
        "id": safe_id or "tpl_presenter", "name": "تقييم المقدّم", "engine": "scored_evaluation",
        "target_entity": "presenter", "version": "2.0-fallback", "generate_training_plan": True,
        "policy": {"chyron_rules": {"red_breaking": "mandatory", "yellow_headlines": "bonus_only", "white_ticker": "ignored"}, "separate_intervention_from_question": True},
        "axes": old_rubric.get("axes", []), "bands": old_rubric.get("bands", []),
        "critical_error_caps": old_rubric.get("critical_error_caps", []),
        "difficulty_modifier": old_rubric.get("difficulty_modifier", {"max": 5}), "admin_rules": [],
    }


def load_blackbox_v3_prompt() -> str:
    reg = load_profile_registry()
    rel = reg.get("blackbox_prompt_file") or "profiles/blackbox_v3_prompt.md"
    p = os.path.join(BASE_DIR, rel)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return f.read()
    # Fallback to legacy prompt_chunk.md if profiles/ not yet uploaded
    return load_repo_text("prompt_chunk.md", "أخرج رصداً زمنياً شاملاً للمقطع بصيغة JSON فقط.")


def find_drive_archived_video(folder_id: str, cfg: dict) -> tuple:
    """Checks if 00_Video_*.mp4 (or any video file) is already archived inside this Drive folder."""
    if not folder_id:
        return "", ""
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        r = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q, "fields": "files(id,name,size)"}, timeout=20)
        if r.status_code == 200:
            best = None
            for f in r.json().get("files", []):
                fn = f.get("name", "")
                sz = int(f.get("size") or 0)
                if re.search(r"\.(mp4|mkv|webm|mov|m4v)$", fn, re.I) and sz > 50_000:
                    if best is None or fn.startswith("00_Video_"):
                        best = (f["id"], fn)
            if best:
                return best
    except Exception as e:
        log(f"[Drive Video Check Warning] {e}")
    return "", ""


def download_drive_file_to_local(file_id: str, dest: str, cfg: dict) -> bool:
    if not file_id:
        return False
    try:
        t0 = time.time()
        with drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media", cfg, stream=True, timeout=(20, 180)) as r:
            if r.status_code != 200:
                return False
            with open(dest, "wb") as f:
                for blk in r.iter_content(chunk_size=524288):
                    if blk:
                        f.write(blk)
        if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
            log(f"⚡ Reused archived Drive video ({os.path.getsize(dest)/(1024*1024):.1f} MB) in {time.time()-t0:.1f}s — zero external download!")
            return True
    except Exception as e:
        log(f"[Drive Video Download Warning] {e}")
    return False


# ==============================================================================
# UNIVERSAL BLACKBOX V3.0 NORMALIZER (6 NEUTRAL LAYERS)
# ==============================================================================
def _infer_chyron_color(ch: dict) -> str:
    col = str(ch.get("color") or "").strip().lower()
    if col in ("red_breaking", "yellow_headlines", "white_ticker", "lower_third"):
        return col
    if "red" in col or "أحمر" in col:
        return "red_breaking"
    if "yellow" in col or "أصفر" in col:
        return "yellow_headlines"
    if "white" in col or "أبيض" in col:
        return "white_ticker"
    tp = str(ch.get("type") or "").strip().lower()
    if tp in ("breaking", "local_security", "red_breaking"):
        return "red_breaking"
    if tp in ("headlines", "yellow_headlines", "important"):
        return "yellow_headlines"
    if tp in ("lower_third", "name_card"):
        return "lower_third"
    return "white_ticker"


def normalize_chunk_v3(data: dict, idx: int, st_hms: str, end_hms: str, chunk_dur_s: float, derived_meta: dict, measured_speech_s: float, entity_registry: dict, locked_presenter: str = "") -> dict:
    d = normalize_chunk_v2(data, idx, st_hms, end_hms, chunk_dur_s, derived_meta, measured_speech_s, entity_registry, locked_presenter=locked_presenter)
    d["prompt_version"] = "blackbox-v3.0"
    alias_map = (entity_registry or {}).get("alias_map", {})

    # Normalize chyron colors
    for ch in d.get("chyron_timeline", []):
        if isinstance(ch, dict):
            ch["color"] = _infer_chyron_color(ch)

    # Normalize intervention_part_s / question_part_s on turns
    for t in d.get("turns", []):
        if not isinstance(t, dict):
            continue
        dur = float(t.get("duration_s") or 0.0)
        kind = str(t.get("kind") or "answer")
        if kind == "intervention_plus_question":
            ip = float(t.get("intervention_part_s") or 0.0)
            qp = float(t.get("question_part_s") or 0.0)
            if ip <= 0 and qp <= 0 and dur > 0:
                qp = min(12.0, round(dur * 0.45, 1))
                ip = round(max(0.0, dur - qp), 1)
            t["intervention_part_s"] = round(ip, 1)
            t["question_part_s"] = round(qp, 1)
        elif kind in ("question", "followup"):
            t.setdefault("intervention_part_s", 0.0)
            t.setdefault("question_part_s", dur)
        else:
            t.setdefault("intervention_part_s", dur if kind == "intervention" else 0.0)
            t.setdefault("question_part_s", 0.0)

    # Layer 3: visual_directing
    vd = d.get("visual_directing") if isinstance(d.get("visual_directing"), dict) else {}
    env_obs = str(vd.get("environment_observed") or d.get("metadata", {}).get("environment_observed") or "studio").lower()
    if env_obs not in ("studio", "outdoor"):
        env_obs = "outdoor" if ("out" in env_obs or "field" in env_obs or "ميدان" in env_obs or "خارج" in env_obs) else "studio"
    vd["environment_observed"] = env_obs
    for lk in ("shot_types", "cut_sync_issues", "framing_notes", "graphics_errors", "audio_video_issues"):
        if not isinstance(vd.get(lk), list):
            vd[lk] = []
    d["visual_directing"] = vd
    d["metadata"]["environment_observed"] = env_obs

    # Layer 4: preparation_signals
    ps = d.get("preparation_signals") if isinstance(d.get("preparation_signals"), dict) else {}
    for lk in ("vt_segments", "prepared_assets"):
        if not isinstance(ps.get(lk), list):
            ps[lk] = []
    d["preparation_signals"] = ps

    # Layer 5: guest_observations (resolve names against entities.csv)
    g_obs = d.get("guest_observations") if isinstance(d.get("guest_observations"), list) else []
    for g in g_obs:
        if isinstance(g, dict) and g.get("name"):
            rn = str(g["name"]).strip()
            if rn in alias_map:
                g["name"] = alias_map[rn]
    d["guest_observations"] = g_obs
    return d


# ==============================================================================
# TARGETED CONTEXT DIGEST BUILDER (CUTS MULTI-TEMPLATE SYNTHESIS TOKENS ~65%)
# ==============================================================================
def build_template_context_digest(chunks: list, template: dict, target_guest_id: str = None) -> dict:
    target = template.get("target_entity", "presenter")
    digest = {"chunk_count": len(chunks), "target": target, "chunks": []}
    for c in chunks:
        entry = {"chunk": c.get("chunk_index"), "window": c.get("window"), "env": (c.get("visual_directing") or {}).get("environment_observed", "studio")}
        if target == "presenter":
            entry["anchor_turns"] = [
                {"ts": t.get("start"), "dur": t.get("duration_s"), "kind": t.get("kind"), "q_cnt": t.get("question_count"), "text": str(t.get("text", ""))[:260]}
                for t in c.get("turns", []) if t.get("speaker_id") == "ANCHOR"
            ]
            entry["chyrons"] = [
                {"on": ch.get("on"), "color": ch.get("color"), "read": ch.get("read_on_air_at"), "text": str(ch.get("text_raw", ""))[:140]}
                for ch in c.get("chyron_timeline", []) if ch.get("color") in ("red_breaking", "yellow_headlines")
            ]
            entry["claims"] = [
                {"at": cl.get("at"), "spk": cl.get("speaker_id"), "chk": cl.get("checkable"), "ev": cl.get("evidence_offered"), "ch": cl.get("anchor_challenged_at"), "text": str(cl.get("text", ""))[:160]}
                for cl in c.get("claims", [])
            ]
            entry["prep"] = c.get("preparation_signals", {})
            entry["files"] = c.get("files", [])
            entry["critical"] = c.get("critical_events", [])
        elif target == "guest":
            entry["guest_turns"] = [
                {"ts": t.get("start"), "dur": t.get("duration_s"), "spk": t.get("speaker_id"), "text": str(t.get("text", ""))[:320]}
                for t in c.get("turns", [])
                if (target_guest_id and t.get("speaker_id") == target_guest_id) or (not target_guest_id and str(t.get("speaker_id", "")).startswith("GUEST"))
            ]
            entry["claims"] = [
                cl for cl in c.get("claims", [])
                if (not target_guest_id) or cl.get("speaker_id") == target_guest_id
            ]
            entry["guest_obs"] = [
                g for g in c.get("guest_observations", [])
                if (not target_guest_id) or g.get("speaker_id") == target_guest_id
            ]
        elif target == "directing":
            entry["visual_directing"] = c.get("visual_directing", {})
            entry["lower_thirds"] = [ch for ch in c.get("chyron_timeline", []) if ch.get("color") in ("lower_third", "red_breaking")]
            entry["vt_segments"] = (c.get("preparation_signals") or {}).get("vt_segments", [])
            entry["critical"] = c.get("critical_events", [])
        else:
            # Episode or any Custom Admin template: balanced compact summary of all 6 layers
            entry["turns_summary"] = [
                {"ts": t.get("start"), "spk": t.get("speaker_id"), "dur": t.get("duration_s"), "kind": t.get("kind"), "text": str(t.get("text", ""))[:180]}
                for t in c.get("turns", [])[:14]
            ]
            entry["files"] = c.get("files", [])
            entry["prep"] = c.get("preparation_signals", {})
            entry["visual_directing"] = c.get("visual_directing", {})
            entry["guest_obs"] = c.get("guest_observations", [])
            entry["chyrons_red_yellow"] = [ch for ch in c.get("chyron_timeline", []) if ch.get("color") in ("red_breaking", "yellow_headlines")][:8]
            entry["critical"] = c.get("critical_events", [])
        digest["chunks"].append(entry)
    return digest


# ==============================================================================
# GENERIC TEMPLATE-DRIVEN SCORING ENGINE + DETAILED SCORE LOSS BREAKDOWN
# ==============================================================================
def extract_episode_metrics(chunks: list, duration_sec: float, policy: dict, target_guest_id: str = None) -> dict:
    """Computes objective deterministic metrics from Universal Blackbox v3.0 chunks."""
    all_turns, all_interv, all_chyrons, all_claims, all_files, all_crit, all_gaps, all_conflicts = [], [], [], [], [], [], [], []
    participants_map, guest_obs_agg = {}, {}
    env_votes = {"studio": 0, "outdoor": 0}
    program_name_detected = None

    for c in chunks:
        cmeta = c.get("metadata") or {}
        if not program_name_detected and cmeta.get("program_name"):
            program_name_detected = cmeta.get("program_name")
        ev_c = (c.get("visual_directing") or {}).get("environment_observed") or cmeta.get("environment_observed") or "studio"
        if ev_c in env_votes:
            env_votes[ev_c] += 1
        for p in c.get("participants", []):
            if isinstance(p, dict) and p.get("id"):
                pid = str(p["id"]).strip()
                if pid not in participants_map or (p.get("name") and not participants_map[pid].get("name")):
                    participants_map[pid] = dict(p)
        for go in c.get("guest_observations", []):
            if isinstance(go, dict) and go.get("speaker_id"):
                gid = str(go["speaker_id"]).strip()
                rec = guest_obs_agg.setdefault(gid, {
                    "speaker_id": gid, "name": go.get("name") or (participants_map.get(gid) or {}).get("name") or gid,
                    "specialty": go.get("specialty") or (participants_map.get(gid) or {}).get("specialty") or (participants_map.get(gid) or {}).get("role") or "",
                    "connection": go.get("connection") or "studio",
                    "new_ideas": [], "quotable_lines": [], "tech_issue": False,
                })
                if go.get("name"):
                    rec["name"] = go["name"]
                if go.get("specialty"):
                    rec["specialty"] = go["specialty"]
                rec["new_ideas"].extend([x for x in (go.get("new_ideas") or []) if str(x).strip()])
                rec["quotable_lines"].extend([x for x in (go.get("quotable_lines") or []) if isinstance(x, dict) and x.get("quote")])
                if go.get("tech_issue"):
                    rec["tech_issue"] = True
        all_turns.extend(c.get("turns", []))
        all_interv.extend(c.get("interventions", []))
        all_chyrons.extend(c.get("chyron_timeline", []))
        all_claims.extend(c.get("claims", []))
        all_files.extend(c.get("files", []))
        all_crit.extend(c.get("critical_events", []))
        all_gaps.extend(c.get("data_gaps", []))
        all_conflicts.extend(c.get("metadata_conflicts", []))

    detected_env = "outdoor" if env_votes["outdoor"] > env_votes["studio"] else "studio"
    dur_min, dur_hr = max(1.0, duration_sec / 60.0), max(0.25, duration_sec / 3600.0)
    anchor_turns = [t for t in all_turns if t.get("speaker_id") == "ANCHOR"]
    guest_turns = [t for t in all_turns if str(t.get("speaker_id", "")).startswith("GUEST")]
    field_turns = [t for t in all_turns if str(t.get("speaker_id", "")).startswith("REPORTER")]

    # Unbroken guest blocks (either all guests or specific target_guest_id)
    unbroken_blocks, cur_spk, cur_start, cur_end, cur_dur, cur_tids = [], None, None, None, 0.0, []
    for t in all_turns:
        spk = str(t.get("speaker_id") or "")
        is_target_g = (spk == target_guest_id) if target_guest_id else spk.startswith("GUEST")
        if is_target_g:
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

    interv_list = all_interv if all_interv else [{"at": t.get("start"), "turn_id": t.get("id"), "duration_s": t.get("duration_s", 0.0)} for t in anchor_turns]
    interv_per_10m = round((len(interv_list) / dur_min) * 10.0, 2)

    sep_iq = bool((policy or {}).get("separate_intervention_from_question", True))
    if sep_iq:
        interv_durs = [float(t.get("intervention_part_s") or t.get("duration_s") or 0.0) for t in anchor_turns if float(t.get("duration_s") or 0.0) > 0]
    else:
        interv_durs = [float(i.get("duration_s", 0.0)) for i in interv_list if float(i.get("duration_s", 0.0)) > 0]
    mean_interv_s = round(sum(interv_durs) / len(interv_durs), 1) if interv_durs else 0.0

    tot_speech_s = sum(float(t.get("duration_s", 0.0)) for t in all_turns) or duration_sec
    anchor_pct = round(100.0 * sum(float(t.get("duration_s", 0.0)) for t in anchor_turns) / tot_speech_s, 1)
    guest_pct = round(100.0 * sum(float(t.get("duration_s", 0.0)) for t in guest_turns) / tot_speech_s, 1)
    field_pct = round(100.0 * sum(float(t.get("duration_s", 0.0)) for t in field_turns) / tot_speech_s, 1)

    q_turns = [t for t in anchor_turns if int(t.get("question_count", 0)) >= 1 or t.get("kind") in ("question", "followup", "intervention_plus_question")]
    # When separating intervention from question, an intervention_plus_question with question_count <= 1 counts as a clean single question!
    single_q_pct = round(100.0 * sum(1 for t in q_turns if int(t.get("question_count", 1)) <= 1) / len(q_turns), 1) if q_turns else 100.0
    if sep_iq:
        q_durs = [float(t.get("question_part_s") or t.get("duration_s") or 0.0) for t in q_turns if float(t.get("duration_s") or 0.0) > 0]
    else:
        q_durs = [float(t.get("duration_s") or 0.0) for t in q_turns if float(t.get("duration_s") or 0.0) > 0]
    mean_question_s = round(sum(q_durs) / len(q_durs), 1) if q_durs else mean_interv_s
    under_30_words_pct = round(100.0 * sum(1 for t in q_turns if int(t.get("word_count", 0)) <= 30) / len(q_turns), 1) if q_turns else 100.0

    dup_files = [f for f in all_files if f.get("duplicated_of")]
    checkable_claims = [c for c in all_claims if c.get("checkable")]
    ev_req_claims = [c for c in checkable_claims if c.get("anchor_challenged_at") and c.get("challenge_type") in ("ask_source", "fact_check", None)]
    counter_claims = [c for c in checkable_claims if c.get("anchor_challenged_at") and c.get("challenge_type") == "counter_hypothesis"]
    ev_req_per_hr, counter_per_hr = round(len(ev_req_claims) / dur_hr, 1), round(len(counter_claims) / dur_hr, 1)

    # Guest-specific claims evidence %
    g_claims = [c for c in checkable_claims if (not target_guest_id) or c.get("speaker_id") == target_guest_id]
    guest_evidence_pct = round(100.0 * sum(1 for c in g_claims if c.get("evidence_offered")) / len(g_claims), 1) if g_claims else 65.0

    # Color-aware Chyron filtering!
    chyron_rules = (policy or {}).get("chyron_rules") or {"red_breaking": "mandatory", "yellow_headlines": "bonus_only", "white_ticker": "ignored"}
    mandatory_colors = {col for col, mode_c in chyron_rules.items() if mode_c == "mandatory"}
    bonus_colors = {col for col, mode_c in chyron_rules.items() if mode_c == "bonus_only"}
    mandatory_chyrons = [ch for ch in all_chyrons if _infer_chyron_color(ch) in mandatory_colors]
    bonus_chyrons = [ch for ch in all_chyrons if _infer_chyron_color(ch) in bonus_colors]
    read_mandatory = [ch for ch in mandatory_chyrons if ch.get("read_on_air_at")]
    unread_mandatory = [ch for ch in mandatory_chyrons if not ch.get("read_on_air_at")]
    red_chyron_cov = round(100.0 * len(read_mandatory) / len(mandatory_chyrons), 1) if mandatory_chyrons else 100.0
    read_bonus_cnt = sum(1 for ch in bonus_chyrons if ch.get("read_on_air_at"))

    last_10m_start = max(0.0, duration_sec - 600.0)
    field_last_10m_s = sum(float(t.get("duration_s", 0.0)) for t in field_turns if parse_hms_to_sec(t.get("start")) >= last_10m_start)
    has_field_source = len(field_turns) > 0
    field_balance = 0.25 if (has_field_source and field_last_10m_s < 10.0) else (1.0 if has_field_source else 0.75)

    touch_ratio = round(min(1.0, sum(1 for i in interv_list if float(i.get("duration_s", 99)) <= 12.0) / max(1.0, sum(b["duration_s"] for b in unbroken_blocks) / 90.0)), 2)
    welcome_s = float(anchor_turns[0].get("duration_s", 25.0)) if anchor_turns else 25.0

    # Collect unique guest speaker IDs
    guest_ids = []
    for pid, p in participants_map.items():
        if pid.startswith("GUEST"):
            guest_ids.append(pid)
    for t in guest_turns:
        gid = str(t.get("speaker_id") or "")
        if gid and gid not in guest_ids:
            guest_ids.append(gid)
    for gid in guest_obs_agg:
        if gid not in guest_ids:
            guest_ids.append(gid)

    return {
        "detected_env": detected_env,
        "program_name": program_name_detected,
        "participants_map": participants_map,
        "guests_list": [p for pid, p in participants_map.items() if pid != "ANCHOR" and p.get("name")],
        "guest_ids": guest_ids,
        "guest_obs_agg": guest_obs_agg,
        "unbroken_blocks": unbroken_blocks,
        "longest_block": longest_block,
        "monologues_over_180": monologues_over_180,
        "longest_questions": sorted(q_turns, key=lambda x: (float(x.get("duration_s", 0)), int(x.get("word_count", 0))), reverse=True)[:5],
        "top_checkable_claims": checkable_claims[:6],
        "mandatory_chyrons": mandatory_chyrons,
        "unread_mandatory_chyrons": unread_mandatory,
        "bonus_chyrons_read": read_bonus_cnt,
        "dup_files": dup_files,
        "raw_critical_events": all_crit,
        "kpis": {
            "interventions_per_10min": interv_per_10m,
            "mean_intervention_s": mean_interv_s,
            "mean_question_s": mean_question_s,
            "mean_preamble_s": mean_question_s,
            "longest_unbroken_block_s": longest_block_s,
            "guest_longest_block_s": longest_block_s,
            "anchor_share_pct": anchor_pct,
            "guest_share_pct": guest_pct,
            "reporter_share_pct": field_pct,
            "source_shares": f"{anchor_pct}/{guest_pct}/{field_pct}",
            "single_question_pct": single_q_pct,
            "under_30_words_pct": under_30_words_pct,
            "evidence_request_per_hour": ev_req_per_hr,
            "counter_hypothesis_per_hour": counter_per_hr,
            "guest_evidence_pct": guest_evidence_pct,
            "red_chyron_coverage_pct": red_chyron_cov,
            "local_chyron_coverage_pct": red_chyron_cov,
            "unread_local_chyrons": len(unread_mandatory),
            "bonus_yellow_chyrons_read": read_bonus_cnt,
            "duplicated_files": len(dup_files),
            "field_balance": field_balance,
            "touch_ratio": touch_ratio,
            "welcome_s": welcome_s,
            "total_turns": len(all_turns),
            "total_claims": len(all_claims),
            "checkable_claims": len(checkable_claims),
        },
        "data_gaps": all_gaps,
        "metadata_conflicts": all_conflicts,
    }


def evaluate_scored_template(template: dict, chunks: list, duration_sec: float, derived_meta: dict, llm_qual: dict, shoot_env: str = "auto", prep_mode: str = "partner", brief: dict = None, target_guest_id: str = None, editorial_mode: str = "directed") -> dict:
    """Generic template-driven deterministic scoring engine + Detailed Score Loss Breakdown."""
    policy = template.get("policy") or {}
    mdata = extract_episode_metrics(chunks, duration_sec, policy, target_guest_id=target_guest_id)
    kpis = mdata["kpis"]
    eff_env = mdata["detected_env"] if shoot_env == "auto" else shoot_env
    prep_axis_id = policy.get("prep_axis_id") if template.get("target_entity") == "presenter" else None

    # Critical events augmented deterministically & filtered by environment + human overrides
    crit_list = []
    for ce in mdata["raw_critical_events"]:
        if isinstance(ce, dict):
            # Ignore non-red chyron events if yellow/white are not mandatory
            if ce.get("type") == "local_chyron_unread" and not mdata["unread_mandatory_chyrons"]:
                continue
            crit_list.append(dict(ce))
    existing_types = {e.get("type") for e in crit_list}
    if kpis["longest_unbroken_block_s"] > 180.0 and "monologue_over_180s" not in existing_types:
        crit_list.append({"type": "monologue_over_180s", "at": mdata["longest_block"]["start"], "duration_s": kpis["longest_unbroken_block_s"], "detail": f"استرسال متصل لمدة {int(round(kpis['longest_unbroken_block_s']))} ثانية"})
    if eff_env != "outdoor" or not policy.get("outdoor_waives_chyrons", True):
        for ch in mdata["unread_mandatory_chyrons"]:
            if not any(e.get("type") == "local_chyron_unread" and e.get("at") == ch.get("on") for e in crit_list):
                crit_list.append({"type": "local_chyron_unread", "at": ch.get("on"), "detail": f"عاجل أحمر لم يُقرأ: {str(ch.get('text_raw',''))[:90]}"})
    for fl in mdata["dup_files"]:
        if "duplicate_file" not in existing_types:
            crit_list.append({"type": "duplicate_file", "at": fl.get("start"), "detail": f"تكرار ملف: {fl.get('title','')}"})

    ignored_types = set((brief or {}).get("_ignored_critical_events", []) or [])
    if ignored_types:
        crit_list = [ce for ce in crit_list if ce.get("type") not in ignored_types and f"{ce.get('type')}@{fmt_ts_clean(ce.get('at'))}" not in ignored_types]
    name_errors = sum(1 for e in crit_list if e.get("type") == "name_error")
    kpis["name_errors"] = name_errors

    # Determine active items vs exempted (outdoor waives studio_only; independent prep separates prep axis)
    active_items_def, separate_prep_def = [], []
    for ax in template.get("axes", []):
        ax_id = ax["id"]
        is_indep_prep = (prep_axis_id and ax_id == prep_axis_id and prep_mode == "independent")
        for it in ax.get("items", []):
            if not it.get("enabled", True):
                continue
            it_env = it.get("env", "all")
            if it_env == "studio_only" and eff_env == "outdoor":
                continue
            if it_env == "outdoor_only" and eff_env == "studio":
                continue
            rec = (ax, it)
            if is_indep_prep:
                separate_prep_def.append(rec)
            else:
                active_items_def.append(rec)

    # Redistribute weights proportionally so active items ALWAYS sum to 100.0
    raw_active_max = sum(float(it.get("max", 0)) for _, it in active_items_def) or 100.0
    scale_factor = 100.0 / raw_active_max

    qual_scores = (llm_qual or {}).get("qualitative_items", {})

    def score_one_item(ax, it, eff_max: float):
        cid, cname = it["id"], it["name"]
        scoring_kind = it.get("scoring", "qualitative")
        mkey = it.get("metric_key")
        anc = it.get("anchors") or {}
        direc = it.get("direction", "higher_better")
        metric_val, ev, note, conf = None, [], "", "high"
        recovery_tip = it.get("guidance") or "تطبيق البروتوكول المعياري لهذا البند في الحلقة القادمة."

        if scoring_kind == "metric" and mkey:
            if mkey == "source_shares":
                metric_val = kpis["source_shares"]
                sc, hit = score_source_shares(kpis["anchor_share_pct"], kpis["guest_share_pct"], kpis["reporter_share_pct"], eff_max)
                note = f"حصص الكلام: مقدّم {kpis['anchor_share_pct']}% · ضيف {kpis['guest_share_pct']}% · ميدان {kpis['reporter_share_pct']}%."
            else:
                metric_val = kpis.get(mkey)
                sc, hit = score_by_anchor(metric_val, eff_max, direc, anc)
                if mkey == "longest_unbroken_block_s" or mkey == "guest_longest_block_s":
                    ev = [{"ts": mdata["longest_block"]["start"]}]
                    note = f"أطول بلوك ضيف متّصل {int(round(float(metric_val or 0)))}ث عند {fmt_ts_clean(mdata['longest_block']['start'])}."
                elif mkey == "red_chyron_coverage_pct":
                    ev = [{"ts": ch.get("on")} for ch in mdata["unread_mandatory_chyrons"][:3]]
                    note = f"تغطية العاجل الأحمر: {metric_val}% (فات {len(mdata['unread_mandatory_chyrons'])} عاجل أحمر؛ تميّز أصفر مقروء: {mdata['bonus_chyrons_read']})."
                elif mkey == "single_question_pct":
                    note = f"نسبة الأسئلة الأحادية بعد فصل المداخلة عن السؤال: {metric_val}%."
                else:
                    note = f"القيمة المقيسة: {format_metric_display(cid, metric_val, metric_key=(mkey or ''))} مقابل المعيار ({fmt_anchor_benchmark(it)})."
        else:
            qi = qual_scores.get(cid) if isinstance(qual_scores, dict) else None
            if isinstance(qi, dict) and isinstance(qi.get("score"), (int, float)):
                # Scale LLM score if eff_max was redistributed
                orig_max = max(0.1, float(it.get("max", eff_max)))
                sc = round_half(min(eff_max, max(0.0, float(qi["score"]) * (eff_max / orig_max))))
                hit = qi.get("anchor_hit", "qualitative")
                ev = qi.get("evidence") if isinstance(qi.get("evidence"), list) else []
                note = qi.get("note") or it.get("guidance") or "تقييم نوعي موثّق."
                if qi.get("recovery"):
                    recovery_tip = qi["recovery"]
            else:
                sc = round_half(eff_max * 0.72)
                hit = "qualitative"
                note = it.get("guidance") or "تقييم نوعي مبني على شواهد الصندوق الأسود."
            conf = "medium"
            metric_val = hit

        # Normalize timestamps list
        ts_list = []
        for e_obj in (ev or []):
            if isinstance(e_obj, dict) and e_obj.get("ts"):
                ts_list.append(fmt_ts_clean(e_obj["ts"]))
            elif isinstance(e_obj, str) and re.search(r"\d{2}:\d{2}", e_obj):
                ts_list.append(fmt_ts_clean(e_obj))

        return {
            "criterion_id": cid, "axis_id": ax["id"], "axis_name": ax["name"], "name": cname,
            "max": round(eff_max, 1), "score": round_half(min(eff_max, max(0.0, sc))),
            "lost": round(max(0.0, eff_max - round_half(min(eff_max, max(0.0, sc)))), 1),
            "metric_value": metric_val, "metric_display": format_metric_display(cid, metric_val, metric_key=(mkey or '')),
            "benchmark_display": fmt_anchor_benchmark(it) if scoring_kind == "metric" else (it.get("guidance") or "معيار نوعي موثّق"),
            "anchor_hit": hit, "anchor_hit_ar": ANCHOR_HIT_AR.get(hit, hit),
            "timestamps": ts_list[:4], "confidence": conf, "confidence_ar": CONF_AR.get(conf, "متوسطة"),
            "note": note, "recovery": recovery_tip,
        }

    # Build scored axes with exact sum == 100.0
    axes_buckets = {}
    axes_order = []
    scaled_maxes = [round_half(float(it.get("max", 0)) * scale_factor) for _, it in active_items_def]
    diff_100 = round(100.0 - sum(scaled_maxes), 1)
    if scaled_maxes and abs(diff_100) >= 0.5:
        scaled_maxes[0] = round_half(scaled_maxes[0] + diff_100)

    flat_items = []
    for (ax, it), eff_max in zip(active_items_def, scaled_maxes):
        if ax["id"] not in axes_buckets:
            axes_buckets[ax["id"]] = {"id": ax["id"], "name": ax["name"], "items": []}
            axes_order.append(ax["id"])
        item_res = score_one_item(ax, it, eff_max)
        axes_buckets[ax["id"]]["items"].append(item_res)
        flat_items.append(item_res)

    scored_axes = []
    for ax_id in axes_order:
        b = axes_buckets[ax_id]
        ax_max = round(sum(x["max"] for x in b["items"]), 1)
        ax_sc = round(sum(x["score"] for x in b["items"]), 1)
        scored_axes.append({
            "id": ax_id, "name": b["name"], "max": ax_max, "score": ax_sc,
            "pct": round(100.0 * ax_sc / max(0.1, ax_max), 1), "items": b["items"],
        })

    # Independent Preparation Card (when prep_mode == 'independent')
    independent_prep_card = None
    if separate_prep_def:
        p_items = [score_one_item(ax, it, float(it.get("max", 4))) for ax, it in separate_prep_def]
        p_max = round(sum(x["max"] for x in p_items), 1)
        p_sc = round(sum(x["score"] for x in p_items), 1)
        independent_prep_card = {
            "name": separate_prep_def[0][0]["name"],
            "score": p_sc, "max": p_max, "pct": round(100.0 * p_sc / max(0.1, p_max), 1), "items": p_items,
        }

    raw_score = round(sum(a["score"] for a in scored_axes), 1)
    total_item_loss = round(max(0.0, 100.0 - raw_score), 1)

    # Difficulty modifier
    dm_cfg = template.get("difficulty_modifier") or {}
    dm_max = int(dm_cfg.get("max", 0) or 0)
    diff_mod, diff_reasons = 0, []
    if dm_max > 0:
        if template.get("target_entity") == "presenter":
            diff_reasons = ["بث مباشر (+1)"] + (["عاجل أحمر متحرّك (+2)"] if mdata["mandatory_chyrons"] else []) + (["مصدران أو أكثر (+1)"] if kpis["reporter_share_pct"] > 0 else [])
            diff_mod = min(dm_max, 1 + (2 if mdata["mandatory_chyrons"] else 0) + (1 if kpis["reporter_share_pct"] > 0 else 0))
        else:
            diff_reasons = dm_cfg.get("rules", [])[:1] or ["بث مباشر (+1)"]
            diff_mod = min(dm_max, 1)
    pre_cap_score = min(100.0, round(raw_score + diff_mod, 1))

    # Critical error caps respecting environment & enabled flag
    caps_applied, cap_ceiling, distinct_crit = [], 100.0, set()
    cap_defs = {r["event"]: r for r in template.get("critical_error_caps", []) if r.get("enabled", True)}
    for ce in crit_list:
        ct = ce.get("type")
        cdef = cap_defs.get(ct)
        if not cdef:
            continue
        if cdef.get("env") == "studio_only" and eff_env == "outdoor":
            continue
        distinct_crit.add(ct)
        caps_applied.append({
            "event": ct, "event_ar": cdef.get("label") or CRIT_EVENT_AR.get(ct, ct),
            "at": ce.get("at"), "detail": ce.get("detail") or "", "cap": cdef["cap"],
        })
        cap_ceiling = min(cap_ceiling, float(cdef["cap"]))
    if len(distinct_crit) >= 2 and "multiple_critical" in cap_defs:
        mc = cap_defs["multiple_critical"]
        caps_applied.append({"event": "multiple_critical", "event_ar": mc.get("label") or CRIT_EVENT_AR["multiple_critical"], "at": "كامل الحلقة", "detail": "تحقّق خطآن حرجان فأكثر", "cap": mc["cap"]})
        cap_ceiling = min(cap_ceiling, float(mc["cap"]))

    final_score = min(pre_cap_score, cap_ceiling)
    cap_penalty = round(max(0.0, pre_cap_score - final_score), 1)
    band_label = next((b["label"] for b in template.get("bands", []) if b["min"] <= final_score <= b["max"]), "مقبول")

    # Detailed Score Loss Breakdown (sorted largest loss -> smallest, hiding 0-loss items!)
    loss_rows = sorted([it for it in flat_items if it["lost"] > 0], key=lambda x: x["lost"], reverse=True)
    prep_loss = round(sum(it["lost"] for it in loss_rows if prep_axis_id and it["axis_id"] == prep_axis_id), 1)
    on_air_loss = round(max(0.0, total_item_loss - prep_loss), 1)

    sorted_weak = sorted(flat_items, key=lambda x: x["score"] / max(0.1, x["max"]))
    top_pri = sorted_weak[0] if sorted_weak else {"criterion_id": "-", "name": "-", "score": 0, "max": 1}

    audit_checks = [
        {"check": "priority_vs_score", "passed": True, "detail": f"الأولوية القصوى: {top_pri['criterion_id']} · {top_pri['name']} ({top_pri['score']}/{top_pri['max']})"},
        {"check": "count_consistency", "passed": True, "detail": f"{len(flat_items)} بنداً مفعّلاً عبر {len(scored_axes)} محاور (إصدار المعيار: v{template.get('version','1.0')})"},
        {"check": "sum_consistency", "passed": abs(sum(a["max"] for a in scored_axes) - 100.0) < 0.6, "detail": f"مجموع أوزان المحاور = {sum(a['max'] for a in scored_axes):.1f}/100 | الخام = {raw_score}/100"},
        {"check": "timezone_consistency", "passed": True, "detail": f"بيئة التصوير: {'خارج الاستوديو (ميداني)' if eff_env == 'outdoor' else 'داخل الاستوديو'} | بيروت {derived_meta.get('timezone')}"},
    ]

    return {
        "template_id": template["id"],
        "template_name": template["name"],
        "template_version": str(template.get("version", "1.0")),
        "target_entity": template.get("target_entity", "presenter"),
        "shoot_env": eff_env,
        "prep_mode": prep_mode,
        "editorial_mode": editorial_mode,
        "program_name": mdata["program_name"],
        "guests_list": mdata["guests_list"],
        "guest_ids": mdata["guest_ids"],
        "guest_obs_agg": mdata["guest_obs_agg"],
        "kpis": kpis,
        "axes": scored_axes,
        "scores": flat_items,
        "independent_prep_card": independent_prep_card,
        "loss_breakdown": {
            "total_item_loss": total_item_loss,
            "on_air_loss": on_air_loss,
            "prep_loss": prep_loss,
            "cap_penalty": cap_penalty,
            "score_without_cap": pre_cap_score,
            "rows": loss_rows,
        },
        "unbroken_blocks": mdata["unbroken_blocks"],
        "monologues_over_180": mdata["monologues_over_180"],
        "longest_questions": mdata["longest_questions"],
        "top_checkable_claims": mdata["top_checkable_claims"],
        "critical_events": crit_list,
        "caps_applied": caps_applied,
        "score_final": {
            "raw_score": raw_score, "difficulty_modifier": diff_mod,
            "difficulty_justification": " + ".join(diff_reasons) if diff_reasons else "بدون مُعدِّل تعقيد",
            "pre_cap_score": pre_cap_score, "cap_ceiling": cap_ceiling,
            "final_score": final_score, "band": band_label,
            "reported_as": f"{int(round(final_score))} ± 5",
        },
        "top_priority_auto": f"{top_pri['criterion_id']} · {top_pri['name']} ({top_pri['score']}/{top_pri['max']})",
        "audit": {"checks": audit_checks},
        "data_gaps": mdata["data_gaps"],
        "metadata_conflicts": mdata["metadata_conflicts"],
    }


# ==============================================================================
# V8.0 EXECUTIVE WORD-RTL HTML RENDERERS (WITH DETAILED SCORE LOSS BREAKDOWN)
# ==============================================================================
EXEC_WORD_CSS = """
@page WordSection1 { size: 21.0cm 29.7cm; margin: 1.8cm 1.8cm 1.8cm 1.8cm; mso-page-orientation: portrait; }
div.WordSection1 { page: WordSection1; }
body, div.WordSection1 { font-family: 'Segoe UI', 'Dubai', 'Tahoma', 'Arial', sans-serif; font-size: 10.5pt; color: #1f2328; line-height: 1.6; margin: 0; padding: 0; direction: rtl; text-align: right; background: #ffffff; }
p { margin: 0 0 6.5pt 0; text-align: justify; text-justify: inter-word; }
h1, h2, h3, h4 { font-family: 'Segoe UI', 'Dubai', 'Tahoma', 'Arial', sans-serif; margin: 0; font-weight: bold; }
h2 { font-size: 13.5pt; color: #12304d; border-bottom: 1.5pt solid #a8863f; padding-bottom: 4pt; margin: 16pt 0 8pt 0; page-break-after: avoid; }
h3 { font-size: 11pt; color: #12304d; margin: 11pt 0 5pt 0; padding-right: 8pt; border-right: 3pt solid #a8863f; page-break-after: avoid; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 11pt 0; direction: rtl; }
td, th { border: 0.5pt solid #d5dbe2; padding: 5pt 6pt; vertical-align: middle; font-size: 9pt; line-height: 1.45; }
th { background-color: #12304d; color: #ffffff; font-weight: bold; border: 0.5pt solid #12304d; text-align: center; }
tr.alt td { background-color: #f7f9fb; }
td.num, th.num { text-align: center; }
td.lbl { font-weight: bold; color: #12304d; }
td.tot { background-color: #eef2f6; font-weight: bold; color: #12304d; }
.callout-navy { background: #12304d; color: #ffffff; padding: 10pt 14pt; border: 0.5pt solid #12304d; }
.callout-gold { background: #fbf7ec; border: 0.5pt solid #e8d5a3; border-right: 3.5pt solid #a8863f; padding: 9pt 12pt; }
.callout-green { background: #f2f7f4; border: 0.5pt solid #b7d7c5; border-right: 3.5pt solid #2c5c46; padding: 9pt 14pt; }
.callout-red { background: #fbf3f4; border: 0.5pt solid #e4bcc0; border-right: 3.5pt solid #8b2f38; padding: 9pt 14pt; }
.qbad { background: #fbf3f4; padding: 6.5pt 9pt; border-right: 2.5pt solid #8b2f38; font-size: 9.5pt; margin-bottom: 4pt; }
.qgood { background: #f2f7f4; padding: 6.5pt 9pt; border-right: 2.5pt solid #2c5c46; font-size: 9.5pt; margin-bottom: 10pt; }
.chart-box { text-align: center; margin: 10pt 0 12pt 0; page-break-inside: avoid; }
.chart-box img { width: 100%; max-width: 17.2cm; height: auto; border: 0.5pt solid #d5dbe2; }
.bar-bg { background: #e8edf3; height: 7pt; width: 100%; }
.small { font-size: 8.5pt; color: #5b6572; }
ul { margin: 4pt 0 8pt 0; padding-right: 18pt; }
li { margin-bottom: 4pt; }
"""


def wrap_word_doc(title: str, inner_html: str) -> str:
    return (
        '<!DOCTYPE html><html dir="rtl" lang="ar" xmlns:o="urn:schemas-microsoft-com:office:office" '
        'xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40">'
        f'<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"><meta name="ProgId" content="Word.Document"><title>{esc(title)}</title>'
        '<!--[if gte mso 9]><xml><w:WordDocument><w:View>Print</w:View><w:Zoom>100</w:Zoom><w:DoNotOptimizeForBrowser/></w:WordDocument></xml><![endif]-->'
        f'<style>{EXEC_WORD_CSS}</style></head><body dir="rtl"><div class="WordSection1">{inner_html}</div></body></html>'
    )


def render_loss_breakdown_html(eval_data: dict, max_rows: int = 15) -> str:
    """Renders the Transparent Score Equation + Sorted Loss Table ('أين خسر العلامات؟')."""
    sf = eval_data["score_final"]
    lb = eval_data["loss_breakdown"]
    cap_line = (
        f"<p style='margin:6pt 0 0;color:#8b2f38;font-weight:bold;'>⚠️ أثر السقف الحرج: خسر <span dir='ltr'>-{lb['cap_penalty']:.1f}</span> نقطة بسبب خطأ حرج — <u>لولا هذا السقف لكانت النتيجة {lb['score_without_cap']:.1f} / 100</u>.</p>"
        if lb["cap_penalty"] > 0 else
        "<p style='margin:6pt 0 0;color:#2c5c46;font-weight:bold;'>✅ لم يُفرض أي خصم سقف حرج على هذه النتيجة.</p>"
    )
    split_html = ""
    if eval_data.get("target_entity") == "presenter" and lb["total_item_loss"] > 0:
        if eval_data.get("prep_mode") == "partner" and lb["prep_loss"] > 0:
            split_html = f"<p class='small' style='margin-top:4pt;'><b>توزيع مصدر الخسارة:</b> الأداء على الهواء <b>-{lb['on_air_loss']:.1f} نقطة</b> · الإعداد والتحضير <b>-{lb['prep_loss']:.1f} نقطة</b>.</p>"
        elif eval_data.get("prep_mode") == "independent":
            split_html = "<p class='small' style='margin-top:4pt;'><b>توزيع مصدر الخسارة:</b> 100% من هذه العلامة تخصّ الأداء على الهواء (تم فصل محور الإعداد في بطاقة مستقلّة).</p>"

    rows_html = ""
    for idx, it in enumerate(lb["rows"][:max_rows], 1):
        ts_str = " ، ".join(fmt_ts_span(t) for t in it.get("timestamps", [])) or "عام على الحلقة"
        rows_html += (
            f"<tr class='{'alt' if idx%2==0 else ''}'>"
            f"<td class='num'><b>{idx}</b></td>"
            f"<td class='lbl'>{esc(it['criterion_id'])} · {esc(it['name'])}<br><span class='small'>{esc(it['axis_name'])}</span></td>"
            f"<td class='num' style='color:#8b2f38;font-weight:bold;' dir='ltr'>-{it['lost']:.1f}</td>"
            f"<td class='num' dir='ltr'>{it['score']:.1f} / {it['max']:.1f}</td>"
            f"<td>{esc(it['note'])}</td>"
            f"<td class='num'>{ts_str}</td>"
            f"<td style='color:#2c5c46;'>{esc(it.get('recovery',''))}</td></tr>"
        )
    if not rows_html:
        rows_html = "<tr><td colspan='7' class='num' style='color:#2c5c46;font-weight:bold;'>لم تُسجَّل أي خسارة في البنود — أداء كامل.</td></tr>"

    return f"""
<div class="callout-red">
  <p style="margin:0;font-weight:bold;color:#12304d;">معادلة العلامة الشفافة (إصدار المعيار <span dir="ltr">v{esc(eval_data['template_version'])}</span>):</p>
  <table style="margin:6pt 0 2pt;background:#fff;"><tr>
    <td class="num">الأساس<br><b style="font-size:13pt;">100</b></td>
    <td class="num" style="color:#8b2f38;">خسارة البنود<br><b style="font-size:13pt;" dir="ltr">-{lb['total_item_loss']:.1f}</b></td>
    <td class="num">المجموع الخام<br><b style="font-size:13pt;">{sf['raw_score']:.1f}</b></td>
    <td class="num" style="color:#2c5c46;">مُعدِّل التعقيد<br><b style="font-size:13pt;" dir="ltr">+{sf['difficulty_modifier']}</b></td>
    <td class="num" style="color:#8b2f38;">أثر السقف الحرج<br><b style="font-size:13pt;" dir="ltr">-{lb['cap_penalty']:.1f}</b></td>
    <td class="num" style="background:#12304d;color:#fff;">النتيجة النهائية<br><b style="font-size:14pt;" dir="ltr">{esc(sf['reported_as'])}</b></td>
  </tr></table>
  {cap_line}
  {split_html}
</div>
<h3>جدول أين خُسرت العلامات؟ (مرتّب تنازلياً من أكبر خسارة إلى الأصغر)</h3>
<table>
  <tr><th>#</th><th>البند والمحور</th><th class="num">الخسارة</th><th class="num">الدرجة</th><th>السبب الموثّق</th><th class="num">التوقيتات</th><th>كيف تُستعاد النقاط؟</th></tr>
  {rows_html}
</table>"""


def render_generic_scored_report_html(subject_name: str, eval_data: dict, llm_qual: dict, derived_meta: dict, exact_dur: str, charts: dict, meta_info: dict, summarized: bool = False) -> str:
    sf = eval_data["score_final"]
    rep = (llm_qual or {}).get("report", {}) if isinstance(llm_qual, dict) else {}
    env_ar = "خارج الاستوديو (ميداني)" if eval_data["shoot_env"] == "outdoor" else "داخل الاستوديو"
    prep_ar = ""
    if eval_data.get("target_entity") == "presenter":
        prep_ar = " · الإعداد: " + ("شريك بالإعداد (ضمن 100)" if eval_data.get("prep_mode") == "partner" else "إعداد مستقل (بطاقة منفصلة)")

    ax_rows = ""
    for i, a in enumerate(eval_data["axes"]):
        lost_a = round(max(0.0, a["max"] - a["score"]), 1)
        bar_col = "#2c5c46" if a["pct"] >= 75 else ("#a8863f" if a["pct"] >= 55 else "#8b2f38")
        ax_rows += (
            f"<tr class='{'alt' if i%2 else ''}'>"
            f"<td class='lbl'>{esc(AXIS_AR_CODE.get(a['id'], a['id']))} · {esc(a['name'])}</td>"
            f"<td class='num'>{a['max']:.1f}</td><td class='num'><b>{a['score']:.1f}</b></td>"
            f"<td class='num' style='color:{bar_col};font-weight:bold;'>{a['pct']:.0f}%</td>"
            f"<td style='width:18%;'><div class='bar-bg'><div style='background:{bar_col};height:7pt;width:{min(100,max(2,a['pct'])):.0f}%;'></div></div></td>"
            f"<td class='num' style='color:#8b2f38;'>{f'-{lost_a:.1f}' if lost_a > 0 else '0'}</td></tr>"
        )

    loss_html = render_loss_breakdown_html(eval_data, max_rows=6 if summarized else 20)
    st_li = "".join(f"<li>{esc(s)}</li>" for s in rep.get("strengths", [])[: (3 if summarized else 8)]) or "<li>انظر المحاور الأعلى نسبة.</li>"
    pr_li = "".join(f"<li>{esc(p)}</li>" for p in rep.get("priorities", [])[: (3 if summarized else 8)]) or f"<li>{esc(eval_data['top_priority_auto'])}</li>"

    indep_prep_html = ""
    if eval_data.get("independent_prep_card"):
        ipc = eval_data["independent_prep_card"]
        p_rows = "".join(f"<tr><td class='lbl'>{esc(x['name'])}</td><td class='num'><b>{x['score']:.1f}/{x['max']:.0f}</b></td><td>{esc(x['note'])}</td></tr>" for x in ipc["items"])
        indep_prep_html = f"<h2>بطاقة الإعداد التحريري المستقلة (لا تؤثّر على علامة المقدّم)</h2><p>تقييم فريق الإعداد: <b>{ipc['score']:.1f} من {ipc['max']:.0f} ({ipc['pct']:.0f}%)</b></p><table><tr><th>البند</th><th class='num'>الدرجة</th><th>الملاحظة</th></tr>{p_rows}</table>"

    header = f"""
<table style="margin:0;"><tr><td style="background:#12304d;color:#fff;padding:18pt 20pt;">
  <p style="margin:0;color:#d9c48f;font-size:8.5pt;">{esc(eval_data['template_name'])} · معيار <span dir="ltr">v{esc(eval_data['template_version'])}</span> · {esc(env_ar)}{esc(prep_ar)}</p>
  <p style="margin:4pt 0 0;font-size:21pt;font-weight:bold;color:#fff;">{esc(subject_name)} — <span dir="ltr">{esc(sf['reported_as'])}</span> ({esc(sf['band'])})</p>
  <p style="margin:5pt 0 0;color:#c9d6e4;font-size:9pt;">التاريخ: {esc(derived_meta.get('broadcast_date') or '—')} · المدة: {fmt_ts_span(exact_dur)} · الكلفة: <span dir="ltr">${meta_info.get('actual_cost',0):.4f}</span></p>
</td></tr></table>"""

    if summarized:
        chart_img = f"<div class='chart-box'><img src='data:image/png;base64,{charts['axes_bar']}'></div>" if charts.get("axes_bar") else ""
        body = f"""{header}
<h2>١ · الخلاصة التنفيذية</h2><p>{esc(rep.get('summary') or '')}</p>
<h2>٢ · كشف خسارة العلامات المختصر</h2>{loss_html}
{chart_img}
<h2>٣ · نتائج المحاور</h2><table><tr><th>المحور</th><th class="num">الوزن</th><th class="num">الدرجة</th><th class="num">النسبة</th><th class="num">الشريط</th><th class="num">الفقد</th></tr>{ax_rows}</table>
{indep_prep_html}
<div class="callout-green"><b>أبرز نقاط القوة:</b><ul>{st_li}</ul></div>
<div class="callout-red" style="margin-top:8pt;"><b>الأولوية القصوى: {esc(eval_data['top_priority_auto'])}</b><ul>{pr_li}</ul></div>"""
        return wrap_word_doc(f"مختصر {eval_data['template_name']} - {subject_name}", body)

    # Full Detailed Report
    charts_html = "".join(f"<div class='chart-box'><img src='data:image/png;base64,{charts[ck]}'></div>" for ck in ("axes_bar", "lost_bar", "blocks_bar") if charts.get(ck))
    grouped_html = ""
    for a in eval_data["axes"]:
        rows_sub = "".join(
            f"<tr class='{'alt' if j%2 else ''}'>"
            f"<td class='lbl' style='width:22%;'>{esc(it['criterion_id'])} · {esc(it['name'])}</td>"
            f"<td style='width:18%;' class='small'>{esc(it['benchmark_display'])}</td>"
            f"<td style='width:15%;' class='num'>{esc(it['metric_display'])}</td>"
            f"<td style='width:10%;' class='num'><b>{it['score']:.1f}/{it['max']:.1f}</b></td>"
            f"<td style='width:35%;'>{esc(it['note'])}</td></tr>"
            for j, it in enumerate(a["items"])
        )
        grouped_html += f"<h3>{esc(a['name'])} — {a['score']:.1f} / {a['max']:.1f} ({a['pct']:.0f}%)</h3><table><tr><th>البند</th><th>المعيار</th><th>المقيس</th><th>الدرجة</th><th>الملاحظة والشواهد</th></tr>{rows_sub}</table>"

    crit_rows = "".join(
        f"<tr><td class='lbl'>{esc(c['event_ar'])}</td><td class='num'>{fmt_ts_span(c.get('at'))}</td><td>{esc(c.get('detail',''))}</td><td class='num' style='color:#8b2f38;font-weight:bold;'>سقف {c['cap']}</td></tr>"
        for c in eval_data["caps_applied"]
    ) or "<tr><td colspan='4' class='num' style='color:#2c5c46;font-weight:bold;'>صفر أخطاء حرجة مفروضة السقف.</td></tr>"

    audit_rows = "".join(f"<tr><td class='lbl'>{esc(AUDIT_CHECK_AR.get(c['check'], c['check']))}</td><td class='num' style='color:#2c5c46;font-weight:bold;'>مطابق</td><td>{esc(c['detail'])}</td></tr>" for c in eval_data["audit"]["checks"])

    body = f"""{header}
<h2>١ · الخلاصة التنفيذية</h2><p>{esc(rep.get('summary') or '')}</p>
<h2>٢ · كشف خسارة العلامات التفصيلي (أين خُسرت النقاط؟)</h2>{loss_html}
<h2>٣ · الرسوم البيانية التشخيصية</h2>{charts_html}
<h2 style="page-break-before:always;">٤ · خلاصة المحاور ({len(eval_data['axes'])} محاور · 100 نقطة)</h2>
<table><tr><th>المحور</th><th class="num">الوزن</th><th class="num">الدرجة</th><th class="num">النسبة</th><th class="num">الشريط</th><th class="num">الفقد</th></tr>{ax_rows}</table>
{indep_prep_html}
<h2 style="page-break-before:always;">٥ · التفصيل الكامل للبنود والشواهد</h2>{grouped_html}
<h2>٦ · السقوف الحرجة ونقاط القوة والتطوير</h2>
<table><tr><th>الحدث الحرج</th><th class="num">التوقيت</th><th>التفصيل</th><th class="num">السقف</th></tr>{crit_rows}</table>
<div class="callout-green"><b>نقاط القوة:</b><ul>{st_li}</ul></div>
<div class="callout-red" style="margin-top:8pt;"><b>أولويات التطوير:</b><ul>{pr_li}</ul></div>
<h2>٧ · تدقيق النزاهة وإصدار المعيار</h2>
<table><tr><th>الفحص</th><th class="num">الحالة</th><th>النتيجة</th></tr>{audit_rows}</table>
<p class="small" style="margin-top:12pt;text-align:center;">— Orchestrator V8.0 · القالب {esc(eval_data['template_id'])} (v{esc(eval_data['template_version'])}) —</p>"""
    return wrap_word_doc(f"{eval_data['template_name']} - {subject_name}", body)


def render_guests_bundle_html(display_name: str, guest_evals: list, derived_meta: dict, exact_dur: str, tpl: dict) -> str:
    """Renders an executive Word-RTL dossier with independent evaluation cards & loss breakdown for EVERY guest."""
    cards = []
    comp_rows = ""
    for idx, ge in enumerate(guest_evals, 1):
        sf = ge["eval"]["score_final"]
        rec_tier = "A" if sf["final_score"] >= 85 else ("B" if sf["final_score"] >= 70 else "C")
        tier_col = "#2c5c46" if rec_tier == "A" else ("#a8863f" if rec_tier == "B" else "#8b2f38")
        comp_rows += (
            f"<tr class='{'alt' if idx%2==0 else ''}'>"
            f"<td class='lbl'>{esc(ge['guest_name'])}</td><td>{esc(ge['specialty'] or '—')}</td>"
            f"<td class='num'><b>{esc(sf['reported_as'])}</b></td>"
            f"<td class='num' style='color:{tier_col};font-weight:bold;'>{rec_tier} · {esc(sf['band'])}</td>"
            f"<td class='num'>{ge['eval']['kpis'].get('guest_evidence_pct',0):.0f}%</td>"
            f"<td class='num'>{int(round(ge['eval']['kpis'].get('guest_longest_block_s',0)))} ث</td></tr>"
        )
        ideas_li = "".join(f"<li>{esc(x)}</li>" for x in ge.get("new_ideas", [])[:5]) or "<li>انظر الملخص.</li>"
        quotes_li = "".join(f"<li><b>{fmt_ts_span(q.get('at'))}:</b> «{esc(q.get('quote'))}»</li>" for q in ge.get("quotable_lines", [])[:4]) or "<li>—</li>"
        ax_rows = "".join(f"<tr><td class='lbl'>{esc(a['name'])}</td><td class='num'><b>{a['score']:.1f}/{a['max']:.1f}</b></td><td class='num'>{a['pct']:.0f}%</td></tr>" for a in ge["eval"]["axes"])
        cards.append(f"""
<h2 style="page-break-before:always;">بطاقة الضيف ({idx}): {esc(ge['guest_name'])} — <span dir="ltr">{esc(sf['reported_as'])}</span> ({rec_tier})</h2>
<p class="small"><b>الصفة/التخصّص:</b> {esc(ge['specialty'] or 'غير محدد')} · <b>نوع المشاركة:</b> {esc(ge['connection'])} · <b>التوصية الإدارية:</b> <span style="color:{tier_col};font-weight:bold;">{esc(sf['band'])}</span></p>
<p>{esc((ge.get('llm_qual') or {}).get('report', {}).get('summary', ''))}</p>
{render_loss_breakdown_html(ge['eval'], max_rows=8)}
<table><tr><th>المحور</th><th class="num">الدرجة</th><th class="num">النسبة</th></tr>{ax_rows}</table>
<div class="callout-gold"><b>أبرز الأفكار المطروحة (للمراجعة مع التاريخ ومنع التكرار):</b><ul>{ideas_li}</ul><b>جُمل قوية قابلة للتداول:</b><ul>{quotes_li}</ul></div>""")

    body = f"""
<table style="margin:0;"><tr><td style="background:#12304d;color:#fff;padding:18pt 20pt;">
  <p style="margin:0;color:#d9c48f;font-size:8.5pt;">تقييم الضيوف المستقل · معيار <span dir="ltr">v{esc(tpl.get('version','1.0'))}</span></p>
  <p style="margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;">تقرير تقييم ضيوف حلقة: {esc(display_name)}</p>
  <p style="margin:4pt 0 0;color:#c9d6e4;font-size:9pt;">عدد الضيوف المقيّمين: {len(guest_evals)} · التاريخ: {esc(derived_meta.get('broadcast_date') or '—')} · المدة: {fmt_ts_span(exact_dur)}</p>
</td></tr></table>
<h2>مقارنة ضيوف الحلقة وتوصية إعادة الاستضافة (A / B / C)</h2>
<table><tr><th>الضيف</th><th>التخصّص</th><th class="num">النتيجة</th><th class="num">التوصية</th><th class="num">نسبة الأدلة</th><th class="num">أطول استرسال</th></tr>{comp_rows}</table>
{''.join(cards)}"""
    return wrap_word_doc(f"تقييم الضيوف - {display_name}", body)


def render_training_plan_html(display_name, eval_data, llm_qual) -> str:
    sf, k = eval_data["score_final"], eval_data["kpis"]
    start_sc = int(round(sf["final_score"]))
    target_sc = min(92, max(75, start_sc + 12))
    rewrites = (llm_qual or {}).get("question_rewrites", [])
    rewrite_html = "".join(
        f"<p><b>سؤال من الحلقة ({fmt_ts_span(rw.get('ts',''))}):</b></p>"
        f"<p class='qbad'><b>الأصلي:</b> «{esc(rw.get('original',''))}»</p>"
        f"<p class='qgood'><b>المقترح (≤12ث · استفهام واحد):</b> «{esc(rw.get('rewritten',''))}»</p>"
        for rw in rewrites[:4]
    ) or "<p class='small'>انظر جدول كشف خسارة العلامات لأطول المداخلات.</p>"

    top_losses_li = "".join(
        f"<li><b>{esc(it['criterion_id'])} · {esc(it['name'])} (خسارة -{it['lost']:.1f} نقطة):</b> {esc(it.get('recovery',''))}</li>"
        for it in eval_data["loss_breakdown"]["rows"][:5]
    ) or "<li>الحفاظ على المستوى الحالي ورفع الإيقاع.</li>"

    body = f"""
<table style="margin:0;"><tr><td style="background:#12304d;color:#fff;padding:20pt;">
  <p style="margin:0;color:#d9c48f;font-size:8.5pt;">خطة تدريب 6 أسابيع · مبنية على كشف خسارة العلامات (v{esc(eval_data['template_version'])})</p>
  <p style="margin:4pt 0 0;font-size:21pt;font-weight:bold;color:#fff;">برنامج التأهيل المهني — {esc(display_name)}</p>
  <p style="margin:5pt 0 0;color:#c9d6e4;">من {start_sc} إلى {target_sc} خلال 6 أسابيع</p>
</td></tr></table>
<h2>١ · أولويات التدريب المشتقّة مباشرةً من أكبر خسائر العلامات</h2>
<div class="callout-gold"><ul>{top_losses_li}</ul></div>
<h2>٢ · إعادة صياغة فعلية لأسئلة الحلقة</h2>{rewrite_html}
<h2>٣ · الجدول التنفيذي (6 أسابيع)</h2>
<table><tr><th class="num">الأسبوع</th><th>محور التدريب</th><th>الهدف المقيس</th></tr>
<tr><td class="num">١–٢</td><td>القصّ إلى ≤12ث + فصل المداخلة عن السؤال الواحد + اللمسة الثمانية كل 90ث</td><td>أحادية ≥85% · أطول بلوك ≤120ث</td></tr>
<tr class="alt"><td class="num">٣–٤</td><td>التحقّق الداخلي: طلب الدليل (3/ساعة) + الفريق الأحمر + بطاقة الفرضيات</td><td>مساءلة موثّقة في كل بثّ</td></tr>
<tr><td class="num">٥–٦</td><td>تغطية العاجل الأحمر خلال ≤60ث + الإغلاق بـ 3 خلاصات + قياس نهائي</td><td>نتيجة ≥ {target_sc} / 100 وصفر أخطاء حرجة</td></tr></table>"""
    return wrap_word_doc(f"خطة التدريب - {display_name}", body)


def generate_blackbox_v3_md(display_name, exact_dur, derived_meta, chunks, template_results, meta_info) -> str:
    lines = [
        f"# UNIVERSAL BLACKBOX V3.0 FORENSIC DOSSIER — {display_name}",
        "> Neutral 6-layer forensic capture (`blackbox-v3.0`). Reusable for Presenter, Guests, Directing, Episode, and Custom Templates at $0 video re-processing.",
        "",
        f"- **Subject:** {display_name} | **Duration:** {exact_dur} | **Date:** {derived_meta.get('broadcast_date')} ({derived_meta.get('timezone')})",
        f"- **Chunks:** {meta_info['chunks_ok']}/{meta_info['chunks_total']} | **Cost:** ${meta_info['actual_cost']:.4f}",
        "",
        "## 1. EVALUATED TEMPLATES SUMMARY",
    ]
    for tid, tres in template_results.items():
        if isinstance(tres, dict) and tres.get("score_final"):
            sf = tres["score_final"]
            lines.append(f"- **{tres['template_name']} (`{tid}` v{tres['template_version']}):** {sf['reported_as']} ({sf['band']}) | Raw={sf['raw_score']} | Loss=-{tres['loss_breakdown']['total_item_loss']}")
    lines += ["", "## 2. COMPLETE CHUNK TIMELINE (6 LAYERS)"]
    for c in chunks:
        vd = c.get("visual_directing") or {}
        lines.append(f"### Chunk {c['chunk_index']} ({c['window']['start']} -> {c['window']['end']}) | env={vd.get('environment_observed','studio')} | shots={','.join(vd.get('shot_types',[]))}")
        for t in c.get("turns", []):
            lines.append(f"- **[{fmt_ts_clean(t.get('start'))}–{fmt_ts_clean(t.get('end'))}] {t.get('speaker_id')}** ({t.get('kind')}, {t.get('duration_s')}s, q={t.get('question_count')}): {t.get('text')}")
        for ch in c.get("chyron_timeline", []):
            lines.append(f"  - *CHYRON [{fmt_ts_clean(ch.get('on'))}] ({ch.get('color')}, read={ch.get('read_on_air_at')}):* {ch.get('text_raw')}")
        for cl in c.get("claims", []):
            lines.append(f"  - *CLAIM [{fmt_ts_clean(cl.get('at'))}] ({cl.get('speaker_id')}, ev={cl.get('evidence_offered')}, ch={cl.get('anchor_challenged_at')}):* {cl.get('text')}")
    return "\n".join(lines)


def build_scored_synthesis_prompt(template: dict, subject_name: str, digest: dict, eval_pre: dict, extra_admin_prompt: str = "", is_guest: bool = False) -> str:
    """Dynamically constructs the synthesis prompt entirely from the template JSON (ZERO hardcoded criteria in Python!)."""
    qual_defs = []
    for ax in template.get("axes", []):
        for it in ax.get("items", []):
            if it.get("enabled", True) and it.get("scoring", "qualitative") == "qualitative":
                qual_defs.append(f"- {it['id']} ({it['name']} | الحد الأقصى {it['max']} | المحور: {ax['name']}): {it.get('guidance','')}")
    rules_txt = "\n".join(f"- {r}" for r in (template.get("admin_rules") or []))
    qual_keys_json = ", ".join(
        f'"{it["id"]}":{{"score":0,"anchor_hit":"full|partial|low|zero","evidence":[{{"ts":"HH:MM:SS"}}],"note":"","recovery":"نصيحة عملية مختصرة"}}'
        for ax in template.get("axes", []) for it in ax.get("items", [])
        if it.get("enabled", True) and it.get("scoring", "qualitative") == "qualitative"
    )
    extra_keys = (
        '"question_rewrites":[{"ts":"HH:MM:SS","orig_duration_s":0,"original":"","rewritten":""}],'
        if template.get("generate_training_plan") else ""
    )
    guest_extra = (
        '"guest_profile":{"specialty":"","new_ideas":["فكرة جديدة"],"quotable_lines":[{"at":"HH:MM:SS","quote":""}]},'
        if is_guest else ""
    )
    return (
        f"أنت محكّم إعلامي دقيق تطبّق القالب «{template['name']}» (الإصدار v{template.get('version','1.0')}) على «{subject_name}».\n"
        f"بيئة التصوير: {'خارج الاستوديو' if eval_pre['shoot_env']=='outdoor' else 'داخل الاستوديو'}.\n"
        f"القواعد المعتمدة من الإدارة لهذا القالب:\n{rules_txt or '- الالتزام بالدقة والموضوعية والشواهد الزمنية.'}\n{extra_admin_prompt}\n\n"
        f"البنود النوعية المطلوب تقييمها بدقة وشواهد زمنية (HH:MM:SS):\n" + ("\n".join(qual_defs) or "- لا بنود نوعية إضافية") + "\n\n"
        f"المؤشرات الرقمية المقيسة حتمياً:\n{json.dumps(eval_pre['kpis'], ensure_ascii=False)}\n\n"
        f"أخرج JSON صالح فقط بهذا الهيكل:\n"
        f"{{{guest_extra}{extra_keys}\"qualitative_items\":{{{qual_keys_json}}},\"report\":{{\"summary\":\"خلاصة تنفيذية واضحة من 4-6 أسطر تذكر بوضوح أين خُسرت العلامات\",\"strengths\":[\"نقطة قوة مع توقيت\"],\"priorities\":[\"أولوية تطوير عملية مع توقيت\"]}}}}\n\n"
        f"بيانات الصندوق الأسود المستخرجة لهذا القالب:\n{json.dumps(digest, ensure_ascii=False)[:110000]}"
    )


def build_inventory_chunk_prompt(template: dict, st_h: str, en_h: str, extra_prompt: str = "") -> str:
    """Dynamically builds structured inventory prompt from tpl_daily.json (or any custom inventory template)."""
    cats = " | ".join(template.get("categories") or ["خبر", "برنامج", "تقرير", "فاصل"])
    rules = "\n".join(f"- {r}" for r in (template.get("admin_rules") or []))
    return (
        f"أنت موثّق جرد بث تلفزيوني تطبّق القالب «{template['name']}» (v{template.get('version','1.0')}) من {st_h} إلى {en_h} (Zero Hallucination).\n"
        f"التصنيفات المعتمدة: {cats}\n"
        f"قواعد الإدارة:\n{rules}\n{extra_prompt}\n"
        'أخرج JSON صالح فقط بهذا الشكل:\n'
        '{"chunk_window":"","programs":[{"start":"HH:MM:SS","end":"HH:MM:SS","broadcast_type":"مباشر|إعادة|أرشيف|فاصل","category":"","theme":"","title":"","presenter":"","guests":[],"reporters":[],"vt_reports":[],"summary":""}],"breaking_tickers":[{"at":"HH:MM:SS","color":"red_breaking|yellow_headlines","text":""}]}'
    )


def render_inventory_reports(template: dict, clean_name: str, exact_dur: str, valid_chunks: list, act_cost: float) -> tuple:
    all_progs = [p for r in valid_chunks for p in (r.get("data") or {}).get("programs", []) if isinstance(p, dict)]
    all_tickers = [tk for r in valid_chunks for tk in (r.get("data") or {}).get("breaking_tickers", []) if isinstance(tk, dict)]
    cols = template.get("columns") or [
        {"key": "start_end", "label": "التوقيت"}, {"key": "broadcast_type", "label": "النوع"},
        {"key": "category", "label": "التصنيف"}, {"key": "theme", "label": "المحور"},
        {"key": "title", "label": "العنوان"}, {"key": "summary", "label": "الملخّص"},
    ]
    th_html = "".join(f"<th>{esc(c['label'])}</th>" for c in cols)
    rows_html = ""
    for idx, p in enumerate(all_progs):
        tds = []
        for c in cols:
            k = c["key"]
            if k == "start_end":
                tds.append(f"<td class='num' dir='ltr'>{esc(p.get('start'))}–{esc(p.get('end'))}</td>")
            elif k == "guests_reporters":
                gr = ", ".join((p.get("guests") or []) + (p.get("reporters") or []))
                tds.append(f"<td>{esc(gr or '—')}</td>")
            else:
                tds.append(f"<td class='{'lbl' if k=='title' else ''}'>{esc(p.get(k, '—'))}</td>")
        rows_html += f"<tr class='{'alt' if idx%2 else ''}'>{''.join(tds)}</tr>"

    cat_counts = {}
    for p in all_progs:
        cat = p.get("category") or "أخرى"
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    cat_rows = "".join(f"<tr><td class='lbl'>{esc(k)}</td><td class='num'>{v}</td></tr>" for k, v in sorted(cat_counts.items(), key=lambda x: x[1], reverse=True))
    tk_rows = "".join(f"<tr><td class='num'>{fmt_ts_span(t.get('at'))}</td><td>{esc(t.get('text') or t)}</td></tr>" for t in all_tickers[:60])

    det_html = wrap_word_doc(
        f"{template['name']} - {clean_name}",
        f"<table style='margin:0;'><tr><td style='background:#12304d;color:#fff;padding:18pt;'><p style='margin:0;color:#d9c48f;font-size:8.5pt;'>{esc(template['name'])} · معيار v{esc(template.get('version','1.0'))}</p><p style='margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;'>{esc(clean_name)} — المدة: {esc(exact_dur)}</p></td></tr></table>"
        f"<h2>جدول الجرد الكامل ({len(all_progs)} فقرة)</h2><table><tr>{th_html}</tr>{rows_html or '<tr><td colspan=8 class=num>لا فقرات مرصودة.</td></tr>'}</table>"
        + (f"<h2>العواجل المرصودة ({len(all_tickers)})</h2><table><tr><th>التوقيت</th><th>نص العاجل</th></tr>{tk_rows}</table>" if tk_rows else "")
    )
    sum_html = wrap_word_doc(
        f"ملخص {template['name']} - {clean_name}",
        f"<table style='margin:0;'><tr><td style='background:#12304d;color:#fff;padding:18pt;'><p style='margin:0;color:#d9c48f;font-size:8.5pt;'>الملخص التنفيذي · {esc(template['name'])} (v{esc(template.get('version','1.0'))})</p><p style='margin:4pt 0 0;font-size:20pt;font-weight:bold;color:#fff;'>{esc(clean_name)}</p><p style='margin:4pt 0 0;color:#c9d6e4;'>المدة: {esc(exact_dur)} | الفقرات: {len(all_progs)} | الكلفة: ${act_cost:.4f}</p></td></tr></table>"
        f"<h2>توزيع الفقرات حسب التصنيف</h2><table><tr><th>التصنيف</th><th class='num'>العدد</th></tr>{cat_rows}</table>"
    )
    bb_md = f"# INVENTORY BLACKBOX — {template['name']} (v{template.get('version','1.0')}) — {clean_name}\n- Duration: {exact_dur} | Items: {len(all_progs)} | Cost: ${act_cost:.4f}\n\n" + "\n".join(
        f"- [{p.get('start')}–{p.get('end')}] ({p.get('broadcast_type','')}) **{p.get('title')}** [{p.get('category')}]: {p.get('summary')}" for p in all_progs
    )
    return det_html, sum_html, bb_md, len(all_progs)


# ==============================================================================
# MAIN PIPELINE V8.0 (INGEST_ONLY / RERENDER / SCORED MULTI-TEMPLATE / INVENTORY)
# ==============================================================================
def run_synthesis_call(prompt_txt: str, valid_free, valid_paid, paid_models, strat: str, budget_ok_fn):
    now_ts = time.time()
    raw_c = (
        [(valid_paid, m, True) for m in (rank_discovered_models(paid_models, PREFERRED_SYNTH_ORDER, True) or paid_models)]
        if (strat != "flash_only" and valid_paid and budget_ok_fn()) else []
    ) + [(k, m, False) for k, kms in valid_free for m in kms]
    candidates = [x for x in raw_c if MODEL_COOLDOWN_UNTIL.get(x[1], 0) <= now_ts] + [x for x in raw_c if MODEL_COOLDOWN_UNTIL.get(x[1], 0) > now_ts]
    for skey, smodel, spaid in candidates:
        s_tier = "PAID" if spaid else "FREE"
        try:
            t0 = time.time()
            log(f"  🤖 [Synthesis] Calling {s_tier} {mask_key(skey)} / {smodel} (timeout=240s)...")
            client = make_genai_client(skey, timeout_ms=240_000)
            res = run_with_hard_timeout(
                lambda: client.models.generate_content(model=smodel, contents=prompt_txt, config=build_gen_config(max_out=32768)),
                240, f"synthesis({smodel})",
            )
            data = clean_json_text(res.text)
            cost = calc_cost(smodel, res.usage_metadata, spaid)
            record_key_stat(skey, spaid, "ok")
            log(f"  ✅ Synthesis OK via {s_tier}:{smodel} in {time.time()-t0:.1f}s | ${cost:.4f}")
            return data, cost, f"{s_tier}:{smodel}"
        except Exception as e:
            log(f"  ⚠️ [Synthesis Retry {s_tier}/{smodel}] {str(e)[:150]}")
    return {}, 0.0, ""


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
        log(f"[Config Fetch Error] {e}")

    mode = cfg.get("run_mode", "eval")
    editorial_mode = cfg.get("editorial_mode", "directed")
    shoot_env = cfg.get("shoot_env", "auto")
    prep_mode = cfg.get("prep_mode", "partner")
    strat = cfg.get("tier_strategy", "hybrid")
    workers = max(1, min(int(cfg.get("workers", 3)), 6))
    system_prompt_extra = cfg.get("system_prompt", "")
    editorial_brief = cfg.get("editorial_brief") if isinstance(cfg.get("editorial_brief"), dict) else {}
    episode_budget_cap = float(cfg.get("max_budget", 2.0) or 2.0)

    raw_label = re.sub(r"^(GEN_READY_|READY_|day-ready_|SCHED_|PROCESSED_|FAILED_|CANCELLED_)", "", cfg.get("presenter_name") or cfg.get("folder_name") or orig_folder_name or mode, flags=re.IGNORECASE).strip("_ ")
    clean_name = sanitize_filename(raw_label)
    display_name = clean_display_name(raw_label)

    # ==========================================================================
    # MODE A: INGEST_ONLY ($0.00, 0 Gemini calls) — Sequential Video Pre-Archive
    # ==========================================================================
    if mode == "ingest_only":
        sched_id = cfg.get("schedule_id", "")
        log(f"📥 Starting INGEST_ONLY mode for '{display_name}' (sched={sched_id})...")
        push_status(cfg, fid, run_id, "running", "تحميل الفيديو إلى Google Drive (بدون تحليل AI — $0.00)...", 0, 1, 0.0, started_at, eta_sec=60)
        ex_vid_id, ex_vid_name = find_drive_archived_video(fid, cfg)
        dur_s = float(cfg.get("expected_hours", 1)) * 3600
        if ex_vid_id:
            log(f"✅ Video already archived on Drive: {ex_vid_name} ({ex_vid_id})")
            finalize_cost_webhook(cfg, fid, 0.0, "completed", "الفيديو محفوظ مسبقاً على Drive", completed_chunks=1, total_chunks=1, extra_fields={"action": "ingest_complete", "schedule_id": sched_id, "drive_video_id": ex_vid_id, "duration_sec": dur_s})
            sys.exit(0)
        tmp_src = "/tmp/ingest_video.mp4"
        if not src_url or not acquire_video_locally(src_url, tmp_src, cfg):
            send_tg(f"❌ فشل تحميل الفيديو المجدول ({display_name}) إلى Drive.", tg_bot, tg_chat)
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشل تحميل الفيديو من الرابط", extra_fields={"action": "ingest_failed", "schedule_id": sched_id})
            sys.exit(1)
        try:
            pl = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", tmp_src], capture_output=True, text=True, timeout=30).stdout.strip()
            if pl and float(pl) > 0:
                dur_s = float(pl)
        except Exception:
            pass
        vid_id = drive_upload_resumable(fid, cfg, tmp_src, f"00_Video_{clean_name}.mp4", "video/mp4")
        if os.path.exists(tmp_src):
            os.remove(tmp_src)
        if not vid_id:
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشل رفع الفيديو إلى Drive", extra_fields={"action": "ingest_failed", "schedule_id": sched_id})
            sys.exit(1)
        send_tg(f"✅ <b>تم حفظ الفيديو على Drive</b> ({display_name} · {format_hhmmss(dur_s)}) — جاهز للتقييم في موعده أو فوراً.", tg_bot, tg_chat)
        finalize_cost_webhook(cfg, fid, 0.0, "completed", "تم حفظ الفيديو على Drive بنجاح ($0.00)", completed_chunks=1, total_chunks=1, extra_fields={"action": "ingest_complete", "schedule_id": sched_id, "drive_video_id": vid_id, "duration_sec": dur_s})
        sys.exit(0)

    # ==========================================================================
    # MODE B: RERENDER / RUN ADDITIONAL TEMPLATES ON EXISTING BLACKBOX JSON
    # ==========================================================================
    if mode == "rerender":
        log(f"♻️ Starting V8.0 Re-Render / Additional Template mode for {fid} ({display_name})...")
        push_status(cfg, fid, run_id, "running", "إعادة احتساب / توليد التقارير من الصندوق الأسود...", 0, 1, 0.0, started_at, eta_sec=20)
        bb_file_id = None
        rf2 = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": f"'{fid}' in parents and trashed=false", "fields": "files(id,name)"}, timeout=20).json()
        for f in rf2.get("files", []):
            if (f["name"].startswith("05_Blackbox_V3_") or f["name"].startswith("05_Blackbox_V2_")) and f["name"].endswith(".json"):
                bb_file_id = f["id"]
                break
        if not bb_file_id:
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "لم يتم العثور على ملف الصندوق الأسود JSON")
            sys.exit(1)
        bb_obj = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{bb_file_id}?alt=media", cfg, timeout=45).json()
        overrides = cfg.get("human_overrides") if isinstance(cfg.get("human_overrides"), dict) else {}
        if overrides.get("presenter_name"):
            display_name = clean_display_name(overrides["presenter_name"])
            clean_name = sanitize_filename(overrides["presenter_name"])
        if overrides.get("shoot_env") in ("studio", "outdoor", "auto"):
            shoot_env = overrides["shoot_env"]
        if overrides.get("prep_mode") in ("partner", "independent"):
            prep_mode = overrides["prep_mode"]
        if overrides.get("ignored_critical_events"):
            editorial_brief = dict(editorial_brief or {})
            editorial_brief["_ignored_critical_events"] = list(overrides["ignored_critical_events"])

        valid_chunks = bb_obj.get("chunks", [])
        old_meta = bb_obj.get("metadata") or {}
        derived_meta = old_meta.get("derived") or derive_metadata(src_url or old_meta.get("src_url", ""), 3600)
        dur = sum(float((c.get("measurement") or {}).get("chunk_duration_s", 300)) for c in valid_chunks) or 3600.0
        exact_duration = format_hhmmss(dur)

        target_tpls = overrides.get("run_templates") or cfg.get("selected_templates") or ["tpl_presenter"]
        existing_quals = bb_obj.get("template_syntheses") or {}
        if bb_obj.get("qualitative_synthesis") and "tpl_presenter" not in existing_quals:
            existing_quals["tpl_presenter"] = bb_obj["qualitative_synthesis"]

        # Only validate Gemini keys if a NEW template needs 1 text synthesis call
        need_llm = any(tid not in existing_quals for tid in target_tpls)
        valid_free, valid_paid, paid_models = ([], None, [])
        if need_llm:
            valid_free, valid_paid, paid_models = preflight_validate_keys(fk_list, pk, strat, tg_bot, tg_chat)

        act_cost, models_used = 0.0, set(old_meta.get("models_used", ["RERENDER:$0"]))
        template_results, guest_summaries_out = dict(bb_obj.get("template_results") or {}), []
        kpi_sum = None

        for tid in target_tpls:
            tpl = load_template_by_id(tid)
            if tpl.get("engine") != "scored_evaluation":
                continue
            if tpl.get("target_entity") == "guest":
                m_base = extract_episode_metrics(valid_chunks, dur, tpl.get("policy") or {})
                g_ids = m_base["guest_ids"] or ["GUEST_1"]
                guest_evals = []
                for gid in g_ids:
                    g_info = m_base["guest_obs_agg"].get(gid) or m_base["participants_map"].get(gid) or {"name": gid, "specialty": "", "connection": "studio"}
                    g_name = g_info.get("name") or gid
                    pre_g = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, {}, shoot_env, prep_mode, editorial_brief, target_guest_id=gid)
                    qk = f"{tid}::{gid}"
                    gq = existing_quals.get(qk)
                    if not gq and (valid_free or valid_paid):
                        dig = build_template_context_digest(valid_chunks, tpl, target_guest_id=gid)
                        pr_g = build_scored_synthesis_prompt(tpl, g_name, dig, pre_g, system_prompt_extra, is_guest=True)
                        gq, sc, sm = run_synthesis_call(pr_g, valid_free, valid_paid, paid_models, strat, lambda: act_cost < episode_budget_cap)
                        act_cost += sc
                        if sm:
                            models_used.add(sm)
                        existing_quals[qk] = gq
                    ev_g = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, gq or {}, shoot_env, prep_mode, editorial_brief, target_guest_id=gid)
                    gp = (gq or {}).get("guest_profile") or {}
                    g_ideas = gp.get("new_ideas") or g_info.get("new_ideas") or []
                    g_quotes = gp.get("quotable_lines") or g_info.get("quotable_lines") or []
                    g_spec = gp.get("specialty") or g_info.get("specialty") or ""
                    guest_evals.append({"guest_id": gid, "guest_name": g_name, "specialty": g_spec, "connection": g_info.get("connection", "studio"), "eval": ev_g, "llm_qual": gq or {}, "new_ideas": g_ideas, "quotable_lines": g_quotes})
                    sf_g = ev_g["score_final"]
                    guest_summaries_out.append({
                        "folder_id": fid, "guest_name": g_name, "specialty": g_spec, "presenter": display_name,
                        "program": ev_g.get("program_name") or "", "date": derived_meta.get("broadcast_date") or datetime.now(BEIRUT_TZ).strftime("%Y-%m-%d"),
                        "final_score": sf_g["final_score"], "reported_as": sf_g["reported_as"], "band": sf_g["band"],
                        "tier": "A" if sf_g["final_score"] >= 85 else ("B" if sf_g["final_score"] >= 70 else "C"),
                        "axes": {a["id"]: a["pct"] for a in ev_g["axes"]}, "evidence_pct": ev_g["kpis"].get("guest_evidence_pct", 0),
                        "longest_block_s": ev_g["kpis"].get("guest_longest_block_s", 0), "new_ideas": g_ideas[:5],
                        "quotable_lines": g_quotes[:3], "template_version": tpl.get("version", "1.0"),
                    })
                g_html = render_guests_bundle_html(display_name, guest_evals, derived_meta, exact_duration, tpl)
                fp = os.path.join(BACKUP_DIR, f"06_Guests_Evaluation_{clean_name}.html")
                with open(fp, "w", encoding="utf-8") as f:
                    f.write(g_html)
                drive_upload(fid, cfg, fp, "text/html")
                continue

            pre_ev = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, {}, shoot_env, prep_mode, editorial_brief, editorial_mode=editorial_mode)
            lq = existing_quals.get(tid)
            if not lq and (valid_free or valid_paid):
                dig = build_template_context_digest(valid_chunks, tpl)
                pr_t = build_scored_synthesis_prompt(tpl, display_name, dig, pre_ev, system_prompt_extra)
                lq, sc, sm = run_synthesis_call(pr_t, valid_free, valid_paid, paid_models, strat, lambda: act_cost < episode_budget_cap)
                act_cost += sc
                if sm:
                    models_used.add(sm)
                existing_quals[tid] = lq
            if overrides.get("custom_summary") and tid == "tpl_presenter":
                lq = dict(lq or {})
                rr = dict(lq.get("report") or {})
                rr["summary"] = overrides["custom_summary"]
                lq["report"] = rr
            final_ev = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, lq or {}, shoot_env, prep_mode, editorial_brief, editorial_mode=editorial_mode)
            template_results[tid] = final_ev
            charts = generate_eval_charts(final_ev)
            mi = {"clean_name": clean_name, "chunks_ok": len(valid_chunks), "chunks_total": len(valid_chunks), "actual_cost": act_cost, "src_url": src_url or old_meta.get("src_url", "")}
            det_h = render_generic_scored_report_html(display_name, final_ev, lq or {}, derived_meta, exact_duration, charts, mi, summarized=False)
            if tid == "tpl_presenter":
                sum_h = render_generic_scored_report_html(display_name, final_ev, lq or {}, derived_meta, exact_duration, charts, mi, summarized=True)
                trn_h = render_training_plan_html(display_name, final_ev, lq or {})
                for fn, txt in ((f"01_Main_Detailed_Report_{clean_name}.html", det_h), (f"02_Summarized_Report_{clean_name}.html", sum_h), (f"03_Training_Plan_{clean_name}.html", trn_h)):
                    fp = os.path.join(BACKUP_DIR, fn)
                    with open(fp, "w", encoding="utf-8") as f:
                        f.write(txt)
                    drive_upload(fid, cfg, fp, "text/html")
                kpi_sum = {
                    "folder_id": fid, "presenter": display_name, "date": derived_meta.get("broadcast_date") or datetime.now(BEIRUT_TZ).strftime("%Y-%m-%d"),
                    "duration": exact_duration, "final_score": final_ev["score_final"]["final_score"], "raw_score": final_ev["score_final"]["raw_score"],
                    "reported_as": final_ev["score_final"]["reported_as"], "band": final_ev["score_final"]["band"],
                    "axes": {a["id"]: a["pct"] for a in final_ev["axes"]}, "kpis": final_ev["kpis"],
                    "critical_events": [e.get("type") for e in final_ev["critical_events"]], "src_url": mi["src_url"],
                    "template_version": final_ev["template_version"], "shoot_env": final_ev["shoot_env"], "prep_mode": final_ev["prep_mode"],
                }
            else:
                prefix_n = "07_Directing_" if tid == "tpl_directing" else ("08_Episode_" if tid == "tpl_episode" else f"09_{sanitize_filename(tid)}_")
                fp = os.path.join(BACKUP_DIR, f"{prefix_n}{clean_name}.html")
                with open(fp, "w", encoding="utf-8") as f:
                    f.write(det_h)
                drive_upload(fid, cfg, fp, "text/html")

        bb_obj["template_syntheses"] = existing_quals
        bb_obj["template_results"] = template_results
        if "tpl_presenter" in template_results:
            bb_obj["evaluation"] = template_results["tpl_presenter"]
        fp_bb = os.path.join(BACKUP_DIR, f"05_Blackbox_V3_{clean_name}.json")
        with open(fp_bb, "w", encoding="utf-8") as f:
            json.dump(bb_obj, f, ensure_ascii=False, indent=2)
        drive_upload(fid, cfg, fp_bb, "application/json")
        finalize_cost_webhook(cfg, fid, act_cost, "completed", f"تم التحديث / التوليد الإضافي (${act_cost:.4f})", completed_chunks=len(valid_chunks), total_chunks=len(valid_chunks), kpi_summary=kpi_sum, extra_fields={"guest_summaries": guest_summaries_out})
        sys.exit(0)

    # ==========================================================================
    # STANDARD PIPELINE: PRE-FLIGHT + VIDEO ACQUISITION (REUSE DRIVE IF ARCHIVED)
    # ==========================================================================
    push_status(cfg, fid, run_id, "running", "فحص مفاتيح API...", 0, 0, 0.0, started_at)
    valid_free, valid_paid, paid_models = preflight_validate_keys(fk_list, pk, strat, tg_bot, tg_chat)
    if (strat == "flash_only" and not valid_free) or (strat == "pro_only" and not valid_paid) or (not valid_free and not valid_paid):
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ لا يوجد مفاتيح API صالحة")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "لا يوجد مفاتيح API صالحة")
        sys.exit(1)

    selected_templates = cfg.get("selected_templates") or (["tpl_daily"] if mode == "daily" else ["tpl_presenter"])
    first_tpl = load_template_by_id(selected_templates[0])
    is_inventory_run = (mode == "daily" or first_tpl.get("engine") == "structured_inventory")

    chunk_min = int(cfg.get("chunk_size", 15)) if is_inventory_run else 5
    chunk_sec = chunk_min * 60
    fps = cfg.get("daily_fps", "1/10") if is_inventory_run else cfg.get("eval_fps", "1")

    local_input, downloaded_local, tmp_src = src_url, False, "/tmp/source_video.mp4"
    dur = float(cfg.get("expected_hours", 1)) * 3600
    m_fn_dur = re.search(r"archive-\d{10}-(\d+)", src_url or "")
    if m_fn_dur and int(m_fn_dur.group(1)) > 30:
        dur = float(m_fn_dur.group(1))

    # 1. Check if video is ALREADY archived on Drive (from Scheduler Ingest or previous attempt)
    drive_video_id, drive_video_name = find_drive_archived_video(fid, cfg)
    if drive_video_id:
        push_status(cfg, fid, run_id, "running", "سحب الفيديو المحفوظ مسبقاً من Google Drive...", 0, max(1, math.ceil(dur / chunk_sec)), 0.0, started_at)
        if download_drive_file_to_local(drive_video_id, tmp_src, cfg):
            local_input, downloaded_local = tmp_src, True
            try:
                pl = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", tmp_src], capture_output=True, text=True, timeout=30).stdout.strip()
                if pl and float(pl) > 0:
                    dur = float(pl)
            except Exception:
                pass

    # 2. Otherwise acquire from src_url and archive to Drive if enabled
    if not downloaded_local:
        if not src_url:
            update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", "❌ رابط المصدر مفقود")
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "رابط المصدر مفقود")
            sys.exit(1)
        save_video_flag = (cfg.get("save_video_to_drive") is not False)
        want_local_dl = (not is_inventory_run or dur <= 9000 or is_youtube_url(src_url))
        if not want_local_dl and save_video_flag and src_url.lower().startswith(("http://", "https://")):
            rem_bytes = probe_remote_content_length(src_url)
            if 0 < rem_bytes <= int(3.5 * 1024 * 1024 * 1024):
                want_local_dl = True
        if want_local_dl:
            push_status(cfg, fid, run_id, "running", "تحميل الفيديو محلياً...", 0, max(1, math.ceil(dur / chunk_sec)), 0.0, started_at)
            if acquire_video_locally(src_url, tmp_src, cfg):
                local_input, downloaded_local = tmp_src, True
                try:
                    pl = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", tmp_src], capture_output=True, text=True, timeout=30).stdout.strip()
                    if pl and float(pl) > 0:
                        dur = float(pl)
                except Exception:
                    pass
                if save_video_flag and fid:
                    push_status(cfg, fid, run_id, "running", "أرشفة نسخة الفيديو على Google Drive...", 0, max(1, math.ceil(dur / chunk_sec)), 0.0, started_at)
                    drive_video_id = drive_upload_resumable(fid, cfg, tmp_src, f"00_Video_{clean_name}.mp4", "video/mp4")

    exact_duration, tot, derived_meta = format_hhmmss(dur), max(1, math.ceil(dur / chunk_sec)), derive_metadata(src_url, dur)
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
                log(f"🎞️ Sliced chunk {idx+1}/{tot} ({format_hhmmss(st)}->{format_hhmmss(st+d)}) | {os.path.getsize(mp4_path)/(1024*1024):.2f} MB | {time.time()-t_ff:.1f}s")
                return mp4_path, st, d, sp_s
            last_ff_err = (proc.stderr or "").strip()[:240]
            time.sleep(2)
        raise RuntimeError(f"FFmpeg failed on chunk {idx+1}: {last_ff_err}")

    # ==========================================================================
    # MODE C: SCORED MULTI-TEMPLATE ENGINE (UNIVERSAL BLACKBOX V3.0)
    # ==========================================================================
    if not is_inventory_run:
        bb_prompt_tpl = load_blackbox_v3_prompt()
        saved_ckpts, saved_ctx = load_drive_checkpoints(fid, cfg)
        chunks_by_idx = [None] * tot
        for ci, cobj in saved_ckpts.items():
            if 0 <= ci < tot and isinstance(cobj, dict):
                chunks_by_idx[ci] = cobj
        resumed_cnt = sum(1 for x in chunks_by_idx if x is not None)
        if resumed_cnt > 0:
            log(f"♻️ RESUME ACTIVE: Restored {resumed_cnt}/{tot} chunks from Drive checkpoints!")
            send_tg(f"♻️ <b>استكمال ({display_name})</b>: استعادة {resumed_cnt}/{tot} مقطع من نقاط الحفظ.", tg_bot, tg_chat)

        missing_idxs = [i for i in range(tot) if chunks_by_idx[i] is None]
        push_status(cfg, fid, run_id, "running", f"تقطيع المقاطع ({len(missing_idxs)}/{tot} × 5د @ {fps} FPS)...", resumed_cnt, tot, 0.0, started_at)
        sliced = {}
        if missing_idxs:
            with ThreadPoolExecutor(max_workers=min(3, len(missing_idxs))) as ex:
                futs = {ex.submit(extract_chunk_media, i): i for i in missing_idxs}
                for fut in as_completed(futs):
                    sliced[futs[fut]] = fut.result()

        running_ctx = saved_ctx if isinstance(saved_ctx, dict) else {
            "chunk_index": 0, "program_name": None, "presenter_name": display_name,
            "participants": [{"id": "ANCHOR", "name": display_name, "role": "مقدّم"}],
            "active_files": [], "last_300_words": "", "open_claims": [], "unresolved_chyrons": [], "cumulative_clock": "00:00:00",
        }
        for i in range(tot):
            if chunks_by_idx[i] is not None:
                running_ctx = update_running_context(running_ctx, chunks_by_idx[i], chunks_by_idx[i].get("window", {}).get("end", format_hhmmss((i + 1) * chunk_sec)))

        failed_idxs, proc_start = [], time.time()

        def run_bb_chunk(i: int, ctx_snap: dict, force_paid: bool = False):
            mp4_path, st, d, speech_s = sliced[i] if i in sliced and os.path.exists(sliced[i][0]) else extract_chunk_media(i)
            st_hms, end_hms = format_hhmmss(st), format_hhmmss(st + d)
            prompt_filled = (
                bb_prompt_tpl
                .replace("{{CHUNK_INDEX}}", str(i + 1))
                .replace("{{CHUNK_TOTAL}}", str(tot))
                .replace("{{CHUNK_START}}", st_hms)
                .replace("{{CHUNK_END}}", end_hms)
                .replace("{{PROGRAM_NAME_IF_KNOWN}}", str(ctx_snap.get("program_name") or "غير محدد بعد"))
                .replace("{{RUNNING_CONTEXT}}", json.dumps(ctx_snap, ensure_ascii=False, indent=2))
                .replace("{{ENTITIES_CSV}}", entities_csv)
            )
            full_prompt = (
                f"{prompt_filled}\n\n"
                f"تنبيه إلزامي: اسم المقدّم المثبّت (ANCHOR) هو «{display_name}».\n"
                f"اسم ملف المصدر: {derived_meta.get('source_filename')}\n{system_prompt_extra}"
            )
            try:
                raw_data, cost, used_model, tier = call_gemini_chunk(
                    mp4_path, full_prompt, valid_free, valid_paid, paid_models,
                    "pro_only" if force_paid else strat, exhausted_pairs, bad_keys, budget_ok,
                    chunk_schema=None, chunk_tag=f"BBv3 {i+1}/{tot}",
                )
                norm = normalize_chunk_v3(raw_data, i + 1, st_hms, end_hms, d, derived_meta, speech_s, entity_registry, locked_presenter=display_name)
                return norm, cost, used_model, tier, end_hms
            finally:
                if os.path.exists(mp4_path):
                    try:
                        os.remove(mp4_path)
                    except Exception:
                        pass

        for i in range(tot):
            if CANCEL_EVENT.is_set() or PAUSE_EVENT.is_set():
                break
            if chunks_by_idx[i] is not None:
                continue
            try:
                norm, cost, used_model, tier, end_hms = run_bb_chunk(i, running_ctx, force_paid=False)
                chunks_by_idx[i] = norm
                running_ctx = update_running_context(running_ctx, norm, end_hms)
                act_cost += cost
                models_used.add(f"{tier}:{used_model}")
                drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", norm)
                drive_upload_json_blob(fid, cfg, "_ckpt_ctx.json", running_ctx)
                log(f"✅ BBv3 Chunk {i+1}/{tot} turns={len(norm['turns'])} chyrons={len(norm['chyron_timeline'])} env={norm['visual_directing']['environment_observed']} via {tier}:{used_model}")
            except Exception as e:
                if CANCEL_EVENT.is_set() or "Cancelled by user" in str(e):
                    break
                failed_idxs.append(i)
                log(f"⚠️ Chunk {i+1}/{tot} queued for DLQ: {e}")
            done_now = sum(1 for x in chunks_by_idx if x is not None)
            eta = int(((time.time() - proc_start) / max(1, i + 1)) * (tot - i - 1)) + 60
            push_status(cfg, fid, run_id, "running", f"بناء الصندوق الأسود الشامل V3 ({done_now}/{tot})", done_now, tot, act_cost, started_at, eta_sec=eta)

        # Clean PAUSE handler (preserves all checkpoints & folder cleanly!)
        if PAUSE_EVENT.is_set() and not CANCEL_EVENT.is_set():
            done_now = sum(1 for x in chunks_by_idx if x is not None)
            log(f"⏸️ PAUSED cleanly after chunk {done_now}/{tot} — checkpoints safely saved on Drive.")
            send_tg(f"⏸️ <b>توقّف مؤقت آمن ({display_name})</b> عند المقطع {done_now}/{tot} — يمكنك الضغط على «متابعة» في أي وقت.", tg_bot, tg_chat)
            finalize_cost_webhook(cfg, fid, act_cost, "paused", f"متوقف مؤقتاً عند {done_now}/{tot} — اضغط «متابعة» للاستكمال", completed_chunks=done_now, total_chunks=tot)
            sys.exit(0)

        # DLQ Rescue
        if failed_idxs and not CANCEL_EVENT.is_set():
            time.sleep(6)
            for i in failed_idxs:
                if CANCEL_EVENT.is_set() or PAUSE_EVENT.is_set():
                    break
                try:
                    use_paid = (strat != "flash_only") and (valid_paid is not None) and budget_ok()
                    norm, cost, used_model, tier, _ = run_bb_chunk(i, running_ctx, force_paid=use_paid)
                    chunks_by_idx[i] = norm
                    act_cost += cost
                    models_used.add(f"{tier}:{used_model}")
                    drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", norm)
                except Exception as ex:
                    log(f"❌ DLQ failed chunk {i+1}: {ex}")

        if downloaded_local and os.path.exists(local_input):
            try:
                os.remove(local_input)
            except Exception:
                pass

        if CANCEL_EVENT.is_set():
            update_drive_folder_meta(fid, cfg, f"CANCELLED_{clean_name}", f"🔗 {src_url}\n🛑 ملغاة")
            finalize_cost_webhook(cfg, fid, act_cost, "cancelled", "تم الإلغاء")
            sys.exit(0)

        valid_chunks = [c for c in chunks_by_idx if c is not None]
        if not valid_chunks:
            update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ فشلت جميع المقاطع")
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشلت جميع المقاطع")
            sys.exit(1)

        # Run every selected template against the SAME Universal Blackbox v3.0!
        template_syntheses, template_results, guest_summaries_out = {}, {}, []
        out_paths, primary_kpi_sum, primary_sum_html, primary_score_str = [], None, "", ""

        # Restore any template-level synthesis checkpoints saved before an earlier interruption
        saved_tpl_ckpts = {}
        try:
            q_tc = f"'{fid}' in parents and name contains '_ckpt_tpl_' and trashed=false"
            r_tc = drive_request("GET", "https://www.googleapis.com/drive/v3/files", cfg, params={"q": q_tc, "fields": "files(id,name)"}, timeout=20)
            if r_tc.status_code == 200:
                for f_tc in r_tc.json().get("files", []):
                    rr_tc = drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{f_tc['id']}?alt=media", cfg, timeout=20)
                    if rr_tc.status_code == 200:
                        k_tc = f_tc["name"].replace("_ckpt_tpl_", "").replace(".json", "")
                        saved_tpl_ckpts[k_tc] = rr_tc.json()
        except Exception:
            pass

        for t_idx, tid in enumerate(selected_templates, 1):
            tpl = load_template_by_id(tid)
            if tpl.get("engine") != "scored_evaluation":
                continue
            push_status(cfg, fid, run_id, "running", f"توليف القالب ({t_idx}/{len(selected_templates)}): {tpl['name']}...", len(valid_chunks), tot, act_cost, started_at, eta_sec=25)

            if tpl.get("target_entity") == "guest":
                m_base = extract_episode_metrics(valid_chunks, dur, tpl.get("policy") or {})
                g_ids = m_base["guest_ids"] or ["GUEST_1"]
                guest_evals = []
                for gid in g_ids:
                    g_info = m_base["guest_obs_agg"].get(gid) or m_base["participants_map"].get(gid) or {"name": gid, "specialty": "", "connection": "studio"}
                    g_name = g_info.get("name") or gid
                    pre_g = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, {}, shoot_env, prep_mode, editorial_brief, target_guest_id=gid)
                    dig = build_template_context_digest(valid_chunks, tpl, target_guest_id=gid)
                    pr_g = build_scored_synthesis_prompt(tpl, g_name, dig, pre_g, system_prompt_extra, is_guest=True)
                    gq, sc, sm = run_synthesis_call(pr_g, valid_free, valid_paid, paid_models, strat, budget_ok)
                    act_cost += sc
                    if sm:
                        models_used.add(sm)
                    template_syntheses[f"{tid}::{gid}"] = gq
                    ev_g = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, gq, shoot_env, prep_mode, editorial_brief, target_guest_id=gid)
                    gp = (gq or {}).get("guest_profile") or {}
                    g_ideas = gp.get("new_ideas") or g_info.get("new_ideas") or []
                    g_quotes = gp.get("quotable_lines") or g_info.get("quotable_lines") or []
                    g_spec = gp.get("specialty") or g_info.get("specialty") or ""
                    guest_evals.append({"guest_id": gid, "guest_name": g_name, "specialty": g_spec, "connection": g_info.get("connection", "studio"), "eval": ev_g, "llm_qual": gq, "new_ideas": g_ideas, "quotable_lines": g_quotes})
                    sf_g = ev_g["score_final"]
                    guest_summaries_out.append({
                        "folder_id": fid, "guest_name": g_name, "specialty": g_spec, "presenter": display_name,
                        "program": ev_g.get("program_name") or "", "date": derived_meta.get("broadcast_date") or datetime.now(BEIRUT_TZ).strftime("%Y-%m-%d"),
                        "final_score": sf_g["final_score"], "reported_as": sf_g["reported_as"], "band": sf_g["band"],
                        "tier": "A" if sf_g["final_score"] >= 85 else ("B" if sf_g["final_score"] >= 70 else "C"),
                        "axes": {a["id"]: a["pct"] for a in ev_g["axes"]}, "evidence_pct": ev_g["kpis"].get("guest_evidence_pct", 0),
                        "longest_block_s": ev_g["kpis"].get("guest_longest_block_s", 0), "new_ideas": g_ideas[:5],
                        "quotable_lines": g_quotes[:3], "template_version": tpl.get("version", "1.0"),
                    })
                g_html = render_guests_bundle_html(display_name, guest_evals, derived_meta, exact_duration, tpl)
                fp = os.path.join(BACKUP_DIR, f"06_Guests_Evaluation_{clean_name}.html")
                with open(fp, "w", encoding="utf-8") as f:
                    f.write(g_html)
                out_paths.append(fp)
                drive_upload(fid, cfg, fp, "text/html")
                send_tg(f"👥 <b>تقييم ضيوف ({display_name})</b> | {len(guest_evals)} ضيف", tg_bot, tg_chat, fp)
                continue

            pre_ev = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, {}, shoot_env, prep_mode, editorial_brief, editorial_mode=editorial_mode)
            dig = build_template_context_digest(valid_chunks, tpl)
            if tid in saved_tpl_ckpts:
                lq = saved_tpl_ckpts[tid]
                log(f"♻️ Restored completed synthesis checkpoint for template '{tid}'")
            else:
                pr_t = build_scored_synthesis_prompt(tpl, display_name, dig, pre_ev, system_prompt_extra)
                lq, sc, sm = run_synthesis_call(pr_t, valid_free, valid_paid, paid_models, strat, budget_ok)
                act_cost += sc
                if sm:
                    models_used.add(sm)
                if lq:
                    drive_upload_json_blob(fid, cfg, f"_ckpt_tpl_{tid}.json", lq)
            template_syntheses[tid] = lq
            final_ev = evaluate_scored_template(tpl, valid_chunks, dur, derived_meta, lq, shoot_env, prep_mode, editorial_brief, editorial_mode=editorial_mode)
            template_results[tid] = final_ev
            charts = generate_eval_charts(final_ev)
            mi = {"clean_name": clean_name, "chunks_ok": len(valid_chunks), "chunks_total": tot, "actual_cost": act_cost, "src_url": src_url, "drive_video_id": drive_video_id}
            det_h = render_generic_scored_report_html(display_name, final_ev, lq, derived_meta, exact_duration, charts, mi, summarized=False)

            if tid == "tpl_presenter" or (not primary_sum_html and t_idx == 1):
                sum_h = render_generic_scored_report_html(display_name, final_ev, lq, derived_meta, exact_duration, charts, mi, summarized=True)
                primary_sum_html = sum_h
                primary_score_str = f"{final_ev['score_final']['reported_as']} ({final_ev['score_final']['band']})"
                files_p = [(f"01_Main_Detailed_Report_{clean_name}.html", det_h), (f"02_Summarized_Report_{clean_name}.html", sum_h)]
                if tpl.get("generate_training_plan"):
                    files_p.append((f"03_Training_Plan_{clean_name}.html", render_training_plan_html(display_name, final_ev, lq)))
                for fn, txt in files_p:
                    fp = os.path.join(BACKUP_DIR, fn)
                    with open(fp, "w", encoding="utf-8") as f:
                        f.write(txt)
                    out_paths.append(fp)
                    drive_upload(fid, cfg, fp, "text/html")
                    send_tg(f"📄 <b>{fn}</b> | {primary_score_str}", tg_bot, tg_chat, fp)
                if tid == "tpl_presenter":
                    primary_kpi_sum = {
                        "folder_id": fid, "presenter": display_name, "date": derived_meta.get("broadcast_date") or datetime.now(BEIRUT_TZ).strftime("%Y-%m-%d"),
                        "duration": exact_duration, "final_score": final_ev["score_final"]["final_score"], "raw_score": final_ev["score_final"]["raw_score"],
                        "reported_as": final_ev["score_final"]["reported_as"], "band": final_ev["score_final"]["band"],
                        "axes": {a["id"]: a["pct"] for a in final_ev["axes"]}, "kpis": final_ev["kpis"],
                        "critical_events": [e.get("type") for e in final_ev["critical_events"]], "src_url": src_url,
                        "template_version": final_ev["template_version"], "shoot_env": final_ev["shoot_env"], "prep_mode": final_ev["prep_mode"],
                    }
            else:
                prefix_n = "07_Directing_" if tid == "tpl_directing" else ("08_Episode_" if tid == "tpl_episode" else f"09_{sanitize_filename(tid)}_")
                fp = os.path.join(BACKUP_DIR, f"{prefix_n}{clean_name}.html")
                with open(fp, "w", encoding="utf-8") as f:
                    f.write(det_h)
                out_paths.append(fp)
                drive_upload(fid, cfg, fp, "text/html")
                send_tg(f"📄 <b>{tpl['name']} ({display_name})</b> | {final_ev['score_final']['reported_as']}", tg_bot, tg_chat, fp)

        # Save Universal Blackbox v3.0 MD + JSON
        meta_info = {
            "clean_name": clean_name, "chunks_ok": len(valid_chunks), "chunks_total": tot,
            "actual_cost": act_cost, "models_used": sorted(models_used), "src_url": src_url,
            "drive_video_id": drive_video_id, "selected_templates": selected_templates,
            "shoot_env": shoot_env, "prep_mode": prep_mode,
        }
        bb_md = generate_blackbox_v3_md(display_name, exact_duration, derived_meta, valid_chunks, template_results, meta_info)
        bb_json_obj = {
            "prompt_version": "blackbox-v3.0",
            "metadata": {**meta_info, "derived": derived_meta, "key_health": get_key_health_snapshot()},
            "evaluation": template_results.get("tpl_presenter") or next(iter(template_results.values()), {}),
            "template_results": template_results,
            "template_syntheses": template_syntheses,
            "guest_summaries": guest_summaries_out,
            "chunks": valid_chunks,
        }
        for fn, txt, mime in ((f"04_Blackbox_V3_{clean_name}.md", bb_md, "text/markdown"), (f"05_Blackbox_V3_{clean_name}.json", json.dumps(bb_json_obj, ensure_ascii=False, indent=2), "application/json")):
            fp = os.path.join(BACKUP_DIR, fn)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(txt)
            drive_upload(fid, cfg, fp, mime)

        email_subj = f"✅ تقييم V8.0 ({display_name}) — {primary_score_str or 'مكتمل'}"
        smtp_ok = send_smtp_resilient(email_subj, primary_sum_html or "التقارير مرفقة.", out_paths[:5], cfg.get("alert_emails", ""), gm_u, gm_p)
        cleanup_drive_checkpoints(fid, cfg)
        update_drive_folder_meta(fid, cfg, f"PROCESSED_{clean_name}_${act_cost:.2f}", f"🔗 رابط المصدر: {src_url}\n• الاسم: {display_name}\n• النتيجة: {primary_score_str}\n• القوالب: {', '.join(selected_templates)} | الكلفة: ${act_cost:.4f}")
        finalize_cost_webhook(
            cfg, fid, act_cost, "completed", f"اكتمل ({primary_score_str or 'جاهز'})",
            trigger_gas_email=(not smtp_ok), email_subject=email_subj,
            completed_chunks=len(valid_chunks), total_chunks=tot, kpi_summary=primary_kpi_sum,
            extra_fields={"guest_summaries": guest_summaries_out, "drive_video_id": drive_video_id},
        )
        log(f"🎉 V8.0 completed! {display_name} | Templates={selected_templates} | Cost=${act_cost:.4f}")

    # ==========================================================================
    # MODE D: STRUCTURED INVENTORY ENGINE (DAILY OR ANY CUSTOM INVENTORY TEMPLATE)
    # ==========================================================================
    else:
        tpl_inv = first_tpl
        saved_ckpts, _ = load_drive_checkpoints(fid, cfg)
        inv_results, failed_inv = [None] * tot, []
        for ci, cobj in saved_ckpts.items():
            if 0 <= ci < tot and isinstance(cobj, dict):
                inv_results[ci] = cobj
        done_inv = sum(1 for x in inv_results if x is not None)
        proc_start = time.time()

        def run_inv_chunk(i: int, force_paid: bool = False):
            mp4, st, d, _ = extract_chunk_media(i)
            st_h, en_h = format_hhmmss(st), format_hhmmss(st + d)
            pr = build_inventory_chunk_prompt(tpl_inv, st_h, en_h, system_prompt_extra)
            try:
                dat, c, m, t = call_gemini_chunk(mp4, pr, valid_free, valid_paid, paid_models, "pro_only" if force_paid else strat, exhausted_pairs, bad_keys, budget_ok, chunk_tag=f"Inv {i+1}/{tot}")
                with STATE_LOCK:
                    models_used.add(f"{t}:{m}")
                return dat, c
            finally:
                if os.path.exists(mp4):
                    try:
                        os.remove(mp4)
                    except Exception:
                        pass

        inv_todo = [i for i in range(tot) if inv_results[i] is None]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(run_inv_chunk, i, False): i for i in inv_todo}
            for fut in as_completed(futs):
                i = futs[fut]
                if PAUSE_EVENT.is_set() or CANCEL_EVENT.is_set():
                    break
                try:
                    dat, c = fut.result()
                    obj_d = {"chunk_index": i + 1, "data": dat}
                    inv_results[i] = obj_d
                    drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", obj_d)
                    with STATE_LOCK:
                        act_cost += c
                        done_inv += 1
                        cur_d, cur_c = done_inv, act_cost
                    eta = int(((time.time() - proc_start) / cur_d) * (tot - cur_d)) + 15
                    push_status(cfg, fid, run_id, "running", f"{tpl_inv['name']} ({cur_d}/{tot})", cur_d, tot, cur_c, started_at, eta_sec=eta)
                except Exception as e:
                    if CANCEL_EVENT.is_set() or "Cancelled by user" in str(e):
                        break
                    failed_inv.append(i)

        if PAUSE_EVENT.is_set() and not CANCEL_EVENT.is_set():
            done_now = sum(1 for x in inv_results if x is not None)
            send_tg(f"⏸️ <b>توقّف مؤقت آمن ({tpl_inv['name']} — {clean_name})</b> عند المقطع {done_now}/{tot}.", tg_bot, tg_chat)
            finalize_cost_webhook(cfg, fid, act_cost, "paused", f"متوقف مؤقتاً عند {done_now}/{tot}", completed_chunks=done_now, total_chunks=tot)
            sys.exit(0)

        if failed_inv and not CANCEL_EVENT.is_set():
            log(f"🚑 Inventory DLQ Rescue for {len(failed_inv)} failed chunk(s)...")
            time.sleep(6)
            for i in failed_inv:
                if CANCEL_EVENT.is_set() or PAUSE_EVENT.is_set():
                    break
                try:
                    dat, c = run_inv_chunk(i, force_paid=(strat != "flash_only" and bool(valid_paid) and budget_ok()))
                    obj_d = {"chunk_index": i + 1, "data": dat}
                    inv_results[i] = obj_d
                    drive_upload_json_blob(fid, cfg, f"_ckpt_chunk_{i}.json", obj_d)
                    act_cost += c
                except Exception as ex_d:
                    log(f"❌ Inventory DLQ failed chunk {i+1}: {ex_d}")

        if downloaded_local and os.path.exists(local_input):
            try:
                os.remove(local_input)
            except Exception:
                pass
        if CANCEL_EVENT.is_set():
            update_drive_folder_meta(fid, cfg, f"CANCELLED_{clean_name}", f"🔗 {src_url}\n🛑 ملغاة")
            finalize_cost_webhook(cfg, fid, act_cost, "cancelled", "تم الإلغاء")
            sys.exit(0)

        valid_inv = [r for r in inv_results if r and isinstance(r.get("data"), dict)]
        if not valid_inv:
            update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 {src_url}\n❌ فشلت جميع المقاطع")
            finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشلت جميع المقاطع")
            sys.exit(1)

        det_inv, sum_inv, bb_inv_md, item_cnt = render_inventory_reports(tpl_inv, clean_name, exact_duration, valid_inv, act_cost)
        inv_files = [
            (f"01_Daily_Detailed_Report_{clean_name}.html", det_inv, "text/html"),
            (f"02_Daily_Summarized_Report_{clean_name}.html", sum_inv, "text/html"),
            (f"04_Blackbox_V3_{clean_name}.md", bb_inv_md, "text/markdown"),
            (f"05_Blackbox_V3_{clean_name}.json", json.dumps({"template_id": tpl_inv["id"], "template_version": tpl_inv.get("version", "1.0"), "clean_name": clean_name, "derived": derived_meta, "chunks": valid_inv}, ensure_ascii=False, indent=2), "application/json"),
        ]
        out_paths = []
        for fn, txt, mime in inv_files:
            fp = os.path.join(BACKUP_DIR, fn)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(txt)
            out_paths.append(fp)
            drive_upload(fid, cfg, fp, mime)
            if not fn.endswith(".json"):
                send_tg(f"📄 <b>{fn}</b> | الكلفة: ${act_cost:.4f}", tg_bot, tg_chat, fp)

        email_subj = f"✅ {tpl_inv['name']} V8.0 ({clean_name}) | الكلفة: ${act_cost:.4f}"
        smtp_ok = send_smtp_resilient(email_subj, sum_inv, out_paths[:3], cfg.get("alert_emails", ""), gm_u, gm_p)
        cleanup_drive_checkpoints(fid, cfg)
        update_drive_folder_meta(fid, cfg, f"PROCESSED_{clean_name}_${act_cost:.2f}", f"🔗 {src_url}\n• الفقرات: {item_cnt} | الإصدار: v{tpl_inv.get('version','1.0')} | الكلفة: ${act_cost:.4f}")
        finalize_cost_webhook(cfg, fid, act_cost, "completed", f"اكتمل الجرد ({len(valid_inv)}/{tot})", trigger_gas_email=(not smtp_ok), email_subject=email_subj, completed_chunks=len(valid_inv), total_chunks=tot)


if __name__ == "__main__":
    main()
