#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V5 - FAST PRE-FLIGHT & DYNAMIC MODEL EDITION
- Pre-flight validates all Free & Paid keys in parallel via /v1beta/models (2-4 seconds total, hard 8s timeout).
- Dynamically discovers active generateContent-capable models per key (eliminates 404s & retired model IDs).
- Uses official `google-genai` SDK with single-pass muxed MP4 chunks + local download for eval/short runs.
- Smart error separation: 400/401/403 bans bad key; 429 rotates (key, model); 503 switches model without banning key.
"""
import os
import re
import sys
import json
import time
import math
import random
import smtplib
import threading
import subprocess
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from google import genai
from google.genai import types

BEIRUT_TZ = timezone(timedelta(hours=3))
STATE_LOCK = threading.Lock()
FFMPEG_SEM = threading.Semaphore(3)

# Preferred model priority (fallback only if dynamic discovery returns empty)
PREFERRED_FLASH_ORDER = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.0-flash",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
]

PREFERRED_SYNTH_ORDER = [
    "gemini-3.8-flash",
    "gemini-4-argon",
    "gemini-3.6-flash",
    "gemini-2.5-pro",
    "gemini-2.5-flash",
]

PRICING = {
    "gemini-3.8-flash":      {"in": 0.75, "out": 3.75},
    "gemini-3.6-flash":      {"in": 0.50, "out": 2.50},
    "gemini-3.5-flash-lite": {"in": 0.30, "out": 2.50},
    "gemini-4-argon":        {"in": 2.00, "out": 10.00},
    "gemini-2.5-pro":        {"in": 1.25, "out": 10.00},
    "gemini-2.5-flash":      {"in": 0.30, "out": 2.50},
    "gemini-2.5-flash-lite": {"in": 0.10, "out": 0.40},
}

DAILY_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "programs": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "start": {"type": "STRING"},
                    "end": {"type": "STRING"},
                    "category": {"type": "STRING"},
                    "theme": {"type": "STRING"},
                    "title": {"type": "STRING"},
                    "summary": {"type": "STRING"},
                },
                "required": ["start", "end", "category", "theme", "title", "summary"],
            },
        }
    },
    "required": ["programs"],
}

EVAL_CHUNK_PROMPT = (
    'استخرج من هذا المقطع التوقيتات الدقيقة للمتحدثين والتقارير. '
    'ركز على تقييم لغة الجسد، بناء الأسئلة، واسترسال الضيوف. '
    'لا تعاقب استخدام اللهجة اللبنانية المحكية. '
    'سجّل أي مونولوج للضيف يتجاوز 3 دقائق دون تدخل من المقدم. '
    'أخرج JSON صالح فقط بهذا الشكل: '
    '{"speakers": [], "vt_reports": [], "presenter_notes": []}'
)

EVAL_SYNTHESIS_PROMPT = (
    'أنت كبير مقيمي الأداء التلفزيوني. أصدر التقييم النهائي بناءً على الملاحظات التالية فقط دون أي اختلاق. '
    'المعيار التحريري: {editorial}. '
    'سلم العلامات 90 نقطة: إدارة الحوار (20)، إدارة الضيف (20)، بناء الأسئلة (20)، المتابعة والإنصات (15)، الحضور ولغة الجسد (10)، إدارة الوقت (5). '
    'قاعدة اللهجة: لا تخصم أي علامة على اللهجة اللبنانية المحكية. '
    'قاعدة المونولوج: اخصم علامات إذا ترك المقدم الضيف يتحدث أكثر من 3 دقائق متواصلة بلا مقاطعة وظيفية. '
    'أخرج JSON صالح فقط بهذا الشكل: '
    '{{"presenter":"...", "total_score":0, "percentage":0, "top_strength":"...", "top_priority":"...", '
    '"core_scores":{{"management":0, "guest":0, "questions":0, "followup":0, "presence":0, "time":0}}, '
    '"sub_criteria":[{{"name":"...", "score":0, "max":0, "note":"..."}}], '
    '"strengths":[], "priorities":[]}}\n\nالملاحظات المستخرجة:\n'
)

ROYAL_CSS = """
@page { size: A4; margin: 1.5cm; }
body { font-family: "Segoe UI", Tahoma, Arial, sans-serif; direction: rtl; text-align: right; color: #0F172A; max-width: 900px; margin: 20px auto; line-height: 1.6; }
table { width: 100%; border-collapse: collapse; margin-bottom: 20px; page-break-inside: avoid; }
th, td { border: 1px solid #CBD5E1; padding: 10px; text-align: right; }
th { background-color: #1E3A8A; color: white; }
tr:nth-child(even) td { background-color: #F8FAFC; }
.green-box { background-color: #F0FDF4; border-right: 6px solid #16A34A; padding: 15px; margin-bottom: 15px; }
.amber-box { background-color: #FFFBEB; border-right: 6px solid #D97706; padding: 15px; margin-bottom: 15px; }
h1 { color: #1E3A8A; border-bottom: 3px solid #F59E0B; padding-bottom: 10px; }
h2 { color: #1D4ED8; margin-top: 25px; }
.score-cell { font-weight: bold; text-align: center; background-color: #EFF6FF !important; color: #1D4ED8; }
"""


def mask_key(k: str) -> str:
    k = (k or "").strip()
    if len(k) <= 10:
        return "***"
    return f"{k[:6]}...{k[-4:]}"


def clean_json_text(text: str) -> dict:
    raw = (text or "").strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    return json.loads(raw)


def send_tg(msg, token, chat, file_path=None):
    if not (token and chat):
        return
    try:
        if file_path and os.path.exists(file_path):
            with open(file_path, "rb") as f:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendDocument",
                    data={"chat_id": chat, "caption": msg[:1000], "parse_mode": "HTML"},
                    files={"document": f},
                    timeout=30,
                )
        else:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": msg[:4000], "parse_mode": "HTML"},
                timeout=15,
            )
    except Exception as e:
        print(f"[TG Warning] {e}", flush=True)


def send_smtp(subject, body, files, emails, user, pwd):
    if not (user and pwd and emails):
        return False
    recips = [e.strip() for e in emails.split(",") if e.strip()]
    if not recips:
        return False
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = ", ".join(recips)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html", "utf-8"))
    for pf in files:
        if os.path.exists(pf):
            with open(pf, "rb") as f:
                p = MIMEBase("application", "octet-stream")
                p.set_payload(f.read())
            encoders.encode_base64(p)
            p.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(pf)}"')
            msg.attach(p)
    for _ in range(3):
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20) as s:
                s.login(user, pwd)
                s.sendmail(user, recips, msg.as_string())
            return True
        except Exception as e:
            print(f"[SMTP Retry] {e}", flush=True)
            time.sleep(2)
    return False


def drive_upload(folder_id, token, filepath, mimetype):
    filename = os.path.basename(filepath)
    q = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get("https://www.googleapis.com/drive/v3/files", headers=headers, params={"q": q}, timeout=20)
        files_found = r.json().get("files", [])
        fid = files_found[0].get("id") if files_found else None
        with open(filepath, "rb") as f:
            data = f.read()
        if fid:
            requests.patch(
                f"https://www.googleapis.com/upload/drive/v3/files/{fid}?uploadType=media",
                headers=headers,
                data=data,
                timeout=60,
            )
        else:
            bound = "===BNDRY==="
            meta = json.dumps({"name": filename, "parents": [folder_id]})
            body = (
                f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n"
                f"--{bound}\r\nContent-Type: {mimetype}\r\n\r\n"
            ).encode("utf-8") + data + f"\r\n--{bound}--\r\n".encode("utf-8")
            requests.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                headers={"Authorization": f"Bearer {token}", "Content-Type": f"multipart/related; boundary={bound}"},
                data=body,
                timeout=60,
            )
    except Exception as e:
        print(f"[Drive Upload Warning] {e}", flush=True)


def rank_discovered_models(available_models: list, preferred_order: list, allow_pro: bool = False) -> list:
    """
    Orders available generateContent-capable models: preferred models first,
    then any other active flash (and optionally pro/argon) models sorted newest-first.
    Excludes image/tts/embedding/audio-only/thinking-only/legacy 1.0-1.5 models.
    """
    skip_tokens = ("tts", "image", "imagen", "veo", "embedding", "aqa", "bison", "gecko", "1.0", "1.5", "robotics", "computer-use", "live")
    avail_set = set(available_models)
    ordered = [m for m in preferred_order if m in avail_set]

    extras = []
    for m in available_models:
        low = m.lower()
        if m in ordered or not low.startswith("gemini-"):
            continue
        if any(tok in low for tok in skip_tokens):
            continue
        if "flash" in low or (allow_pro and ("pro" in low or "argon" in low)):
            extras.append(m)

    def version_key(name: str):
        nums = [int(x) for x in re.findall(r"\d+", name)]
        is_preview = 0 if ("preview" in name or "exp" in name) else 1
        return (nums[:2] if nums else [0, 0], is_preview, name)

    extras.sort(key=version_key, reverse=True)
    ordered.extend(extras)
    return ordered[:6]


def validate_single_key(key: str, preferred_models: list, allow_pro: bool = False) -> tuple:
    """
    Fast validation (~0.5-1.5s): checks key auth and lists active generateContent models
    via REST with a strict (5s connect, 8s read) timeout. No slow generate_content ping.
    """
    key = key.strip().strip('"').strip("'")
    if not key:
        return False, [], "empty"
    try:
        r = requests.get(
            "https://generativelanguage.googleapis.com/v1beta/models?pageSize=200",
            headers={"x-goog-api-key": key},
            timeout=(5, 8),
        )
        if r.status_code != 200:
            return False, [], f"AUTH FAILED ({r.status_code}): {r.text[:140]}"

        raw_models = r.json().get("models", [])
        gen_models = [
            m.get("name", "").replace("models/", "")
            for m in raw_models
            if "generateContent" in m.get("supportedGenerationMethods", [])
        ]
        working = rank_discovered_models(gen_models, preferred_models, allow_pro=allow_pro)
        if not working:
            working = list(preferred_models[:3])
        return True, working, f"OK ({', '.join(working[:3])})"
    except Exception as e:
        return False, [], f"NETWORK ERROR: {e}"


def preflight_validate_keys(raw_free_keys: list, raw_paid_key: str, strategy: str, tg_bot: str, tg_chat: str):
    """
    Validates all configured Free and Paid keys IN PARALLEL in ~2-4 seconds total
    BEFORE downloading video or running FFmpeg.
    """
    print("=" * 60, flush=True)
    print("PRE-FLIGHT API KEY VALIDATION (FAST PARALLEL)", flush=True)
    print("=" * 60, flush=True)

    valid_free = []
    valid_paid = None
    paid_models = []
    report_lines = []

    tasks = [(f"FREE #{i}", k, PREFERRED_FLASH_ORDER, False) for i, k in enumerate(raw_free_keys, 1)]
    if raw_paid_key:
        tasks.append(("PAID", raw_paid_key, PREFERRED_FLASH_ORDER, True))

    if tasks:
        with ThreadPoolExecutor(max_workers=min(10, len(tasks))) as ex:
            futs = {
                ex.submit(validate_single_key, k, pref, is_paid): (lbl, k, is_paid)
                for lbl, k, pref, is_paid in tasks
            }
            for fut in as_completed(futs):
                lbl, k, is_paid = futs[fut]
                ok, models, detail = fut.result()
                line = f"{'✅' if ok else '❌'} {lbl} ({mask_key(k)}): {detail}"
                print(line, flush=True)
                report_lines.append(line)
                if ok:
                    if is_paid:
                        valid_paid = k
                        paid_models = models
                    else:
                        valid_free.append((k, models))

    if not raw_paid_key:
        print("ℹ️ PAID key: not configured", flush=True)
        report_lines.append("ℹ️ PAID key: not configured")

    print("=" * 60, flush=True)

    can_run = (
        (strategy == "flash_only" and len(valid_free) > 0)
        or (strategy == "pro_only" and valid_paid is not None)
        or (strategy == "hybrid" and (len(valid_free) > 0 or valid_paid is not None))
    )

    send_tg(
        f"🔑 <b>فحص المفاتيح المسبق ({strategy})</b>\n"
        f"• المجانية الصالحة: {len(valid_free)}/{len(raw_free_keys)}\n"
        f"• المفتاح المدفوع: {'✅ صالح' if valid_paid else '❌ غير صالح / غير مضاف'}",
        tg_bot,
        tg_chat,
    )

    if not can_run:
        send_tg(
            f"❌ <b>توقف فوري: لا يوجد أي مفتاح API صالح ({strategy})</b>\n" + "\n".join(report_lines[:8]),
            tg_bot,
            tg_chat,
        )
        return [], None, []

    return valid_free, valid_paid, paid_models


def build_gen_config(schema=None):
    cfg_args = {
        "temperature": 0.0,
        "response_mime_type": "application/json",
        "safety_settings": [
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
        ],
    }
    if schema:
        cfg_args["response_schema"] = schema
    return types.GenerateContentConfig(**cfg_args)


def wait_file_active(client, uploaded_file, timeout_sec=120):
    t0 = time.time()
    f = uploaded_file
    while True:
        state_str = str(getattr(f, "state", "")).upper()
        if "ACTIVE" in state_str:
            return f
        if "FAILED" in state_str:
            raise ValueError(f"Google File API processing failed for {f.name}")
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(f"Timed out waiting for file {f.name} (state={state_str})")
        time.sleep(2)
        f = client.files.get(name=f.name)


def calc_cost(model_name: str, usage, is_paid: bool) -> float:
    if not is_paid or not usage:
        return 0.0
    in_t = getattr(usage, "prompt_token_count", 0) or 0
    out_t = getattr(usage, "candidates_token_count", 0) or 0
    rates = PRICING.get(model_name, {"in": 0.75, "out": 3.75})
    return (in_t / 1e6) * rates["in"] + (out_t / 1e6) * rates["out"]


def call_gemini_chunk(media_path, prompt, schema, valid_free, valid_paid, paid_models, strategy, exhausted_pairs, bad_keys):
    """
    Uploads ONE muxed MP4 file per key attempt and tries that key's discovered models:
    - 400 API_KEY_INVALID / 401 / 403 -> bans key permanently
    - 429 RESOURCE_EXHAUSTED -> marks only (key, model) exhausted, rotates model on same uploaded file
    - 503 UNAVAILABLE -> retries once then moves to next model without banning key
    - 404 NOT_FOUND -> marks (key, model) unavailable and moves to next model
    """
    last_err = ""
    config = build_gen_config(schema)

    def run_with_key(api_key, model_candidates, is_paid):
        nonlocal last_err
        with STATE_LOCK:
            if api_key in bad_keys:
                return None

        models_to_try = []
        with STATE_LOCK:
            for m in model_candidates:
                if (api_key, m) not in exhausted_pairs:
                    models_to_try.append(m)
        if not models_to_try:
            return None

        client = genai.Client(api_key=api_key)
        uploaded = None
        try:
            uploaded = client.files.upload(file=media_path)
            uploaded = wait_file_active(client, uploaded)

            for model_name in models_to_try:
                for attempt in range(2):
                    try:
                        res = client.models.generate_content(
                            model=model_name,
                            contents=[uploaded, prompt],
                            config=config,
                        )
                        data = clean_json_text(res.text)
                        cost = calc_cost(model_name, res.usage_metadata, is_paid)
                        return data, cost, model_name, ("PAID" if is_paid else "FREE")
                    except Exception as e:
                        err = str(e)
                        low = err.lower()
                        last_err = f"[{model_name}] {err[:220]}"
                        print(f"API Error ({'PAID' if is_paid else 'FREE'} {mask_key(api_key)} / {model_name}): {err[:200]}", flush=True)

                        if "api_key_invalid" in low or "access_token_type_unsupported" in low or "401" in low or "403" in low:
                            with STATE_LOCK:
                                bad_keys.add(api_key)
                            return None
                        elif "429" in low or "resource_exhausted" in low or "quota" in low:
                            with STATE_LOCK:
                                exhausted_pairs.add((api_key, model_name))
                            break
                        elif "404" in low or "not_found" in low or "no longer available" in low:
                            with STATE_LOCK:
                                exhausted_pairs.add((api_key, model_name))
                            break
                        elif "503" in low or "unavailable" in low or "overloaded" in low:
                            if attempt == 0:
                                time.sleep(random.uniform(2, 4))
                                continue
                            break  # move to next model without banning key
                        else:
                            time.sleep(2)
                            break
        except Exception as e:
            err = str(e)
            low = err.lower()
            last_err = f"[UPLOAD] {err[:220]}"
            print(f"Upload Error ({'PAID' if is_paid else 'FREE'} {mask_key(api_key)}): {err[:200]}", flush=True)
            if "api_key_invalid" in low or "access_token_type_unsupported" in low or "401" in low or "403" in low:
                with STATE_LOCK:
                    bad_keys.add(api_key)
        finally:
            if uploaded is not None:
                try:
                    client.files.delete(name=uploaded.name)
                except Exception:
                    pass
        return None

    # 1. Free tier attempts
    if strategy in ("flash_only", "hybrid") and valid_free:
        free_order = list(valid_free)
        random.shuffle(free_order)
        for key, key_models in free_order:
            out = run_with_key(key, key_models, is_paid=False)
            if out is not None:
                return out

    # 2. Paid fallback / pro_only
    if strategy in ("hybrid", "pro_only") and valid_paid:
        out = run_with_key(valid_paid, paid_models or PREFERRED_FLASH_ORDER, is_paid=True)
        if out is not None:
            return out

    raise RuntimeError(f"All attempts failed. Last Error: {last_err}")


def build_daily_html(results, cost):
    rows = "".join(
        f"<tr><td>{p.get('start', '')}-{p.get('end', '')}</td>"
        f"<td>{p.get('category', '')}</td>"
        f"<td>{p.get('title', '')}</td>"
        f"<td>{p.get('summary', '')}</td></tr>"
        for r in results
        if r and isinstance(r.get("data"), dict)
        for p in r["data"].get("programs", [])
    )
    return (
        f'<html dir="rtl" lang="ar"><head><meta charset="utf-8"><style>{ROYAL_CSS}</style></head>'
        f'<body><h1>تشريح البث اليومي</h1><div class="green-box">التكلفة الفعلية: ${cost:.4f}</div>'
        f'<table><tr><th>الوقت</th><th>النوع</th><th>العنوان</th><th>الملخص</th></tr>{rows}</table></body></html>'
    )


def build_eval_html(data, pres, cost):
    strengths_li = "".join(f"<li>{s}</li>" for s in data.get("strengths", []))
    priorities_li = "".join(f"<li>{p}</li>" for p in data.get("priorities", []))
    sub_rows = "".join(
        f"<tr><td>{sub.get('name', '')}</td>"
        f"<td class='score-cell'>{sub.get('score', 0)}/{sub.get('max', 0)}</td>"
        f"<td>{sub.get('note', '')}</td></tr>"
        for sub in data.get("sub_criteria", [])
    )
    return (
        f'<html dir="rtl" lang="ar"><head><meta charset="utf-8"><style>{ROYAL_CSS}</style></head>'
        f'<body><h1>تقييم الأداء: {pres}</h1>'
        f'<div class="green-box"><b>النتيجة: {data.get("total_score", 0)}/90 ({data.get("percentage", 0)}%)</b>'
        f'<br>التكلفة الفعلية: ${cost:.4f}</div>'
        f'<h2>نقاط القوة</h2><div class="green-box"><ul>{strengths_li}</ul></div>'
        f'<h2>أولويات التطوير</h2><div class="amber-box"><ul>{priorities_li}</ul></div>'
        f'<h2>تفاصيل المعايير</h2>'
        f'<table><tr><th>المعيار</th><th>العلامة</th><th>ملاحظات</th></tr>{sub_rows}</table></body></html>'
    )


def generate_blackbox_md(mode, raw_data, final_data, meta):
    md = f"# BLACKBOX DATA - {meta['name']} ({meta['mode']})\n"
    md += f"**Date:** {datetime.now(BEIRUT_TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
    md += f"**System Prompt Executed:** {meta.get('system_prompt') or 'Default'}\n\n"
    if mode == "eval":
        md += "## EVALUATION SUMMARY\n"
        md += f"**Score:** {final_data.get('total_score', 0)}/90 ({final_data.get('percentage', 0)}%)\n\n"
        md += "### CHUNK ANALYSIS\n```json\n"
        md += json.dumps(raw_data, ensure_ascii=False, indent=2) + "\n```\n"
    else:
        md += "## DAILY BROADCAST LOG\n```json\n"
        md += json.dumps(raw_data, ensure_ascii=False, indent=2) + "\n```\n"
    return md


def refund_webhook(cfg, actual_cost: float):
    if cfg.get("webhook_url"):
        try:
            requests.post(
                cfg["webhook_url"],
                json={
                    "action": "finalize_cost",
                    "estimated_cost": cfg.get("pre_authorized_cost", 0),
                    "actual_cost": actual_cost,
                },
                timeout=15,
            )
        except Exception as e:
            print(f"[Webhook Warning] {e}", flush=True)


def download_video_resilient(url: str, dest: str, max_retries: int = 3) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=(15, 60)) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for block in r.iter_content(chunk_size=1024 * 256):
                        if block:
                            f.write(block)
            if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
                return True
        except Exception as e:
            print(f"[Download Attempt {attempt}/{max_retries} Failed] {e}", flush=True)
            time.sleep(3)
    return False


def main():
    fid, d_tok, tg_bot, tg_chat, gm_u, gm_p = map(
        os.environ.get,
        ["FOLDER_ID", "DRIVE_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "GMAIL_USER", "GMAIL_APP_PASSWORD"],
    )
    fk_list = [k.strip().strip('"').strip("'") for k in os.environ.get("GEMINI_FREE_API_KEYS", "").split(",") if k.strip()]
    pk = os.environ.get("GEMINI_PAID_API_KEY", "").strip().strip('"').strip("'")

    cfg = {"run_mode": "daily", "expected_hours": 1}
    src_url = ""
    try:
        rf = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers={"Authorization": f"Bearer {d_tok}"},
            params={"q": f"'{fid}' in parents and trashed=false", "fields": "files(id,name)"},
            timeout=20,
        ).json()
        for f in rf.get("files", []):
            if f["name"] == "v5_config.json":
                cfg = requests.get(
                    f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media",
                    headers={"Authorization": f"Bearer {d_tok}"},
                    timeout=20,
                ).json()
            if f["name"] == "_source_link.txt":
                src_url = requests.get(
                    f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media",
                    headers={"Authorization": f"Bearer {d_tok}"},
                    timeout=20,
                ).text.strip()
    except Exception as e:
        print(f"[Config Fetch Error] {e}", flush=True)

    mode = cfg.get("run_mode", "daily")
    chunk_sec = int(cfg.get("chunk_size", 10)) * 60
    workers = max(1, min(int(cfg.get("workers", 3)), 5))
    strat = cfg.get("tier_strategy", "hybrid")
    system_prompt = cfg.get("system_prompt", "")
    fps = cfg.get("daily_fps", "1/5") if mode == "daily" else cfg.get("eval_fps", "1")

    # STEP 1: FAST PARALLEL PRE-FLIGHT KEY VALIDATION (~2-4 seconds total)
    valid_free, valid_paid, paid_models = preflight_validate_keys(fk_list, pk, strat, tg_bot, tg_chat)
    if (strat == "flash_only" and not valid_free) or (strat == "pro_only" and not valid_paid) or (not valid_free and not valid_paid):
        refund_webhook(cfg, 0.0)
        sys.exit(1)

    if not src_url:
        send_tg("❌ لا يوجد رابط فيديو في `_source_link.txt`.", tg_bot, tg_chat)
        refund_webhook(cfg, 0.0)
        sys.exit(1)

    # STEP 2: PROBE DURATION & HYBRID LOCAL DOWNLOAD
    dur = float(cfg.get("expected_hours", 1)) * 3600
    try:
        probe_out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
        if probe_out:
            dur = float(probe_out)
    except Exception as e:
        print(f"[ffprobe warning] {e}", flush=True)

    tot = math.ceil(dur / chunk_sec)
    if tot <= 0:
        send_tg("❌ الرابط غير صالح أو مدته صفر.", tg_bot, tg_chat)
        refund_webhook(cfg, 0.0)
        sys.exit(1)

    local_input = src_url
    downloaded_local = False
    if mode == "eval" or dur <= 9000:
        send_tg(f"📥 تحميل الحلقة محلياً ({dur/60:.1f} دقيقة) لمنع انقطاع البث...", tg_bot, tg_chat)
        tmp_src = "/tmp/source_video.mp4"
        if download_video_resilient(src_url, tmp_src):
            local_input = tmp_src
            downloaded_local = True
            size_mb = os.path.getsize(tmp_src) / (1024 * 1024)
            print(f"✅ Downloaded locally: {size_mb:.1f} MB", flush=True)
        else:
            print("⚠️ Local download failed; falling back to resilient HTTP streaming.", flush=True)

    send_tg(f"🚀 بدء التحليل V5 | المسار: {mode} | المقاطع: {tot} | الاستراتيجية: {strat}", tg_bot, tg_chat)

    results = [None] * tot
    act_cost = 0.0
    exhausted_pairs = set()
    bad_keys = set()
    failed = []

    def process_chunk(idx, force_paid=False):
        time.sleep(random.uniform(0.3, 1.8) * (idx % workers))
        st = idx * chunk_sec
        d = min(chunk_sec, dur - st)
        mp4_path = f"/tmp/chunk_{idx}.mp4"

        extracted = False
        for attempt in range(1, 4):
            cmd = ["ffmpeg", "-y", "-loglevel", "error"]
            if not downloaded_local:
                cmd += ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5"]
            cmd += [
                "-ss", str(st),
                "-t", str(d),
                "-i", local_input,
                "-vf", f"fps={fps},scale=640:360",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
                "-c:a", "aac", "-ac", "1", "-ar", "16000", "-b:a", "24k",
                "-movflags", "+faststart",
                mp4_path,
            ]
            with FFMPEG_SEM:
                proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode == 0 and os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 5000:
                extracted = True
                break
            print(f"[FFmpeg Retry {attempt}/3 on chunk {idx+1}] rc={proc.returncode} err={proc.stderr[:160]}", flush=True)
            time.sleep(2)

        if not extracted:
            raise RuntimeError(f"FFmpeg failed to extract valid media for chunk {idx+1}")

        chunk_time_hint = f"[المقطع الزمني: من الدقيقة {st//60} إلى الدقيقة {int((st+d)//60)}]"
        if mode == "eval":
            pr = f"{system_prompt}\n\n{chunk_time_hint}\n{EVAL_CHUNK_PROMPT}".strip()
            schema = None
        else:
            pr = f"{system_prompt}\n\n{chunk_time_hint}\nقم بتفريغ وتصنيف كل الفقرات والبرامج بدقة زمنية كاملة دون اختزال.".strip()
            schema = DAILY_SCHEMA

        run_strat = "pro_only" if force_paid else strat
        try:
            data, cost, used_model, tier = call_gemini_chunk(
                mp4_path, pr, schema, valid_free, valid_paid, paid_models, run_strat, exhausted_pairs, bad_keys
            )
            print(f"✅ Chunk {idx+1}/{tot} done via {tier} ({used_model}) | cost=${cost:.4f}", flush=True)
            return data, cost
        finally:
            if os.path.exists(mp4_path):
                try:
                    os.remove(mp4_path)
                except Exception:
                    pass

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_chunk, i, False): i for i in range(tot)}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                data, c = fut.result()
                results[i] = {"chunk_index": i + 1, "data": data}
                with STATE_LOCK:
                    act_cost += c
            except Exception as e:
                failed.append(i)
                print(f"⚠️ Chunk {i+1} Failed: {e}", flush=True)

    # DLQ Rescue (before cleaning up local source video)
    if failed:
        print(f"Entering Dead-Letter Queue (DLQ) for {len(failed)} chunks...", flush=True)
        time.sleep(10)
        for i in failed:
            try:
                use_paid = (strat != "flash_only") and (valid_paid is not None)
                data, c = process_chunk(i, force_paid=use_paid)
                results[i] = {"chunk_index": i + 1, "data": data}
                with STATE_LOCK:
                    act_cost += c
                print(f"🚑 Chunk {i+1} Rescued.", flush=True)
            except Exception as ex:
                print(f"❌ DLQ Rescue Failed for chunk {i+1}: {ex}", flush=True)
                results[i] = None
                time.sleep(4)

    if downloaded_local and os.path.exists(local_input):
        try:
            os.remove(local_input)
        except Exception:
            pass

    valid_chunks = [r for r in results if r is not None and r.get("data")]
    if not valid_chunks:
        send_tg("❌ فشلت جميع المقاطع ولم يتم إنتاج أي تحليل. تم استرداد الرصيد المحجوز.", tg_bot, tg_chat)
        refund_webhook(cfg, 0.0)
        sys.exit(1)

    # STEP 3: SYNTHESIS & REPORT GENERATION
    out_files = []
    final_data = {}

    if mode == "eval":
        synth_candidates = []
        if strat != "flash_only" and valid_paid:
            synth_order = rank_discovered_models(paid_models, PREFERRED_SYNTH_ORDER, allow_pro=True) or paid_models
            for m in synth_order:
                synth_candidates.append((valid_paid, m, True))
        for k, kmodels in valid_free:
            for m in kmodels:
                synth_candidates.append((k, m, False))

        synth_prompt = EVAL_SYNTHESIS_PROMPT.format(editorial=cfg.get("editorial_mode", "directed")) + json.dumps(valid_chunks, ensure_ascii=False)
        for skey, smodel, spaid in synth_candidates:
            try:
                client = genai.Client(api_key=skey)
                res = client.models.generate_content(
                    model=smodel,
                    contents=synth_prompt,
                    config=build_gen_config(None),
                )
                final_data = clean_json_text(res.text)
                act_cost += calc_cost(smodel, res.usage_metadata, spaid)
                print(f"✅ Synthesis complete via {'PAID' if spaid else 'FREE'} ({smodel})", flush=True)
                break
            except Exception as e:
                print(f"[Synthesis Retry {smodel}] {str(e)[:160]}", flush=True)

        if not final_data:
            final_data = {"total_score": 0, "percentage": 0, "strengths": [], "priorities": [], "sub_criteria": []}

        h_path = f"/tmp/Eval_{cfg.get('presenter_name', 'Presenter')}.html"
        with open(h_path, "w", encoding="utf-8") as f:
            f.write(build_eval_html(final_data, cfg.get("presenter_name", ""), act_cost))
        out_files.append(h_path)
    else:
        h_path = "/tmp/Daily_Report.html"
        with open(h_path, "w", encoding="utf-8") as f:
            f.write(build_daily_html(valid_chunks, act_cost))
        out_files.append(h_path)

    meta_info = {
        "name": cfg.get("presenter_name", "Broadcast") if mode == "eval" else "Daily_Broadcast",
        "mode": mode,
        "system_prompt": system_prompt,
    }
    bb_path = f"/tmp/Blackbox_{mode}.md"
    with open(bb_path, "w", encoding="utf-8") as f:
        f.write(generate_blackbox_md(mode, valid_chunks, final_data, meta_info))
    out_files.append(bb_path)

    # STEP 4: DELIVERY & WALLET RECONCILIATION
    for fpath in out_files:
        send_tg(f"📄 {os.path.basename(fpath)} | المقاطع الناجحة: {len(valid_chunks)}/{tot} | الكلفة: ${act_cost:.4f}", tg_bot, tg_chat, fpath)
        mime = "text/markdown" if fpath.endswith(".md") else "text/html"
        drive_upload(fid, d_tok, fpath, mime)

    send_smtp(
        f"تقرير V5 ({mode}) - {len(valid_chunks)}/{tot}",
        f"التقرير جاهز ومرفق. الكلفة الفعلية: ${act_cost:.4f}",
        out_files,
        cfg.get("alert_emails", ""),
        gm_u,
        gm_p,
    )

    try:
        requests.patch(
            f"https://www.googleapis.com/drive/v3/files/{fid}",
            headers={"Authorization": f"Bearer {d_tok}"},
            json={"name": f"PROCESSED_{mode}_{act_cost:.2f}$"},
            timeout=15,
        )
    except Exception:
        pass

    refund_webhook(cfg, act_cost)
    print(f"🎉 Pipeline completed successfully! Chunks: {len(valid_chunks)}/{tot} | Total Cost: ${act_cost:.4f}", flush=True)


if __name__ == "__main__":
    main()
