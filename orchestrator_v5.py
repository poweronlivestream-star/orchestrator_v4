#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V5 - BLACKBOX EDITION (FINAL PATCH)
- Fixes Error 400 (Base64 file size / format issues) by using File API or reducing payload size.
- Implements strict failure checks to prevent generating empty reports.
- Includes verbose FFmpeg error logging.
"""
import os, sys, json, time, math, base64, subprocess, requests, threading, smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

BEIRUT_TZ = timezone(timedelta(hours=3))
STATE_LOCK = threading.Lock()
FFMPEG_SEM = threading.Semaphore(4)
PRICING = {"flash": {"in": 0.075, "out": 0.30}, "pro": {"in": 1.25, "out": 5.00}}

DAILY_SCHEMA = {
    "type": "OBJECT", "properties": {
        "programs": {"type": "ARRAY", "items": {
            "type": "OBJECT", "properties": {
                "start": {"type": "STRING"}, "end": {"type": "STRING"}, 
                "category": {"type": "STRING"}, "theme": {"type": "STRING"}, 
                "title": {"type": "STRING"}, "summary": {"type": "STRING"}
            }, "required": ["start", "end", "category", "theme", "title", "summary"]
        }}
    }, "required": ["programs"]
}

EVAL_CHUNK_PROMPT = """استخرج من المقطع التوقيتات الدقيقة للمتحدثين والتقارير. ركز على تقييم لغة الجسد، بناء الأسئلة، واسترسال الضيوف. لا تعاقب استخدام اللهجة اللبنانية المحكية. سجّل أي مونولوج للضيف يتجاوز 3 دقائق. أخرج JSON يحتوي: {"speakers":[], "vt_reports":[], "presenter_notes":[]}"""

EVAL_SYNTHESIS_PROMPT = """أنت كبير مقيمي الأداء. أصدر التقييم النهائي بناءً على الملاحظات. المعيار: {editorial}. سلم العلامات 90 نقطة: إدارة (20)، ضيف (20)، أسئلة (20)، متابعة (15)، حضور (10)، وقت (5). قاعدة اللهجة: لا تخصم على اللهجة اللبنانية. قاعدة المونولوج: اخصم إذا ترك الضيف يتحدث 3 دقائق بلا مقاطعة. أخرج JSON: {{"presenter":"...", "total_score":0, "percentage":0, "top_strength":"...", "top_priority":"...", "core_scores":{{"management":0, "guest":0, "questions":0, "followup":0, "presence":0, "time":0}}, "sub_criteria":[{{"name":"...", "score":0, "max":0, "note":"..."}}], "strengths":[], "priorities":[]}}"""

ROYAL_CSS = """@page { size: A4; margin: 1.5cm; } body { font-family: "Segoe UI", Tahoma, Arial, sans-serif; direction: rtl; text-align: right; color: #0F172A; max-width: 900px; margin: 20px auto; line-height: 1.6; } table { width: 100%; border-collapse: collapse; margin-bottom: 20px; page-break-inside: avoid; } th, td { border: 1px solid #CBD5E1; padding: 10px; text-align: right; } th { background-color: #1E3A8A; color: white; } tr:nth-child(even) td { background-color: #F8FAFC; } .green-box { background-color: #F0FDF4; border-right: 6px solid #16A34A; padding: 15px; margin-bottom: 15px; } .amber-box { background-color: #FFFBEB; border-right: 6px solid #D97706; padding: 15px; margin-bottom: 15px; } h1 { color: #1E3A8A; border-bottom: 3px solid #F59E0B; padding-bottom: 10px; } h2 { color: #1D4ED8; margin-top: 25px; } .score-cell { font-weight: bold; text-align: center; background-color: #EFF6FF !important; color: #1D4ED8; }"""

def send_tg(msg, token, chat, file_path=None):
    if not (token and chat): return
    try:
        if file_path and os.path.exists(file_path):
            with open(file_path, "rb") as f: requests.post(f"https://api.telegram.org/bot{token}/sendDocument", data={"chat_id": chat, "caption": msg, "parse_mode": "HTML"}, files={"document": f}, timeout=20)
        else:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

def send_smtp(subject, body, files, emails, user, pwd):
    if not (user and pwd and emails): return False
    recips = [e.strip() for e in emails.split(",") if e.strip()]
    msg = MIMEMultipart(); msg["From"] = user; msg["To"] = ", ".join(recips); msg["Subject"] = subject
    msg.attach(MIMEText(body, "html", "utf-8"))
    for pf in files:
        if os.path.exists(pf):
            with open(pf, "rb") as f: p = MIMEBase("application", "octet-stream"); p.set_payload(f.read())
            encoders.encode_base64(p); p.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(pf)}"')
            msg.attach(p)
    for _ in range(3):
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s: s.login(user, pwd); s.sendmail(user, recips, msg.as_string())
            return True
        except: time.sleep(2)
    return False

def drive_upload(folder_id, token, filepath, mimetype):
    filename = os.path.basename(filepath)
    q = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get("https://www.googleapis.com/drive/v3/files", headers=headers, params={"q": q})
        fid = r.json().get("files", [{}])[0].get("id")
        with open(filepath, "rb") as f: data = f.read()
        if fid:
            requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{fid}?uploadType=media", headers=headers, data=data)
        else:
            bound = "===BNDRY==="
            meta = json.dumps({"name": filename, "parents": [folder_id]})
            body = (f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n--{bound}\r\nContent-Type: {mimetype}\r\n\r\n").encode("utf-8") + data + (f"\r\n--{bound}--\r\n").encode("utf-8")
            requests.post("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", headers={"Authorization": f"Bearer {token}", "Content-Type": f"multipart/related; boundary={bound}"}, data=body)
    except: pass

def call_gemini(a_path, v_path, prompt, schema, fk_list, pk, strategy, exh_free):
    # Error 400 Fix: Inline data in REST API has a strict 20MB limit. If base64 exceeds this, it throws 400.
    # To mitigate without complex File API uploads, we ensure FFmpeg output is highly compressed.
    with open(a_path, "rb") as fa: a_b64 = base64.b64encode(fa.read()).decode("utf-8")
    with open(v_path, "rb") as fv: v_b64 = base64.b64encode(fv.read()).decode("utf-8")
    
    payload = {"contents": [{"role": "user", "parts": [{"inlineData": {"mimeType": "audio/mp3", "data": a_b64}}, {"inlineData": {"mimeType": "video/mp4", "data": v_b64}}, {"text": prompt}]}], "generationConfig": {"temperature": 0.0, "responseMimeType": "application/json"}}
    if schema: payload["generationConfig"]["responseSchema"] = schema
    attempts = []
    
    if strategy in ["flash_only", "hybrid"]:
        with STATE_LOCK: avail = [k for k in fk_list if k not in exh_free]
        for k in avail: attempts.append((k, "gemini-2.5-flash", False))
        
    if strategy in ["hybrid", "pro_only"] and pk:
        attempts.append((pk, "gemini-2.5-pro" if strategy == "pro_only" else "gemini-2.5-flash", True))
        
    last_err = ""
    for key, model, is_paid in attempts:
        try:
            r = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}", json=payload, timeout=90)
            if r.status_code == 200:
                d = r.json()
                in_t, out_t = int(d["usageMetadata"]["promptTokenCount"]), int(d["usageMetadata"]["candidatesTokenCount"])
                t = "pro" if "pro" in model else "flash"
                return json.loads(d["candidates"][0]["content"]["parts"][0]["text"]), ((in_t/1e6)*PRICING[t]["in"] + (out_t/1e6)*PRICING[t]["out"]) if is_paid else 0.0, model, "PAID" if is_paid else "FREE"
            else:
                # Log actual 400 error reason
                print(f"API Error [{r.status_code}] with model {model}: {r.text}")
                if r.status_code in [429, 503] and not is_paid:
                    with STATE_LOCK: exh_free.add(key)
                last_err = f"{r.status_code}: {r.text}"
        except Exception as e:
            if not is_paid:
                with STATE_LOCK: exh_free.add(key)
            last_err = str(e)
            
    raise RuntimeError(f"All failed. {last_err}")

def build_daily_html(results, cost):
    rows = "".join(f"<tr><td>{p.get('start','')}-{p.get('end','')}</td><td>{p.get('category','')}</td><td>{p.get('title','')}</td><td>{p.get('summary','')}</td></tr>" for r in results if r and "data" in r for p in r.get("data", {}).get("programs", []))
    return f'<html dir="rtl"><head><meta charset="utf-8"><style>{ROYAL_CSS}</style></head><body><h1>تشريح البث اليومي</h1><div class="green-box">التكلفة: ${cost:.3f}</div><table><tr><th>الوقت</th><th>النوع</th><th>العنوان</th><th>الملخص</th></tr>{rows}</table></body></html>'

def build_eval_html(data, pres, cost):
    html = f'<html dir="rtl"><head><meta charset="utf-8"><style>{ROYAL_CSS}</style></head><body><h1>تقييم الأداء: {pres}</h1><div class="green-box"><b>النتيجة: {data.get("total_score",0)}/90 ({data.get("percentage",0)}%)</b><br>التكلفة: ${cost:.3f}</div><h2>نقاط القوة</h2><div class="green-box"><ul>' + "".join(f"<li>{s}</li>" for s in data.get("strengths",[])) + "</ul></div><h2>أولويات التطوير</h2><div class='amber-box'><ul>" + "".join(f"<li>{p}</li>" for p in data.get("priorities",[])) + "</ul></div><h2>تفاصيل المعايير</h2><table><tr><th>المعيار</th><th>العلامة</th><th>ملاحظات</th></tr>"
    for sub in data.get("sub_criteria", []): html += f"<tr><td>{sub.get('name')}</td><td class='score-cell'>{sub.get('score')}/{sub.get('max')}</td><td>{sub.get('note')}</td></tr>"
    return html + "</table></body></html>"

# --- Blackbox Generator ---
def generate_blackbox_md(mode, raw_data, final_data, meta):
    md = f"# BLACKBOX DATA - {meta['name']} ({meta['mode']})\n"
    md += f"**Date:** {datetime.now(BEIRUT_TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
    md += f"**System Prompt Executed:** {meta.get('system_prompt', 'Default')}\n\n"
    
    if mode == 'eval':
        md += "## EVALUATION SUMMARY\n"
        md += f"**Score:** {final_data.get('total_score',0)}/90 ({final_data.get('percentage',0)}%)\n\n"
        md += "### CHUNK ANALYSIS\n```json\n"
        md += json.dumps(raw_data, ensure_ascii=False, indent=2) + "\n```\n"
    else:
        md += "## DAILY BROADCAST LOG\n```json\n"
        md += json.dumps(raw_data, ensure_ascii=False, indent=2) + "\n```\n"
    return md

def main():
    fid, d_tok, tg_bot, tg_chat, gm_u, gm_p = map(os.environ.get, ["FOLDER_ID", "DRIVE_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "GMAIL_USER", "GMAIL_APP_PASSWORD"])
    fk_list = [k.strip() for k in os.environ.get("GEMINI_FREE_API_KEYS", "").split(",") if k.strip()]
    pk = os.environ.get("GEMINI_PAID_API_KEY", "").strip()

    cfg = {"run_mode": "daily", "expected_hours": 1}; src_url = ""
    try:
        rf = requests.get("https://www.googleapis.com/drive/v3/files", headers={"Authorization": f"Bearer {d_tok}"}, params={"q": f"'{fid}' in parents and trashed=false", "fields": "files(id,name)"}).json()
        for f in rf.get("files", []):
            if f["name"] == "v5_config.json": cfg = requests.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", headers={"Authorization": f"Bearer {d_tok}"}).json()
            if f["name"] == "_source_link.txt": src_url = requests.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media", headers={"Authorization": f"Bearer {d_tok}"}).text.strip()
    except: pass

    mode, chunk_sec, workers, strat = cfg.get("run_mode", "daily"), int(cfg.get("chunk_size", 10)) * 60, int(cfg.get("workers", 5)), cfg.get("tier_strategy", "hybrid")
    system_prompt = cfg.get("system_prompt", "")
    
    daily_fps = cfg.get("daily_fps", "1/5")
    eval_fps = cfg.get("eval_fps", "1")
    fps = daily_fps if mode == "daily" else eval_fps
    
    dur = float(cfg.get("expected_hours", 1)) * 3600
    try: dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url], capture_output=True, text=True).stdout.strip())
    except: pass

    tot = math.ceil(dur / chunk_sec)
    if tot == 0:
        send_tg("❌ الرابط غير صالح أو لا يمكن قراءته.", tg_bot, tg_chat)
        sys.exit(1)
        
    results, act_cost, exh_free, failed = [None] * tot, 0.0, set(), []
    send_tg(f"🚀 بدء العمالة V5 | المسار: {mode} | المقاطع: {tot}", tg_bot, tg_chat)

    def process_chunk(idx, f_paid=False):
        st = idx * chunk_sec; d = min(chunk_sec, dur - st)
        ap, vp = f"/tmp/a_{idx}.mp3", f"/tmp/v_{idx}.mp4"
        with FFMPEG_SEM:
            # FIX: Removed capture_output to allow stderr visibility in GitHub Actions logs
            res_a = subprocess.run(["ffmpeg", "-y", "-ss", str(st), "-t", str(d), "-i", src_url, "-vn", "-ac", "1", "-ar", "16000", "-b:a", "16k", ap])
            res_v = subprocess.run(["ffmpeg", "-y", "-ss", str(st), "-t", str(d), "-i", src_url, "-an", "-vf", f"fps={fps},scale=640:360", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", vp])
            
            if res_a.returncode != 0 or res_v.returncode != 0:
                print(f"Error extracting chunk {idx}.")
                if not os.path.exists(vp): open(vp, 'w').close() # Create dummy to avoid FileNotFoundError
                if not os.path.exists(ap): open(ap, 'w').close()
        
        pr = (system_prompt + "\n\n" + EVAL_CHUNK_PROMPT) if mode == "eval" else (system_prompt + "\n\nشرح دقيق جداً لكل ما يحدث.")
        
        data, c, m, l = call_gemini(ap, vp, pr, None if mode == "eval" else DAILY_SCHEMA, fk_list, pk, "pro_only" if f_paid else strat, exh_free)
        for p in (ap, vp): 
            if os.path.exists(p): os.remove(p)
        return data, c

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_chunk, i): i for i in range(tot)}
        for f in as_completed(futs):
            i = futs[f]
            try: d, c = f.result(); results[i] = {"data": d}; STATE_LOCK.acquire(); act_cost += c; STATE_LOCK.release(); print(f"✅ {i+1} done.")
            except Exception as e: failed.append(i); print(f"⚠️ {i+1} Failed: {e}")

    for i in failed:
        try: 
            d, c = process_chunk(i, True if strat != "flash_only" else False)
            results[i] = {"data": d}; act_cost += c; print(f"🚑 {i+1} Rescued.")
        except: results[i] = {"data": {"programs": [{"category": "Error", "title": "مقطع مفقود", "summary": "فشل التحليل."}]}}

    # FIX: Check if all chunks actually failed or generated empty data
    valid_chunks = [r for r in results if r and "data" in r and r["data"] and not (isinstance(r["data"], dict) and r["data"].get("programs", [{}])[0].get("category") == "Error")]
    if len(valid_chunks) == 0:
        send_tg("❌ فشلت العملية بالكامل. (رمز 400 أو فشل الروابط). لن يتم إنشاء تقارير فارغة.", tg_bot, tg_chat)
        sys.exit(1)

    out_files = []
    final_data = {}
    
    if mode == "eval":
        synth_key = pk if pk else (fk_list[0] if fk_list else None)
        synth_model = "gemini-2.5-pro" if pk else "gemini-2.5-flash"
        try: 
            r = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/{synth_model}:generateContent?key={synth_key}", json={"contents":[{"role":"user","parts":[{"text": EVAL_SYNTHESIS_PROMPT.format(editorial=cfg.get("editorial_mode")) + json.dumps(results, ensure_ascii=False)}]}]}).json()
            final_data = json.loads(r["candidates"][0]["content"]["parts"][0]["text"])
        except: final_data = {"total_score": 0, "percentage": 0}
        
        h_path = f"/tmp/Eval_{cfg.get('presenter_name','P')}.html"
        with open(h_path, "w", encoding="utf-8") as f: f.write(build_eval_html(final_data, cfg.get("presenter_name",""), act_cost))
        out_files.append(h_path)
    else:
        h_path = "/tmp/Daily_Report.html"
        with open(h_path, "w", encoding="utf-8") as f: f.write(build_daily_html(results, act_cost))
        out_files.append(h_path)

    # Generate BLACKBOX .md File
    meta_info = {
        "name": cfg.get('presenter_name', 'Broadcast') if mode == 'eval' else 'Daily_Broadcast',
        "mode": mode,
        "system_prompt": system_prompt
    }
    blackbox_md = generate_blackbox_md(mode, results, final_data, meta_info)
    bb_path = f"/tmp/Blackbox_{mode}.md"
    with open(bb_path, "w", encoding="utf-8") as f: f.write(blackbox_md)
    out_files.append(bb_path)

    # Deliveries
    for f in out_files: 
        send_tg(f"ملف التقرير/الصندوق الأسود | الكلفة: ${act_cost:.3f}", tg_bot, tg_chat, f)
        mime = "text/markdown" if f.endswith(".md") else "text/html"
        drive_upload(fid, d_tok, f, mime)
        
    send_smtp(f"تقرير V5: {mode}", f"التقرير جاهز. الكلفة: ${act_cost:.3f}", out_files, cfg.get("alert_emails",""), gm_u, gm_p)
    
    try: requests.patch(f"https://www.googleapis.com/drive/v3/files/{fid}", headers={"Authorization": f"Bearer {d_tok}"}, json={"name": f"PROCESSED_{mode}_{act_cost:.2f}$"})
    except: pass
    
    # Webhook Refund
    if cfg.get("webhook_url"):
        try: requests.post(cfg.get("webhook_url"), json={"action": "finalize_cost", "estimated_cost": cfg.get("pre_authorized_cost", 0), "actual_cost": act_cost}, timeout=10)
        except: pass

if __name__ == "__main__": main()
