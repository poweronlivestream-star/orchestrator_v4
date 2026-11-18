#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V5 - HYBRID DOWNLOAD EDITION
- For daily broadcasts (long files): Uses robust FFmpeg streaming with DLQ and 503 backoffs.
- For eval mode (short files): Downloads the full video to the local runner first, 
  extracts all chunks locally without dropped connections, and deletes the local file to save space.
- Uses official google-generativeai SDK for proper large file handling (Fixes REST API 400s).
- Bulletproof Fallback: Guarantees fallback to Paid Pro key if free keys fail.
"""
import os, sys, json, time, math, subprocess, requests, threading, smtplib, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

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

def get_model(api_key, model_name, is_json, schema=None):
    genai.configure(api_key=api_key)
    safe = {HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE, HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE, HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE, HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE}
    cfg = {"temperature": 0.0}
    if is_json: 
        cfg["response_mime_type"] = "application/json"
        if schema: cfg["response_schema"] = schema
    return genai.GenerativeModel(model_name=model_name, generation_config=cfg, safety_settings=safe)

def call_gemini(a_path, v_path, prompt, schema, fk_list, pk, strategy, exh_free):
    free_attempts = []
    if strategy in ["flash_only", "hybrid"]:
        with STATE_LOCK: 
            avail = [k for k in fk_list if k not in exh_free]
        for k in avail: 
            free_attempts.append((k, "gemini-1.5-flash"))

    last_err = ""
    
    # 1. ATTEMPT FREE KEYS
    for key, model_name in free_attempts:
        if not key: continue
        uploaded_files = []
        try:
            genai.configure(api_key=key)
            audio_file = genai.upload_file(path=a_path)
            video_file = genai.upload_file(path=v_path)
            uploaded_files.extend([audio_file, video_file])
            
            while video_file.state.name == "PROCESSING":
                time.sleep(2)
                video_file = genai.get_file(video_file.name)
            if video_file.state.name == "FAILED": raise ValueError("Video processing failed.")

            model = get_model(key, model_name, True, schema)
            res = model.generate_content([audio_file, video_file, prompt])
            
            data = json.loads(res.text)
            for uf in uploaded_files:
                try: uf.delete()
                except: pass
            
            return data, 0.0, model_name, "FREE"
            
        except Exception as e:
            err_str = str(e).lower()
            print(f"Free SDK Error: {err_str}")
            if "api_key_invalid" in err_str or "400" in err_str or "429" in err_str or "503" in err_str:
                with STATE_LOCK: exh_free.add(key)
            if "503" in err_str or "429" in err_str: 
                time.sleep(random.uniform(2, 5))
            
            for uf in uploaded_files:
                try: uf.delete()
                except: pass
            last_err = str(e)
            
    # 2. GUARANTEED FALLBACK TO PAID KEY
    if strategy in ["hybrid", "pro_only"] and pk:
        print("⚠️ All free keys exhausted or invalid. Rescuing with Paid PRO Key.")
        uploaded_files = []
        try:
            genai.configure(api_key=pk)
            audio_file = genai.upload_file(path=a_path)
            video_file = genai.upload_file(path=v_path)
            uploaded_files.extend([audio_file, video_file])
            
            while video_file.state.name == "PROCESSING":
                time.sleep(2)
                video_file = genai.get_file(video_file.name)
            if video_file.state.name == "FAILED": raise ValueError("Video processing failed.")

            model = get_model(pk, "gemini-1.5-pro", True, schema)
            res = model.generate_content([audio_file, video_file, prompt])
            
            in_t, out_t = res.usage_metadata.prompt_token_count, res.usage_metadata.candidates_token_count
            cost = ((in_t/1e6)*PRICING["pro"]["in"] + (out_t/1e6)*PRICING["pro"]["out"])
            
            data = json.loads(res.text)
            for uf in uploaded_files:
                try: uf.delete()
                except: pass
            
            return data, cost, "gemini-1.5-pro", "PAID"
            
        except Exception as e:
            for uf in uploaded_files:
                try: uf.delete()
                except: pass
            last_err = str(e)
            
    raise RuntimeError(f"All attempts (Free and Paid) failed. Last Error: {last_err}")

def build_daily_html(results, cost):
    rows = "".join(f"<tr><td>{p.get('start','')}-{p.get('end','')}</td><td>{p.get('category','')}</td><td>{p.get('title','')}</td><td>{p.get('summary','')}</td></tr>" for r in results if r and "data" in r for p in r.get("data", {}).get("programs", []))
    return f'<html dir="rtl"><head><meta charset="utf-8"><style>{ROYAL_CSS}</style></head><body><h1>تشريح البث اليومي</h1><div class="green-box">التكلفة: ${cost:.3f}</div><table><tr><th>الوقت</th><th>النوع</th><th>العنوان</th><th>الملخص</th></tr>{rows}</table></body></html>'

def build_eval_html(data, pres, cost):
    html = f'<html dir="rtl"><head><meta charset="utf-8"><style>{ROYAL_CSS}</style></head><body><h1>تقييم الأداء: {pres}</h1><div class="green-box"><b>النتيجة: {data.get("total_score",0)}/90 ({data.get("percentage",0)}%)</b><br>التكلفة: ${cost:.3f}</div><h2>نقاط القوة</h2><div class="green-box"><ul>' + "".join(f"<li>{s}</li>" for s in data.get("strengths",[])) + "</ul></div><h2>أولويات التطوير</h2><div class='amber-box'><ul>" + "".join(f"<li>{p}</li>" for p in data.get("priorities",[])) + "</ul></div><h2>تفاصيل المعايير</h2><table><tr><th>المعيار</th><th>العلامة</th><th>ملاحظات</th></tr>"
    for sub in data.get("sub_criteria", []): html += f"<tr><td>{sub.get('name')}</td><td class='score-cell'>{sub.get('score')}/{sub.get('max')}</td><td>{sub.get('note')}</td></tr>"
    return html + "</table></body></html>"

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
    fps = cfg.get("daily_fps", "1/5") if mode == "daily" else cfg.get("eval_fps", "1")
    
    dur = float(cfg.get("expected_hours", 1)) * 3600
    try: dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url], capture_output=True, text=True).stdout.strip())
    except: pass

    tot = math.ceil(dur / chunk_sec)
    if tot == 0:
        send_tg("❌ الرابط غير صالح أو لا يمكن قراءته.", tg_bot, tg_chat)
        sys.exit(1)
        
    results, act_cost, exh_free, failed = [None] * tot, 0.0, set(), []
    
    # --- HYBRID DOWNLOAD LOGIC ---
    # If mode is eval, we download the file locally to avoid network drops during extraction.
    local_dvr_path = src_url
    if mode == "eval":
        send_tg("📥 جاري تحميل الحلقة محلياً لضمان عدم انقطاع التقطيع...", tg_bot, tg_chat)
        local_dvr_path = "/tmp/local_eval_video.mp4"
        try:
            # Download file using requests to handle streams smoothly
            with requests.get(src_url, stream=True) as r:
                r.raise_for_status()
                with open(local_dvr_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            send_tg(f"✅ تم التحميل محلياً. بدء التقطيع المتوازي: {workers}x", tg_bot, tg_chat)
        except Exception as e:
            send_tg(f"⚠️ فشل التحميل المحلي، سيتم العودة للبث المباشر. الخطأ: {e}", tg_bot, tg_chat)
            local_dvr_path = src_url # Fallback to live stream url if download fails
    else:
        send_tg(f"🚀 بدء العمالة V5 المباشرة (Streaming) | المسار: {mode} | المقاطع: {tot} | التوازي: {workers}x", tg_bot, tg_chat)

    def process_chunk(idx, f_paid=False):
        time.sleep(random.uniform(0.5, 3.0) * (idx % workers))
        
        st = idx * chunk_sec; d = min(chunk_sec, dur - st)
        ap, vp = f"/tmp/a_{idx}.mp3", f"/tmp/v_{idx}.mp4"
        
        # 🔴 ROBUST FFMPEG RETRY LOGIC FOR DROPPED STREAMS
        extracted = False
        for attempt in range(3):
            with FFMPEG_SEM:
                subprocess.run(["ffmpeg", "-y", "-ss", str(st), "-t", str(d), "-i", local_dvr_path, "-vn", "-ac", "1", "-ar", "16000", "-b:a", "16k", ap], stderr=subprocess.DEVNULL)
                subprocess.run(["ffmpeg", "-y", "-ss", str(st), "-t", str(d), "-i", local_dvr_path, "-an", "-vf", f"fps={fps},scale=640:360", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", vp], stderr=subprocess.DEVNULL)
            
            if os.path.exists(vp) and os.path.getsize(vp) > 1024 and os.path.exists(ap) and os.path.getsize(ap) > 1024:
                extracted = True
                break
            else:
                print(f"FFmpeg returned empty/corrupted file for chunk {idx}. Retrying...")
                time.sleep(2)
                
        if not extracted:
            print(f"FAILED to extract chunk {idx} after 3 attempts.")
            return {"programs": [{"start": "N/A", "end": "N/A", "category": "Error", "theme": "فصل تقني", "title": "مقطع مفقود", "summary": "انقطع البث أثناء التحميل"}]}, 0.0
        
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

    # Clean up local file if we downloaded it
    if mode == "eval" and local_dvr_path != src_url and os.path.exists(local_dvr_path):
        try: os.remove(local_dvr_path)
        except: pass

    if failed:
        print(f"Entering Dead-Letter Queue (DLQ) for {len(failed)} chunks...")
        time.sleep(15) 
        
    for i in failed:
        try:
            d, c = process_chunk(i, True if strat != "flash_only" else False)
            results[i] = {"data": d}; act_cost += c; print(f"🚑 {i+1} Rescued.")
        except Exception as ex: 
            print(f"DLQ Rescue Failed for chunk {i+1}: {ex}")
            results[i] = None 
            time.sleep(5) 

    valid_chunks = [r for r in results if r is not None and "data" in r and r["data"] and not (isinstance(r["data"], dict) and r["data"].get("programs", [{}])[0].get("category") == "Error")]
    
    if len(valid_chunks) == 0:
        send_tg("❌ فشلت العملية بالكامل. (انقطاع البث أو أخطاء مفاتيح). تم استرداد الرصيد.", tg_bot, tg_chat)
        if cfg.get("webhook_url"):
            try: requests.post(cfg.get("webhook_url"), json={"action": "finalize_cost", "estimated_cost": cfg.get("pre_authorized_cost", 0), "actual_cost": 0.0}, timeout=10)
            except: pass
        sys.exit(1)

    out_files = []
    final_data = {}
    
    if mode == "eval":
        synth_key = pk if pk else (fk_list[0] if fk_list else None)
        synth_model = "gemini-1.5-pro" if pk else "gemini-1.5-flash"
        try: 
            m = get_model(synth_key, synth_model, True)
            r = m.generate_content(EVAL_SYNTHESIS_PROMPT.format(editorial=cfg.get("editorial_mode")) + json.dumps(valid_chunks, ensure_ascii=False))
            final_data = json.loads(r.text)
        except: final_data = {"total_score": 0, "percentage": 0}
        
        h_path = f"/tmp/Eval_{cfg.get('presenter_name','P')}.html"
        with open(h_path, "w", encoding="utf-8") as f: f.write(build_eval_html(final_data, cfg.get("presenter_name",""), act_cost))
        out_files.append(h_path)
    else:
        h_path = "/tmp/Daily_Report.html"
        with open(h_path, "w", encoding="utf-8") as f: f.write(build_daily_html(valid_chunks, act_cost))
        out_files.append(h_path)

    meta_info = {
        "name": cfg.get('presenter_name', 'Broadcast') if mode == 'eval' else 'Daily_Broadcast',
        "mode": mode,
        "system_prompt": system_prompt
    }
    blackbox_md = generate_blackbox_md(mode, valid_chunks, final_data, meta_info)
    bb_path = f"/tmp/Blackbox_{mode}.md"
    with open(bb_path, "w", encoding="utf-8") as f: f.write(blackbox_md)
    out_files.append(bb_path)

    for f in out_files: 
        send_tg(f"ملف التقرير/الصندوق الأسود | الكلفة: ${act_cost:.3f}", tg_bot, tg_chat, f)
        mime = "text/markdown" if f.endswith(".md") else "text/html"
        drive_upload(fid, d_tok, f, mime)
        
    send_smtp(f"تقرير V5: {mode}", f"التقرير جاهز. الكلفة: ${act_cost:.3f}", out_files, cfg.get("alert_emails",""), gm_u, gm_p)
    
    try: requests.patch(f"https://www.googleapis.com/drive/v3/files/{fid}", headers={"Authorization": f"Bearer {d_tok}"}, json={"name": f"PROCESSED_{mode}_{act_cost:.2f}$"})
    except: pass
    
    if cfg.get("webhook_url"):
        try: requests.post(cfg.get("webhook_url"), json={"action": "finalize_cost", "estimated_cost": cfg.get("pre_authorized_cost", 0), "actual_cost": act_cost}, timeout=10)
        except: pass

if __name__ == "__main__": main()
