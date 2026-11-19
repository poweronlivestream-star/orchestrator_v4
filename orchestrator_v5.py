#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Orchestrator V5.7 - COMPLETE PRODUCTION ENGINE
Upgrades & Fixes:
1. Restores full evaluate.py 90-point rubric (6 core + 23 sub-criteria), Directed & Neutral
   editorial modes, Lebanese dialect rule, monologue >3m rule, verbatim last_spoken_words,
   and BOTH Detailed + Summarized Word-ready RTL HTML reports (Royal Executive Theme).
2. Preserves presenter/episode name in renamed Drive folder (`PROCESSED_<name>_$<cost>`)
   and preserves source URL + full telemetry in folder description.
3. Generates an ultra-detailed, self-contained Forensic Blackbox (`03_Blackbox_Forensic_<name>.md`)
   including rubric specification, chunk-by-chunk chronological tables, verbatim quotes,
   visual/body-language log, monologue audit, vocal log, and raw JSON appendix so any
   external AI (DeepSeek, ChatGPT, Claude) can analyze it with zero prior context.
4. Bulletproof delivery:
   - Auto-refreshes expired Google Drive OAuth tokens (fixes 60-min expiry on 18h runs).
   - Dual-port SMTP (465 SSL -> 587 STARTTLS) + Apps Script GmailApp fallback (fixes SMTP drops).
   - Local `./backup_reports/` export for GitHub Actions Artifact backup + Telegram documents.
5. Supports direct DVR/MP4 links, Google Drive file links, and YouTube links (yt-dlp + fallback).
6. Fast parallel key validation, key deduplication, 1-attempt-per-model fast failover on 503,
   round-robin across free keys, and per-episode budget ceiling enforcement.
"""
import os
import re
import sys
import json
import time
import math
import random
import shutil
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

try:
    from google import genai
    from google.genai import types
except ModuleNotFoundError:
    print("⚙️ google-genai not found — installing automatically...", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--upgrade", "google-genai"])
    from google import genai
    from google.genai import types

BEIRUT_TZ = timezone(timedelta(hours=3))
STATE_LOCK = threading.Lock()
FFMPEG_SEM = threading.Semaphore(3)
CANCEL_EVENT = threading.Event()
DRIVE_TOKEN_LOCK = threading.Lock()
BACKUP_DIR = os.path.abspath("./backup_reports")
os.makedirs(BACKUP_DIR, exist_ok=True)

# Active Gemini 3.x / 4.x priority order
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

# Runtime mutable token state (supports auto-refresh on 401)
RUNTIME_DRIVE_TOKEN = ""

# ==============================================================================
# EDITORIAL RULES & FULL 90-POINT RUBRIC (Restored verbatim from evaluate.py)
# ==============================================================================
EDITORIAL_RULE_DIRECTED = """3. مراعاة الخط التحريري وسياق القناة (قاعدة جوهرية لا يجوز مخالفتها):
   - يُجرى التقييم مع الأخذ في الحسبان الهوية التحريرية للقناة (إعلام داعم ومتبنٍّ لخط المقاومة).
   - يُمنع منعاً باتاً خصم علامات أو انتقاد المقدم بسبب تبنيه لخط المقاومة أو استخدامه مصطلحاتها (مثل: كيان العدو، المقاومة، الشهيد، الاعتداءات الإسرائيلية) أو عدم لعبه دور "المحايد" تجاه العدو؛ فهذا التزام طبيعي بالسياسة التحريرية للقناة وليس عيباً مهنياً.
   - التمايز بين "المونولوج العبثي" و"التوظيف التحريري الهادف": إذا كان الضيف حليفاً وفي مسار تفكيك لسرديات خصوم المقاومة، فإن ترك المساحة له قد يكون قراراً تحريرياً مقصوداً.
   - حدود التوظيف التحريري: المونولوج الذي يتجاوز 3 دقائق متصلة دون أي مقاطعة وظيفية أو سؤال تلخيصي أو توجيهي من المقدم يُعد فشلاً تقنياً في إدارة الإيقاع التلفزيوني (Broken TV Rhythm)، ويجب المحاسبة عليه بخصم النقاط في معايير "التعامل مع الضيف"، "المتابعة والتعقيب"، و"ضبط الوقت".
   - في معيار "الاختصار والحياد وتجنب تعدد الأسئلة"، يُقصد بالحياد المهني هنا: عدم مصادرة رأي الضيف بمقدمات تقريرية مطولة تفرض عليه الإجابة، وليس الحياد السياسي تجاه العدو."""

EDITORIAL_RULE_GENERAL_NEUTRAL = """3. المعيار المهني الإعلامي العام والحياد الحواري (تقييم غير موجّه - بدون خلفية قناة موجهة):
   - يُجرى التقييم وفق المعايير المهنية التلفزيونية العامة والمستقلة، دون افتراض وجود خط أيديولوجي أو سياسي موجّه للقناة، ودون منح أي استثناء أيديولوجي للمقدم أو الضيف.
   - يُحاسب المقدم مهنياً على التزامه بمبدأ المسافة المهنية الواحدة، وعدم الانحياز الشخصي أو تبني سردية الضيف أو التسليم بمقدماته دون تمحيص.
   - يُطلب من المقدم لعب دور "المحاور المسائل والمدقق" (Critical Interviewer) مع جميع الضيوف بلا استثناء؛ فيُحاسب على مدى مواجهته للضيف بالأسئلة الاستقصائية والتدقيق في التناقضات ومقاطعته مهنياً عند التهرب من السؤال أو إلقاء سرديات إنشائية.
   - أي استرسال أو مونولوج للضيف يتجاوز دقيقتين إلى 3 دقائق متصلة دون مقاطعة وظيفية أو توجيه أو استجواب من المقدم يُعدّ ضعفاً تقنياً ومهنياً في السيطرة على الاستوديو وإدارة الإيقاع التلفزيوني، ويُحاسب عليه بصرامة في معايير "التعامل مع الضيف"، "المتابعة والتعقيب"، و"ضبط الوقت".
   - في معيار "الاختصار والحياد وتجنب تعدد الأسئلة"، يُقصد بالحياد المهني: الحياد الكامل في صياغة السؤال (عدم تضمين السؤال أحكاماً مسبقة أو مقدمات توجيهية منحازة أو مصادرة لإجابة الضيف)."""

RUBRIC_90_SPEC = """سلم العلامات المعتمد حصرياً (90 نقطة — 6 معايير أساسية و23 معياراً فرعياً):
1) إدارة الحلقة — 20 نقطة:
   - بناء وتسلسل الملفات: 5
   - إغلاق الملف والانتقال: 5
   - السيطرة على المسار والمرونة: 5
   - الافتتاح والخاتمة: 5
2) التعامل مع الضيف — 20 نقطة:
   - الإصغاء واحترام الجواب: 5
   - توزيع المساحة وعدم المقاطعة غير الضرورية: 5
   - المقاطعة الضرورية ومواجهة الضيف: 5
   - احتواء المونولوج وإعادة المسار: 5
3) جودة الأسئلة — 20 نقطة:
   - وضوح السؤال: 4
   - دقة السؤال: 4
   - عمق السؤال: 4
   - الارتباط بالمحور: 4
   - الاختصار والحياد وتجنب تعدد الأسئلة: 4
4) المتابعة والتعقيب — 15 نقطة:
   - المتابعة المبنية على الجواب: 4
   - التقاط النقاط المهمة والتناقضات: 4
   - طلب التوضيح والتحقق: 3.5
   - بناء السؤال التالي من الجواب: 3.5
5) الحضور والأداء — 10 نقاط:
   - الجاذبية التلفزيونية: 3
   - لغة الجسد والتواصل البصري: 2.5
   - الصوت والإلقاء: 2.5
   - اللغة والطلاقة: 2 (لا خصم على اللهجة اللبنانية المحكية؛ المحاسبة فقط على النطق والتلعثم وأخطاء الفصحى الفادحة)
6) ضبط الوقت — 5 نقاط:
   - توزيع الوقت بين الملفات والضيوف: 1.7
   - منع استنزاف الوقت في نقطة واحدة: 1.6
   - إنهاء المحاور والخاتمة: 1.7"""


def build_eval_chunk_prompt(chunk_start_hms: str, chunk_end_hms: str, is_last_chunk: bool, editorial_mode: str, custom_prompt: str) -> str:
    ed_rule = EDITORIAL_RULE_GENERAL_NEUTRAL if editorial_mode == "neutral" else EDITORIAL_RULE_DIRECTED
    last_clause = (
        '\n- هام جداً (المقطع الأخير): استمع بدقة للدقيقة الأخيرة ودوّن في حقل "last_spoken_words" '
        'النص الحرفي (Verbatim) لآخر جملتين أو ثلاث جمل قيلت قبل ختام الحلقة مع توقيتها المطلق.'
        if is_last_chunk else ""
    )
    extra = f"\nتعليمات إضافية من المشغّل:\n{custom_prompt}\n" if custom_prompt else ""
    return f"""أنت محلل جنائي إعلامي دقيق (Zero Hallucination). تحلل الآن مقطعاً من حلقة تلفزيونية يمتد في التوقيت المطلق للحلقة من [{chunk_start_hms}] إلى [{chunk_end_hms}].
{extra}
القواعد الإلزامية الصارمة:
1. التوقيت المطلق المتصل: اجمع دائماً بداية هذا المقطع ({chunk_start_hms}) إلى أي توقيت داخلي، واكتب كل التوقيتات بصيغة HH:MM:SS (مثلاً: من {chunk_start_hms} إلى {chunk_end_hms}).
2. المنع المطلق للتخمين: لا تخترع أي اسم أو صفة أو اقتباس غير مسموع أو ظاهر على الشاشة. اكتب "غير محدد في المقطع" عند عدم توفر المعلومة.
3. قاعدة اللهجة اللبنانية: استخدام اللهجة اللبنانية المحكية ليس عيباً أبداً، بل يُرصد كأسلوب تواصلي طبيعي.
{ed_rule}
4. استخرج **كل تفصيل دقيق** (سمعي وبصري ونصي) بحيث يكون هذا السجل مرجعاً شاملاً ومستقلاً بذاته لأي محلل خارجي.{last_clause}

أخرج JSON صالح فقط يطابق هذا الهيكل التفصيلي بالكامل:
{{
  "chunk_window": "من {chunk_start_hms} إلى {chunk_end_hms}",
  "program_and_episode_clues": {{
    "program_name_seen_or_heard": "",
    "episode_title_or_date_seen": "",
    "presenter_name_seen_or_heard": "",
    "studio_or_location_setting": ""
  }},
  "chunk_summary": "ملخص سردي مكثف لكل ما جرى في هذا المقطع",
  "participants": [
    {{"name": "", "role_or_title": "", "appearance": "استديو/Zoom/هاتف/ميداني/تقرير", "approx_speaking_seconds": 0, "notes": ""}}
  ],
  "chronological_segments": [
    {{"start": "HH:MM:SS", "end": "HH:MM:SS", "segment_type": "افتتاح/محور حواري/مداخلة مراسل/تقرير VT/فاصل/خاتمة", "topic": "", "details": ""}}
  ],
  "presenter_questions_and_followups": [
    {{"timestamp": "HH:MM:SS", "duration_sec": 0, "question_type": "سؤال رئيسي/تعقيب/توضيح/مقاطعة وظيفية/سؤال مركب", "verbatim_quote": "النص الحرفي الدقيق", "linked_to_guest_answer": true, "professional_note": ""}}
  ],
  "guest_monologues": [
    {{"speaker": "", "start": "HH:MM:SS", "end": "HH:MM:SS", "duration_sec": 0, "presenter_intervened": false, "editorial_or_rhythm_note": ""}}
  ],
  "visual_and_body_language_log": [
    {{"timestamp": "HH:MM:SS", "category": "جلسة/نظرات/يدين/عادات حركية/قلم/تعابير وجه/كادر وغرافيك", "observation": "", "impact": "إيجابي/سلبي/محايد"}}
  ],
  "vocal_and_language_log": [
    {{"timestamp": "HH:MM:SS", "aspect": "نبرة وإلقاء/طلاقة/تلعثم/خطأ فصحى/مزاوجة باللهجة اللبنانية/تداخل صوتي", "verbatim_or_detail": "", "impact": "إيجابي/سلبي/محايد"}}
  ],
  "vt_and_field_reports": [
    {{"start": "HH:MM:SS", "end": "HH:MM:SS", "title_or_reporter": "", "summary": "", "presenter_handoff_note": ""}}
  ],
  "on_screen_graphics_and_chyrons": [
    {{"timestamp": "HH:MM:SS", "text": ""}}
  ],
  "rubric_evidence_notes": {{
    "management": "",
    "guest_handling": "",
    "questions_quality": "",
    "followup": "",
    "presence_and_delivery": "",
    "time_control": ""
  }},
  "last_spoken_words": ""
}}"""


def build_eval_synthesis_prompt(clean_name: str, exact_duration: str, editorial_mode: str, custom_prompt: str) -> str:
    is_neutral = (editorial_mode == "neutral")
    ed_label = (
        "المنظومة الثالثة: تقييم مهني إعلامي عام ومحايد (بدون خلفية قناة موجهة)"
        if is_neutral
        else "المنظومة الثانية: تقييم مع مراعاة الخط التحريري للقناة الموجهة"
    )
    ed_rule = EDITORIAL_RULE_GENERAL_NEUTRAL if is_neutral else EDITORIAL_RULE_DIRECTED
    meth_note = (
        "التقييم مهني وتقني مستقل وفق المعايير الإعلامية العامة والحياد الحواري (بدون خلفية قناة موجهة) مع مراعاة قاعدة المزاوجة اللغوية المحببة باللهجة اللبنانية"
        if is_neutral
        else "التقييم مهني وتقني مع مراعاة الخط التحريري للقناة وقاعدة المزاوجة اللغوية المحببة باللهجة اللبنانية"
    )
    extra = f"\nتعليمات إضافية من المشغّل:\n{custom_prompt}\n" if custom_prompt else ""

    return f"""أنت كبير خبراء تقييم الأداء التلفزيوني. أمامك السجلات الجنائية التفصيلية لجميع مقاطع حلقة ({clean_name}) كاملة من 00:00:00 حتى ({exact_duration}).
نوع التقييم: [{ed_label}].{extra}

القواعد الإلزامية الصارمة:
1. المنع المطلق للتخمين (Zero Hallucination): اعتمد حصراً على السجلات المرفقة. إذا غابت معلومة اكتب "غير محدد في المقطع".
2. التوقيت المتصل: اكتب التوقيتات بصيغة `من 00:08:24 إلى 00:08:28`. ممنوع نهائياً ذكر "الجزء الأول" أو "المقطع الثاني".
{ed_rule}
4. قاعدة اللهجة اللبنانية المحكية: يُمنع منعاً باتاً خصم أي علامة أو إدراجها كعيب أو أولوية تطوير؛ التطعيم باللهجة اللبنانية أسلوب تواصلي محبب. في معيار اللغة والطلاقة (2 نقطة) يُحاسب فقط على سلامة النطق والطلاقة وأخطاء الفصحى الفادحة.
5. الدقة الحسابية المطلقة: يجب أن يساوي مجموع العلامات الفرعية علامة كل معيار أساسي بدقة، وأن يساوي مجموع المعايير الستة العلامة الإجمالية من 90، مع حساب النسبة المئوية بدقة.

{RUBRIC_90_SPEC}

القالب الإلزامي لـ mainDetailedHtml (التقرير الرئيسي المفصل - استخدم فئات CSS نفسها تماماً):
- `<div class="header-banner"><h1>تقييم تفصيلي وفق المعايير الموسعة</h1><div class="subtitle">[المقدم] - [الحلقة/التاريخ]</div></div>`
- `<table class="info-table">` (المقدم، الحلقة، معيار التقييم: {ed_label}، الضيوف، المادة المسجلة، مدة الحلقة: {exact_duration})
- `<h2>الخلاصة التنفيذية</h2><div class="exec-box"><p>[فقرة معمقة 6-8 أسطر]</p></div>`
- `<table class="summary-strip">` (العلامة من 90، النسبة %، أبرز نقطة قوة، أولوية التطوير)
- `<h2>منهج التقييم</h2><p>تم تطبيق السلم المعتمد: إدارة الحلقة 20، التعامل مع الضيف 20، جودة الأسئلة 20، المتابعة والتعقيب 15، الحضور والأداء 10، وضبط الوقت 5. {meth_note}، وجرى التحقق من الانتقالات والحضور البصري على كامل مدة الحلقة.</p>`
- `<h2>العلامات الأساسية</h2><table class="core-grid">` (صفّان × 3 أعمدة للمعايير الستة)
- الأقسام الستة المفصلة `<h2>1. إدارة الحلقة - [ ]/20</h2>` حتى `<h2>6. ضبط الوقت - [ ]/5</h2>`، تحت كل قسم جدول بجميع معاييره الفرعية الـ 23 مع `<td class="score-cell">` وملاحظات غنية مدعومة بالاقتباسات الحرفية والشواهد البصرية والتوقيتات.
- `<h2>أمثلة موثقة من الحلقة</h2>` (جدول يضم ما لا يقل عن 10 أمثلة موثقة موزعة من بداية الحلقة حتى ختامها: التوقيت | المثال حرفياً أو بصرياً | الدلالة المهنية).
- `<h2>نقاط القوة الرئيسية</h2><div class="strengths-box"><ul>... (4-5 نقاط بالتوقيت)</ul></div>`
- `<h2>أولويات التطوير</h2><div class="priorities-box"><ul>... (4-5 نقاط عملية بالتوقيت - ممنوع ذكر اللهجة اللبنانية كعيب)</ul></div>`
- `<h2>الخلاصة النهائية</h2><div class="exec-box"><p>العلامة الحالية: [ ] من 90 ([ ]%). ...</p></div>`

القالب الإلزامي لـ summarizedHtml (التقرير المختصر):
- `<div class="header-banner"><h1>نموذج تقييم أداء مقدم/ة برنامج (تقرير مختصر)</h1><div class="subtitle">[المقدم/ة] - [البرنامج/التاريخ]</div></div>`
- `<table class="info-table">` (اسم المقدم/ة، التاريخ، البرنامج/نوع الحلقة، معيار التقييم، الضيوف والمشاركون، المواد المسجلة، مدة الحلقة)
- `<h2>الخلاصة التنفيذية</h2><div class="exec-box"><p>...</p></div>`
- `<h2>النتيجة الإجمالية</h2><table class="summary-strip">`
- `<h2>العلامات الأساسية</h2>` (جدول المعايير الـ 6 + صف المجموع الكلي من 90)
- `<h2>نقاط القوة الرئيسية</h2><div class="strengths-box"><ul>... (3-4 نقاط)</ul></div>`
- `<h2>أولويات التطوير</h2><div class="priorities-box"><ul>... (3-4 نقاط)</ul></div>`

أخرج JSON صالح حصراً بهذه الحقول:
{{
  "presenter": "اسم المقدم كما ورد في الحلقة أو المجلد",
  "episode_title": "عنوان الحلقة أو البرنامج",
  "editorial_label": "{ed_label}",
  "total_score": 0.0,
  "percentage": 0.0,
  "top_strength": "",
  "top_priority": "",
  "lastSpokenWords": "النص الحرفي لآخر كلام في الحلقة مع التوقيت المتصل",
  "core_scores": {{"management": 0, "guest": 0, "questions": 0, "followup": 0, "presence": 0, "time": 0}},
  "sub_criteria": [{{"section": "", "name": "", "score": 0, "max": 0, "note": ""}}],
  "strengths": [],
  "priorities": [],
  "mainDetailedHtml": "<div class=\\"header-banner\\">...</div>...",
  "summarizedHtml": "<div class=\\"header-banner\\">...</div>..."
}}

السجلات الجنائية التفصيلية لمقاطع الحلقة:
"""


def build_daily_chunk_prompt(chunk_start_hms: str, chunk_end_hms: str, custom_prompt: str) -> str:
    extra = f"\nتعليمات إضافية من المشغّل:\n{custom_prompt}\n" if custom_prompt else ""
    return f"""أنت محلل رصد بث تلفزيوني يومي دقيق (Zero Hallucination). تحلل الآن مقطعاً من البث اليومي يمتد في التوقيت المطلق من [{chunk_start_hms}] إلى [{chunk_end_hms}].
{extra}
القواعد الإلزامية:
1. احسب التوقيت المطلق المتصل بإضافة بداية المقطع ({chunk_start_hms}) لكل حدث.
2. لا تخمّن أي اسم مقدم أو ضيف أو مراسل أو عنوان غير ظاهر على الشاشة أو مسموع صراحة.
3. دوّن كل فقرة، نشرة، برنامج، تقرير VT، مداخلة مراسل، فاصل، أو خلل تقني بأقصى تفصيل ممكن.

أخرج JSON صالح حصراً بهذا الهيكل:
{{
  "chunk_window": "من {chunk_start_hms} إلى {chunk_end_hms}",
  "programs": [
    {{
      "start": "HH:MM:SS",
      "end": "HH:MM:SS",
      "duration_min": 0,
      "category": "نشرة أخبار/برنامج حواري/تغطية مباشرة/وثائقي/تقرير/فاصل إعلاني أو ترويجي/بث متوقف أو شاشة ألوان",
      "theme": "سياسي/أمني-ميداني/اقتصادي/اجتماعي/ديني/ثقافي/إداري",
      "title": "",
      "presenter": "",
      "guests": [{{"name": "", "role": "", "mode": "استديو/Zoom/هاتف"}}],
      "reporters": [{{"name": "", "location": ""}}],
      "vt_reports": [{{"timestamp": "HH:MM:SS", "title": ""}}],
      "key_verbatim_quotes": [""],
      "summary": "تفصيل دقيق للمحتوى والمحاور والرسائل الأساسية",
      "technical_or_visual_notes": ""
    }}
  ],
  "on_screen_tickers_and_breaking": [""]
}}"""


def build_daily_synthesis_prompt(clean_name: str, exact_duration: str, custom_prompt: str) -> str:
    extra = f"\nتعليمات إضافية من المشغّل:\n{custom_prompt}\n" if custom_prompt else ""
    return f"""أنت مدير وحدة الرصد والتحليل التلفزيوني. أمامك السجل الجنائي الكامل لبث يومي ({clean_name}) بمدة إجمالية ({exact_duration}).{extra}
المطلوب إنتاج تقريرين احترافيين بـ HTML عربي RTL (باستخدام فئات التنسيق: header-banner, info-table, exec-box, summary-strip, strengths-box, priorities-box, score-cell):
1. `mainDetailedHtml`: التقرير اليومي المفصل (جدول جرد كامل لجميع الفقرات والبرامج بالتوقيت المتصل، جدول جرد الضيوف والمراسلين والتقارير VT، وجدول الملاحظات التحريرية والتقنية).
2. `summarizedHtml`: الملخص التنفيذي للإدارة (خلاصة تنفيذية، شريط مؤشرات رقمية: إجمالي الساعات، عدد البرامج، عدد الضيوف، عدد المراسلين، جدول توزيع ساعات البث حسب التصنيف والموضوع، أبرز نقاط التميز، وأبرز الملاحظات والتوصيات الاستراتيجية للإدارة).

أخرج JSON صالح حصراً بهذه الحقول:
{{
  "broadcast_label": "{clean_name}",
  "total_programs": 0,
  "total_guests": 0,
  "total_reporters": 0,
  "total_vt_reports": 0,
  "executive_summary": "",
  "category_breakdown": [{{"category": "", "minutes": 0, "percentage": 0}}],
  "mainDetailedHtml": "<div class=\\"header-banner\\">...</div>...",
  "summarizedHtml": "<div class=\\"header-banner\\">...</div>..."
}}

سجلات مقاطع البث:
"""


# ==============================================================================
# HTML WRAPPER (Royal Executive Theme - MS Word A4 Print Compatible)
# ==============================================================================
def clean_inner_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    body_match = re.search(r"<body[^>]*>(.*)</body>", raw_html, flags=re.DOTALL | re.IGNORECASE)
    if body_match:
        return body_match.group(1).strip()
    raw_html = re.sub(r"<!DOCTYPE[^>]*>", "", raw_html, flags=re.IGNORECASE)
    raw_html = re.sub(r"</?html[^>]*>", "", raw_html, flags=re.IGNORECASE)
    raw_html = re.sub(r"<head>.*?</head>", "", raw_html, flags=re.DOTALL | re.IGNORECASE)
    raw_html = re.sub(r"</?body[^>]*>", "", raw_html, flags=re.IGNORECASE)
    return raw_html.strip()


def wrap_royal_html(title: str, body_content: str, banner_html: str = "") -> str:
    cleaned = clean_inner_html(body_content)
    return f"""<!DOCTYPE html>
<html xmlns:o="urn:schemas-microsoft-com:office:office"
      xmlns:w="urn:schemas-microsoft-com:office:word"
      xmlns="http://www.w3.org/TR/REC-html40"
      dir="rtl" lang="ar">
<head>
<meta charset="utf-8">
<title>{title}</title>
<!--[if gte mso 9]>
<xml>
  <w:WordDocument>
    <w:View>Print</w:View>
    <w:Zoom>100</w:Zoom>
    <w:DoNotOptimizeForBrowser/>
  </w:WordDocument>
</xml>
<![endif]-->
<style>
  @page {{ size: A4; margin: 1.8cm; }}
  body {{
      font-family: "Segoe UI", "Tahoma", "Arial", sans-serif;
      font-size: 12pt;
      line-height: 1.65;
      direction: rtl;
      text-align: right;
      color: #0F172A;
      max-width: 980px;
      margin: 24px auto;
      padding: 24px;
      background-color: #FFFFFF;
  }}
  .thinking-note {{
      font-size: 9.5pt;
      color: #1E3A8A;
      background-color: #EFF6FF;
      border: 1px solid #BFDBFE;
      border-right: 4px solid #2563EB;
      padding: 7px 12px;
      margin-bottom: 16px;
      border-radius: 4px;
      text-align: right;
      font-weight: bold;
  }}
  .header-banner {{
      background-color: #1E3A8A;
      color: #FFFFFF;
      padding: 22px 24px;
      border-radius: 8px;
      border-bottom: 5px solid #F59E0B;
      margin-bottom: 22px;
      text-align: center;
  }}
  h1 {{ text-align: center; font-size: 20pt; font-weight: bold; color: #1E3A8A; margin: 0 0 6px 0; page-break-after: avoid; }}
  .header-banner h1 {{ color: #FFFFFF; }}
  .subtitle {{ text-align: center; font-size: 13.5pt; font-weight: bold; color: #475569; margin: 0 0 14px 0; }}
  .header-banner .subtitle {{ color: #E2E8F0; margin: 0; }}
  h2 {{
      font-size: 14pt; font-weight: bold; color: #1E3A8A;
      background-color: #F1F5F9; border-right: 6px solid #1E3A8A;
      border-bottom: 1px solid #CBD5E1; padding: 8px 14px;
      margin-top: 24px; margin-bottom: 12px; border-radius: 4px; page-break-after: avoid;
  }}
  p {{ margin-top: 6px; margin-bottom: 12px; text-align: justify; }}
  .exec-box {{
      background-color: #EFF6FF; border: 1px solid #BFDBFE;
      border-right: 6px solid #1D4ED8; padding: 14px 18px;
      border-radius: 6px; margin-bottom: 18px;
  }}
  .strengths-box {{
      background-color: #F0FDF4; border: 1px solid #BBF7D0;
      border-right: 6px solid #16A34A; padding: 14px 20px;
      border-radius: 6px; margin-bottom: 18px;
  }}
  .strengths-box li {{ color: #064E3B; font-weight: 500; }}
  .priorities-box {{
      background-color: #FFFBEB; border: 1px solid #FDE68A;
      border-right: 6px solid #D97706; padding: 14px 20px;
      border-radius: 6px; margin-bottom: 18px;
  }}
  .priorities-box li {{ color: #78350F; font-weight: 500; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 18px; page-break-inside: avoid; }}
  table, th, td {{ border: 1px solid #CBD5E1; }}
  th, td {{ padding: 9px 12px; text-align: right; vertical-align: top; page-break-inside: avoid; }}
  tr {{ page-break-inside: avoid; page-break-after: auto; }}
  tr:nth-child(even) td {{ background-color: #F8FAFC; }}
  th {{ background-color: #1E3A8A; color: #FFFFFF; font-weight: bold; font-size: 11.5pt; }}
  .info-table td:first-child {{
      font-weight: bold; background-color: #EFF6FF; color: #1E3A8A;
      width: 22%; border-left: 2px solid #93C5FD;
  }}
  .summary-strip td {{
      background-color: #F8FAFC; text-align: center; width: 25%;
      padding: 12px 10px; border-top: 4px solid #1E3A8A;
  }}
  .summary-strip td:nth-child(1) {{ background-color: #EFF6FF; border-top-color: #2563EB; }}
  .summary-strip td:nth-child(2) {{ background-color: #F0FDFA; border-top-color: #0D9488; }}
  .summary-strip td:nth-child(3) {{ background-color: #F0FDF4; border-top-color: #16A34A; }}
  .summary-strip td:nth-child(4) {{ background-color: #FFFBEB; border-top-color: #D97706; }}
  .summary-strip .label {{ font-size: 10.5pt; color: #475569; font-weight: bold; display: block; margin-bottom: 4px; }}
  .summary-strip .val {{ font-size: 13.5pt; font-weight: bold; color: #0F172A; }}
  .core-grid td {{
      width: 33.33%; background-color: #F1F5F9; color: #1E3A8A;
      font-weight: bold; text-align: center; padding: 11px; font-size: 12pt;
  }}
  .score-cell {{
      text-align: center; font-weight: bold; color: #1E3A8A;
      background-color: #EFF6FF !important; font-size: 11.5pt;
  }}
  ul {{ margin-top: 6px; margin-bottom: 8px; padding-right: 22px; }}
  li {{ margin-bottom: 8px; page-break-inside: avoid; }}
</style>
</head>
<body>{banner_html}{cleaned}</body>
</html>"""


# ==============================================================================
# ULTRA-DETAILED SELF-CONTAINED FORENSIC BLACKBOX GENERATOR
# ==============================================================================
def generate_forensic_blackbox_md(mode: str, valid_chunks: list, final_data: dict, meta: dict) -> str:
    """
    Produces a completely self-contained Markdown dossier so ANY external AI
    (DeepSeek, ChatGPT, Claude, etc.) can analyze it accurately with zero prior context.
    """
    lines = []
    lines.append(f"# FORENSIC BLACKBOX DOSSIER — {meta['clean_name']} ({mode.upper()})")
    lines.append("")
    lines.append("> **Notice for External AI Systems (ChatGPT / DeepSeek / Claude / etc.):**")
    lines.append("> This file is a complete, self-contained forensic dataset extracted directly from the full video & audio of a TV broadcast/episode with zero hallucination. Every timestamp is absolute (`HH:MM:SS`) from the start of the recording (`00:00:00`). Use ONLY the evidence, quotes, visual observations, and structured data inside this file to answer questions, build comparative reports, or audit scores.")
    lines.append("")

    # 1. Master Metadata
    lines.append("## 1. MASTER METADATA")
    lines.append(f"- **Episode / Folder Name:** {meta['clean_name']}")
    lines.append(f"- **Presenter / Subject:** {meta.get('presenter_name') or 'Not specified'}")
    lines.append(f"- **Run Mode:** {mode}")
    lines.append(f"- **Editorial Standard:** {meta.get('editorial_label', 'N/A')}")
    lines.append(f"- **Exact Full Duration:** {meta.get('exact_duration', 'Unknown')}")
    lines.append(f"- **Chunks Analyzed:** {meta.get('chunks_ok', 0)} / {meta.get('chunks_total', 0)} (Chunk size: {meta.get('chunk_min', 10)} min, Video FPS: {meta.get('fps', 'N/A')})")
    lines.append(f"- **Source URL:** {meta.get('src_url', 'N/A')}")
    lines.append(f"- **Models Used:** {', '.join(sorted(meta.get('models_used', []))) or 'N/A'}")
    lines.append(f"- **Actual Cost (USD):** ${meta.get('actual_cost', 0.0):.4f}")
    lines.append(f"- **Generated At (Beirut Time):** {datetime.now(BEIRUT_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    if meta.get("system_prompt"):
        lines.append(f"- **Custom Operator Prompt:** {meta['system_prompt']}")
    if final_data.get("lastSpokenWords"):
        lines.append(f"- **Verbatim Last Spoken Words (End Verification):** «{final_data['lastSpokenWords']}»")
    lines.append("")

    # 2. Scoring System & Rules Reference (so external AI knows the exact framework!)
    if mode == "eval":
        lines.append("## 2. EVALUATION FRAMEWORK & MANDATORY RULES")
        lines.append(RUBRIC_90_SPEC)
        lines.append("")
        lines.append("### Editorial & Language Rules Applied")
        lines.append(EDITORIAL_RULE_GENERAL_NEUTRAL if meta.get("editorial_mode") == "neutral" else EDITORIAL_RULE_DIRECTED)
        lines.append("- **Lebanese Spoken Dialect Rule:** Spoken Lebanese dialect is a natural, engaging communicative style and MUST NEVER be penalized or listed as a weakness/development priority.")
        lines.append("")

        # 3. Synthesized Scores Overview
        lines.append("## 3. SYNTHESIZED EVALUATION SCORES")
        lines.append(f"- **Total Score:** {final_data.get('total_score', 0)} / 90 ({final_data.get('percentage', 0)}%)")
        lines.append(f"- **Top Strength:** {final_data.get('top_strength', 'N/A')}")
        lines.append(f"- **Top Priority:** {final_data.get('top_priority', 'N/A')}")
        core = final_data.get("core_scores", {})
        if core:
            lines.append("")
            lines.append("| Core Criterion | Score | Max |")
            lines.append("|---|---:|---:|")
            lines.append(f"| إدارة الحلقة (Episode Management) | {core.get('management', 0)} | 20 |")
            lines.append(f"| التعامل مع الضيف (Guest Handling) | {core.get('guest', 0)} | 20 |")
            lines.append(f"| جودة الأسئلة (Questions Quality) | {core.get('questions', 0)} | 20 |")
            lines.append(f"| المتابعة والتعقيب (Follow-up) | {core.get('followup', 0)} | 15 |")
            lines.append(f"| الحضور والأداء (Presence & Delivery) | {core.get('presence', 0)} | 10 |")
            lines.append(f"| ضبط الوقت (Time Control) | {core.get('time', 0)} | 5 |")
        if final_data.get("sub_criteria"):
            lines.append("")
            lines.append("### 23 Sub-Criteria Breakdown")
            lines.append("| Section | Sub-Criterion | Score | Evidence / Note |")
            lines.append("|---|---|---:|---|")
            for sc in final_data.get("sub_criteria", []):
                note = str(sc.get("note", "")).replace("\n", " ").replace("|", "/")
                lines.append(f"| {sc.get('section', '')} | {sc.get('name', '')} | {sc.get('score', 0)}/{sc.get('max', 0)} | {note} |")
        lines.append("")

    else:
        lines.append("## 2. DAILY BROADCAST EXECUTIVE OVERVIEW")
        lines.append(f"- **Total Programs/Segments:** {final_data.get('total_programs', 0)}")
        lines.append(f"- **Total Guests:** {final_data.get('total_guests', 0)}")
        lines.append(f"- **Total Reporters:** {final_data.get('total_reporters', 0)}")
        lines.append(f"- **Total VT Reports:** {final_data.get('total_vt_reports', 0)}")
        if final_data.get("executive_summary"):
            lines.append(f"- **Executive Summary:** {final_data['executive_summary']}")
        lines.append("")

    # 4. Human- & LLM-Readable Forensic Chunk-by-Chunk Log
    lines.append("## 4. EXHAUSTIVE CHRONOLOGICAL FORENSIC LOG (CHUNK BY CHUNK)")
    for item in valid_chunks:
        c_idx = item.get("chunk_index", "?")
        c_win = item.get("window", "")
        d = item.get("data") or {}
        lines.append(f"### Chunk {c_idx} — {c_win}")
        if not isinstance(d, dict):
            lines.append(str(d))
            continue

        if mode == "eval":
            clues = d.get("program_and_episode_clues") or {}
            if any(clues.values()):
                lines.append(f"- **Identified Clues:** Program=`{clues.get('program_name_seen_or_heard','')}` | Title/Date=`{clues.get('episode_title_or_date_seen','')}` | Presenter=`{clues.get('presenter_name_seen_or_heard','')}` | Setting=`{clues.get('studio_or_location_setting','')}`")
            if d.get("chunk_summary"):
                lines.append(f"- **Summary:** {d['chunk_summary']}")

            if d.get("participants"):
                lines.append("#### Participants")
                for p in d["participants"]:
                    lines.append(f"- **{p.get('name','?')}** ({p.get('role_or_title','')}) [{p.get('appearance','')}] ~{p.get('approx_speaking_seconds',0)}s — {p.get('notes','')}")

            if d.get("chronological_segments"):
                lines.append("#### Chronological Segments")
                lines.append("| Start | End | Type | Topic | Details |")
                lines.append("|---|---|---|---|---|")
                for s in d["chronological_segments"]:
                    det = str(s.get("details", "")).replace("\n", " ").replace("|", "/")
                    lines.append(f"| {s.get('start','')} | {s.get('end','')} | {s.get('segment_type','')} | {s.get('topic','')} | {det} |")

            if d.get("presenter_questions_and_followups"):
                lines.append("#### Presenter Questions, Follow-ups & Verbatim Quotes")
                for q in d["presenter_questions_and_followups"]:
                    lines.append(f"- **[{q.get('timestamp','')}]** ({q.get('question_type','')}, ~{q.get('duration_sec',0)}s, linked={q.get('linked_to_guest_answer')}): «{q.get('verbatim_quote','')}» — *{q.get('professional_note','')}*")

            if d.get("guest_monologues"):
                lines.append("#### Guest Monologues Audit")
                for m in d["guest_monologues"]:
                    lines.append(f"- **{m.get('speaker','')}** ({m.get('start','')} -> {m.get('end','')}, {m.get('duration_sec',0)}s, intervened={m.get('presenter_intervened')}): {m.get('editorial_or_rhythm_note','')}")

            if d.get("visual_and_body_language_log"):
                lines.append("#### Visual & Body Language Log")
                for v in d["visual_and_body_language_log"]:
                    lines.append(f"- **[{v.get('timestamp','')}]** ({v.get('category','')} | {v.get('impact','')}): {v.get('observation','')}")

            if d.get("vocal_and_language_log"):
                lines.append("#### Vocal & Language Log")
                for vl in d["vocal_and_language_log"]:
                    lines.append(f"- **[{vl.get('timestamp','')}]** ({vl.get('aspect','')} | {vl.get('impact','')}): {vl.get('verbatim_or_detail','')}")

            if d.get("vt_and_field_reports"):
                lines.append("#### VT & Field Reports")
                for vt in d["vt_and_field_reports"]:
                    lines.append(f"- **[{vt.get('start','')} -> {vt.get('end','')}]** {vt.get('title_or_reporter','')}: {vt.get('summary','')} | Handoff: {vt.get('presenter_handoff_note','')}")

            if d.get("on_screen_graphics_and_chyrons"):
                lines.append("#### On-Screen Graphics / Chyrons")
                for g in d["on_screen_graphics_and_chyrons"]:
                    lines.append(f"- **[{g.get('timestamp','')}]** {g.get('text','')}")

            if d.get("rubric_evidence_notes"):
                lines.append("#### Chunk Rubric Evidence")
                for k_ev, v_ev in d["rubric_evidence_notes"].items():
                    if v_ev:
                        lines.append(f"- **{k_ev}:** {v_ev}")

            if d.get("last_spoken_words"):
                lines.append(f"- **Last Spoken Words Captured:** «{d['last_spoken_words']}»")
        else:
            for p in d.get("programs", []):
                lines.append(f"- **[{p.get('start','')} -> {p.get('end','')}] {p.get('title','')}** ({p.get('category','')} | {p.get('theme','')}) — Presenter: {p.get('presenter','')}")
                if p.get("summary"):
                    lines.append(f"  - Summary: {p['summary']}")
                if p.get("guests"):
                    g_str = ", ".join(f"{g.get('name','')} ({g.get('role','')})" for g in p["guests"])
                    lines.append(f"  - Guests: {g_str}")
                if p.get("reporters"):
                    r_str = ", ".join(f"{r.get('name','')} ({r.get('location','')})" for r in p["reporters"])
                    lines.append(f"  - Reporters: {r_str}")
                if p.get("vt_reports"):
                    v_str = ", ".join(f"[{v.get('timestamp','')}] {v.get('title','')}" for v in p["vt_reports"])
                    lines.append(f"  - VT Reports: {v_str}")
                if p.get("key_verbatim_quotes"):
                    lines.append(f"  - Quotes: {' | '.join(p['key_verbatim_quotes'])}")
                if p.get("technical_or_visual_notes"):
                    lines.append(f"  - Tech/Visual: {p['technical_or_visual_notes']}")
            if d.get("on_screen_tickers_and_breaking"):
                lines.append(f"- **Breaking / Tickers:** {' ; '.join(d['on_screen_tickers_and_breaking'])}")

        lines.append("")

    # 5. Structured Machine-Readable JSON Appendix (excludes giant HTML strings to keep clean)
    clean_final_meta = {k: v for k, v in final_data.items() if k not in ("mainDetailedHtml", "summarizedHtml")}
    lines.append("## 5. COMPLETE STRUCTURED JSON APPENDIX")
    lines.append("```json")
    lines.append(json.dumps({"metadata": {k: (list(v) if isinstance(v, set) else v) for k, v in meta.items()}, "final_synthesis": clean_final_meta, "chunks": valid_chunks}, ensure_ascii=False, indent=2))
    lines.append("```")
    return "\n".join(lines)


# ==============================================================================
# HELPERS: FORMATTING, TELEGRAM, DUAL-PORT SMTP, AUTO-REFRESH DRIVE
# ==============================================================================
def mask_key(k: str) -> str:
    k = (k or "").strip()
    return "***" if len(k) <= 10 else f"{k[:6]}...{k[-4:]}"


def format_hhmmss(seconds: float) -> str:
    s = max(0, int(round(seconds)))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def sanitize_filename(name: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]+', "_", (name or "Episode").strip())
    s = re.sub(r"[\s_]+", "_", s)
    return s.strip("_") or "Episode"


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
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end > start:
            return json.loads(raw[start : end + 1])
        raise


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
    """
    Fixes `[SMTP Retry] Connection unexpectedly closed` on GitHub Actions:
    - Strips accidental whitespace from Gmail App Password.
    - Tries Port 465 (SMTP_SSL) then falls back to Port 587 (SMTP + STARTTLS).
    - Sends per-recipient like evaluate.py.
    """
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

    # Attempt 1: Port 465 SSL
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=25) as s:
            s.ehlo()
            s.login(user, pwd)
            for rec in recips:
                s.sendmail(user, [rec], build_msg(rec).as_string())
        print(f"✅ SMTP (465 SSL) sent to: {recips}", flush=True)
        return True
    except Exception as e465:
        print(f"[SMTP 465 Warning] {e465} — trying STARTTLS port 587...", flush=True)

    # Attempt 2: Port 587 STARTTLS
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=25) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(user, pwd)
            for rec in recips:
                s.sendmail(user, [rec], build_msg(rec).as_string())
        print(f"✅ SMTP (587 STARTTLS) sent to: {recips}", flush=True)
        return True
    except Exception as e587:
        print(f"[SMTP 587 Failed] {e587} — will trigger Apps Script GmailApp backup email.", flush=True)
        return False


def refresh_drive_token_via_webhook(cfg) -> bool:
    global RUNTIME_DRIVE_TOKEN
    webhook = cfg.get("webhook_url")
    secret = cfg.get("webhook_secret", "")
    if not (webhook and secret):
        return False
    try:
        r = requests.post(
            webhook,
            json={"action": "refresh_drive_token", "webhook_secret": secret},
            timeout=15,
        )
        if r.status_code == 200:
            new_tok = r.json().get("drive_token", "").strip()
            if new_tok:
                with DRIVE_TOKEN_LOCK:
                    RUNTIME_DRIVE_TOKEN = new_tok
                print("🔄 Refreshed Google Drive OAuth token successfully.", flush=True)
                return True
    except Exception as e:
        print(f"[Token Refresh Warning] {e}", flush=True)
    return False


def drive_request(method, url, cfg, **kwargs):
    """Auto-retrying Drive HTTP wrapper that refreshes the OAuth token if 401 occurs."""
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
        r = drive_request(
            "GET",
            "https://www.googleapis.com/drive/v3/files",
            cfg,
            params={"q": q, "fields": "files(id)"},
            timeout=20,
        )
        files_found = r.json().get("files", []) if r.status_code == 200 else []
        fid = files_found[0].get("id") if files_found else None
        with open(filepath, "rb") as f:
            data = f.read()
        if fid:
            up = drive_request(
                "PATCH",
                f"https://www.googleapis.com/upload/drive/v3/files/{fid}?uploadType=media",
                cfg,
                headers={"Content-Type": f"{mimetype}; charset=UTF-8"},
                data=data,
                timeout=90,
            )
            return up.status_code in (200, 201)
        else:
            bound = "===BNDRY==="
            meta = json.dumps({"name": filename, "parents": [folder_id]})
            body = (
                f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n"
                f"--{bound}\r\nContent-Type: {mimetype}; charset=UTF-8\r\n\r\n"
            ).encode("utf-8") + data + f"\r\n--{bound}--\r\n".encode("utf-8")
            up = drive_request(
                "POST",
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                cfg,
                headers={"Content-Type": f"multipart/related; boundary={bound}"},
                data=body,
                timeout=90,
            )
            return up.status_code in (200, 201)
    except Exception as e:
        print(f"[Drive Upload Warning] {filename}: {e}", flush=True)
        return False


def update_drive_folder_meta(folder_id, cfg, new_name, description_text=""):
    payload = {"name": new_name}
    if description_text:
        payload["description"] = description_text
    try:
        r = drive_request(
            "PATCH",
            f"https://www.googleapis.com/drive/v3/files/{folder_id}",
            cfg,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=25,
        )
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[Drive Rename Warning] {e}", flush=True)
        return False


STATUS_FILE_DRIVE_ID = None


def write_drive_status_and_check_cancel(folder_id, cfg, payload):
    global STATUS_FILE_DRIVE_ID
    if not folder_id:
        return False
    try:
        chk = drive_request(
            "GET",
            "https://www.googleapis.com/drive/v3/files",
            cfg,
            params={"q": f"'{folder_id}' in parents and name='_cancel.flag' and trashed=false", "fields": "files(id)"},
            timeout=10,
        )
        if chk.status_code == 200 and chk.json().get("files"):
            CANCEL_EVENT.set()
            return True

        raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if not STATUS_FILE_DRIVE_ID:
            r = drive_request(
                "GET",
                "https://www.googleapis.com/drive/v3/files",
                cfg,
                params={"q": f"'{folder_id}' in parents and name='_status.json' and trashed=false", "fields": "files(id)"},
                timeout=10,
            )
            files_found = r.json().get("files", []) if r.status_code == 200 else []
            if files_found:
                STATUS_FILE_DRIVE_ID = files_found[0]["id"]

        if STATUS_FILE_DRIVE_ID:
            drive_request(
                "PATCH",
                f"https://www.googleapis.com/upload/drive/v3/files/{STATUS_FILE_DRIVE_ID}?uploadType=media",
                cfg,
                headers={"Content-Type": "application/json; charset=UTF-8"},
                data=raw_bytes,
                timeout=15,
            )
        else:
            bound = "===BNDRY_ST==="
            meta = json.dumps({"name": "_status.json", "parents": [folder_id]})
            body = (
                f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n"
                f"--{bound}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            ).encode("utf-8") + raw_bytes + f"\r\n--{bound}--\r\n".encode("utf-8")
            up = drive_request(
                "POST",
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id",
                cfg,
                headers={"Content-Type": f"multipart/related; boundary={bound}"},
                data=body,
                timeout=15,
            )
            if up.status_code in (200, 201):
                STATUS_FILE_DRIVE_ID = up.json().get("id")
    except Exception as e:
        print(f"[Drive Status Warning] {e}", flush=True)
    return CANCEL_EVENT.is_set()


def push_status(cfg, folder_id, run_id, status, phase, completed, total, act_cost, started_at, eta_sec=None, message=""):
    elapsed = int(time.time() - started_at)
    if eta_sec is None:
        if completed > 0 and total > completed:
            eta_sec = int((elapsed / completed) * (total - completed))
        elif total > 0 and status == "running":
            eta_sec = int(total * 35)
        else:
            eta_sec = 0
    payload = {
        "action": "update_status",
        "job_id": folder_id,
        "run_id": str(run_id or ""),
        "status": status,
        "phase": phase,
        "completed_chunks": int(completed),
        "total_chunks": int(total),
        "elapsed_sec": elapsed,
        "eta_sec": max(0, int(eta_sec)),
        "actual_cost": round(float(act_cost), 4),
        "message": message,
        "updated_at_epoch": int(time.time()),
    }
    # 1. Always write _status.json to Drive (works even if Web App access is "Only myself")
    write_drive_status_and_check_cancel(folder_id, cfg, payload)

    # 2. Also push via Apps Script webhook if reachable
    webhook = cfg.get("webhook_url")
    if webhook:
        try:
            r = requests.post(webhook, json=payload, timeout=10)
            ctype = (r.headers.get("Content-Type") or "").lower()
            if r.status_code == 200 and "json" in ctype and r.json().get("cancel_requested"):
                CANCEL_EVENT.set()
                return True
        except Exception as e:
            print(f"[Status Webhook Warning] {e}", flush=True)
    return CANCEL_EVENT.is_set()


def finalize_cost_webhook(cfg, folder_id, actual_cost: float, final_status="completed", message="", trigger_gas_email=False, email_subject=""):
    write_drive_status_and_check_cancel(folder_id, cfg, {
        "job_id": folder_id,
        "run_id": str(os.environ.get("GITHUB_RUN_ID", "")),
        "status": final_status,
        "phase": message or final_status,
        "eta_sec": 0,
        "actual_cost": round(float(actual_cost), 4),
        "message": message,
        "updated_at_epoch": int(time.time()),
    })
    webhook = cfg.get("webhook_url")
    if not webhook:
        return
    try:
        requests.post(
            webhook,
            json={
                "action": "finalize_cost",
                "webhook_secret": cfg.get("webhook_secret", ""),
                "job_id": folder_id,
                "status": final_status,
                "estimated_cost": cfg.get("pre_authorized_cost", 0),
                "actual_cost": round(float(actual_cost), 4),
                "message": message,
                "send_backup_email": trigger_gas_email,
                "email_subject": email_subject,
                "alert_emails": cfg.get("alert_emails", ""),
            },
            timeout=20,
        )
    except Exception as e:
        print(f"[Finalize Webhook Warning] {e}", flush=True)


# ==============================================================================
# KEY PRE-FLIGHT VALIDATION & DYNAMIC MODEL DISCOVERY
# ==============================================================================
def rank_discovered_models(available_models: list, preferred_order: list, allow_pro: bool = False) -> list:
    skip_tokens = ("tts", "image", "imagen", "veo", "embedding", "aqa", "bison", "gecko", "1.0", "1.5", "robotics", "computer-use", "live")
    has_gen3_plus = any(re.search(r"gemini-[3-9]\b|gemini-[3-9]\.", m.lower()) for m in available_models)

    avail_set = set(available_models)
    ordered = [m for m in preferred_order if m in avail_set]

    extras = []
    for m in available_models:
        low = m.lower()
        if m in ordered or not low.startswith("gemini-"):
            continue
        if any(tok in low for tok in skip_tokens):
            continue
        if has_gen3_plus and (low.startswith("gemini-2.") or low.startswith("gemini-2-")):
            continue
        if "flash" in low or (allow_pro and ("pro" in low or "argon" in low)):
            extras.append(m)

    def version_key(name: str):
        nums = [int(x) for x in re.findall(r"\d+", name)]
        is_preview = 0 if ("preview" in name or "exp" in name) else 1
        return (nums[:2] if nums else [0, 0], is_preview, name)

    extras.sort(key=version_key, reverse=True)
    ordered.extend(extras)
    return ordered[:4]


def validate_single_key(key: str, preferred_models: list, allow_pro: bool = False) -> tuple:
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
    print("=" * 60, flush=True)
    print("PRE-FLIGHT API KEY VALIDATION (FAST PARALLEL)", flush=True)
    print("=" * 60, flush=True)

    unique_free = list(dict.fromkeys(raw_free_keys))
    if len(unique_free) < len(raw_free_keys):
        print(f"⚠️ Removed {len(raw_free_keys) - len(unique_free)} duplicate key(s) from GEMINI_FREE_API_KEYS!", flush=True)

    valid_free = []
    valid_paid = None
    paid_models = []
    report_lines = []

    tasks = [(f"FREE #{i}", k, PREFERRED_FLASH_ORDER, False) for i, k in enumerate(unique_free, 1)]
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
        f"• المجانية الصالحة (الفريدة): {len(valid_free)}/{len(unique_free)}\n"
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


# ==============================================================================
# GEMINI CALLS (OFFICIAL `google-genai` SDK)
# ==============================================================================
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
    t0 = time.time()
    f = uploaded_file
    while True:
        if CANCEL_EVENT.is_set():
            raise RuntimeError("Cancelled by user")
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
    think_t = getattr(usage, "thoughts_token_count", 0) or 0
    rates = PRICING.get(model_name, {"in": 0.75, "out": 3.75})
    return (in_t / 1e6) * rates["in"] + ((out_t + think_t) / 1e6) * rates["out"]


def call_gemini_chunk(media_path, prompt, valid_free, valid_paid, paid_models, strategy, exhausted_pairs, bad_keys, budget_ok_fn):
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
            uploaded = client.files.upload(file=media_path)
            uploaded = wait_file_active(client, uploaded)

            for model_name in models_to_try:
                if CANCEL_EVENT.is_set():
                    raise RuntimeError("Cancelled by user")
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
                    print(f"API Error ({'PAID' if is_paid else 'FREE'} {mask_key(api_key)} / {model_name}): {err[:180]}", flush=True)

                    if "api_key_invalid" in low or "access_token_type_unsupported" in low or "401" in low or "403" in low:
                        with STATE_LOCK:
                            bad_keys.add(api_key)
                        return None
                    elif "429" in low or "resource_exhausted" in low or "quota" in low or "404" in low or "not_found" in low or "no longer available" in low:
                        with STATE_LOCK:
                            exhausted_pairs.add((api_key, model_name))
                        continue
                    elif "503" in low or "unavailable" in low or "overloaded" in low:
                        time.sleep(random.uniform(1.5, 3.0))
                        continue
                    else:
                        time.sleep(1.5)
                        continue
        except Exception as e:
            err = str(e)
            low = err.lower()
            last_err = f"[UPLOAD] {err[:220]}"
            print(f"Upload Error ({'PAID' if is_paid else 'FREE'} {mask_key(api_key)}): {err[:180]}", flush=True)
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

    if strategy in ("flash_only", "hybrid") and valid_free:
        n = len(valid_free)
        with STATE_LOCK:
            start_idx = FREE_RR_COUNTER % n
            FREE_RR_COUNTER += 1
        rotated = [valid_free[(start_idx + offset) % n] for offset in range(n)]
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
    return ("youtube.com/watch" in u) or ("youtu.be/" in u) or ("youtube.com/live/" in u) or ("youtube.com/shorts/" in u)


def acquire_video_locally(url: str, dest: str, cfg: dict) -> bool:
    # 1. Google Drive file link
    gd_match = re.search(r"(?:/file/d/|id=)([a-zA-Z0-9_-]{20,})", url)
    if gd_match and "drive.google.com" in url:
        file_id = gd_match.group(1)
        try:
            with drive_request("GET", f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media", cfg, stream=True, timeout=(15, 120)) as r:
                if r.status_code == 200:
                    with open(dest, "wb") as f:
                        for block in r.iter_content(chunk_size=1024 * 512):
                            if block:
                                f.write(block)
            if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
                return True
        except Exception as e:
            print(f"[Drive File Pull Warning] {e}", flush=True)

    # 2. YouTube via yt-dlp
    if is_youtube_url(url):
        print("🎬 YouTube link detected — pulling via yt-dlp...", flush=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-U", "yt-dlp"], check=False)
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist", "--no-check-certificates",
            "--extractor-args", "youtube:player_client=ios,android,mweb,tv",
            "-f", "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[ext=mp4]/best",
            "--merge-output-format", "mp4",
            "-o", dest, url,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0 and os.path.exists(dest) and os.path.getsize(dest) > 50_000:
            return True
        print(f"[yt-dlp Warning] {(res.stderr or '')[-200:]}", flush=True)
        return False

    # 3. Direct HTTP/HTTPS media download
    for attempt in range(1, 4):
        try:
            with requests.get(url, stream=True, timeout=(15, 60), headers={"User-Agent": "Mozilla/5.0"}) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for block in r.iter_content(chunk_size=1024 * 256):
                        if block:
                            f.write(block)
            if os.path.exists(dest) and os.path.getsize(dest) > 50_000:
                return True
        except Exception as e:
            print(f"[Download Attempt {attempt}/3 Failed] {e}", flush=True)
            time.sleep(3)
    return False


# ==============================================================================
# FALLBACK HTML BUILDERS (only if synthesis HTML was empty/malformed)
# ==============================================================================
def fallback_eval_detailed_html(data: dict, clean_name: str, dur: str, ed_label: str) -> str:
    rows = "".join(
        f"<tr><td>{sc.get('name','')}</td><td class='score-cell'>{sc.get('score',0)}/{sc.get('max',0)}</td><td>{sc.get('note','')}</td></tr>"
        for sc in data.get("sub_criteria", [])
    )
    st_li = "".join(f"<li>{s}</li>" for s in data.get("strengths", []))
    pr_li = "".join(f"<li>{p}</li>" for p in data.get("priorities", []))
    return (
        f'<div class="header-banner"><h1>تقييم تفصيلي وفق المعايير الموسعة</h1><div class="subtitle">{clean_name}</div></div>'
        f'<table class="info-table"><tr><td>المقدم</td><td>{data.get("presenter", clean_name)}</td></tr>'
        f'<tr><td>معيار التقييم</td><td>{ed_label}</td></tr><tr><td>مدة الحلقة</td><td>{dur}</td></tr></table>'
        f'<table class="summary-strip"><tr><td><span class="label">العلامة</span><span class="val">{data.get("total_score",0)} من 90</span></td>'
        f'<td><span class="label">النسبة</span><span class="val">{data.get("percentage",0)}%</span></td>'
        f'<td><span class="label">أبرز نقطة قوة</span><span class="val">{data.get("top_strength","")}</span></td>'
        f'<td><span class="label">أولوية التطوير</span><span class="val">{data.get("top_priority","")}</span></td></tr></table>'
        f'<h2>تفاصيل المعايير</h2><table><tr><th>المعيار</th><th>العلامة</th><th>الملاحظات</th></tr>{rows}</table>'
        f'<h2>نقاط القوة</h2><div class="strengths-box"><ul>{st_li}</ul></div>'
        f'<h2>أولويات التطوير</h2><div class="priorities-box"><ul>{pr_li}</ul></div>'
    )


def fallback_daily_detailed_html(valid_chunks: list, clean_name: str, dur: str) -> str:
    rows = "".join(
        f"<tr><td>من {p.get('start','')} إلى {p.get('end','')}</td><td>{p.get('category','')}</td><td>{p.get('theme','')}</td><td>{p.get('title','')}</td><td>{p.get('summary','')}</td></tr>"
        for r in valid_chunks if isinstance(r.get("data"), dict)
        for p in r["data"].get("programs", [])
    )
    return (
        f'<div class="header-banner"><h1>تقرير تحليل البث اليومي المفصل</h1><div class="subtitle">{clean_name} — المدة: {dur}</div></div>'
        f'<table><tr><th>التوقيت</th><th>التصنيف</th><th>المحور</th><th>العنوان</th><th>التفاصيل</th></tr>{rows}</table>'
    )


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================
def main():
    global RUNTIME_DRIVE_TOKEN
    started_at = time.time()

    fid = (os.environ.get("FOLDER_ID") or "").strip()
    RUNTIME_DRIVE_TOKEN = (os.environ.get("DRIVE_TOKEN") or "").strip()
    tg_bot = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    tg_chat = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    gm_u = (os.environ.get("GMAIL_USER") or "").strip()
    gm_p = (os.environ.get("GMAIL_APP_PASSWORD") or "").strip()
    run_id = (os.environ.get("GITHUB_RUN_ID") or "").strip()

    fk_list = [k.strip().strip('"').strip("'") for k in re.split(r"[,;\s]+", os.environ.get("GEMINI_FREE_API_KEYS", "")) if k.strip()]
    pk = (os.environ.get("GEMINI_PAID_API_KEY") or "").strip().strip('"').strip("'")

    cfg = {"run_mode": "daily", "expected_hours": 1}
    src_url = ""
    orig_folder_name = ""
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

    mode = cfg.get("run_mode", "daily")
    editorial_mode = cfg.get("editorial_mode", "directed")
    chunk_min = int(cfg.get("chunk_size", 10))
    chunk_sec = chunk_min * 60
    workers = max(1, min(int(cfg.get("workers", 3)), 6))
    strat = cfg.get("tier_strategy", "hybrid")
    system_prompt = cfg.get("system_prompt", "")
    fps = cfg.get("daily_fps", "1/5") if mode == "daily" else cfg.get("eval_fps", "1")
    episode_budget_cap = float(cfg.get("max_budget", 2.0) or 2.0)

    # Preserve clean human-readable name (presenter or date) for reports & Drive folder rename!
    raw_label = cfg.get("presenter_name") or cfg.get("folder_name") or orig_folder_name or mode
    raw_label = re.sub(r"^(GEN_READY_|READY_|day-ready_|PROCESSED_|FAILED_)", "", raw_label, flags=re.IGNORECASE).strip("_ ")
    clean_name = sanitize_filename(raw_label)

    editorial_label = (
        "المنظومة الثالثة: تقييم مهني إعلامي عام ومحايد (بدون خلفية قناة موجهة)"
        if editorial_mode == "neutral"
        else "المنظومة الثانية: تقييم مع مراعاة الخط التحريري للقناة الموجهة"
    )

    push_status(cfg, fid, run_id, "running", "فحص مفاتيح API...", 0, 0, 0.0, started_at)

    # STEP 1: FAST PARALLEL PRE-FLIGHT KEY VALIDATION
    valid_free, valid_paid, paid_models = preflight_validate_keys(fk_list, pk, strat, tg_bot, tg_chat)
    if (strat == "flash_only" and not valid_free) or (strat == "pro_only" and not valid_paid) or (not valid_free and not valid_paid):
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 رابط المصدر: {src_url}\n❌ لا يوجد مفاتيح API صالحة")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "لا يوجد مفاتيح API صالحة")
        sys.exit(1)

    if not src_url:
        send_tg("❌ لا يوجد رابط فيديو في `_source_link.txt`.", tg_bot, tg_chat)
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", "❌ رابط المصدر مفقود")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "الرابط مفقود")
        sys.exit(1)

    # STEP 2: LOCAL DOWNLOAD (eval / <= 2.5h / YouTube) & DURATION PROBE
    local_input = src_url
    downloaded_local = False
    tmp_src = "/tmp/source_video.mp4"

    dur = float(cfg.get("expected_hours", 1)) * 3600
    if not is_youtube_url(src_url):
        try:
            probe_out = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", src_url],
                capture_output=True, text=True, timeout=30,
            ).stdout.strip()
            if probe_out:
                dur = float(probe_out)
        except Exception as e:
            print(f"[ffprobe warning] {e}", flush=True)

    if mode == "eval" or dur <= 9000 or is_youtube_url(src_url):
        push_status(cfg, fid, run_id, "running", "تحميل الفيديو محلياً...", 0, max(1, math.ceil(dur / chunk_sec)), 0.0, started_at)
        send_tg(f"📥 تحميل الحلقة محلياً ({clean_name}) لمنع انقطاع البث...", tg_bot, tg_chat)
        if acquire_video_locally(src_url, tmp_src, cfg):
            local_input = tmp_src
            downloaded_local = True
            size_mb = os.path.getsize(tmp_src) / (1024 * 1024)
            print(f"✅ Downloaded locally: {size_mb:.1f} MB", flush=True)
            try:
                probe_local = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", tmp_src],
                    capture_output=True, text=True, timeout=30,
                ).stdout.strip()
                if probe_local:
                    dur = float(probe_local)
            except Exception:
                pass
        else:
            print("⚠️ Local download failed; falling back to resilient HTTP streaming.", flush=True)

    exact_duration = format_hhmmss(dur)
    tot = math.ceil(dur / chunk_sec)
    if tot <= 0:
        send_tg("❌ الرابط غير صالح أو مدته صفر.", tg_bot, tg_chat)
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 رابط المصدر: {src_url}\n❌ مدة الفيديو صفر")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "مدة الفيديو صفر")
        sys.exit(1)

    if push_status(cfg, fid, run_id, "running", f"بدء تحليل المقاطع (0/{tot})", 0, tot, 0.0, started_at):
        send_tg("🛑 تم إلغاء العملية بناءً على طلب المستخدم.", tg_bot, tg_chat)
        finalize_cost_webhook(cfg, fid, 0.0, "cancelled", "تم الإلغاء")
        sys.exit(0)

    send_tg(f"🚀 بدء التحليل V5.7 | {clean_name} | المدة: {exact_duration} | المقاطع: {tot} | المسار: {strat}", tg_bot, tg_chat)

    results = [None] * tot
    act_cost = 0.0
    completed_count = 0
    exhausted_pairs = set()
    bad_keys = set()
    models_used = set()
    failed = []
    processing_start = time.time()

    def budget_ok() -> bool:
        with STATE_LOCK:
            return act_cost < episode_budget_cap

    def process_chunk(idx, force_paid=False):
        if CANCEL_EVENT.is_set():
            raise RuntimeError("Cancelled by user")
        time.sleep(random.uniform(0.3, 1.5) * (idx % workers))
        st = idx * chunk_sec
        d = min(chunk_sec, dur - st)
        st_hms = format_hhmmss(st)
        end_hms = format_hhmmss(st + d)
        mp4_path = f"/tmp/chunk_{idx}.mp4"

        extracted = False
        for attempt in range(1, 4):
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

        if mode == "eval":
            pr = build_eval_chunk_prompt(st_hms, end_hms, idx == tot - 1, editorial_mode, system_prompt)
        else:
            pr = build_daily_chunk_prompt(st_hms, end_hms, system_prompt)

        run_strat = "pro_only" if force_paid else strat
        try:
            data, cost, used_model, tier = call_gemini_chunk(
                mp4_path, pr, valid_free, valid_paid, paid_models, run_strat, exhausted_pairs, bad_keys, budget_ok
            )
            with STATE_LOCK:
                models_used.add(f"{tier}:{used_model}")
            print(f"✅ Chunk {idx+1}/{tot} ({st_hms}->{end_hms}) done via {tier} ({used_model}) | cost=${cost:.4f}", flush=True)
            return data, cost, f"من {st_hms} إلى {end_hms}"
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
                data, c, win = fut.result()
                results[i] = {"chunk_index": i + 1, "window": win, "data": data}
                with STATE_LOCK:
                    act_cost += c
                    completed_count += 1
                    cur_done = completed_count
                    cur_cost = act_cost
                proc_elapsed = time.time() - processing_start
                eta = int((proc_elapsed / cur_done) * (tot - cur_done)) + 25
                push_status(cfg, fid, run_id, "running", f"تحليل المقاطع ({cur_done}/{tot})", cur_done, tot, cur_cost, started_at, eta_sec=eta)
            except Exception as e:
                if CANCEL_EVENT.is_set() or "Cancelled by user" in str(e):
                    break
                failed.append(i)
                print(f"⚠️ Chunk {i+1} Failed: {e}", flush=True)

    if CANCEL_EVENT.is_set():
        if downloaded_local and os.path.exists(local_input):
            try:
                os.remove(local_input)
            except Exception:
                pass
        send_tg(f"🛑 تم إيقاف العملية ({clean_name}). الكلفة: ${act_cost:.4f}", tg_bot, tg_chat)
        update_drive_folder_meta(fid, cfg, f"CANCELLED_{clean_name}", f"🔗 رابط المصدر: {src_url}\n🛑 ملغاة من لوحة التحكم")
        finalize_cost_webhook(cfg, fid, act_cost, "cancelled", "تم الإلغاء من لوحة التحكم")
        sys.exit(0)

    # DLQ Rescue
    if failed:
        print(f"Entering Dead-Letter Queue (DLQ) for {len(failed)} chunks...", flush=True)
        push_status(cfg, fid, run_id, "running", f"إنقاذ المقاطع المتعثرة ({len(failed)})...", completed_count, tot, act_cost, started_at)
        time.sleep(8)
        for i in failed:
            if CANCEL_EVENT.is_set():
                break
            try:
                use_paid = (strat != "flash_only") and (valid_paid is not None) and budget_ok()
                data, c, win = process_chunk(i, force_paid=use_paid)
                results[i] = {"chunk_index": i + 1, "window": win, "data": data}
                with STATE_LOCK:
                    act_cost += c
                    completed_count += 1
                push_status(cfg, fid, run_id, "running", f"إنقاذ المقاطع ({completed_count}/{tot})", completed_count, tot, act_cost, started_at)
                print(f"🚑 Chunk {i+1} Rescued.", flush=True)
            except Exception as ex:
                print(f"❌ DLQ Rescue Failed for chunk {i+1}: {ex}", flush=True)
                results[i] = None
                time.sleep(3)

    if downloaded_local and os.path.exists(local_input):
        try:
            os.remove(local_input)
        except Exception:
            pass

    valid_chunks = [r for r in results if r is not None and r.get("data")]
    if not valid_chunks:
        send_tg(f"❌ فشلت جميع المقاطع ({clean_name}). تم استرداد الرصيد المحجوز.", tg_bot, tg_chat)
        update_drive_folder_meta(fid, cfg, f"FAILED_{clean_name}", f"🔗 رابط المصدر: {src_url}\n❌ فشلت جميع المقاطع")
        finalize_cost_webhook(cfg, fid, 0.0, "failed", "فشلت جميع المقاطع")
        sys.exit(1)

    # STEP 3: SYNTHESIS -> DETAILED HTML + SUMMARIZED HTML + FORENSIC BLACKBOX
    push_status(cfg, fid, run_id, "running", "تجميع التقريرين المفصل والمختصر والصندوق الأسود...", len(valid_chunks), tot, act_cost, started_at, eta_sec=25)

    synth_candidates = []
    if strat != "flash_only" and valid_paid and budget_ok():
        synth_order = rank_discovered_models(paid_models, PREFERRED_SYNTH_ORDER, allow_pro=True) or paid_models
        for m in synth_order:
            synth_candidates.append((valid_paid, m, True))
    for k, kmodels in valid_free:
        for m in kmodels:
            synth_candidates.append((k, m, False))

    if mode == "eval":
        synth_prompt = build_eval_synthesis_prompt(clean_name, exact_duration, editorial_mode, system_prompt) + json.dumps(valid_chunks, ensure_ascii=False)
    else:
        synth_prompt = build_daily_synthesis_prompt(clean_name, exact_duration, system_prompt) + json.dumps(valid_chunks, ensure_ascii=False)

    final_data = {}
    synth_used_label = ""
    for skey, smodel, spaid in synth_candidates:
        try:
            client = genai.Client(api_key=skey)
            res = client.models.generate_content(
                model=smodel,
                contents=synth_prompt,
                config=build_gen_config(max_out=65536),
            )
            final_data = clean_json_text(res.text)
            c_syn = calc_cost(smodel, res.usage_metadata, spaid)
            act_cost += c_syn
            synth_used_label = f"{'PAID' if spaid else 'FREE'}:{smodel}"
            models_used.add(synth_used_label)
            print(f"✅ Synthesis complete via {synth_used_label} | cost=${c_syn:.4f}", flush=True)
            break
        except Exception as e:
            print(f"[Synthesis Retry {smodel}] {str(e)[:180]}", flush=True)

    # Pull last_spoken_words from last valid chunk if synthesis omitted it
    if mode == "eval" and not final_data.get("lastSpokenWords"):
        for r in reversed(valid_chunks):
            lsw = (r.get("data") or {}).get("last_spoken_words", "")
            if lsw:
                final_data["lastSpokenWords"] = lsw
                break

    banner_html = (
        f'<div class="thinking-note">المنظومة: V5.7 ({editorial_label if mode == "eval" else "تحليل البث اليومي"}) '
        f'| المقاطع: {len(valid_chunks)}/{tot} | التجميع: {synth_used_label or "Auto"} | التكلفة: ${act_cost:.4f}</div>'
    )

    if mode == "eval":
        det_inner = final_data.get("mainDetailedHtml") or fallback_eval_detailed_html(final_data, clean_name, exact_duration, editorial_label)
        sum_inner = final_data.get("summarizedHtml") or det_inner
        det_filename = f"01_Main_Detailed_Report_{clean_name}.html"
        sum_filename = f"02_Summarized_Report_{clean_name}.html"
    else:
        det_inner = final_data.get("mainDetailedHtml") or fallback_daily_detailed_html(valid_chunks, clean_name, exact_duration)
        sum_inner = final_data.get("summarizedHtml") or det_inner
        det_filename = f"01_Daily_Detailed_Report_{clean_name}.html"
        sum_filename = f"02_Daily_Summarized_Report_{clean_name}.html"

    bb_filename = f"03_Blackbox_Forensic_{clean_name}.md"

    det_html = wrap_royal_html(f"التقرير المفصل - {clean_name}", det_inner, banner_html)
    sum_html = wrap_royal_html(f"التقرير المختصر - {clean_name}", sum_inner, banner_html)

    meta_info = {
        "clean_name": clean_name,
        "presenter_name": final_data.get("presenter") or cfg.get("presenter_name") or clean_name,
        "mode": mode,
        "editorial_mode": editorial_mode,
        "editorial_label": editorial_label if mode == "eval" else "رصد وتحليل بث يومي شامل",
        "exact_duration": exact_duration,
        "chunks_ok": len(valid_chunks),
        "chunks_total": tot,
        "chunk_min": chunk_min,
        "fps": fps,
        "src_url": src_url,
        "models_used": models_used,
        "actual_cost": act_cost,
        "system_prompt": system_prompt,
    }
    blackbox_md = generate_forensic_blackbox_md(mode, valid_chunks, final_data, meta_info)

    elapsed_sec = int(time.time() - started_at)
    elapsed_str = f"{elapsed_sec // 60} دقيقة و {elapsed_sec % 60} ثانية"

    run_summary = {
        "status": "SUCCESS",
        "clean_name": clean_name,
        "mode": mode,
        "editorial_label": meta_info["editorial_label"],
        "models_used": sorted(models_used),
        "chunks_ok": len(valid_chunks),
        "chunks_total": tot,
        "duration_hms": exact_duration,
        "elapsed_str": elapsed_str,
        "actual_cost_usd": round(act_cost, 4),
        "last_spoken_words": final_data.get("lastSpokenWords", ""),
        "total_score": final_data.get("total_score", None),
        "percentage": final_data.get("percentage", None),
    }

    # Save all files locally in ./backup_reports (for GitHub Actions Artifacts + uploads)
    det_path = os.path.join(BACKUP_DIR, det_filename)
    sum_path = os.path.join(BACKUP_DIR, sum_filename)
    bb_path = os.path.join(BACKUP_DIR, bb_filename)
    js_path = os.path.join(BACKUP_DIR, "_run_summary.json")

    with open(det_path, "w", encoding="utf-8") as f:
        f.write(det_html)
    with open(sum_path, "w", encoding="utf-8") as f:
        f.write(sum_html)
    with open(bb_path, "w", encoding="utf-8") as f:
        f.write(blackbox_md)
    with open(js_path, "w", encoding="utf-8") as f:
        json.dump(run_summary, f, ensure_ascii=False, indent=2)

    out_files = [det_path, sum_path, bb_path]

    # STEP 4: MULTI-CHANNEL DELIVERY (Drive + Telegram + SMTP/GmailApp Backup)
    for fpath in out_files:
        mime = "text/markdown" if fpath.endswith(".md") else "text/html"
        drive_upload(fid, cfg, fpath, mime)
        send_tg(f"📄 <b>{os.path.basename(fpath)}</b>\n• المدة: {exact_duration} | المقاطع: {len(valid_chunks)}/{tot} | الكلفة: ${act_cost:.4f}", tg_bot, tg_chat, fpath)
    drive_upload(fid, cfg, js_path, "application/json")

    email_subj = f"✅ اكتمل تقرير V5.7 ({clean_name}) | الكلفة: ${act_cost:.4f}"
    smtp_ok = send_smtp_resilient(email_subj, sum_html, out_files, cfg.get("alert_emails", ""), gm_u, gm_p)

    # Rename Drive folder PRESERVING presenter/episode name + full rich description!
    final_folder_name = f"PROCESSED_{clean_name}_${act_cost:.2f}"
    folder_desc = (
        f"🔗 رابط المصدر: {src_url}\n\n"
        f"COST_USD: ${act_cost:.4f} | CHUNKS: {len(valid_chunks)}/{tot}\n"
        f"• الاسم: {clean_name}\n"
        f"• المسار: {meta_info['editorial_label']}\n"
        f"• النماذج المستخدمة: {', '.join(sorted(models_used))}\n"
        f"• المدة الإجمالية: {exact_duration} | وقت التنفيذ: {elapsed_str}\n"
        + (f"• العلامة النهائية: {final_data.get('total_score',0)}/90 ({final_data.get('percentage',0)}%)\n" if mode == "eval" else "")
        + (f"• آخر كلام في الحلقة:\n«{final_data.get('lastSpokenWords','')}»\n" if final_data.get("lastSpokenWords") else "")
    )
    update_drive_folder_meta(fid, cfg, final_folder_name, folder_desc)

    folder_url = f"https://drive.google.com/drive/folders/{fid}"
    send_tg(
        f"✅ <b>اكتمل التحليل بنجاح: {clean_name}</b>\n"
        f"• المسار: {meta_info['editorial_label']}\n"
        + (f"• العلامة: <b>{final_data.get('total_score',0)}/90 ({final_data.get('percentage',0)}%)</b>\n" if mode == "eval" else "")
        + f"• المقاطع: {len(valid_chunks)}/{tot} | المدة: {exact_duration} | الاستغراق: {elapsed_str}\n"
        f"• الكلفة الفعلية: <b>${act_cost:.4f}</b>\n"
        + (f"• آخر كلام في الحلقة: «{final_data.get('lastSpokenWords','')}»\n" if final_data.get("lastSpokenWords") else "")
        + f"📁 المجلد: {folder_url}",
        tg_bot,
        tg_chat,
    )

    finalize_cost_webhook(
        cfg,
        fid,
        act_cost,
        "completed",
        f"اكتمل بنجاح ({len(valid_chunks)}/{tot})",
        trigger_gas_email=(not smtp_ok),
        email_subject=email_subj,
    )
    print(f"🎉 Pipeline V5.7 completed! Folder: {final_folder_name} | Chunks: {len(valid_chunks)}/{tot} | Cost: ${act_cost:.4f}", flush=True)


if __name__ == "__main__":
    main()
