# تعليمات المقطع (Chunk Prompt)

> يُعبَّأ ويُلصق بعد `prompt_system.md`. مرفق معه: `entities.csv`.

---

## معطيات هذه الاستدعاء

| الحقل | القيمة |
|---|---|
| رقم المقطع | `{{CHUNK_INDEX}}` من `{{CHUNK_TOTAL}}` |
| يبدأ عند | `{{CHUNK_START}}` من التسجيل الكامل |
| ينتهي عند | `{{CHUNK_END}}` |
| البرنامج | `{{PROGRAM_NAME_IF_KNOWN}}` |
| سياق المقطع السابق | أدناه |

**سياق تراكمي (`running_context.json`):**
```json
{{RUNNING_CONTEXT}}
```

**سجل الكيانات:**
```csv
{{ENTITIES_CSV}}
```

> **تذكير:** كل طابع زمني تكتبه يجب أن يكون **مطلقاً** من بداية التسجيل، لا من بداية هذا المقطع. هذا المقطع يبدأ عند `{{CHUNK_START}}`.

---

## مهمّتك: سبع مهام بالترتيب

### المهمة ١ — الميتاداتا: استخرجها ولا تنتظرها

افعل ثلاثة أشياء:

**أ. اقرأ اسم ملف المصدر.** إن احتوى طابع Unix (مثل `archive-1795033243-3025.mp4`)، فحوّله إلى تاريخ ووقت UTC ثم إلى توقيت بيروت. سجّل النتيجة في `metadata.derived.air_datetime_local`.

**ب. اقرأ ساعة الشاشة.** إن ظهرت ساعة على الشاشة في أي إطار، فسجّل قيمتها في `metadata.on_screen_clock` مع الطابع الزمني لظهورها.

**ج. قارن الاثنين.** إن اتفقا في حدود ±3 دقائق، فضعهما في `metadata.confidence: "high"`. وإن اختلفا أو غاب أحدهما، فسجّل الفرق صريحاً في `metadata_conflicts[]` ولا تختر بصمت.

> لا تكتب `"غير محدد"` في حقل التاريخ إن كان اسم الملف يحمل التاريخ. ابحث أولاً.

---

### المهمة ٢ — تفكيك الأدوار (`turns[]`): أهم مخرج في هذه الحزمة

سجّل **كل** دور كلامي، بلا استثناء ولا انتقاء. هذا الجدول هو ما سيُقاس عليه كل شيء لاحقاً.

| الحقل | القاعدة |
|---|---|
| `id` | متسلسل يبدأ من `{{CHUNK_INDEX}}*1000` |
| `speaker_id` | `ANCHOR` / `GUEST_1` / `REPORTER_1` (من السياق التراكمي) |
| `start`, `end` | `HH:MM:SS.mmm` — دقّة عُشر الثانية |
| `duration_s` | محسوب |
| `word_count` | عدّ الكلمات في `text` (الكلمات مفصولة بمسافة) |
| `text` | **النصّ الكامل غير المقتطع** |
| `kind` | `question` / `followup` / `interrupt` / `transition` / `summary` / `chyron_read` / `closing` / `answer` / `monologue` / `report` |
| `question_count` | عدد الاستفهامات الفعلية في الدور |
| `interrupted_speaker` | من كان يتكلّم قبل هذا الدور (أو `null`) |
| `mid_sentence` | `true` إن بدأ هذا الدور والآخر في منتصف جملة |
| `overlap_s` | ثواني التراكب الصوتي |
| `verbatim_confidence` | `high` / `medium` / `low` |
| `possible_asr_error` | مصفوفة كلمات يشتبه في تفريغها |

**ثلاث قواعد صارمة:**

1. **لا تختصر النصّ.** «النص الكامل» يعني كل كلمة نُطقت. المقتطف يحرم التحليل من قياس طول السؤال.
2. **افصل كل جملة تبادل.** لا تدمج ثلاثة أسئلة في دور واحد إن كانت نُطقت متصلة — لكن لا تُفكّك دوراً واحداً إلى ثلاثة إن كان استفهاماً واحداً طويلاً.
3. **`word_count` حقيقي.** لا تقدّره. عدّه.

**مثال — دور واحد:**

```json
{
  "id": 3128,
  "speaker_id": "ANCHOR",
  "start": "00:31:28.400",
  "end": "00:32:00.100",
  "duration_s": 31.7,
  "word_count": 79,
  "text": "دكتور وسام كيف يمكن للإيراني أن يستفيد من الواقع الإقليمي الجديد؟ ذكرت قبل قليل عناوين… اليمني أصبح قريباً من باب المندب بل مسيطراً… هناك ضغوط عسكرية… كيف يستفيد الإيراني من كل هذه العوامل؟",
  "kind": "question",
  "question_count": 3,
  "interrupted_speaker": null,
  "mid_sentence": false,
  "overlap_s": 0.0,
  "verbatim_confidence": "high",
  "possible_asr_error": []
}
```

> لاحظ: `question_count: 3` و`word_count: 79`. هذان الحقلان هما ما يجعل «السؤال المطوّل» قابلاً للقياس بدل أن يكون انطباعاً.

---

### المهمة ٣ — التدخّلات (`interventions[]`)

كل دور للمقدّم **ليس الإجابة** هو تدخّل. سجّله ككائن مستقلّ:

```json
{
  "id": 41,
  "turn_id": 3128,
  "at": "00:31:28.400",
  "type": "redirect",
  "cue_source": "none",
  "interrupted_speaker": null,
  "mid_sentence": false,
  "duration_s": 31.7,
  "result": "new_file_opened"
}
```

`type`: `polite_hold` / `summarize_handback` / `pin_claim` / `redirect` / `time_warning` / `cut_for_breaking` / `question` / `closing`.
`cue_source`: **`guest_words`** (بدأ بكلمة من كلام الضيف) · `chyron` · `own_phrase` · `none`.
`result`: ماذا أنتج التدخّل: `new_file_opened` / `claim_pinned` / `returned_to_guest` / `pace_corrected` / `none`.

> `cue_source` هو الحقل الذي يقيس جودة التدخّل لا عدده فقط. التدخّل المبني على كلام الضيف أفضل من التدخّل المبني على ورقة المقدّم.

---

### المهمة ٤ — الشريط الزمني (`chyron_timeline[]`)

**كل** ما يظهر على الشاشة من نصّ: الشريط العاجل، الشرائط الجارية، بطاقات التعريف، العناوين.

```json
{
  "id": "CH-14",
  "on": "00:44:30.000",
  "off": "00:45:12.000",
  "type": "local_security",
  "text_raw": "مراسل المنار: غارتان صهيونيتان استهدفتا أطراف بلدة حداثا جنوبي لبنان",
  "ocr_confidence": 0.93,
  "read_on_air_at": null,
  "read_by": null,
  "response_latency_s": null
}
```

`type`: `breaking` / `local_security` / `regional` / `lower_third` / `ticker` / `program_bug`.

**قاعدتان حاسمتان:**

1. **سجّل الشريط ولو لم يُقرأ على الهواء.** الشريط غير المقروء أهمّ من المقروء، لأنه يكشف ما فات.
2. **`read_on_air_at = null` إن لم يُنطق.** لا تضع وقتاً تقديرياً. الفارغ هنا قيمة دلالية، لا نقص.

---

### المهمة ٥ — الادعاءات (`claims[]`)

استخرج كل ادعاء **قابل للمساءلة**: رقم، تاريخ، قرار، نسبة، سبب، نفي، إثبات.

```json
{
  "id": "C-07",
  "speaker_id": "GUEST_1",
  "turn_id": 3155,
  "at": "00:33:10.200",
  "text": "الوجود الأميركي بات عبئاً على داعميه",
  "type": "causal",
  "checkable": true,
  "evidence_offered": false,
  "evidence_type": null,
  "anchor_challenged_at": null,
  "challenge_type": null
}
```

`type`: `numeric` / `causal` / `predictive` / `historical` / `attribution` / `denial`.
`evidence_type`: `number` / `document` / `official_statement` / `witness` / `none`.
`challenge_type`: `ask_source` / `counter_hypothesis` / `fact_check` / `none`.

**قاعدة الفصل:** إن كان الادعاء لطرف ثالث (وسائل إعلام، مسؤول لم يظهر)، فاجعل `speaker_id` للطرف الذي نقله، وضع في `attribution` صاحب الادعاء الأصلي.

**ممنوع** أن تضع `anchor_challenged_at` تقديرياً. إن لم يُسأل، فالقيمة `null` — وهذا هو المقياس نفسه.

---

### المهمة ٦ — الكادر (`shots[]`)

بمعدّل إطار لكل ثانية، تستطيع تحديد: من على الشاشة، بأي حجم كادر، وأي إشارة جسدية ظاهرة.

```json
{
  "start": "00:42:15.000",
  "end": "00:42:31.000",
  "framing": "MS",
  "subject": "ANCHOR",
  "gaze": "camera",
  "gesture": "point",
  "confidence": "medium"
}
```

`framing`: `WS` / `MS` / `MCU` / `CU` / `2S` (كادر ثنائي) / `GRAPHIC` / `SPLIT` / `VT`.
`gesture`: `none` / `point` / `pen` / `open_hand` / `chin` / `nod` / `shrug`.
`confidence`: **لا تتجاوز `medium`** في هذا الحقل — معدّل الإطارات لا يسمح بأكثر.

> سجّل فقط ما تراه بوضوح. لا تُفسّر النيّة. «وضع يده على ذقنه» معلومة؛ «يُظهر إصغاءً عميقاً» تفسير، وليس من مهمّتك هنا.

---

### المهمة ٧ — الملفات والأحداث الحرجة

**`files[]`** — تجميع المقاطع في ملفات موضوعية. إن كان الملف مكرّراً لملف سابق، فسجّل `duplicated_of`.

```json
{
  "id": "F-03",
  "title": "قواعد الاشتباك — الكويت والبحرين",
  "start": "00:15:00.000",
  "end": "00:24:00.000",
  "sources": ["GUEST_1"],
  "questions_count": 3,
  "duplicated_of": null
}
```

**`critical_events[]`** — الأحداث التي تستوجب تدخّلاً في التقييم. اكشفها آلياً:

```json
[
  {"type": "monologue_over_180s", "at": "00:32:00.000", "duration_s": 290.0, "speaker_id": "GUEST_1"},
  {"type": "local_chyron_unread", "at": "00:44:30.000", "latency_s": null},
  {"type": "duplicate_file", "at": "00:31:28.400", "of": "00:27:16.000"},
  {"type": "name_error", "at": "00:49:50.000", "said": "محمد عازم", "expected": "محمد قاسم", "confidence": "medium"}
]
```

`type`: `monologue_over_180s` / `local_chyron_unread` / `duplicate_file` / `name_error` / `factual_error_uncorrected` / `guest_uninterrupted_over_300s` / `topic_abandoned`.

---

## المخرجات

أخرج كائناً JSON واحداً بهذا الشكل، مطابقاً لـ`schema_chunk.json`:

```json
{
  "chunk_index": {{CHUNK_INDEX}},
  "window": {"start": "{{CHUNK_START}}", "end": "{{CHUNK_END}}"},
  "prompt_version": "blackbox-v2.0",
  "metadata": { "...": "..." },
  "metadata_conflicts": [],
  "participants": [],
  "turns": [],
  "interventions": [],
  "chyron_timeline": [],
  "claims": [],
  "shots": [],
  "files": [],
  "critical_events": [],
  "measurement": { "...": "..." },
  "data_gaps": [],
  "self_check": { "...": "..." }
}
```

### `measurement` — إفصاح إلزامي

```json
{
  "method": "asr_vad",
  "speech_segments_measured": true,
  "chunk_duration_s": 300,
  "sum_of_turns_s": 274.3,
  "unaccounted_s": 25.7,
  "unaccounted_reason": "فيديو بلا تعليق + صمت قبل الانتقال",
  "forced_to_match_chunk": false
}
```

> `forced_to_match_chunk` يجب أن تكون `false` دائماً. إن وجدت نفسك تعدّل الأرقام لتصل إلى مدّة المقطع، فأنت تُفسد البيانات. **الفارق غير المحسوب مطلوب، لا ممنوع.**

### `self_check` — فحص ذاتي قبل الإخراج

```json
{
  "all_timestamps_absolute": true,
  "all_timestamps_within_chunk": true,
  "every_turn_has_word_count": true,
  "every_claim_has_turn_id": true,
  "no_forced_time_normalization": true,
  "all_chyrons_logged_even_if_unread": true,
  "names_resolved_against_entities": true,
  "data_gaps_declared": true
}
```

إن كانت أي قيمة `false`، **صحّح قبل الإخراج**. لا تُخرج كائناً وأنت تعلم أن أحد هذه الفحوص فاشل.

---

## ثلاث محرّمات

1. **لا تلخّص بدل أن تُعدّ.** إن كتبت فقرة عن «إدارة الحلقة» بدل جدول الأدوار، فأنت تُعيد إنتاج عطب الإصدار الأول.
2. **لا تضبط المجموع.** إن كان مجموع الأدوار 274 ثانية ومقطعك 300 (5 دقائق)، فاكتب 274.
3. **لا تُقيّم.** لا «نجح»، لا «أخفقت»، لا «جيد». التقييم في استدعاء آخر.
