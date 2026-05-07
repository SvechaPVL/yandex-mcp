# Yandex MCP Shorts Series — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Произвести и опубликовать серию из 6 вертикальных шортсов о MCP-сервере `yandex-mcp` через AI-нативный пайплайн (mcp-video + video-edit MCP + Whisper) с реальными кейсами оптимизации asiapk.ru.

**Architecture:** Многофазный конвейер: pre-production (Claude пишет сценарии и Playwright-spec'и) → production (человек записывает face-cam + voice одной сессией) → assembly (Claude через MCP-серверы записывает screencast'ы и собирает финальные mp4) → polish → publish. После Шорта #1 — контрольная точка для решения о продолжении или откате на CapCut-fallback.

**Tech Stack:**
- **Запись:** OBS Studio (face-cam Portrait сцена, Voice-only сцена)
- **Скринкасты:** `mcp-video` MCP сервер (Playwright + FFmpeg, 60fps)
- **Монтаж:** `video-edit` MCP сервер (FFmpeg-обёртка)
- **Субтитры:** Whisper через `video-edit` MCP с initial_prompt
- **TTS (опционально):** ElevenLabs MCP
- **Спека:** `~/projects/yandexDirectAsia/yandex-mcp/docs/superpowers/specs/2026-05-07-yandex-mcp-shorts-series-design.md`
- **Workspace:** `~/projects/yandex-mcp-shorts/` (отдельно от MCP-проекта)
- **Платформы:** YouTube Shorts, VK Clips, Telegram, Reels (опц.)

**Спека:** [2026-05-07-yandex-mcp-shorts-series-design.md](../specs/2026-05-07-yandex-mcp-shorts-series-design.md)

---

## Phase 0 — Setup (~3 ч, один раз)

### Task 0.1: Resolve open questions

**Files:**
- Create: `~/projects/yandex-mcp-shorts/answers.md`

- [ ] **Step 1: Подготовить вопросы и получить ответы от пользователя**

В разговоре спросить и записать ответы в `answers.md`:

```markdown
# Project answers (filled before starting Phase 1)

1. **Telegram-канал:** [имя канала или "создаём новый"]
2. **GitHub-репо для CTA:** [SvechaPVL/yandex-mcp или другой URL]
3. **Анонимизация asiapk.ru:** [показываем как есть / blur домена / полная анонимизация]
4. **Музыкальный жанр:** [свой выбор или "по умолчанию minimal tech 120-130 BPM"]
```

- [ ] **Step 2: Commit answers**

```bash
cd ~/projects/yandex-mcp-shorts/
git init
git add answers.md
git commit -m "chore: project answers locked in"
```

---

### Task 0.2: Install MCP video servers

**Files:** (none — вне репозитория, конфигурация Claude Code/Desktop)

- [ ] **Step 1: Проверить, какие MCP уже подключены**

Run: `claude mcp list`
Expected: вывод списка установленных MCP-серверов.

- [ ] **Step 2: Установить mcp-video если отсутствует**

Run (если отсутствует): `claude mcp add mcp-video -- npx -y @modelcontextprotocol/server-mcp-video`
Expected: подтверждение добавления.

- [ ] **Step 3: Установить video-edit MCP если отсутствует**

Run (если отсутствует): `claude mcp add video-edit -- npx -y video-edit-mcp`
Expected: подтверждение добавления.

- [ ] **Step 4: Перезапустить Claude Code чтобы серверы активировались**

Run: закрыть и снова открыть Claude Code.

- [ ] **Step 5: Verify**

Run: `claude mcp list`
Expected: `mcp-video` и `video-edit` в списке со статусом ✓.

---

### Task 0.3: Install Playwright browsers

**Files:** (none — глобальная установка)

- [ ] **Step 1: Установить браузеры Playwright**

Run: `npx playwright install chromium`
Expected: вывод "Downloading Chromium..." и "Chromium installed".

- [ ] **Step 2: Verify**

Run: `npx playwright --version`
Expected: версия Playwright (≥1.40).

---

### Task 0.4: Create workspace structure

**Files:**
- Create: `~/projects/yandex-mcp-shorts/` со структурой папок

- [ ] **Step 1: Создать дерево директорий**

Run (PowerShell):
```powershell
$root = "$env:USERPROFILE\projects\yandex-mcp-shorts"
$dirs = @(
  "scripts",
  "face-cam\hooks",
  "face-cam\ctas",
  "voice",
  "screencasts\specs",
  "screencasts\recordings",
  "final",
  "thumbnails",
  "posts",
  "assets\music",
  "assets\sfx"
)
foreach ($d in $dirs) { New-Item -ItemType Directory -Force -Path "$root\$d" | Out-Null }
```

- [ ] **Step 2: Verify**

Run: `Get-ChildItem -Recurse -Directory ~/projects/yandex-mcp-shorts/ | Select-Object FullName`
Expected: 11 директорий созданы.

- [ ] **Step 3: Add .gitignore for binary artifacts**

Create: `~/projects/yandex-mcp-shorts/.gitignore`
```
face-cam/
voice/*.wav
screencasts/recordings/
final/
assets/music/
assets/sfx/
```

- [ ] **Step 4: Commit workspace skeleton**

```bash
cd ~/projects/yandex-mcp-shorts/
git add .gitignore
git commit -m "chore: workspace skeleton and gitignore"
```

---

### Task 0.5: Configure OBS Studio scenes

**Files:** (none — настройки OBS)

- [ ] **Step 1: Открыть OBS Studio. Создать сцену `Face-cam Portrait 1080×1920`**

В OBS:
- Settings → Video → Base Resolution: 1080×1920, Output Resolution: 1080×1920, FPS: 60
- Scenes → "+" → имя `Face-cam Portrait 1080×1920`
- Sources → "+" → Video Capture Device → выбрать камеру → resize source to fill canvas
- Sources → "+" → Audio Input Capture → выбрать микрофон

- [ ] **Step 2: Создать сцену `Voice-only`**

В OBS:
- Scenes → "+" → имя `Voice-only`
- Sources → "+" → Audio Input Capture → выбрать тот же микрофон
- Settings → Output → Audio Encoder: AAC 192 kbps

- [ ] **Step 3: Verify обе сцены работают**

Переключиться между сценами, проверить, что:
- В Face-cam Portrait видно изображение с камеры в портретной ориентации
- В Voice-only audio meter показывает звук от микрофона

---

### Task 0.6: Test recording — face-cam

**Files:**
- Create: `~/projects/yandex-mcp-shorts/test/face-cam-test.mkv`

- [ ] **Step 1: Записать 30-секундный тест face-cam**

В OBS: переключиться на сцену `Face-cam Portrait 1080×1920` → Start Recording → говорить тестовую фразу → Stop Recording после 30 сек.

- [ ] **Step 2: Переместить запись в test/**

```powershell
Move-Item "$env:USERPROFILE\Videos\*.mkv" "$env:USERPROFILE\projects\yandex-mcp-shorts\test\face-cam-test.mkv"
```

- [ ] **Step 3: Verify качество**

Открыть `face-cam-test.mkv` плеером (VLC или MPC). Проверить:
- Разрешение: 1080×1920 (правый клик → Codec Information в VLC)
- Изображение чёткое, в фокусе
- Свет адекватный (нет жёстких теней под глазами)
- Звук синхронен, без хрипов и эха

- [ ] **Step 4: Если что-то не так — поправить и переснять. Если ок — отметить готово.**

---

### Task 0.7: Test recording — voice-only and verify LUFS

**Files:**
- Create: `~/projects/yandex-mcp-shorts/test/voice-test.wav`

- [ ] **Step 1: Записать 30-секундный voice-тест**

В OBS: сцена `Voice-only` → Start Recording → произнести 5-6 предложений в нормальной артикуляции → Stop.

- [ ] **Step 2: Извлечь аудио в WAV**

Run:
```powershell
ffmpeg -i "$env:USERPROFILE\Videos\last-recording.mkv" -vn -c:a pcm_s16le "$env:USERPROFILE\projects\yandex-mcp-shorts\test\voice-test.wav"
```

- [ ] **Step 3: Проверить LUFS**

Run:
```powershell
ffmpeg -i "$env:USERPROFILE\projects\yandex-mcp-shorts\test\voice-test.wav" -af loudnorm=I=-19:TP=-1.5:LRA=11:print_format=summary -f null NUL 2>&1 | Select-String "Input Integrated"
```
Expected: `Input Integrated: -X.X LUFS` где X между 16 и 22.

- [ ] **Step 4: Если LUFS вне диапазона — отрегулировать гейн микрофона в OBS, перезаписать.**

---

### Task 0.8: Acquire single music track for the series

**Files:**
- Create: `~/projects/yandex-mcp-shorts/assets/music/track.mp3`

- [ ] **Step 1: Зайти на YouTube Audio Library**

URL: https://www.youtube.com/audiolibrary
Filter: Genre = Electronic, Mood = Inspirational/Bright, Length = ≥3 min, Attribution = Not Required.

- [ ] **Step 2: Скачать 3 кандидатные дорожки**

Save в: `~/projects/yandex-mcp-shorts/assets/music/candidates/`

- [ ] **Step 3: Прослушать каждую вместе с тестовым голосом**

Использовать любой DAW или CapCut: положить voice-test.wav поверх кандидата на −12 dB, послушать 30 сек.

- [ ] **Step 4: Выбрать одну, переименовать в `track.mp3`, удалить кандидатов**

```powershell
Move-Item "$env:USERPROFILE\projects\yandex-mcp-shorts\assets\music\candidates\<chosen>.mp3" "$env:USERPROFILE\projects\yandex-mcp-shorts\assets\music\track.mp3"
Remove-Item -Recurse "$env:USERPROFILE\projects\yandex-mcp-shorts\assets\music\candidates\"
```

---

### Task 0.9: Acquire SFX pack (whoosh, bell, bass-drop)

**Files:**
- Create: `~/projects/yandex-mcp-shorts/assets/sfx/whoosh.wav`
- Create: `~/projects/yandex-mcp-shorts/assets/sfx/bell.wav`
- Create: `~/projects/yandex-mcp-shorts/assets/sfx/bass-drop.wav`
- Create: `~/projects/yandex-mcp-shorts/assets/sfx/tick.wav`

- [ ] **Step 1: Скачать SFX с freesound.org или mixkit.co**

Поиск:
- "whoosh transition" короткий 0.3-0.7 сек
- "notification bell" чистый 0.3-0.5 сек
- "bass drop trailer" короткий 0.5-1 сек
- "tick check" короткий 0.1-0.2 сек

- [ ] **Step 2: Конвертировать в WAV 48 kHz 16 bit**

Run для каждого:
```powershell
ffmpeg -i input.mp3 -ar 48000 -ac 2 -c:a pcm_s16le ~/projects/yandex-mcp-shorts/assets/sfx/whoosh.wav
```

- [ ] **Step 3: Verify**

Run: `Get-ChildItem ~/projects/yandex-mcp-shorts/assets/sfx/`
Expected: 4 файла .wav, размером 50-300 KB каждый.

---

### Task 0.10: Phase 0 commit

- [ ] **Step 1: Commit completed setup**

```bash
cd ~/projects/yandex-mcp-shorts/
git add answers.md .gitignore
git commit -m "chore: phase 0 setup complete (mcp-video, video-edit, OBS scenes, music, sfx)"
```

**Gate 0:** Все проверки выше прошли. Phase 0 закрыт.

---

## Phase 1 — Pre-production (Claude автономно ~2 ч + ревизия)

### Task 1.1: Write Short #1 script (template)

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/short-1.md`

- [ ] **Step 1: Написать сценарий по шаблону спеки**

Create `scripts/short-1.md`:
```markdown
# Short #1 — Bounce −7.5 пп

**Длит.:** 58 сек
**Hook:** "Минус 7.5 пунктов отказов за 6 дней. Без программистов."
**Day:** 1 (первый в публикации)
**Источник цифр:** CLAUDE.md, "Результаты эксперимента (29.04 → 05.05)"

## Раскадровка с тайм-кодами

| Время | VOICE (точно по слогам, секундомер) | VISUAL | SFX/Music |
|---|---|---|---|
| 0:00–0:02 | "Минус семь и пять. Пунктов отказов. За шесть дней." | Face-cam, on-screen "−7.5 пп" появляется на удар | Whoosh @0:00 + бит @0:01 |
| 0:02–0:08 | "Это моя реклама в Яндекс Директ. Бюджет — тридцать в неделю. Каждый третий уходит сразу." | Скриншот Метрики, ЕПК-кампания, bounce 31.2% подсвечен красным, zoom-in на цифру | Music in (−18 dB) |
| 0:08–0:18 | "Я открыл Claude. Попросил: покажи воронку и скажи где утечка." | Cut to Claude Desktop, печатается промпт. On-screen text: "Покажи воронку и скажи где утечка" | Клав-тапер |
| 0:18–0:30 | "Через MCP-сервер он залез в Метрику. Нашёл: мобильные дают сорок процентов отказов и ноль лидов." | Claude отвечает таблицей. Mobile bounce 40.8% подсвечена красным | Bell-cue на "ноль лидов" |
| 0:30–0:42 | "Я сказал: мобильные минус девяносто, Кавказ минус девяносто, Владик тоже. Claude применил через MCP-команду за пару секунд." | Новый промпт → Claude отвечает ✓. B-roll: bullet-list api-вызовов | Tick на каждом ✓ |
| 0:42–0:52 | "Через шесть дней. Отказы — двадцать три. Конверсия попапа — в пять и девять раз. Без единой строки кода." | Метрика повторно, bounce 23.7%, счётчик 0.14 → 0.83. Большая надпись "−7.5 ПП" | Bass-drop, music peak |
| 0:52–0:58 | "Это первый из шести. Дальше — восемьдесят минус-слов за три минуты. Подпишись." | Face-cam + overlay "→ подпишись • репо в описании" | Music ducks на 0:55 |

## On-screen overlays

| Элемент | Появление | Длит. | Стиль |
|---|---|---|---|
| Hook "−7.5 пп" | 0:01 | 1.5 с | 200pt белый, drop-shadow |
| Подсветка 31.2% | 0:05 | 2 с | Красный круг + zoom |
| Mobile 40.8% подсветка | 0:24 | 3 с | Красная плашка под строкой |
| ✓ галочки | 0:36-0:40 | 4 с | Зелёные, последовательно |
| Анимация ×5.9 | 0:48 | 3 с | Счётчик 0.14 → 0.83 |
| CTA-плашка | 0:53 | 5 с | Низ экрана, белый текст на чёрном |

## Источники данных в кадре

- Метрика: счётчик 97538360, "Лид в БД" цель ID 490102744
- Direct: кампания "ЕПК | Товарная | Авто из Азии" ID 707438910
- Скриншоты должны быть с датами 29.04.2026 и 05.05.2026

## Анонимизация (по answers.md)

- См. `answers.md` пункт 3 — применить указанный режим (полное имя / blur / полная анонимизация)
```

- [ ] **Step 2: Verify**

Run: `Get-Content ~/projects/yandex-mcp-shorts/scripts/short-1.md | Measure-Object -Line`
Expected: ≥40 строк.

- [ ] **Step 3: Commit**

```bash
cd ~/projects/yandex-mcp-shorts/
git add scripts/short-1.md
git commit -m "scripts: short #1 (bounce -7.5pp)"
```

---

### Task 1.2: Write Short #2 script

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/short-2.md`

- [ ] **Step 1: Написать по тому же формату что Task 1.1, со следующими отличиями:**

```markdown
# Short #2 — Конверсия попапа ×5.9

**Длит.:** 58 сек
**Hook:** "В пять и девять раз. Во столько выросла конверсия попапа после одного промпта."
**Day:** 7 (4-й в публикации)
**Источник цифр:** CLAUDE.md, цели Metrika 549384274/275/276

## Раскадровка

| Время | VOICE | VISUAL | SFX/Music |
|---|---|---|---|
| 0:00–0:02 | "В пять и девять раз. Во столько выросла конверсия попапа." | Face-cam, on-screen "×5.9" | Whoosh + бит |
| 0:02–0:08 | "Это поп-ап на сайте. Он показывался месяцами. Конверсия — ноль целых четырнадцать в день." | Метрика, цель LEAD_CAPTURE_SUBMIT, график 0.14 | Music in |
| 0:08–0:20 | "Я попросил Claude: проанализируй цели LEAD_CAPTURE и скажи где узкое место." | Claude Desktop, промпт печатается | Клав-тапер |
| 0:20–0:34 | "Через MCP он сравнил три цели: открытия, закрытия и отправки. Открытий — сто процентов. Отправок — семь. Закрывают крестиком." | Bar-chart open/close/submit | Bell на "семь" |
| 0:34–0:42 | "Я попросил скорректировать аудиторию, чтобы попап видели только релевантные. Claude применил через direct_set_bid_modifier." | Промпт + Claude применяет ✓ | Tick на каждом ✓ |
| 0:42–0:52 | "Через шесть дней. Конверсия — ноль целых восемьдесят три. Это в пять и девять раз больше." | Анимация 0.14 → 0.83 | Bass-drop |
| 0:52–0:58 | "Дальше — детектив. Как Claude вычислил фейковые лиды по Windows 7. Подпишись." | Face-cam + CTA overlay | Music ducks |

## On-screen overlays
[Аналогично Short #1, цифры свои: ×5.9, 0.14, 0.83, 7%]

## Источники данных в кадре
- Метрика цели: 549384274 (open), 549384275 (close), 549384276 (submit)
- Direct: bid modifier API через MCP

## Анонимизация (по answers.md пункт 3)
```

- [ ] **Step 2: Verify**

Run: `Test-Path ~/projects/yandex-mcp-shorts/scripts/short-2.md`
Expected: True.

- [ ] **Step 3: Commit**

```bash
git add scripts/short-2.md
git commit -m "scripts: short #2 (popup x5.9)"
```

---

### Task 1.3: Write Short #3 script

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/short-3.md`

- [ ] **Step 1: Написать сценарий по 5-актному шаблону из спеки раздел 5, со следующими параметрами:**

Hook: "Раньше я искал минус-слова неделями. Сейчас — восемьдесят за три минуты."
Day: 3 (2-й в публикации)
Источник: CLAUDE.md "Минус-слова (~80 шт, обновлено 18.03.2026)"

Структура 60-сек шаблона (HOOK 0-2, SETUP 2-8, REVEAL 8-42, RESULT 42-52, CTA 52-58). Файл должен содержать: заголовок с этими полями, таблицу раскадровки, секцию on-screen overlays, секцию источников данных, секцию анонимизации.
- REVEAL: открыть SEARCH_QUERY_PERFORMANCE_REPORT в Excel-выгрузке (тысячи строк) → Claude через `direct_get_statistics` → группирует мусор (DIY 47K₽, площадки avito/drom, конкуренты, инфо) → пакетное добавление через `direct_update_campaign`
- RESULT: 47K₽ потраченных на DIY → 0₽
- CTA: "Дальше — самый драматичный кейс. РСЯ архивировал."

- [ ] **Step 2: Commit**

```bash
git add scripts/short-3.md
git commit -m "scripts: short #3 (80 negative keywords)"
```

---

### Task 1.4: Write Short #4 script

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/short-4.md`

- [ ] **Step 1: Написать сценарий по 5-актному шаблону из спеки раздел 5, со следующими параметрами:**

Hook: "Тридцать три тысячи кликов на рекламу. Ноль лидов. Спас двадцать пять тысяч одной командой."
Day: 5 (3-й в публикации)
Источник: CLAUDE.md "Кампания 'РСЯ | Авто из Азии' (ID: 707059356) — АРХИВИРОВАНА"

REVEAL: Скрин РСЯ-кампании 707059356, расход 24,789₽ → Claude через MCP подтягивает данные → 33,127 кликов, 0 конверсий → промпт *"архивируй"* → `direct_archive_campaigns` ✓
RESULT: Сэкономлено 25K₽/мес, бюджет перетёк в поиск. Большая on-screen цифра "33,127 → 0 → archived"
CTA: "Дальше — как Claude поднял конверсию попапа в 5.9 раза одним промптом."

- [ ] **Step 2: Commit**

```bash
git add scripts/short-4.md
git commit -m "scripts: short #4 (RSY archive 25K rub saved)"
```

---

### Task 1.5: Write Short #5 script

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/short-5.md`

- [ ] **Step 1: Написать сценарий по 5-актному шаблону из спеки раздел 5, со следующими параметрами:**

Hook: "Двадцать пять процентов наших лидов — фейк. Claude вычислил их по операционке."
Day: 9 (5-й в публикации)
Источник: CLAUDE.md "Подозрение на фейковые лиды: Windows 7 (25%), ночные заявки (25%)"

REVEAL: Менеджеры жалуются → Claude через `metrika_get_report` с dimensions OS+час → 25% Win7 (норма ~3% в RU) + 25% между 02:00-05:00 → bar-chart Win7 vs Win10 vs прочие
RESULT: Доля грязных лидов в БД упала, CRM-конверсия в оплаты выросла
CTA: "Финал — как этот сервер устроен. 128 инструментов, 1 промпт."

- [ ] **Step 2: Commit**

```bash
git add scripts/short-5.md
git commit -m "scripts: short #5 (Win7 fake leads detective)"
```

---

### Task 1.6: Write Short #6 script

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/short-6.md`

- [ ] **Step 1: Написать сценарий по 5-актному шаблону из спеки раздел 5, со следующими параметрами:**

Hook: "Сто двадцать восемь инструментов. Один промпт. Yandex Direct под капотом ИИ."
Day: 11 (6-й, финал)
Источник: README сервера

REVEAL: Скрин README → 80 Direct + 43 Metrika + 5 Wordstat → "карусель" категорий: Campaigns/AdGroups/Ads/Keywords/Stats/Bid Modifiers/Retargeting/...
DEMO: Claude Desktop, один промпт *"Покажи все мои кампании, статусы и недельные бюджеты"* → markdown-вывод
RESULT: Скрин GitHub-репо со звёздочкой
CTA: "Ссылка на репо в описании. Если зашло — подпишись, в следующем сезоне второй MCP-сервер."

- [ ] **Step 2: Commit**

```bash
git add scripts/short-6.md
git commit -m "scripts: short #6 (128 tools / 1 prompt finale)"
```

---

### Task 1.7: Write storyboard.md (visual specs)

**Files:**
- Create: `~/projects/yandex-mcp-shorts/scripts/storyboard.md`

- [ ] **Step 1: Написать единые визуальные спеки серии**

Create `storyboard.md`:
```markdown
# Storyboard — Visual specs for the series

## Цветовая палитра

- **Фон on-screen цифр:** #0A0A0A (почти чёрный)
- **Цифры hook:** #FFFFFF, font Inter Bold 200pt, drop-shadow 0 4px 24px rgba(0,0,0,0.5)
- **Цифры reveal/result:** #FFFFFF, font Inter Bold 120pt
- **Подсветка проблемы:** #FF4444 (красный)
- **Подсветка результата:** #44DD66 (зелёный)
- **Акцент бренда:** #FFCC00 (жёлтый, для ключевых слов в субтитрах)

## Шрифты

- **Заголовочные цифры:** Inter Bold (или Manrope Bold если Inter недоступен)
- **Субтитры:** Inter SemiBold 56pt
- **Подписи в кадре:** Inter Medium 36pt

## Стиль субтитров

- Положение: центр экрана, 30% от низа (примерно y=1344)
- Цвет основной: белый #FFFFFF
- Outline: 4px чёрный
- Ключевые слова (цифры, термины "MCP", "Claude", "Яндекс.Директ"): жёлтый #FFCC00
- Группировка: 2-3 слова на сегмент
- Появление: word-by-word с задержкой 50 мс

## Переходы между сценами

- Cut to face-cam ↔ screen: hard cut + whoosh
- Внутри screen: smash cut на акценте VOICE
- На результат-цифры: zoom-in 0.3 сек

## Music levels

- Под голос: −18 dB
- Без голоса (переходы, hooks): −12 dB
- На bass-drop: −9 dB
- На CTA: −15 dB и ducks

## Watermark

- Низ-правый угол, мелко: `@<твой_handle>` или GitHub @SvechaPVL — берём из answers.md пункт 2
- Прозрачность 60%
- Не перекрывает субтитры
```

- [ ] **Step 2: Commit**

```bash
git add scripts/storyboard.md
git commit -m "scripts: storyboard with colors, fonts, subtitle style"
```

---

### Task 1.8: Write Playwright spec for Short #1 screencast

**Files:**
- Create: `~/projects/yandex-mcp-shorts/screencasts/specs/short-1.spec.ts`

- [ ] **Step 1: Написать Playwright-сценарий**

Create `screencasts/specs/short-1.spec.ts`:
```typescript
import { test, expect } from '@playwright/test';

// Screencast scenario for Short #1 — Bounce -7.5pp
// Records: 1) Метрика ЕПК bounce 31.2%, 2) Claude Desktop prompt, 3) MCP response, 4) Метрика updated bounce 23.7%
// Output: ~/projects/yandex-mcp-shorts/screencasts/recordings/short-1.webm

test.describe.configure({ mode: 'serial' });

test('short-1 screencast — bounce optimization', async ({ page, context }) => {
  await context.tracing.start({ screenshots: true, snapshots: false });
  await page.setViewportSize({ width: 1080, height: 1920 });

  // Scene A: Метрика, отчёт ЕПК-кампании bounce 31.2% (cohort: до правок)
  await page.goto('https://metrika.yandex.ru/dashboard?id=97538360');
  // (preconfigured login via storage state — see playwright.config.ts)
  await page.waitForSelector('[data-testid="bounce-rate"]', { timeout: 15000 });
  // Highlight bounce element
  await page.evaluate(() => {
    const el = document.querySelector('[data-testid="bounce-rate"]') as HTMLElement;
    if (el) { el.style.outline = '4px solid #FF4444'; el.style.transition = 'outline 0.3s'; }
  });
  await page.waitForTimeout(2000);  // 2 sec hold for SETUP

  // Scene B: Claude Desktop (recorded separately or as embedded screen capture)
  // NB: Claude Desktop is not a web page; this Playwright run records browser tabs.
  // For Claude Desktop screencap: separate OBS recording feeding into video-edit MCP composition.

  // Scene C: Метрика после правок (bounce 23.7%)
  await page.goto('https://metrika.yandex.ru/dashboard?id=97538360&period=last7days');
  await page.waitForSelector('[data-testid="bounce-rate"]', { timeout: 15000 });
  await page.evaluate(() => {
    const el = document.querySelector('[data-testid="bounce-rate"]') as HTMLElement;
    if (el) { el.style.outline = '4px solid #44DD66'; el.style.transition = 'outline 0.3s'; }
  });
  await page.waitForTimeout(3000);

  await context.tracing.stop({ path: 'screencasts/recordings/short-1.zip' });
});
```

- [ ] **Step 2: Создать playwright.config.ts**

Create `screencasts/playwright.config.ts`:
```typescript
import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './specs',
  use: {
    headless: false,
    viewport: { width: 1080, height: 1920 },
    video: { mode: 'on', size: { width: 1080, height: 1920 } },
    storageState: '~/.config/playwright-yandex-auth.json',  // pre-saved login
  },
  outputDir: 'recordings/',
});
```

- [ ] **Step 3: Сохранить login state (один раз вручную)**

Run:
```bash
cd ~/projects/yandex-mcp-shorts/screencasts/
npx playwright codegen --save-storage=$HOME/.config/playwright-yandex-auth.json https://metrika.yandex.ru
```
В открывшемся окне залогиниться в Yandex, перейти в Метрику, закрыть → файл сохранится.

- [ ] **Step 4: Commit**

```bash
git add screencasts/specs/short-1.spec.ts screencasts/playwright.config.ts
git commit -m "screencasts: short #1 Playwright spec + config"
```

---

### Task 1.9: Write Playwright specs for Shorts #2-#6

**Files:**
- Create: `~/projects/yandex-mcp-shorts/screencasts/specs/short-2.spec.ts`
- Create: `~/projects/yandex-mcp-shorts/screencasts/specs/short-3.spec.ts`
- Create: `~/projects/yandex-mcp-shorts/screencasts/specs/short-4.spec.ts`
- Create: `~/projects/yandex-mcp-shorts/screencasts/specs/short-5.spec.ts`
- Create: `~/projects/yandex-mcp-shorts/screencasts/specs/short-6.spec.ts`

- [ ] **Step 1: Скопировать short-1.spec.ts как шаблон, адаптировать каждый**

Каждый spec следует тому же шаблону (storage state + viewport + tracing). Различия:

**short-2.spec.ts:**
- URL: `https://metrika.yandex.ru/dashboard?id=97538360&goal=549384274` (LEAD_CAPTURE_OPEN)
- Highlight: bar-chart open/close/submit
- Цвета: Open зелёным, Close красным, Submit жёлтым

**short-3.spec.ts:**
- URL: Direct UI → SEARCH_QUERY_PERFORMANCE_REPORT для кампании 705141271
- Action: листать длинный отчёт, паузы на DIY-запросах ("своими руками", "видео")
- Output: ~30 сек прокрутки

**short-4.spec.ts:**
- URL: Direct UI → кампания 707059356 (РСЯ архивированная)
- Highlight: badge "АРХИВИРОВАНА", расход 24,789₽, лидов 0
- Action: один кадр, 4 сек

**short-5.spec.ts:**
- URL: Метрика, отчёт по ОС с фильтром на лидов
- Bar-chart: Win7 vs Win10 vs прочие
- Highlight: красная подсветка Win7-бара (25%)

**short-6.spec.ts:**
- URLs: README на GitHub `https://github.com/SvechaPVL/yandex-mcp` (или из answers.md)
- Action: прокрутка README, паузы на разделах "128 tools"
- Затем переход на Claude Desktop (обработать через video-edit отдельно)

- [ ] **Step 2: Verify**

Run: `Get-ChildItem ~/projects/yandex-mcp-shorts/screencasts/specs/`
Expected: 6 файлов `short-N.spec.ts`.

- [ ] **Step 3: Commit**

```bash
git add screencasts/specs/short-{2,3,4,5,6}.spec.ts
git commit -m "screencasts: Playwright specs for shorts #2-#6"
```

---

### Task 1.10: User script review

**Files:** (none — communication step)

- [ ] **Step 1: Сообщить пользователю что сценарии готовы**

Передать список:
- `scripts/short-1.md` ... `scripts/short-6.md`
- `scripts/storyboard.md`
- `screencasts/specs/short-1.spec.ts` ... `short-6.spec.ts`

Попросить прочитать и дать правки.

- [ ] **Step 2: Применить полученные правки**

Если правки есть — отредактировать соответствующие файлы.

- [ ] **Step 3: Commit правок**

```bash
git add scripts/ screencasts/specs/
git commit -m "scripts: apply user review feedback"
```

**Gate 1:** Пользователь явно одобряет сценарии. До этой точки запись не начинается.

---

## Phase 2 — Recording (человек, ~1 ч суммарно)

### Task 2.1: Record 6 face-cam hooks in one session

**Files:**
- Create: `~/projects/yandex-mcp-shorts/face-cam/hooks/short-1-take{1,2,3}.mkv` (3-4 дубля)
- ... аналогично для shorts #2-#6

- [ ] **Step 1: Подготовить место**

Поставить камеру на уровень глаз, расстояние ~80 см, основной свет фронтально-сбоку. Проверить отсутствие отвлекающих элементов в кадре.

- [ ] **Step 2: Включить OBS Face-cam Portrait сцену, начать запись**

В OBS: Scene → `Face-cam Portrait 1080×1920` → Start Recording.

- [ ] **Step 3: Записать каждый hook 3-4 дубля подряд**

Произносить вслух с паузой 5 сек между:
- Short #1: "Минус семь и пять. Пунктов отказов. За шесть дней." (×3)
- Short #2: "В пять и девять раз. Во столько выросла конверсия попапа." (×3)
- Short #3: "Раньше я искал минус-слова неделями. Сейчас — восемьдесят за три минуты." (×3)
- Short #4: "Тридцать три тысячи кликов. Ноль лидов. Спас двадцать пять тысяч одной командой." (×3)
- Short #5: "Двадцать пять процентов наших лидов — фейк. Claude вычислил их по операционке." (×3)
- Short #6: "Сто двадцать восемь инструментов. Один промпт. Yandex Direct под капотом ИИ." (×3)

- [ ] **Step 4: Stop recording**

OBS → Stop Recording.

- [ ] **Step 5: Разрезать длинную запись на отдельные клипы**

Run:
```powershell
# Использовать ffmpeg + примерные тайм-коды
# Пример для одного hook (засёк по началу: 0:30 длит. 4 сек):
ffmpeg -i "$env:USERPROFILE\Videos\hooks-session.mkv" -ss 00:00:30 -t 4 -c copy "$env:USERPROFILE\projects\yandex-mcp-shorts\face-cam\hooks\short-1-take1.mkv"
```

(Точные тайм-коды берёшь после просмотра записи; альтернатива — кликнуть по таймлайну в OBS Replay Buffer и резать вручную в OBS.)

- [ ] **Step 6: Verify**

Run: `Get-ChildItem ~/projects/yandex-mcp-shorts/face-cam/hooks/`
Expected: ≥6 файлов (минимум по одному take на каждый hook).

---

### Task 2.2: Select best hook take per short

**Files:**
- Create: `~/projects/yandex-mcp-shorts/face-cam/hooks/short-1.mkv` (final pick)
- ... аналогично для shorts #2-#6

- [ ] **Step 1: Просмотреть takes для каждого hook**

Открыть каждый `short-N-takeM.mkv` плеером, выбрать лучший:
- Чистая артикуляция
- Прямой взгляд в камеру
- Эмоция-под-цифру (сдержанная уверенность)
- Звук без "пшика" в начале/конце

- [ ] **Step 2: Скопировать выбранный take как финальный**

Run для каждого:
```powershell
Copy-Item "~\projects\yandex-mcp-shorts\face-cam\hooks\short-1-take2.mkv" "~\projects\yandex-mcp-shorts\face-cam\hooks\short-1.mkv"
```

- [ ] **Step 3: Commit (только final версии)**

```bash
cd ~/projects/yandex-mcp-shorts/
# face-cam/ в .gitignore — но финальные версии можно коммитить через git lfs
# Альтернатива: запись метаданных в текстовый файл, бинарники хранить отдельно
echo "Selected takes: short-1=take2, short-2=take1, ..." > face-cam/hooks/SELECTED.txt
git add face-cam/hooks/SELECTED.txt
git commit -m "production: face-cam hooks selected takes documented"
```

---

### Task 2.3: Record 6 face-cam CTAs

**Files:**
- Create: `~/projects/yandex-mcp-shorts/face-cam/ctas/short-1-take{1,2,3}.mkv`
- ... shorts #2-#6

- [ ] **Step 1: OBS Face-cam Portrait → Start Recording**

- [ ] **Step 2: Записать каждый CTA 3 дубля**

CTA-фразы (по answers.md замени `{handle}` на реальный):
- Short #1: "Это первый из шести. Дальше — восемьдесят минус-слов за три минуты. Подпишись."
- Short #2: "Дальше — детектив. Как Claude вычислил фейковые лиды по Windows семь."
- Short #3: "Дальше — самый драматичный кейс. РСЯ архивировал."
- Short #4: "Дальше — как Claude поднял конверсию попапа в пять и девять раз одним промптом."
- Short #5: "Финал — как этот сервер устроен. Сто двадцать восемь инструментов, один промпт."
- Short #6: "Ссылка на репо в описании. Если зашло — подпишись."

- [ ] **Step 3: Stop, разрезать, выбрать лучшие takes (как в Task 2.2)**

- [ ] **Step 4: Verify**

Run: `Get-ChildItem ~/projects/yandex-mcp-shorts/face-cam/ctas/short-*.mkv`
Expected: 6 финальных файлов.

---

### Task 2.4: Record 6 voice-over tracks

**Files:**
- Create: `~/projects/yandex-mcp-shorts/voice/short-1.wav` ... `short-6.wav`

- [ ] **Step 1: Переключиться на сцену Voice-only, начать запись**

OBS → Scene `Voice-only` → Start Recording. Микрофон 15-20 см от рта, поп-фильтр.

- [ ] **Step 2: Прочитать voice-track каждого ролика по сценарию**

VOICE-секции (поля `0:08-0:42` из каждого short-N.md). 2-3 дубля каждого. Между дублями пауза 5 сек.

Для Short #1 (пример) читать всё что в столбце VOICE кроме hook (0:00-0:02) и CTA (0:52-0:58):
- "Это моя реклама в Яндекс Директ. Бюджет — тридцать в неделю. Каждый третий уходит сразу."
- "Я открыл Claude. Попросил: покажи воронку и скажи где утечка."
- "Через MCP-сервер он залез в Метрику. Нашёл: мобильные дают сорок процентов отказов и ноль лидов."
- "Я сказал: мобильные минус девяносто, Кавказ минус девяносто, Владик тоже. Claude применил через MCP-команду за пару секунд."
- "Через шесть дней. Отказы — двадцать три. Конверсия попапа — в пять и девять раз. Без единой строки кода."

- [ ] **Step 3: Stop, экспортировать в WAV для каждого**

```powershell
# Резать сессию на 6 voice-tracks по тайм-кодам
ffmpeg -i "$env:USERPROFILE\Videos\voice-session.mkv" -ss 00:00:00 -t 50 -vn -c:a pcm_s16le "$env:USERPROFILE\projects\yandex-mcp-shorts\voice\short-1.wav"
# Повторить для остальных
```

- [ ] **Step 4: Verify LUFS uniformity**

Run для каждого:
```powershell
ffmpeg -i ~/projects/yandex-mcp-shorts/voice/short-1.wav -af loudnorm=I=-19:TP=-1.5:LRA=11:print_format=summary -f null NUL 2>&1 | Select-String "Input Integrated"
```
Expected: все 6 значений между −20 и −18 LUFS, разница ≤2 dB.

- [ ] **Step 5: Если разброс >2 dB — нормализовать**

```powershell
ffmpeg -i ~/projects/yandex-mcp-shorts/voice/short-1.wav -af loudnorm=I=-19:TP=-1.5:LRA=11 ~/projects/yandex-mcp-shorts/voice/short-1-norm.wav
Move-Item ~/projects/yandex-mcp-shorts/voice/short-1-norm.wav ~/projects/yandex-mcp-shorts/voice/short-1.wav -Force
```

- [ ] **Step 6: Document in commit**

```bash
echo "Voice recording session: $(Get-Date -Format 'yyyy-MM-dd HH:mm'), all tracks at -19 LUFS ±2 dB" > voice/SESSION.txt
git add voice/SESSION.txt
git commit -m "production: voice tracks recorded and normalized"
```

---

### Task 2.5: Hand off to Claude

**Files:** (communication step)

- [ ] **Step 1: Проверить что все файлы готовы**

Run:
```powershell
$expected = @(
  "face-cam/hooks/short-1.mkv","face-cam/hooks/short-2.mkv","face-cam/hooks/short-3.mkv",
  "face-cam/hooks/short-4.mkv","face-cam/hooks/short-5.mkv","face-cam/hooks/short-6.mkv",
  "face-cam/ctas/short-1.mkv","face-cam/ctas/short-2.mkv","face-cam/ctas/short-3.mkv",
  "face-cam/ctas/short-4.mkv","face-cam/ctas/short-5.mkv","face-cam/ctas/short-6.mkv",
  "voice/short-1.wav","voice/short-2.wav","voice/short-3.wav",
  "voice/short-4.wav","voice/short-5.wav","voice/short-6.wav"
)
foreach ($f in $expected) {
  $p = "$env:USERPROFILE\projects\yandex-mcp-shorts\$f"
  if (-not (Test-Path $p)) { Write-Host "MISSING: $f" -ForegroundColor Red }
  else { Write-Host "OK: $f" -ForegroundColor Green }
}
```
Expected: 18 строк "OK".

- [ ] **Step 2: Сообщить Claude "все 18 файлов на месте, можно собирать"**

---

## Phase 3 — Tracer Bullet (Short #1, ~2 часа)

### Task 3.1: Run Playwright screencast for Short #1

**Files:**
- Create: `~/projects/yandex-mcp-shorts/screencasts/recordings/short-1.webm`

- [ ] **Step 1: Запустить Playwright через mcp-video или напрямую**

Если mcp-video MCP-сервер доступен:
```
Через Claude: "Запусти screencasts/specs/short-1.spec.ts через mcp-video, разрешение 1080×1920, 60fps, output в screencasts/recordings/short-1.webm"
```

Альтернатива (прямой запуск Playwright):
```bash
cd ~/projects/yandex-mcp-shorts/screencasts/
npx playwright test specs/short-1.spec.ts --headed
```

- [ ] **Step 2: Verify файл создан**

Run: `Test-Path ~/projects/yandex-mcp-shorts/screencasts/recordings/short-1.webm`
Expected: True, размер ≥5 MB.

- [ ] **Step 3: Просмотреть screencast**

Открыть `short-1.webm` плеером. Проверить:
- Bounce 31.2% подсвечен красным (обведено)
- Bounce 23.7% подсвечен зелёным
- Длительность ~30-40 сек
- Нет видимой капчи / auth-ошибок

- [ ] **Step 4: Если ошибки — диагностика**

| Проблема | Решение |
|---|---|
| Капча | Запустить вручную в headed-режиме, пройти, обновить storage state |
| 401/403 | Re-save storage state (Task 1.8 step 3) |
| Не подсвечивается элемент | Проверить selector в spec, обновить через DevTools |

- [ ] **Step 5: Commit metadata**

```bash
echo "short-1.webm generated $(Get-Date -Format 'yyyy-MM-dd HH:mm'), size: $((Get-Item screencasts/recordings/short-1.webm).Length / 1MB) MB" > screencasts/recordings/SHORT-1-META.txt
git add screencasts/recordings/SHORT-1-META.txt
git commit -m "screencasts: short #1 recorded successfully"
```

---

### Task 3.2: Generate subtitles for Short #1

**Files:**
- Create: `~/projects/yandex-mcp-shorts/voice/short-1.srt`

- [ ] **Step 1: Транскрипция через video-edit MCP с русским initial_prompt**

Через Claude:
```
"Транскрибируй voice/short-1.wav через Whisper в воркфлоу video-edit MCP. Параметры:
- language: ru
- model: large-v3
- initial_prompt: 'Yandex Direct, Яндекс Директ, MCP, API, Claude, Метрика, ЕПК, попап, конверсия, биды, лиды'
- output: word-level SRT в voice/short-1.srt"
```

Альтернатива (CLI):
```bash
whisper ~/projects/yandex-mcp-shorts/voice/short-1.wav --model large-v3 --language ru --initial_prompt "Yandex Direct, MCP, Claude, Метрика, ЕПК, попап, конверсия, биды, лиды" --output_format srt --output_dir ~/projects/yandex-mcp-shorts/voice/
```

- [ ] **Step 2: Verify SRT файл**

Run: `Get-Content ~/projects/yandex-mcp-shorts/voice/short-1.srt | Select-Object -First 20`
Expected: SRT-формат с временем `00:00:00,000 --> 00:00:01,500` и текстом.

- [ ] **Step 3: Ручная вычитка тех-терминов**

Открыть SRT в редакторе. Найти и заменить типичные ошибки:
- "эмписи" → "MCP"
- "клод" → "Claude"
- "епк" / "EPK" → "ЕПК"
- "яндекс директ" → "Яндекс Директ"
- "метрика" остаётся
- Цифры в формате "−7.5 пп" с правильным минусом (U+2212)

- [ ] **Step 4: Commit**

```bash
git add voice/short-1.srt
git commit -m "subtitles: short #1 transcription with manual fixes"
```

---

### Task 3.3: Assemble Short #1 via video-edit MCP

**Files:**
- Create: `~/projects/yandex-mcp-shorts/final/short-1.mp4`

- [ ] **Step 1: Передать в video-edit MCP сборочный план**

Через Claude:
```
"Собери final/short-1.mp4 через video-edit MCP по следующему плану:

1. Concatenate сегменты в порядке:
   - face-cam/hooks/short-1.mkv (0:00-0:02, 2 сек)
   - screencasts/recordings/short-1.webm с указанными ниже подрезаниями
   - face-cam/ctas/short-1.mkv (0:52-0:58, 6 сек)

2. Подложить voice/short-1.wav как audio track (mute оригинальные voice клипов кроме hook + CTA)

3. Добавить субтитры из voice/short-1.srt:
   - position: y=1344, центр
   - font: Inter SemiBold 56pt
   - color: white #FFFFFF, outline 4px black
   - keyword highlight: '7.5', 'минус девяносто', '23 процента', '5.9 раз' → #FFCC00

4. Наложить on-screen графику по storyboard.md:
   - 'Hook -7.5 пп' большая надпись 0:01-0:02.5
   - Подсветка bounce 31.2% red @0:05-0:07
   - Highlight mobile 40.8% red @0:24-0:27
   - Анимация ✓ галочки @0:36-0:40
   - Счётчик 0.14→0.83 @0:48-0:51
   - CTA-плашка '→ подпишись • репо в описании' @0:53-0:58

5. Музыка: assets/music/track.mp3 на −18 dB, ducks под голос. Bass-drop @0:42 (assets/sfx/bass-drop.wav). Whoosh @ каждом cut. Bell @0:24, @0:28. Tick × 3 @0:36-0:40.

6. Экспорт: 1080×1920, 60fps, H.264, max 8 Mbps, ≤80 MB, output → final/short-1.mp4"
```

- [ ] **Step 2: Дождаться рендера, verify файл**

Run: `Test-Path ~/projects/yandex-mcp-shorts/final/short-1.mp4`
Expected: True.

Run: `ffprobe -v error -show_entries stream=width,height,r_frame_rate,bit_rate -show_entries format=duration,size ~/projects/yandex-mcp-shorts/final/short-1.mp4`
Expected:
- width=1080
- height=1920
- r_frame_rate=60/1 (или близко)
- duration около 58.0
- size ≤ 80 MB

- [ ] **Step 3: Просмотреть на телефоне (вертикальный формат)**

Скопировать `short-1.mp4` на телефон, открыть в Photos / Galaxy Player. Проверить:
- Hook читаемый, цифра яркая
- Субтитры не залезают на CTA-плашку
- Звук синхронен
- Громкость комфортна
- Нет lag-фреймов

- [ ] **Step 4: Если проблемы — итерации фиксов**

Документировать каждую итерацию:
```
echo "Iteration 1: subtitles too low, moved to y=1280" >> final/SHORT-1-ITERATIONS.txt
```

Передать Claude конкретные правки → повторный рендер.

- [ ] **Step 5: Commit финал-метаданные**

```bash
echo "short-1.mp4 final, $(ffprobe -v error -show_entries format=size -of default=noprint_wrappers=1:nokey=1 final/short-1.mp4 | %{[math]::Round($_/1MB,2)}) MB" > final/SHORT-1.txt
git add final/SHORT-1.txt final/SHORT-1-ITERATIONS.txt
git commit -m "final: short #1 assembled and reviewed"
```

---

### Task 3.4: Decision gate — tracer bullet result

**Files:**
- Create: `~/projects/yandex-mcp-shorts/TRACER-BULLET-RESULT.md`

- [ ] **Step 1: Зафиксировать сколько часов потребовалось на Шорт #1**

Подсчитать суммарно от начала Phase 3 до commit Task 3.3 step 5.

- [ ] **Step 2: Заполнить decision form**

Create `TRACER-BULLET-RESULT.md`:
```markdown
# Tracer Bullet — Short #1 Result

**Date:** 2026-MM-DD
**Time spent (start of Phase 3 → final commit):** X hours

## Pipeline reliability checklist

- [ ] mcp-video Playwright run succeeded on first attempt
- [ ] mcp-video Playwright captcha-blocked (workaround needed)
- [ ] mcp-video Playwright failed completely

- [ ] video-edit MCP rendered without manual FFmpeg fallbacks
- [ ] video-edit MCP needed CapCut polish for some elements
- [ ] video-edit MCP failed on critical step (e.g., subtitles overlay)

- [ ] Whisper subs were ≥90% accurate after initial_prompt
- [ ] Whisper subs needed substantial manual rewrite

## Decision

Based on the above:

- [ ] **Continue Approach 1 for shorts #2-#6 in parallel**
- [ ] **Hybrid mode for #2-#6**: MCP for stable parts, manual for broken parts (specify which)
- [ ] **Fall back to Approach 2 (OBS+CapCut) for #2-#6**
- [ ] **Stop, retrospect with user**

## Notes for retrospection / future seasons

- ...
```

- [ ] **Step 3: Опубликовать решение пользователю и получить подтверждение**

- [ ] **Step 4: Commit**

```bash
git add TRACER-BULLET-RESULT.md
git commit -m "decision: tracer bullet result and approach for #2-#6"
```

**Gate 3a:** Решение зафиксировано. Phase 3b следует выбранному подходу.

---

## Phase 3b — Build remaining shorts (Claude, parallel after gate)

### Task 3b.1-3b.5: Build Shorts #2 through #6

**Files (per short N=2..6):**
- Create: `~/projects/yandex-mcp-shorts/screencasts/recordings/short-N.webm`
- Create: `~/projects/yandex-mcp-shorts/voice/short-N.srt`
- Create: `~/projects/yandex-mcp-shorts/final/short-N.mp4`

- [ ] **Step 1: Запустить Playwright spec для каждого short**

Для каждого N=2..6 (или fallback OBS-запись если в Task 3.4 решено иначе):

```bash
cd ~/projects/yandex-mcp-shorts/screencasts/
npx playwright test specs/short-N.spec.ts --headed
```

Альтернатива через mcp-video MCP — попросить Claude:
"Запусти screencasts/specs/short-N.spec.ts через mcp-video, разрешение 1080×1920, 60fps, output в screencasts/recordings/short-N.webm".

После запуска проверить: `Test-Path ~/projects/yandex-mcp-shorts/screencasts/recordings/short-N.webm` → True, размер ≥5 MB.

- [ ] **Step 2: Whisper-транскрипция для каждого short**

Для каждого N=2..6 запустить Whisper:

```bash
whisper ~/projects/yandex-mcp-shorts/voice/short-N.wav --model large-v3 --language ru --initial_prompt "Yandex Direct, MCP, Claude, Метрика, ЕПК, попап, конверсия, биды, лиды" --output_format srt --output_dir ~/projects/yandex-mcp-shorts/voice/
```

Затем — ручная вычитка тех-терминов в SRT (замены: "эмписи"→"MCP", "клод"→"Claude", "епк"→"ЕПК", "яндекс директ"→"Яндекс Директ", корректный "−" в цифрах).

- [ ] **Step 3: Сборка через video-edit MCP**

Для каждого N передать Claude сборочный план в формате:
"Собери final/short-N.mp4 через video-edit MCP: concatenate face-cam/hooks/short-N.mkv + screencasts/recordings/short-N.webm + face-cam/ctas/short-N.mkv, подложить voice/short-N.wav, добавить субтитры из voice/short-N.srt по styleguide storyboard.md, наложить on-screen графику и музыку assets/music/track.mp3 на −18 dB с ducking, экспорт 1080×1920 60fps H.264 ≤80 MB."

Подставить уникальные элементы для каждого short:
- face-cam/hooks/short-N.mkv
- face-cam/ctas/short-N.mkv
- voice/short-N.wav
- voice/short-N.srt
- screencasts/recordings/short-N.webm
- on-screen graphics из storyboard для конкретных цифр (×5.9 для #2, "80 / 3 мин" для #3, "33,127 → 0" для #4, "Win7 25%" для #5, "128 / 1" для #6)

- [ ] **Step 4: Verify каждый mp4**

Run для каждого N:
```bash
ffprobe -v error -show_entries format=duration ~/projects/yandex-mcp-shorts/final/short-N.mp4
```
Expected: длительность ≤60 сек.

- [ ] **Step 5: Commit per short**

```bash
git add final/short-N.txt voice/short-N.srt
git commit -m "final: short #N assembled"
```

---

## Phase 4 — Polish (~1.5 ч)

### Task 4.1: Phone review of all 6 shorts

**Files:**
- Create: `~/projects/yandex-mcp-shorts/REVIEW-NOTES.md`

- [ ] **Step 1: Скопировать все 6 mp4 на телефон**

Через USB-кабель или AirDrop / Quick Share / Telegram Saved Messages.

- [ ] **Step 2: Просмотреть подряд в плеере телефона (фейковый "сценарий зрителя")**

Включить airplane mode чтобы избежать отвлечений. Смотреть как обычный зритель — обращать внимание на:
- Hook удерживает внимание в первые 2 сек?
- Громкость одинаковая в серии?
- Субтитры читаемы при ярком свете?
- Конкретные цифры запоминаются?
- CTA понятен?

- [ ] **Step 3: Записать правки в `REVIEW-NOTES.md`**

```markdown
# Review notes (phone watch-through)

## Short #1
- 0:24: subtitle 'мобильные дают сорок процентов' слишком быстро, увеличить read time
- ...

## Short #2
- ...
```

- [ ] **Step 4: Commit notes**

```bash
git add REVIEW-NOTES.md
git commit -m "review: phone-watch notes for all 6 shorts"
```

---

### Task 4.2: Apply review fixes via video-edit MCP

**Files:**
- Modify: `~/projects/yandex-mcp-shorts/final/short-N.mp4` (re-render с правками)

- [ ] **Step 1: Передать REVIEW-NOTES.md Claude с инструкцией**

```
"Применить правки из REVIEW-NOTES.md через video-edit MCP. Для каждой правки — указать какой short, какой timestamp, что меняем."
```

- [ ] **Step 2: Дождаться re-render каждого затронутого short**

- [ ] **Step 3: Phone re-watch затронутых роликов**

- [ ] **Step 4: Если ок — отметить approved**

```bash
echo "$(Get-Date -Format 'yyyy-MM-dd HH:mm'): all 6 shorts approved" >> REVIEW-NOTES.md
git add REVIEW-NOTES.md
git commit -m "review: all 6 shorts approved after fixes"
```

---

### Task 4.3: Generate thumbnails

**Files:**
- Create: `~/projects/yandex-mcp-shorts/thumbnails/short-N.jpg` для каждого N

- [ ] **Step 1: Создать unified thumbnail template через video-edit MCP**

Через Claude:
```
"Сгенерируй cover-frame для каждого short-N.mp4 по шаблону:
- Размер 1080×1920
- Фон: первый кадр face-cam hook
- Накладка: огромная hook-цифра (200pt) центрально-сверху
- Watermark внизу: '@<handle> • yandex-mcp'
- Output: thumbnails/short-N.jpg, JPEG quality 90"
```

- [ ] **Step 2: Verify**

Run: `Get-ChildItem ~/projects/yandex-mcp-shorts/thumbnails/`
Expected: 6 JPEG-файлов.

- [ ] **Step 3: Visual review**

Открыть все 6 в проводнике. Серия должна выглядеть однородно:
- Одинаковый шрифт
- Одинаковая позиция watermark
- Цифры контрастируют с фоном

- [ ] **Step 4: Commit**

```bash
git add thumbnails/
git commit -m "thumbnails: 6 cover-frames generated"
```

---

### Task 4.4: Write platform post texts

**Files:**
- Create: `~/projects/yandex-mcp-shorts/posts/short-N-{youtube,vk,tg,reels}.md` (24 файла)

- [ ] **Step 1: Через Claude сгенерировать тексты**

Для каждого short и каждой платформы — отдельный текст с разной плотностью:

```
"Сгенерируй 24 текста постов:

Платформы:
- youtube.md: title (≤60 chars) + description (≤500 chars) + tags (≤500 chars), русский
- vk.md: один пост-текст (≤500 chars), эмодзи в меру, 3-5 хэштегов
- tg.md: пост для канала (≤1000 chars), без эмодзи, прямой стиль
- reels.md: caption (≤150 chars), 3-5 хэштегов

Для каждого short используй данные из scripts/short-N.md.
В каждом обязательна ссылка на GitHub-репо из answers.md пункт 2.
Output: posts/short-N-platform.md"
```

- [ ] **Step 2: Verify**

Run: `Get-ChildItem ~/projects/yandex-mcp-shorts/posts/ | Measure-Object`
Expected: Count = 24.

- [ ] **Step 3: Commit**

```bash
git add posts/
git commit -m "posts: 24 platform-specific texts (4 platforms × 6 shorts)"
```

---

## Phase 5 — Publication (~30 мин/ролик, по дням)

### Task 5.1: Day 1 — Publish Short #1

**Files:** (none — действия на платформах)

- [ ] **Step 1: YouTube Shorts**

1. Зайти на youtube.com/upload
2. Загрузить `final/short-1.mp4`
3. Title: содержимое из `posts/short-1-youtube.md` (поле title)
4. Description: содержимое из `posts/short-1-youtube.md` (поле description)
5. Tags: из same file
6. Visibility: Public
7. Кнопка Publish

- [ ] **Step 2: VK Clips**

1. m.vk.com/clips/upload (или vk.com/clips → загрузить)
2. Загрузить `final/short-1.mp4`
3. Описание: `posts/short-1-vk.md`
4. Publish

- [ ] **Step 3: Telegram-канал**

1. Открыть канал из answers.md пункт 1
2. Прикрепить `final/short-1.mp4`
3. Caption: `posts/short-1-tg.md`
4. Send

- [ ] **Step 4: Reels (опционально)**

1. Instagram → Create → Reel
2. Upload `final/short-1.mp4`
3. Caption: `posts/short-1-reels.md`
4. Share

- [ ] **Step 5: Документировать факт публикации**

```bash
echo "$(Get-Date -Format 'yyyy-MM-dd HH:mm') Day 1 - Short #1 published: YouTube, VK, TG, Reels" >> PUBLICATION-LOG.md
git add PUBLICATION-LOG.md
git commit -m "publish: day 1 short #1"
```

---

### Task 5.2: Day 1+24h — Analytics check

**Files:**
- Modify: `~/projects/yandex-mcp-shorts/PUBLICATION-LOG.md`

- [ ] **Step 1: Через 24 часа собрать метрики**

YouTube Studio → Shorts → Short #1:
- Views
- Average View Duration
- View Through Rate (% досмотревших до конца)
- Likes / Comments / Shares

VK Clips, TG, Reels: аналогично.

- [ ] **Step 2: Записать в лог**

```markdown
## Short #1 — 24h analytics

YouTube: X views, AVD Y sec (Z%)
VK: X views, Y reactions
TG: X views, Y reactions
Reels: X views, Y likes
```

- [ ] **Step 3: Решение по оставшимся**

Если retention в первые 5 сек > 70% — публикуем остальные как есть.
Если < 50% — пересмотреть хуки оставшихся 5 шортсов перед публикацией.

```bash
git add PUBLICATION-LOG.md
git commit -m "analytics: short #1 day-1 metrics + decision for #2-#6"
```

---

### Task 5.3: Days 3, 5, 7, 9, 11 — Publish remaining

**Files:** (none — действия на платформах)

- [ ] **Step 1: День 3 — публикуем Short #3 (минус-слова) на 4 платформы**

Повторить логику Task 5.1, заменив #1 → #3.

- [ ] **Step 2: День 5 — Short #4 (РСЯ архив)**

- [ ] **Step 3: День 7 — Short #2 (×5.9 попап)**

- [ ] **Step 4: День 9 — Short #5 (Win7 детектив)**

- [ ] **Step 5: День 11 — Short #6 (128 tools финал)**

- [ ] **Step 6: Финальный commit лога**

```bash
git add PUBLICATION-LOG.md
git commit -m "publish: series complete (6 shorts published over 11 days)"
```

---

## Acceptance Criteria

После всех phase:

- [ ] 6 файлов `final/short-N.mp4` существуют в формате 1080×1920, длительность ≤60 сек каждый
- [ ] Каждый ролик имеет face-cam intro (≤3 сек), screencast-середину, face-cam outro (≤6 сек)
- [ ] Каждый ролик имеет word-level русские субтитры с правильными тех-терминами
- [ ] Каждый ролик содержит финальную on-screen графику с hook-цифрой и CTA
- [ ] Звук всех 6 в диапазоне −19 LUFS ±2 dB
- [ ] В кадре нет токенов / приватных данных / имени клиента (если выбрана анонимизация)
- [ ] Все 6 опубликованы на YouTube Shorts (минимум) с правильными title/description/tags
- [ ] В описании каждого ролика — ссылка на GitHub-репо
- [ ] `PUBLICATION-LOG.md` содержит метрики Day-1 хотя бы для Short #1
- [ ] `TRACER-BULLET-RESULT.md` зафиксирован

---

## Открытые вопросы перед стартом (из спеки)

Resolve в Task 0.1:

1. Telegram-канал: имя или решение создать новый
2. GitHub-репо для CTA: URL
3. Анонимизация asiapk.ru: режим
4. Музыкальный трек: жанр или явное предпочтение

---

## Что НЕ входит в этот план (явно out of scope)

- Английская версия серии
- Длинное YouTube-видео по теме
- Habr-статья с embed
- Платная реклама шортсов
- Создание Telegram-канала с нуля (если его нет — Task 0.1 step 1 это раскроет)
