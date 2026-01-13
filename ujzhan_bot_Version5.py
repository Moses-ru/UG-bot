"""
UJZHAN BOT - Version 5 (sanitize_text_value integrated)

Изменения в этой версии:
- Добавлена функция sanitize_text_value для очистки строк, удаления BOM/zero-width символов,
  удаления окружающих кавычек и нормализации.
- apply_loaded_data использует sanitize_texts = [sanitize_text_value(...) ...] при загрузке из Google Sheets.
- Устойчивое show_next_text использует ту же логику очистки перед отправкой.
- Сохранены все предыдущие механики: компактный user_progress.json, защита от повторных ответов,
  редактирование сообщений с вопросами, история попыток, блокировка пересдач и /reload_data.

Требования:
- sheets_loader.py (рядом)
- pip install aiogram requests
"""

import asyncio
import json
import datetime
import math
import logging
import copy
import re
from typing import Dict, List, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# Локальный загрузчик Google Sheet, должен быть рядом
from sheets_loader import load_sheet_data, validate_loaded

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================
API_TOKEN = "7912796914:AAExq3z48tTUtx_OVIO4dNo-m0XT4GnfW4A"
PROGRESS_FILE = "user_progress.json"
SHEET_ID = "158E5Bv70FryY6MFPADvtXAaDgndIvzWgCkTXC8IhmnQ"
ADMIN_IDS = [819157955]  # админ(ы)
BLOCK_COOLDOWN_SECONDS = 2 * 60 * 60  # 2 часа

# ==================== FALLBACK CONTENT (если лист недоступен) ====================
FALLBACK_CHAPTER_1_TEXTS = [
    "🏠 СЕМЬЯ ЮЖАН — fallback: текст отсутствует в таблице."
]
FALLBACK_CHAPTER_1_QUESTIONS = [
    {"q": "Почему 18+?", "options": ["Это цензура", "Эталон хинкали", "Просто красивое число"], "correct": 1, "explanation": "Эталон хинкали — 18 складок"}
]
FALLBACK_MENU = {"хинкали": "🥟 fallback хинкали info"}

# Глобальные структуры (будут обновляться loader-ом)
CHAPTER_1_TEXTS = FALLBACK_CHAPTER_1_TEXTS.copy()
CHAPTER_2_TEXTS: List[str] = []
CHAPTER_3_TEXTS: List[str] = []
CHAPTER_4_TEXTS: List[str] = []

CHAPTER_1_QUESTIONS = FALLBACK_CHAPTER_1_QUESTIONS.copy()
CHAPTER_2_QUESTIONS = []
CHAPTER_3_QUESTIONS = []
CHAPTER_4_QUESTIONS = []

MENU_GUIDE = FALLBACK_MENU.copy()
WINE_PAIRING: Dict[str, str] = {}
ALLERGEN_GUIDE: Dict[str, str] = {}

# ==================== ХРАНЕНИЕ ПРОГРЕССА (compact) ====================
def load_progress() -> Dict[str, Any]:
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_progress(data: Dict[str, Any]) -> None:
    # компактная запись
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

# ==================== НОРМАЛИЗАЦИЯ СТРУКТУРЫ (миграция старых записей) ====================
DEFAULT_USER_TEMPLATE = {
    "b": 1,    # current_block
    "s": "t",  # 't' (text) или 'x' (test)
    "i": 0,    # text_index
    "ts": {},  # test_scores
    "p": [],   # passed_blocks
    "l": {},   # blocks_locked (unlock timestamps)
    "h": {}    # test_history
}

def normalize_user_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Привести запись пользователя к компактной схеме.
    Поддерживает старые ключи:
      current_block -> b
      current_step  -> s ('text' -> 't', 'test' -> 'x')
      text_index    -> i
      test_scores   -> ts
      passed_blocks -> p
      blocks_locked -> l
      test_history  -> h
    """
    if not isinstance(entry, dict):
        entry = {}

    # Перенос старых ключей
    if "current_block" in entry and "b" not in entry:
        try:
            entry["b"] = int(entry.pop("current_block"))
        except Exception:
            entry.pop("current_block", None)
    if "current_step" in entry and "s" not in entry:
        cs = entry.pop("current_step")
        if cs == "text":
            entry["s"] = "t"
        elif cs == "test":
            entry["s"] = "x"
        else:
            entry["s"] = cs
    if "text_index" in entry and "i" not in entry:
        try:
            entry["i"] = int(entry.pop("text_index"))
        except Exception:
            entry.pop("text_index", None)
    if "test_scores" in entry and "ts" not in entry:
        entry["ts"] = entry.pop("test_scores")
    if "passed_blocks" in entry and "p" not in entry:
        entry["p"] = entry.pop("passed_blocks")
    if "blocks_locked" in entry and "l" not in entry:
        entry["l"] = entry.pop("blocks_locked")
    if "test_history" in entry and "h" not in entry:
        entry["h"] = entry.pop("test_history")

    # Гарантируем наличие всех ключей (копируем изменяемые значения)
    for k, v in DEFAULT_USER_TEMPLATE.items():
        if k not in entry:
            entry[k] = copy.deepcopy(v)

    return entry

def normalize_all_progress() -> None:
    """Пройти по всем записям в user_progress.json и нормализовать их."""
    data = load_progress()
    changed = False
    for user_key in list(data.keys()):
        old = data[user_key]
        norm = normalize_user_entry(old)
        if norm != old:
            data[user_key] = norm
            changed = True
    if changed:
        save_progress(data)
        logger.info("Normalized user_progress.json entries to compact schema.")
    else:
        logger.info("user_progress.json already normalized.")

def get_user_progress(user_id: int) -> Dict[str, Any]:
    progress = load_progress()
    user_key = str(user_id)
    if user_key not in progress:
        progress[user_key] = copy.deepcopy(DEFAULT_USER_TEMPLATE)
        save_progress(progress)
        return progress[user_key]
    # Нормализуем существующую запись (если потребуется)
    normalized = normalize_user_entry(progress[user_key])
    if normalized != progress[user_key]:
        progress[user_key] = normalized
        save_progress(progress)
    return progress[user_key]

def update_user_progress(user_id: int, updates: Dict[str, Any]) -> None:
    progress = load_progress()
    user_key = str(user_id)
    if user_key not in progress:
        progress[user_key] = copy.deepcopy(DEFAULT_USER_TEMPLATE)
    entry = progress[user_key]
    for k, v in updates.items():
        entry[k] = v
    progress[user_key] = normalize_user_entry(entry)
    save_progress(progress)

# ==================== HELPERS / SANITIZE ====================
def create_inline_keyboard(buttons: List[Dict[str, str]]) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for b in buttons:
        builder.button(text=b["text"], callback_data=b["callback_data"])
    builder.adjust(1)
    return builder.as_markup()

def format_timedelta_seconds(sec: int) -> str:
    if sec <= 0:
        return "0s"
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    parts = []
    if h:
        parts.append(f"{h}ч")
    if m:
        parts.append(f"{m}м")
    if s:
        parts.append(f"{s}с")
    return " ".join(parts)

def sanitize_text_value(raw) -> str:
    """
    Привести raw к корректной строке для отправки:
    - привести к str
    - удалить BOM и zero-width
    - удалить окружающие парные кавычки (' or ") если они окружают весь текст
    - trim()
    - если строка состоит только из кавычек -> вернуть ""
    """
    if raw is None:
        return ""
    # привести к строке
    if isinstance(raw, str):
        s = raw
    else:
        try:
            s = str(raw)
        except Exception:
            try:
                s = json.dumps(raw, ensure_ascii=False)
            except Exception:
                s = repr(raw)
    # удалим BOM/zero-width
    s = s.replace("\ufeff", "")
    s = re.sub(r'[\u200B\u200C\u200D\uFEFF]', '', s)
    # нормализуем переводы строк
    s = s.replace('\r\n', '\n').replace('\r', '\n')
    # trim краёв
    s = s.strip()
    # если строка состоит только из кавычек — пустая
    if re.fullmatch(r'["\']+', s):
        return ""
    # убрать окружающие парные кавычки многократно
    while len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        inner = s[1:-1].strip()
        if inner == s or inner == "":
            s = inner
            break
        s = inner
    return s.strip()

# ==================== LOADER (apply data from sheets) ====================
def apply_loaded_data(data: Dict[str, Any]) -> None:
    """
    Применить данные, полученные из sheets_loader, к глобальным переменным.
    """
    global CHAPTER_1_TEXTS, CHAPTER_2_TEXTS, CHAPTER_3_TEXTS, CHAPTER_4_TEXTS
    global CHAPTER_1_QUESTIONS, CHAPTER_2_QUESTIONS, CHAPTER_3_QUESTIONS, CHAPTER_4_QUESTIONS
    global MENU_GUIDE, WINE_PAIRING, ALLERGEN_GUIDE

    CT = data.get("chapter_texts", {}) or {}
    Q = data.get("questions", {}) or {}

    # Сохраняем как строки через sanitize_text_value
    def sanitize_texts(lst):
        return [sanitize_text_value(t) for t in (lst or [])]

    CHAPTER_1_TEXTS = sanitize_texts(CT.get(1, []))
    CHAPTER_2_TEXTS = sanitize_texts(CT.get(2, []))
    CHAPTER_3_TEXTS = sanitize_texts(CT.get(3, []))
    CHAPTER_4_TEXTS = sanitize_texts(CT.get(4, []))

    def normalize_question(qobj):
        return {
            "q": qobj.get("q", "") if qobj else "",
            "options": qobj.get("options", []) if qobj else [],
            "correct": int(qobj.get("correct", 0)) if qobj and qobj.get("correct", "") != "" else 0,
            "explanation": qobj.get("explanation", "") if qobj else ""
        }

    CHAPTER_1_QUESTIONS = [normalize_question(q) for q in Q.get(1, [])]
    CHAPTER_2_QUESTIONS = [normalize_question(q) for q in Q.get(2, [])]
    CHAPTER_3_QUESTIONS = [normalize_question(q) for q in Q.get(3, [])]
    CHAPTER_4_QUESTIONS = [normalize_question(q) for q in Q.get(4, [])]

    MENU_GUIDE = data.get("menu", {}) or {}
    WINE_PAIRING = data.get("wine", {}) or {}
    ALLERGEN_GUIDE = data.get("allergens", {}) or {}

    logger.info("Loaded data: texts(%d,%d,%d,%d) questions(%d,%d,%d,%d) menu(%d)",
                len(CHAPTER_1_TEXTS), len(CHAPTER_2_TEXTS), len(CHAPTER_3_TEXTS), len(CHAPTER_4_TEXTS),
                len(CHAPTER_1_QUESTIONS), len(CHAPTER_2_QUESTIONS), len(CHAPTER_3_QUESTIONS), len(CHAPTER_4_QUESTIONS),
                len(MENU_GUIDE))

# ==================== DISPATCHER / HANDLERS ====================
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    progress = get_user_progress(user_id)
    if progress["b"] > 1 or progress["i"] > 0:
        keyboard = create_inline_keyboard([
            {"text": "📚 Продолжить с места остановки", "callback_data": "continue_training"},
            {"text": "🔄 Начать сначала", "callback_data": "reset_training"}
        ])
        await message.answer("Ты уже начинал обучение! Что хочешь сделать?", reply_markup=keyboard)
        return
    await message.answer("""
👋 Привет! Я — Дато, учу официантов быть Южанами.

Пройдёшь 4 блока — будешь готов к смене.

📚 Блоки:
1. Легенды
2. Сервис
3. Кухня
4. Бар и вино

Начинаем с легенды про 18+.
""", reply_markup=create_inline_keyboard([
        {"text": "🚀 Начать блок 1", "callback_data": "start_chapter_1"}
    ]))

@dp.callback_query(F.data == "reset_training")
async def reset_training(callback: CallbackQuery):
    user_id = callback.from_user.id
    progress = load_progress()
    progress[str(user_id)] = copy.deepcopy(DEFAULT_USER_TEMPLATE)
    save_progress(progress)
    await callback.answer("Прогресс сброшен!", show_alert=True)
    await callback.message.answer("Начинаем сначала!")
    await show_next_text(callback.message, user_id)

@dp.callback_query(F.data == "continue_training")
async def continue_training(callback: CallbackQuery):
    await callback.answer("Продолжаем!")
    await show_next_text(callback.message, callback.from_user.id)

@dp.message(Command("reload_data"))
async def cmd_reload_data(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return
    try:
        data = load_sheet_data(SHEET_ID)
        ok, errors = validate_loaded(data)
        if not ok:
            await message.answer("Ошибки при загрузке данных:\n" + "\n".join(errors))
            apply_loaded_data(data)  # применим частично, если есть
            return
        apply_loaded_data(data)
        await message.answer("Данные успешно обновлены из Google Sheets ✅")
    except Exception as e:
        logger.exception("Reload failed")
        await message.answer(f"Ошибка при загрузке: {e}")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = """
📚 Команды:
/start — старт обучения
/reload_data — обновить данные из Google Sheets (только админ)
/menu [блюдо]
/wine [блюдо]
/allergens [аллерген]
/stats — твой прогресс
/reset — сбросить прогресс
"""
    await message.answer(help_text)

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    user_id = message.from_user.id
    progress = get_user_progress(user_id)
    stats_text = f"📊 ВАШ ПРОГРЕСС\n\nТекущий блок: {progress['b']}\nСтатус: {'📖 Чтение' if progress['s']=='t' else '📝 Тест'}\n\nРезультаты тестов:\n"
    for i in range(1, 5):
        if f"chapter_{i}" in progress.get("ts", {}):
            total = len([CHAPTER_1_QUESTIONS, CHAPTER_2_QUESTIONS, CHAPTER_3_QUESTIONS, CHAPTER_4_QUESTIONS][i-1])
            stats_text += f"• Блок {i}: {progress['ts'].get(f'chapter_{i}', 0)}/{total}\n"
    # блокировки
    blocks_locked = progress.get("l", {}) or {}
    if blocks_locked:
        stats_text += "\nБлокировки:\n"
        now_ts = datetime.datetime.now().timestamp()
        for ch, unlock_ts in blocks_locked.items():
            if unlock_ts and unlock_ts > now_ts:
                remaining = int(unlock_ts - now_ts)
                stats_text += f"• {ch}: разблокируется через {format_timedelta_seconds(remaining)}\n"
    await message.answer(stats_text)

@dp.message(Command("reset"))
async def cmd_reset(message: Message):
    keyboard = create_inline_keyboard([
        {"text": "✅ Да, сбросить всё", "callback_data": "reset_training"},
        {"text": "❌ Отмена", "callback_data": "cancel_reset"}
    ])
    await message.answer("Точно хочешь сбросить весь прогресс?", reply_markup=keyboard)

@dp.callback_query(F.data == "cancel_reset")
async def cancel_reset(callback: CallbackQuery):
    await callback.answer("Отмена", show_alert=True)
    try:
        await callback.message.edit_text("Сброс отменен")
    except Exception:
        pass

# ==================== ОТОБРАЖЕНИЕ ТЕКСТОВ ====================
async def show_next_text(message: Message, user_id: int):
    """
    Robust show_next_text with sanitize_text_value usage.
    Пропускает действительно пустые тексты и логирует raw repr для диагностики.
    """
    progress = get_user_progress(user_id)
    block_num = progress.get("b", 1)
    texts_map = {1: CHAPTER_1_TEXTS, 2: CHAPTER_2_TEXTS, 3: CHAPTER_3_TEXTS, 4: CHAPTER_4_TEXTS}
    texts = texts_map.get(block_num, []) or []
    current_index = progress.get("i", 0)

    idx = current_index
    while idx < len(texts):
        raw = texts[idx]
        cleaned = sanitize_text_value(raw)
        # Log diagnostic summary
        logger.debug("show_next_text: user=%s block=%s idx=%s raw_type=%s cleaned_len=%s cleaned_repr=%r",
                     user_id, block_num, idx, type(raw).__name__, len(cleaned), (cleaned[:200] + '...') if len(cleaned) > 200 else cleaned)

        if not cleaned:
            logger.info("show_next_text: skipping empty text at block %s idx %s (raw repr=%r)", block_num, idx, repr(raw)[:200])
            idx += 1
            continue

        # Telegram size guard
        if len(cleaned) > 4096:
            logger.warning("show_next_text: text too long (%d) at block %s idx %s — skipping", len(cleaned), block_num, idx)
            idx += 1
            continue

        # Persist index if we skipped blanks before
        if idx != current_index:
            update_user_progress(user_id, {"i": idx})
            current_index = idx

        # Prepare keyboard
        if idx < len(texts) - 1:
            keyboard_data = [{"text": "Дальше ➡️", "callback_data": f"next_text_{block_num}"}]
        else:
            keyboard_data = [{"text": "📋 Готов к тесту", "callback_data": f"start_test_{block_num}"}]

        # Try to send; on TelegramBadRequest log and skip
        try:
            await message.answer(cleaned, reply_markup=create_inline_keyboard(keyboard_data))
            return
        except TelegramBadRequest as e:
            # Log problematic text snippet
            snippet = (cleaned[:1000] + '...') if len(cleaned) > 1000 else cleaned
            logger.warning("TelegramBadRequest sending text (user=%s block=%s idx=%s len=%s): %s; snippet=%r raw_repr=%r",
                           user_id, block_num, idx, len(cleaned), e, snippet, repr(raw)[:1000])
            idx += 1
            continue
        except Exception as e:
            logger.exception("Unexpected error when sending text (block=%s idx=%s): %s", block_num, idx, e)
            idx += 1
            continue

    # no valid texts -> start test
    await start_test(message, user_id)

@dp.callback_query(F.data.startswith("next_text_"))
async def handle_next_text(callback: CallbackQuery):
    user_id = callback.from_user.id
    progress = get_user_progress(user_id)
    new_index = progress["i"] + 1
    update_user_progress(user_id, {"i": new_index})
    await show_next_text(callback.message, user_id)

# ==================== ТЕСТЫ ====================
async def start_test(message: Message, user_id: int):
    progress = get_user_progress(user_id)
    block_num = progress["b"]
    chapter_key = f"chapter_{block_num}"
    blocks_locked = progress.get("l", {}) or {}
    now_ts = datetime.datetime.now().timestamp()
    locked_until = blocks_locked.get(chapter_key)
    if locked_until and locked_until > now_ts:
        remaining = int(locked_until - now_ts)
        await message.answer(f"⚠️ Этот блок заблокирован для пересдачи. Осталось: {format_timedelta_seconds(remaining)}")
        return
    # Инициализация current_test в прогрессе под ключом '_ct'
    update_user_progress(user_id, {"i": 0, "s": "x"})
    progress = get_user_progress(user_id)
    current_test = {"block": block_num, "answers": [], "chat_id": None, "message_id": None, "start_ts": int(now_ts)}
    progress["_ct"] = current_test
    update_user_progress(user_id, progress)
    questions_map = {1: CHAPTER_1_QUESTIONS, 2: CHAPTER_2_QUESTIONS, 3: CHAPTER_3_QUESTIONS, 4: CHAPTER_4_QUESTIONS}
    questions = questions_map.get(block_num, [])
    await message.answer(f"📝 ТЕСТ: Блок {block_num}\n\nВопросов: {len(questions)}\nМинимум для прохода: {math.ceil(len(questions) * 0.8)} правильных\n\nНачинаем!")
    await show_question(message, user_id, 0)

async def show_question(message: Message, user_id: int, question_index: int):
    progress = get_user_progress(user_id)
    current_test = progress.get("_ct", {"block": progress["b"], "answers": []})
    block_num = current_test.get("block", progress["b"])
    questions_map = {1: CHAPTER_1_QUESTIONS, 2: CHAPTER_2_QUESTIONS, 3: CHAPTER_3_QUESTIONS, 4: CHAPTER_4_QUESTIONS}
    questions = questions_map.get(block_num, [])
    bot = message.bot
    if question_index >= len(questions):
        # редактируем сообщение (если есть) и считаем результат
        if current_test.get("message_id"):
            try:
                await bot.edit_message_text("✅ Все вопросы заданы. Подсчитываем результат...", chat_id=current_test["chat_id"], message_id=current_test["message_id"])
            except Exception:
                pass
        await show_test_results(message, user_id)
        return
    question = questions[question_index]
    keyboard_buttons = []
    for i, option in enumerate(question["options"]):
        keyboard_buttons.append({
            "text": f"{chr(65+i)}) {option}",
            "callback_data": f"answer_{block_num}_{question_index}_{i}"
        })
    markup = create_inline_keyboard(keyboard_buttons)
    q_text = f"❓ Вопрос {question_index + 1}/{len(questions)}\n\n{question['q']}"
    # Редактируем существующее сообщение или отправляем новое и сохраняем id
    if current_test.get("message_id"):
        try:
            await bot.edit_message_text(q_text, chat_id=current_test["chat_id"], message_id=current_test["message_id"], reply_markup=markup)
        except Exception:
            m = await bot.send_message(message.chat.id, q_text, reply_markup=markup)
            current_test["message_id"] = m.message_id
            current_test["chat_id"] = m.chat.id
            progress["_ct"] = current_test
            update_user_progress(user_id, progress)
    else:
        m = await bot.send_message(message.chat.id, q_text, reply_markup=markup)
        current_test["message_id"] = m.message_id
        current_test["chat_id"] = m.chat.id
        progress["_ct"] = current_test
        update_user_progress(user_id, progress)

@dp.callback_query(F.data.startswith("answer_"))
async def handle_answer(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        _, block_num_s, question_index_s, answer_index_s = callback.data.split("_")
    except Exception:
        await callback.answer("Неверный формат данных.", show_alert=True)
        return
    block_num = int(block_num_s); question_index = int(question_index_s); answer_index = int(answer_index_s)
    questions_map = {1: CHAPTER_1_QUESTIONS, 2: CHAPTER_2_QUESTIONS, 3: CHAPTER_3_QUESTIONS, 4: CHAPTER_4_QUESTIONS}
    try:
        question = questions_map[block_num][question_index]
    except Exception:
        await callback.answer("Вопрос не найден.", show_alert=True); return
    is_correct = answer_index == int(question["correct"])
    progress = get_user_progress(user_id)
    current_test = progress.get("_ct", {"block": block_num, "answers": [], "chat_id": callback.message.chat.id, "message_id": callback.message.message_id})
    # Проверка, что пользователь не отвечает на устаревший тест
    if current_test.get("block") != block_num:
        await callback.answer("Тест устарел или уже завершён. Пожалуйста, начните заново.", show_alert=True)
        return
    answers: List[Dict[str, Any]] = current_test.get("answers", [])
    # Защита от повторного ответа на один вопрос
    if any(a.get("question_index") == question_index for a in answers):
        await callback.answer("Вы уже ответили на этот вопрос. Повторный ответ не принимается.", show_alert=True)
        return
    answer_record = {
        "question_index": question_index,
        "selected_index": answer_index,
        "selected_text": question["options"][answer_index] if answer_index < len(question["options"]) else "",
        "correct_index": int(question["correct"]),
        "correct": 1 if is_correct else 0,
        "timestamp": datetime.datetime.now().isoformat()
    }
    answers.append(answer_record)
    current_test["answers"] = answers
    progress["_ct"] = current_test
    update_user_progress(user_id, progress)
    # Отправка обратной связи
    if is_correct:
        await callback.answer("✅ Правильно!", show_alert=False)
    else:
        await callback.answer(f"❌ Неправильно. {question.get('explanation','')}", show_alert=True)
    # Переход к следующему вопросу: редактирование того же message
    await show_question(callback.message, user_id, question_index + 1)

async def show_test_results(message: Message, user_id: int):
    progress = get_user_progress(user_id)
    current_test = progress.get("_ct", {"block": progress["b"], "answers": []})
    block_num = current_test.get("block", progress["b"])
    questions_map = {1: CHAPTER_1_QUESTIONS, 2: CHAPTER_2_QUESTIONS, 3: CHAPTER_3_QUESTIONS, 4: CHAPTER_4_QUESTIONS}
    total_questions = len(questions_map[block_num])
    answers = current_test.get("answers", [])
    score = sum(a.get("correct", 0) for a in answers)
    min_pass = math.ceil(total_questions * 0.8)
    # Сохранение истории попыток
    test_history = progress.get("h", {}) or {}
    chapter_history = test_history.get(f"chapter_{block_num}", [])
    attempt = {
        "timestamp": datetime.datetime.now().isoformat(),
        "score": score,
        "total": total_questions,
        "passed": score >= min_pass,
        "details": answers
    }
    chapter_history.append(attempt)
    test_history[f"chapter_{block_num}"] = chapter_history
    # Обновление test_scores (последний результат)
    scores = progress.get("ts", {})
    scores[f"chapter_{block_num}"] = score
    # Удаляем текущий тест и обновляем прогресс
    update_user_progress(user_id, {
        "ts": scores,
        "s": "t",
        "_ct": {},
        "h": test_history
    })
    result_text = f"✅ ТЕСТ ЗАВЕРШЁН!\n\nРезультат: {score}/{total_questions}\nМинимум для прохода: {min_pass}/{total_questions}\n"
    if score >= min_pass:
        result_text += "\n🎉 Молодец! Ты готов к следующему блоку."
        passed_blocks = progress.get("p", [])
        chapter_key = f"chapter_{block_num}"
        if chapter_key not in passed_blocks:
            passed_blocks.append(chapter_key)
        # снимаем возможную блокировку
        blocks_locked = progress.get("l", {}) or {}
        if chapter_key in blocks_locked:
            blocks_locked.pop(chapter_key, None)
        update_user_progress(user_id, {"p": passed_blocks, "l": blocks_locked})
        # Проверяем, пройдены ли все блоки
        if len(passed_blocks) == 4:
            await message.answer(result_text)
            await message.answer(generate_certificate(user_id))
            await message.answer("🎉 ПОЗДРАВЛЯЮ, ТЫ СТАЛ ЮЖАНИНОМ!")
            return
        next_block = block_num + 1
        keyboard = create_inline_keyboard([
            {"text": f"🚀 Блок {next_block}", "callback_data": f"start_chapter_{next_block}"}
        ])
        await message.answer(result_text, reply_markup=keyboard)
    else:
        # Устанавливаем блокировку на пересдачу
        blocks_locked = progress.get("l", {}) or {}
        unlock_ts = int(datetime.datetime.now().timestamp()) + BLOCK_COOLDOWN_SECONDS
        blocks_locked[f"chapter_{block_num}"] = unlock_ts
        update_user_progress(user_id, {"l": blocks_locked})
        deficit = max(0, min_pass - score)
        result_text += f"\n❌ Не хватило {deficit} баллов.\n\nПересдача будет доступна через {format_timedelta_seconds(BLOCK_COOLDOWN_SECONDS)}."
        keyboard = create_inline_keyboard([
            {"text": "📋 Посмотреть детали попытки", "callback_data": f"view_history_{block_num}"},
        ])
        await message.answer(result_text, reply_markup=keyboard)

@dp.callback_query(F.data.startswith("start_test_"))
async def handle_start_test(callback: CallbackQuery):
    user_id = callback.from_user.id
    block_num = int(callback.data.split("_")[-1])
    progress = get_user_progress(user_id)
    if progress["b"] != block_num:
        await callback.answer("Необходимо прочитать всю теорию!", show_alert=True)
        return
    await start_test(callback.message, user_id)

@dp.callback_query(F.data.startswith("start_chapter_"))
async def handle_start_chapter(callback: CallbackQuery):
    user_id = callback.from_user.id
    block_num = int(callback.data.split("_")[-1])
    update_user_progress(user_id, {"b": block_num, "i": 0, "s": "t"})
    await show_next_text(callback.message, user_id)

@dp.callback_query(F.data.startswith("view_history_"))
async def view_history(callback: CallbackQuery):
    user_id = callback.from_user.id
    block_num = int(callback.data.split("_")[-1])
    progress = get_user_progress(user_id)
    history = progress.get("h", {}) or {}
    chapter_history = history.get(f"chapter_{block_num}", [])
    if not chapter_history:
        await callback.answer("История для этого блока отсутствует.", show_alert=True)
        return
    last = chapter_history[-1]
    details_lines = []
    for a in last.get("details", []):
        qidx = a.get("question_index")
        sel = a.get("selected_index")
        sel_text = a.get("selected_text")
        corr = a.get("correct")
        details_lines.append(f"Q{qidx+1}: {'OK' if corr else 'NO'} — выбран {chr(65+sel)}: {sel_text}")
    details_text = "\n".join(details_lines)
    await callback.message.answer(f"Последняя попытка ({last['timestamp']}): {last['score']}/{last['total']}\n\n{details_text}")
    await callback.answer()

# ==================== СПРАВОЧНИКИ ====================
@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    query = message.text.lower().replace("/menu", "").strip()
    if not query:
        await message.answer("🔍 Введите: /menu название_блюда\n\nПример: /menu хинкали")
        return
    for key, value in MENU_GUIDE.items():
        if query in key.lower() or key.lower() in query:
            await message.answer(value)
            return
    await message.answer(f"❌ Блюдо '{query}' не найдено.")

@dp.message(Command("wine"))
async def cmd_wine(message: Message):
    query = message.text.lower().replace("/wine", "").strip()
    if not query:
        await message.answer("🔍 Введите: /wine название_блюда\n\nПример: /wine хинкали")
        return
    for key, value in WINE_PAIRING.items():
        if query in key.lower() or key.lower() in query:
            await message.answer(f"{value}\n\n💡 Подавать при правильной температуре!")
            return
    await message.answer(f"❌ Пэйринг для '{query}' не найден.")

@dp.message(Command("allergens"))
async def cmd_allergens(message: Message):
    query = message.text.lower().replace("/allergens", "").strip()
    if not query:
        await message.answer("🔍 Введите: /allergens аллерген\n\nПример: /allergens глютен")
        return
    for key, value in ALLERGEN_GUIDE.items():
        if query in key.lower() or key.lower() in query:
            await message.answer(value)
            return
    await message.answer(f"❌ Аллерген '{query}' не найден.")

# ==================== STARTUP / MAIN ====================
async def startup_load_data_and_normalize():
    # Нормализуем user_progress.json перед началом (если нужно)
    try:
        normalize_all_progress()
    except Exception:
        logger.exception("normalize_all_progress failed")

    # Попробуем загрузить данные из Google Sheet
    try:
        data = load_sheet_data(SHEET_ID)
        ok, errors = validate_loaded(data)
        if not ok:
            logger.warning("Sheet loaded with validation errors: %s", errors)
        apply_loaded_data(data)
        logger.info("Initial data load from sheet done.")
    except Exception:
        logger.exception("Initial load from Google Sheet failed — using fallback data")

async def main():
    bot = Bot(token=API_TOKEN)
    await startup_load_data_and_normalize()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    print("Запуск бота...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем")