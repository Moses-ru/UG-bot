"""
Loader for public Google Sheets (published or shared) using CSV export via gviz endpoint.

Usage:
    from sheets_loader import load_sheet_data
    data = load_sheet_data(SHEET_ID)

Expecting these sheet names (case-insensitive lookup supported):
- Texts         : columns -> chapter (1..4), index (0..), text
- Questions     : columns -> chapter, q_index, question, option_1..option_N, correct, explanation
- Menu          : columns -> key, value
- Wine          : columns -> key, value
- Allergens     : columns -> key, value

This loader will try to parse those sheets and return structured dict suitable for the bot.
"""

from typing import Dict, Any, List, Tuple
import requests
import csv
from io import StringIO
import logging

logger = logging.getLogger(__name__)

# These names must match sheet tabs in the Google Sheet.
SHEET_NAMES = {
    "texts": "Texts",
    "questions": "Questions",
    "menu": "Menu",
    "wine": "Wine",
    "allergens": "Allergens"
}

GVIZ_CSV_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

def _fetch_csv(sheet_id: str, sheet_name: str, timeout: int = 10) -> List[dict]:
    url = GVIZ_CSV_URL.format(sheet_id=sheet_id, sheet_name=sheet_name)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    text = resp.content.decode("utf-8")
    reader = csv.DictReader(StringIO(text))
    return list(reader)

def load_sheet_data(sheet_id: str) -> Dict[str, Any]:
    """
    Load data from the Google Sheet and return dict:
    {
      "chapter_texts": {1: [text0, ...], 2: [...], 3: [...], 4: [...]},
      "questions": {1: [qobj,...], 2: [...], 3: [...], 4: [...]},
      "menu": {key: value, ...},
      "wine": {...},
      "allergens": {...}
    }
    """
    result: Dict[str, Any] = {
        "chapter_texts": {1: [], 2: [], 3: [], 4: []},
        "questions": {1: [], 2: [], 3: [], 4: []},
        "menu": {},
        "wine": {},
        "allergens": {}
    }

    # TEXTS
    try:
        rows = _fetch_csv(sheet_id, SHEET_NAMES["texts"])
        for r in rows:
            ch_raw = r.get("chapter") or r.get("Chapter") or r.get("CHAPTER")
            idx_raw = r.get("index") or r.get("Index") or r.get("INDEX")
            text = r.get("text") or r.get("Text") or r.get("TEXT") or ""
            try:
                ch = int(float(ch_raw)) if ch_raw is not None and ch_raw != "" else None
            except Exception:
                ch = None
            try:
                idx = int(float(idx_raw)) if idx_raw is not None and idx_raw != "" else 0
            except Exception:
                idx = 0
            if ch in (1,2,3,4):
                result["chapter_texts"][ch].append((idx, text))
        # sort by idx and keep only text
        for ch in (1,2,3,4):
            items = sorted(result["chapter_texts"][ch], key=lambda x: int(x[0]))
            result["chapter_texts"][ch] = [t for _, t in items]
    except Exception as e:
        logger.exception("Failed to load Texts sheet: %s", e)

    # QUESTIONS
    try:
        rows = _fetch_csv(sheet_id, SHEET_NAMES["questions"])
        for r in rows:
            ch_raw = r.get("chapter") or r.get("Chapter") or r.get("CHAPTER")
            qidx_raw = r.get("q_index") or r.get("qIndex") or r.get("index") or r.get("Q_INDEX")
            qtext = r.get("question") or r.get("Question") or ""
            # collect options by looking for option_1..option_20 or A..Z
            options = []
            for n in range(1, 21):
                key = f"option_{n}"
                if key in r and r.get(key, "") not in (None, ""):
                    options.append(r.get(key))
            if not options:
                # try A,B,C...
                for letter in ("A","B","C","D","E","F","G","H","I","J"):
                    if letter in r and r.get(letter, "") not in (None, ""):
                        options.append(r.get(letter))
            # correct parsing
            correct_raw = r.get("correct") or r.get("Correct") or ""
            correct_idx = None
            if isinstance(correct_raw, (int, float)):
                correct_idx = int(correct_raw)
            elif isinstance(correct_raw, str) and correct_raw.strip() != "":
                cr = correct_raw.strip()
                if cr.isdigit():
                    correct_idx = int(cr)
                else:
                    uc = cr.upper()
                    if len(uc) == 1 and "A" <= uc <= "Z":
                        correct_idx = ord(uc) - ord("A")
            try:
                ch = int(float(ch_raw)) if ch_raw is not None and ch_raw != "" else None
            except Exception:
                ch = None
            try:
                qidx = int(float(qidx_raw)) if qidx_raw not in (None, "") else 0
            except Exception:
                qidx = 0
            explanation = r.get("explanation") or r.get("Explanation") or ""
            qobj = {
                "q_index": qidx,
                "q": qtext,
                "options": options,
                "correct": correct_idx if correct_idx is not None else 0,
                "explanation": explanation
            }
            if ch in (1,2,3,4):
                result["questions"][ch].append((qidx, qobj))
        # sort by q_index
        for ch in (1,2,3,4):
            items = sorted(result["questions"][ch], key=lambda x: int(x[0]))
            result["questions"][ch] = [q for _, q in items]
    except Exception as e:
        logger.exception("Failed to load Questions sheet: %s", e)

    # MENU
    try:
        rows = _fetch_csv(sheet_id, SHEET_NAMES["menu"])
        for r in rows:
            k = (r.get("key") or r.get("Key") or "").strip().lower()
            v = r.get("value") or r.get("Value") or ""
            if k:
                result["menu"][k] = v
    except Exception as e:
        logger.exception("Failed to load Menu sheet: %s", e)

    # WINE
    try:
        rows = _fetch_csv(sheet_id, SHEET_NAMES["wine"])
        for r in rows:
            k = (r.get("key") or r.get("Key") or "").strip().lower()
            v = r.get("value") or r.get("Value") or ""
            if k:
                result["wine"][k] = v
    except Exception as e:
        logger.exception("Failed to load Wine sheet: %s", e)

    # ALLERGENS
    try:
        rows = _fetch_csv(sheet_id, SHEET_NAMES["allergens"])
        for r in rows:
            k = (r.get("key") or r.get("Key") or "").strip().lower()
            v = r.get("value") or r.get("Value") or ""
            if k:
                result["allergens"][k] = v
    except Exception as e:
        logger.exception("Failed to load Allergens sheet: %s", e)

    return result

def validate_loaded(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    ct = data.get("chapter_texts", {})
    for ch in (1,2,3,4):
        if not ct.get(ch):
            errors.append(f"No texts for chapter {ch}")
    qs = data.get("questions", {})
    for ch in (1,2,3,4):
        qlist = qs.get(ch, [])
        if not qlist:
            errors.append(f"No questions for chapter {ch}")
        else:
            for i, q in enumerate(qlist):
                if not q.get("options"):
                    errors.append(f"Question {i} in chapter {ch} has no options")
    if not data.get("menu"):
        errors.append("Menu empty")
    ok = len(errors) == 0
    return ok, errors