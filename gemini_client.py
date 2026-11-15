"""
Gemini API client for resolving ambiguous street name matches.
"""

from __future__ import annotations

import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

API_KEY = os.getenv("GEMINI_API_KEY")
DEFAULT_MODEL = os.getenv(
    "GEMINI_MODEL", "gemini-2.5-flash-preview-09-2025"
)
GEMINI_API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{DEFAULT_MODEL}:generateContent?key={API_KEY}"
)
MAX_RETRIES = 3

SYSTEM_PROMPT = (
    "אתה מערכת GIS אוטומטית שתפקידה למצוא את המזהה המספרי היחיד של רחוב "
    "(LAMAS ID) מתוך רשימת מועמדים, על בסיס שם וקונטקסט גיאוגרפי "
    "(שמות רחובות משיקים). השב עם המספר של ה-ID בלבד, או עם המילה 'None' "
    "אם לא נמצאה התאמה ודאית."
)


def get_ai_resolution(prompt: str, osm_id: str) -> str:
    """
    Call Gemini to resolve a street ID.
    Returns a numeric ID string, or 'None' on failure / no match.
    """
    if not API_KEY:
        logger.error("GEMINI_API_KEY not found or empty. Skipping AI resolution.")
        return "None"

    logger.info("Consulting Gemini for OSM ID: %s", osm_id)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "tools": [{"google_search": {}}],
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GEMINI_API_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=60,
            )
            response.raise_for_status()

            result = response.json()
            text = (
                result.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "None")
                .strip()
            )

            clean_text = "".join(filter(str.isdigit, text))
            if not clean_text:
                return "None"
            return clean_text

        except requests.exceptions.RequestException as e:
            logger.warning(
                "API error (attempt %s/%s) for %s: %s",
                attempt + 1,
                MAX_RETRIES,
                osm_id,
                e,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                return "None"

    return "None"
