"""Whole-city street matching with the Laya multilingual decision model."""

from __future__ import annotations

import logging
from typing import Any

from arabic_matching import has_arabic, transliterate_arabic_to_hebrew

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "convaiinnovations/laya-multilingual"
DEFAULT_CHUNK_SIZE = 20
DEFAULT_BATCH_SIZE = 32


class LayaAIResolver:
    """Match all unmatched OSM streets in a city against the city's LAMAS streets."""

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        try:
            from laya import Agent
        except ImportError as exc:
            raise RuntimeError(
                "The Laya resolver requires the optional 'laya' package. "
                "Install it with: pip install 'laya>=0.4.35,<0.5'"
            ) from exc

        self.agent = Agent(model_name)
        self.chunk_size = max(2, int(chunk_size))
        self.batch_size = max(1, int(batch_size))

    def is_available(self) -> bool:
        return self.agent is not None

    @staticmethod
    def _build_choice_question(
        options: list[tuple[str, list[str]]]
    ) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
        option_ids: dict[str, str] = {}
        criteria: dict[str, str] = {}
        for index, (lamas_id, names) in enumerate(options, start=1):
            key = f"street_{index}"
            option_ids[key] = str(lamas_id)
            label = " | ".join(str(name)[:120] for name in names[:3] if name)
            criteria[key] = f"{label} (ID {lamas_id})" if label else f"ID {lamas_id}"
        criteria["no_match"] = "אף אחד מהרחובות אינו אותו רחוב כמו רחוב OSM"

        questions = {
            "street_match": {
                "type": "choice",
                "instructions": (
                    "איזה רחוב מרשימת למ״ס הוא אותו רחוב כמו רחוב OSM? "
                    "התחשב באיות, כינויים, קיצורים ותרגום עברית/ערבית. "
                    "בחר no_match אם אין התאמה."
                ),
                "criteria": criteria,
            }
        }
        return option_ids, questions

    @staticmethod
    def _parse_answer(
        result: dict[str, Any], option_ids: dict[str, str]
    ) -> tuple[str | None, float, str]:
        answer = result["answers"]["street_match"]
        selected = answer.get("choice")
        probabilities = answer.get("probabilities", {})
        confidence_value = probabilities.get(selected)
        if confidence_value is None:
            confidence_value = answer.get("confidence", 0.0)
        confidence = float(confidence_value)

        if selected == "no_match":
            return None, confidence, "Laya selected no_match"
        if selected in option_ids:
            return option_ids[selected], confidence, f"Laya selected {selected}"
        raise ValueError(f"Laya returned an unknown choice: {selected!r}")

    @staticmethod
    def _build_state(city_name: str, osm_street: str) -> dict[str, str]:
        state = {
            "יישוב": str(city_name)[:120],
            "שם הרחוב ב-OSM": str(osm_street)[:120],
        }
        if has_arabic(osm_street):
            translit = transliterate_arabic_to_hebrew(str(osm_street))
            if translit:
                state["תעתיק עברי"] = translit[:120]
        return state

    def _predict_choice(
        self,
        city_name: str,
        osm_street: str,
        options: list[tuple[str, list[str]]],
    ) -> tuple[str | None, float, str]:
        """Run one Laya choice decision over a bounded slice of LAMAS streets."""
        option_ids, questions = self._build_choice_question(options)
        state = self._build_state(city_name, osm_street)
        result = self.agent.predict(state, questions)
        return self._parse_answer(result, option_ids)

    def _predict_chunk_for_city(
        self,
        city_name: str,
        osm_streets: list[str],
        options: list[tuple[str, list[str]]],
    ) -> list[tuple[str | None, float, str]]:
        """Evaluate one chunk of LAMAS streets across all unmatched OSM streets."""
        option_ids, questions = self._build_choice_question(options)
        states = [self._build_state(city_name, osm_street) for osm_street in osm_streets]
        if len(states) > 1 and callable(getattr(self.agent, "predict_batch", None)):
            raw_results = self.agent.predict_batch(
                states, questions, batch_size=self.batch_size
            )
        else:
            raw_results = [self.agent.predict(state, questions) for state in states]
        return [self._parse_answer(res, option_ids) for res in raw_results]

    def _resolve_winners(
        self,
        city_name: str,
        osm_street: str,
        winners: list[tuple[str, list[str], float, str]],
    ) -> dict[str, Any]:
        """Run tournament tie-breaker rounds when multiple chunks produced a hit."""
        if not winners:
            return {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": "Laya selected no_match across city chunks",
                "method": "local_laya",
            }
        if len(winners) == 1:
            lid, _, conf, reason = winners[0]
            return {
                "lamas_id": lid,
                "confidence": conf,
                "reasoning": reason,
                "method": "local_laya",
            }

        current = [(lid, names) for lid, names, _, _ in winners]
        while len(current) > self.chunk_size:
            next_winners: list[tuple[str, list[str], float, str]] = []
            for start in range(0, len(current), self.chunk_size):
                chunk = current[start : start + self.chunk_size]
                lid, conf, reason = self._predict_choice(city_name, osm_street, chunk)
                if lid is not None:
                    names = next(names for opt_id, names in chunk if opt_id == lid)
                    next_winners.append((lid, names, conf, reason))
            if not next_winners:
                return {
                    "lamas_id": None,
                    "confidence": 0.0,
                    "reasoning": "Laya selected no_match in tournament round",
                    "method": "local_laya",
                }
            if len(next_winners) == 1:
                lid, _, conf, reason = next_winners[0]
                return {
                    "lamas_id": lid,
                    "confidence": conf,
                    "reasoning": reason,
                    "method": "local_laya",
                }
            current = [(lid, names) for lid, names, _, _ in next_winners]

        lid, conf, reason = self._predict_choice(city_name, osm_street, current)
        return {
            "lamas_id": lid,
            "confidence": conf,
            "reasoning": reason,
            "method": "local_laya",
        }

    def resolve_street(
        self,
        city_name: str,
        osm_street: str,
        lamas_streets: dict[str, list[str]],
    ) -> dict[str, Any]:
        """Match one OSM street against all LAMAS streets in the city."""
        decisions = self.resolve_city(city_name, [osm_street], lamas_streets)
        return decisions.get(
            str(osm_street),
            {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": "No LAMAS streets in city",
                "method": "local_laya",
            },
        )

    def resolve_city(
        self,
        city_name: str,
        osm_streets: list[str],
        lamas_streets: dict[str, list[str]],
    ) -> dict[str, dict[str, Any]]:
        """Match every unmatched OSM street in a city against all city LAMAS streets."""
        unique_streets = list(dict.fromkeys(str(s) for s in osm_streets if s))
        options = sorted(
            ((str(lid), list(names)) for lid, names in lamas_streets.items()),
            key=lambda item: item[0],
        )
        if not unique_streets or not options:
            return {}

        # Small city: single batched pass over all LAMAS streets.
        if len(options) <= self.chunk_size:
            try:
                parsed = self._predict_chunk_for_city(city_name, unique_streets, options)
                return {
                    osm_street: {
                        "lamas_id": lid,
                        "confidence": conf,
                        "reasoning": reason,
                        "method": "local_laya",
                    }
                    for osm_street, (lid, conf, reason) in zip(unique_streets, parsed)
                }
            except Exception as exc:
                logger.exception("Laya batch resolution failed for city '%s'", city_name)
                return {
                    osm_street: {
                        "lamas_id": None,
                        "confidence": 0.0,
                        "reasoning": f"Laya error: {exc}",
                        "method": "local_laya_error",
                    }
                    for osm_street in unique_streets
                }

        # Larger city: batch all unmatched streets across each chunk of LAMAS streets,
        # then break ties for any street that matched in multiple chunks.
        winners_by_street: dict[str, list[tuple[str, list[str], float, str]]] = {
            street: [] for street in unique_streets
        }
        try:
            for start in range(0, len(options), self.chunk_size):
                chunk = options[start : start + self.chunk_size]
                chunk_names = {lid: names for lid, names in chunk}
                parsed = self._predict_chunk_for_city(city_name, unique_streets, chunk)
                for osm_street, (lid, conf, reason) in zip(unique_streets, parsed):
                    if lid is not None:
                        winners_by_street[osm_street].append(
                            (lid, chunk_names[lid], conf, reason)
                        )
        except Exception as exc:
            logger.exception("Laya city resolution failed for '%s'", city_name)
            return {
                osm_street: {
                    "lamas_id": None,
                    "confidence": 0.0,
                    "reasoning": f"Laya error: {exc}",
                    "method": "local_laya_error",
                }
                for osm_street in unique_streets
            }

        decisions: dict[str, dict[str, Any]] = {}
        for osm_street in unique_streets:
            try:
                decisions[osm_street] = self._resolve_winners(
                    city_name, osm_street, winners_by_street[osm_street]
                )
            except Exception as exc:
                logger.exception("Laya tie-breaker failed for '%s'", osm_street)
                decisions[osm_street] = {
                    "lamas_id": None,
                    "confidence": 0.0,
                    "reasoning": f"Laya error: {exc}",
                    "method": "local_laya_error",
                }
        return decisions
