"""Local street matching with the Laya multilingual decision model."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class LayaAIResolver:
    """Resolve a street against LAMAS candidates using Laya's choice decision."""

    def __init__(self, model_name: str = "convaiinnovations/laya-multilingual"):
        try:
            from laya import Agent
        except ImportError as exc:
            raise RuntimeError(
                "The Laya resolver requires the optional 'laya' package. "
                "Install it with: pip install 'laya>=0.4.35,<0.5'"
            ) from exc

        self.agent = Agent(model_name)

    def is_available(self) -> bool:
        return self.agent is not None

    def resolve_street(
        self,
        osm_id: str,
        osm_street_name: str,
        city_name: str,
        lamas_candidates: list[dict[str, Any]],
        adjacent_streets: list[str],
        city_lexicon: str = "",
    ) -> dict[str, Any]:
        """Select one LAMAS candidate, or abstain when none is suitable."""
        if not lamas_candidates:
            return {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": "No LAMAS candidates were provided",
                "method": "local_laya",
            }

        candidate_ids: dict[str, str] = {}
        criteria: dict[str, str] = {}
        for index, candidate in enumerate(lamas_candidates, start=1):
            option = f"candidate_{index}"
            candidate_ids[option] = str(candidate["id"])
            names = candidate.get("names") or candidate.get("aliases") or [
                candidate.get("name", "")
            ]
            names = [str(name) for name in names if name]
            criteria[option] = (
                f'{", ".join(names)} (LAMAS ID {candidate["id"]})'
            )
        criteria["no_match"] = "אף אחד מהמועמדים אינו אותו רחוב כמו רחוב OSM"

        state = {
            "יישוב": city_name,
            "שם הרחוב ב-OSM": osm_street_name,
            "רחובות סמוכים ב-OSM": adjacent_streets,
            "מועמדי למ״ס": [
                {
                    "id": candidate["id"],
                    "names": candidate.get("names")
                    or candidate.get("aliases")
                    or [candidate.get("name", "")],
                }
                for candidate in lamas_candidates
            ],
        }
        questions = {
            "street_match": {
                "type": "choice",
                "instructions": (
                    "איזה מועמד למ״ס מתייחס לאותו רחוב כמו רחוב OSM? "
                    "השווה את שמות המועמדים והכינויים שלהם, והיעזר בשם היישוב "
                    "וברחובות הסמוכים. בחר no_match אם אין "
                    "מועמד שמזהה את אותו רחוב."
                ),
                "criteria": criteria,
            }
        }

        try:
            result = self.agent.predict(state, questions)
            answer = result["answers"]["street_match"]
            selected = answer.get("choice")
            probabilities = answer.get("probabilities", {})
            confidence_value = probabilities.get(selected)
            if confidence_value is None:
                confidence_value = answer.get("confidence", 0.0)
            confidence = float(confidence_value)

            if selected == "no_match":
                lamas_id = None
            elif selected in candidate_ids:
                lamas_id = candidate_ids[selected]
            else:
                raise ValueError(f"Laya returned an unknown candidate: {selected!r}")

            return {
                "lamas_id": lamas_id,
                "confidence": confidence,
                "reasoning": f"Laya selected {selected}",
                "method": "local_laya",
            }
        except Exception as exc:
            logger.exception("Laya resolution failed for OSM ID %s", osm_id)
            return {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": f"Laya error: {exc}",
                "method": "local_laya_error",
            }
