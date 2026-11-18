"""
Local AI Resolver using qwen3-VL-8b (Text Only Mode)

This module handles local inference with the qwen3-VL-8b model
for street name matching. It processes text prompts with topological context
to identify the best matching LAMAS ID with confidence scoring.
"""

import os
import re
import json
from typing import Optional, Tuple, Dict, Any
import pandas as pd
import geopandas as gpd

try:
    from transformers import Qwen3VLForConditionalGeneration, AutoProcessor
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    print("Warning: transformers or torch not available. Local AI will be disabled.")


class LocalAIResolver:
    """Handles local AI resolution using qwen3-VL-8b model."""

    def __init__(self, model_name: str = "Qwen/Qwen3-VL-8B-Instruct", device: str = "auto"):
        self.model = None
        self.processor = None
        self.device = device
        self.model_name = model_name
        self.initialized = False

        if TRANSFORMERS_AVAILABLE:
            try:
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    print("  -> Cleared CUDA cache.")
                self._initialize_model()
            except Exception as e:
                print(f"Failed to initialize local AI model: {e}")
                print("Local AI resolution will be disabled. Falling back to Gemini API.")

    def _initialize_model(self):
        print(f"Loading local AI model: {self.model_name}...")
        if self.device == "auto":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.processor = AutoProcessor.from_pretrained(self.model_name)
        if self.device == "cuda":
            self.model = Qwen3VLForConditionalGeneration.from_pretrained(
                self.model_name, torch_dtype=torch.bfloat16, device_map="auto"
            )
        else:
            self.model = Qwen3VLForConditionalGeneration.from_pretrained(
                self.model_name, torch_dtype=torch.float32
            )
            self.model = self.model.to(self.device)
        self.initialized = True
        print(f"Model loaded successfully on {self.device}")

    def is_available(self) -> bool:
        return TRANSFORMERS_AVAILABLE and self.initialized and self.model is not None

    def prepare_prompt(
        self,
        osm_street_name: str,
        city_name: str,
        lamas_candidates: list,
        adjacent_streets: list,
    ) -> str:
        """Build a bounded prompt from the top candidates and nearby streets."""
        osm_street_name = str(osm_street_name)[:120]
        city_name = str(city_name)[:120]
        adjacent_streets = [str(name)[:100] for name in adjacent_streets[:8]]
        prompt = f"""אתה מערכת GIS אוטומטית המתמחה בזיהוי רחובות בישראל.

המשימה: להציע מזהה (LAMAS ID) מתוך רשימת מועמדים; ההצעה אינה אישור התאמה.

**פרטי הרחוב:**
- עיר: {city_name}
- שם הרחוב ב-OSM: "{osm_street_name}"
- רחובות סמוכים (קונטקסט טופולוגי): {', '.join(adjacent_streets) if adjacent_streets else 'אין מידע'}

**מועמדי למ"ס המובילים (עד 8 מועמדים ועד 3 כינויים לכל מועמד):**
"""

        for i, candidate in enumerate(lamas_candidates[:8], 1):
            names = candidate.get("names") or candidate.get("aliases")
            if names:
                names_str = " | ".join(str(n)[:120] for n in names[:3])
            else:
                names_str = str(candidate.get("name", ""))[:120]
            score = candidate.get("score", 0.0)
            try:
                score_s = f"{float(score):.1f}"
            except (TypeError, ValueError):
                score_s = str(score)
            prompt += f"\n{i}. ID: {candidate['id']}, שמות: [{names_str}] (ציון fuzzy: {score_s})"

        prompt += """

**הוראות:**
1. השווה לשמות ולכינויים שסופקו ולציון ה-fuzzy שלהם.
2. קח בחשבון את הרחובות הסמוכים, אך אל תסיק מהם התאמה ללא ראיה בשם.
3. שמות עשויים להשתנות בגלל: כינויים, שמות חלקיים, איות, תארים, תרגום/תעתיק עברית↔ערבית.
4. אם אין מועמד מתאים — החזר null.

**פורמט התשובה (JSON בלבד):**
```json
{
  "lamas_id": "המזהה המספרי או null",
  "confidence": 0.0,
  "reasoning": "הסבר קצר בעברית"
}
```
"""
        return prompt

    def _clean_json_string(self, json_str: str) -> str:
        json_str = re.sub(r"//.*$", "", json_str, flags=re.MULTILINE)
        json_str = re.sub(r",(\s*[}\]])", r"\1", json_str)
        return json_str.strip()

    def parse_response(self, response_text: str) -> Tuple[Optional[str], float, str]:
        try:
            json_match = re.search(r"```json\s*(\{.*?\})\s*```", response_text, re.DOTALL)
            if not json_match:
                json_match = re.search(r"(\{.*?\})", response_text, re.DOTALL)

            if json_match:
                json_str = self._clean_json_string(json_match.group(1))
                try:
                    result = json.loads(json_str)
                    lamas_id = result.get("lamas_id")
                    confidence = float(result.get("confidence", 0.0))
                    reasoning = result.get("reasoning", "No reasoning provided")
                    if lamas_id is not None and str(lamas_id).lower() not in ("null", "none"):
                        lamas_id = str(lamas_id)
                    else:
                        lamas_id = None
                    return lamas_id, confidence, reasoning
                except json.JSONDecodeError:
                    pass

            id_match = re.search(r'"lamas_id"\s*:\s*"?(\d+)"?', response_text)
            if id_match:
                return id_match.group(1), 0.6, "Extracted ID from partial JSON"

            clean_text = response_text.strip()
            if re.match(r"^\d+$", clean_text):
                return clean_text, 0.5, "Response was just a number"

            return None, 0.0, "Could not parse response"
        except Exception as e:
            print(f"Error parsing AI response: {e}")
            return None, 0.0, f"Parse error: {str(e)}"

    def resolve_street(
        self,
        osm_id: str,
        osm_street_name: str,
        city_name: str,
        lamas_candidates: list,
        adjacent_streets: list,
    ) -> Dict[str, Any]:
        if not self.is_available():
            return {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": "Local AI model not available",
                "method": "local_ai_unavailable",
            }

        try:
            prompt = self.prepare_prompt(
                osm_street_name,
                city_name,
                lamas_candidates,
                adjacent_streets,
            )

            messages = [
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            ]

            text = self.processor.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True
            )
            inputs = self.processor(
                text=[text], images=None, return_tensors="pt", padding=True
            )
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.no_grad():
                generated_ids = self.model.generate(
                    **inputs, max_new_tokens=128, do_sample=False
                )

            generated_ids_trimmed = [
                out_ids[len(in_ids) :]
                for in_ids, out_ids in zip(inputs["input_ids"], generated_ids)
            ]

            generated_text = self.processor.batch_decode(
                generated_ids_trimmed,
                skip_special_tokens=True,
                clean_up_tokenization_spaces=False,
            )[0]

            lamas_id, confidence, reasoning = self.parse_response(generated_text)
            print(f"    [DEBUG] Local AI Raw Response: {generated_text[:200]}...")

            return {
                "lamas_id": lamas_id,
                "confidence": confidence,
                "reasoning": reasoning,
                "method": "local_ai",
                "raw_response": generated_text[:500],
            }
        except Exception as e:
            print(f"Error during local AI resolution for OSM ID {osm_id}: {e}")
            return {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": f"Error: {str(e)}",
                "method": "local_ai_error",
            }


def get_local_ai_resolution(
    osm_id: str,
    osm_gdf: gpd.GeoDataFrame,
    lamas_df: pd.DataFrame,
    candidates_row: pd.Series,
    adjacency_map: dict,
    resolver: LocalAIResolver,
) -> Dict[str, Any]:
    """Resolve a street using bounded candidate and adjacency context."""
    from normalization import build_lamas_aliases, select_ai_candidates

    osm_street = osm_gdf[osm_gdf["osm_id"] == osm_id]
    if osm_street.empty:
        return {
            "lamas_id": None,
            "confidence": 0.0,
            "reasoning": "OSM street not found",
            "method": "error",
        }

    osm_street = osm_street.iloc[0]
    osm_street_name = osm_street.get(
        "normalized_name", osm_street.get("osm_name", "Unknown")
    )
    city_name = osm_street.get("city", "Unknown")

    city_lamas = lamas_df
    if "city" in lamas_df.columns and city_name and city_name != "Unknown":
        city_mask = lamas_df["city"].astype(str) == str(city_name)
        if city_mask.any():
            city_lamas = lamas_df[city_mask]

    aliases_by_id = build_lamas_aliases(city_lamas)

    lamas_candidates = []
    candidate_details = candidates_row.get("candidate_details")
    if isinstance(candidate_details, list):
        lamas_candidates = select_ai_candidates(candidate_details, osm_street_name)
    elif pd.notna(candidates_row.get("all_candidates")):
        for line in str(candidates_row["all_candidates"]).split("\n"):
            id_match = re.search(r"ID:\s*(\d+)", line)
            names_bracket = re.search(r"Names:\s*\[([^\]]+)\]", line)
            name_match = re.search(r"Name:\s*['\"]([^'\"]+)['\"]", line)
            score_match = re.search(r"Score:\s*([\d.]+)", line)
            if not id_match:
                continue
            lid = id_match.group(1)
            if lid in aliases_by_id:
                names = aliases_by_id[lid]
            elif names_bracket:
                names = [n.strip() for n in names_bracket.group(1).split("|") if n.strip()]
            elif name_match:
                names = [name_match.group(1)]
            else:
                names = []
            lamas_candidates.append(
                {
                    "id": lid,
                    "name": names[0] if names else "",
                    "names": names,
                    "score": float(score_match.group(1)) if score_match else 0.0,
                }
            )
        lamas_candidates = select_ai_candidates(lamas_candidates, osm_street_name)

    adjacent_ids = adjacency_map.get(osm_id, [])
    adjacent_streets = []
    for adj_id in adjacent_ids[:10]:
        adj_street = osm_gdf[osm_gdf["osm_id"] == adj_id]
        if not adj_street.empty:
            adj_name = adj_street.iloc[0].get(
                "normalized_name", adj_street.iloc[0].get("osm_name")
            )
            if adj_name and pd.notna(adj_name):
                adjacent_streets.append(str(adj_name))

    return resolver.resolve_street(
        osm_id,
        osm_street_name,
        city_name,
        lamas_candidates,
        adjacent_streets[:8],
    )


if __name__ == "__main__":
    print("Local AI Resolver module loaded successfully")
    print(f"Transformers available: {TRANSFORMERS_AVAILABLE}")
    if TRANSFORMERS_AVAILABLE:
        print("Attempting to initialize model...")
        resolver = LocalAIResolver()
        print(f"Model available: {resolver.is_available()}")
