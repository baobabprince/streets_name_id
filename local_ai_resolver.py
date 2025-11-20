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

# Try to import the required libraries for local AI

try:
    from transformers import Qwen2VLForConditionalGeneration, AutoProcessor
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    print("Warning: transformers or torch not available. Local AI will be disabled.")


class LocalAIResolver:
    """
    Handles local AI resolution using qwen3-VL-8b model.
    """
    
    def __init__(self, model_name: str = "Qwen/Qwen3-VL-8B", device: str = "auto"):
        """
        Initialize the local AI resolver.
        
        Args:
            model_name: Hugging Face model name/path
            device: Device to run the model on ('cuda', 'cpu', or 'auto')
        """
        self.model = None
        self.processor = None
        self.device = device
        self.model_name = model_name
        self.initialized = False
        
        if TRANSFORMERS_AVAILABLE:
            try:
                self._initialize_model()
            except Exception as e:
                print(f"Failed to initialize local AI model: {e}")
                print("Local AI resolution will be disabled. Falling back to Gemini API.")
    
    def _initialize_model(self):
        """Load the model and processor."""
        print(f"Loading local AI model: {self.model_name}...")
        
        # Determine device
        if self.device == "auto":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Load processor
        self.processor = AutoProcessor.from_pretrained(self.model_name)
        
        # Load model with appropriate dtype based on device
        if self.device == "cuda":
            # Use bfloat16 for better performance on GPU
            self.model = Qwen2VLForConditionalGeneration.from_pretrained(
                self.model_name,
                torch_dtype=torch.bfloat16,
                device_map="auto"
            )
        else:
            # Use float32 for CPU
            self.model = Qwen2VLForConditionalGeneration.from_pretrained(
                self.model_name,
                torch_dtype=torch.float32
            )
            self.model = self.model.to(self.device)
        
        self.initialized = True
        print(f"Model loaded successfully on {self.device}")
    
    def is_available(self) -> bool:
        """Check if the local AI model is available and initialized."""
        return TRANSFORMERS_AVAILABLE and self.initialized and self.model is not None
    
    def prepare_prompt(
        self,
        osm_street_name: str,
        city_name: str,
        lamas_candidates: list,
        adjacent_streets: list
    ) -> str:
        """
        Prepare the text prompt for the AI model.
        
        Args:
            osm_street_name: The street name from OSM
            city_name: The city name
            lamas_candidates: List of LAMAS candidate dictionaries with 'id', 'name', 'score'
            adjacent_streets: List of adjacent street names
            
        Returns:
            Formatted prompt string
        """
        prompt = f"""אתה מערכת GIS אוטומטית המתמחה בזיהוי רחובות בישראל.

המשימה: למצוא את המזהה (LAMAS ID) הנכון של רחוב מתוך רשימת מועמדים.

**פרטי הרחוב:**
- עיר: {city_name}
- שם הרחוב ב-OSM: "{osm_street_name}"
- רחובות סמוכים (קונטקסט טופולוגי): {', '.join(adjacent_streets) if adjacent_streets else 'אין מידע'}

**מועמדים מבסיס נתוני למ"ס:**
"""
        
        for i, candidate in enumerate(lamas_candidates, 1):
            prompt += f"\n{i}. ID: {candidate['id']}, שם: \"{candidate['name']}\" (ציון התאמה: {candidate['score']:.1f})"
        
        prompt += """

**הוראות:**
1. השווה את שם הרחוב ב-OSM לשמות המועמדים מלמ"ס.
2. קח בחשבון את הרחובות הסמוכים - הם עוזרים להבין את ההקשר (למשל, אם רחוב סמוך נקרא ע"ש אישיות קשורה).
3. **חשוב מאוד:** שמות עשויים להיות שונים בגלל:
    - כינויים (למשל "בן גוריון" לעומת "דוד בן גוריון")
    - שמות חלקיים (למשל "הנביאים" לעומת "שמואל הנביא")
    - שינויי איות או תרגומים
    - תוספת/השמטה של תואר (למשל "הרב", "דוקטור")
    - **במקרה של "בן עמר" מול "תאודור בן עמר" - זוהי התאמה טובה!**

**פורמט התשובה (JSON בלבד):**
```json
{
  "lamas_id": "המזהה המספרי של הרחוב המתאים ביותר או null אם אין התאמה",
  "confidence": "ציון ביטחון בין 0.0 ל-1.0",
  "reasoning": "הסבר קצר בעברית למה בחרת ברחוב זה או למה אין התאמה"
}
```

אם אתה מזהה התאמה סבירה (מעל 0.5), אנא החזר את ה-ID. אל תהסס להתאים אם השם דומה מאוד אך לא זהה.
אם אין שום התאמה הגיונית, החזר lamas_id: null.
"""
        
        return prompt
    
    def _clean_json_string(self, json_str: str) -> str:
        """Clean common JSON formatting issues from LLM output."""
        # Remove comments // ...
        json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
        # Fix trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        return json_str.strip()

    def parse_response(self, response_text: str) -> Tuple[Optional[str], float, str]:
        """
        Parse the AI model's response to extract LAMAS ID and confidence.
        
        Args:
            response_text: Raw response from the model
            
        Returns:
            Tuple of (lamas_id, confidence, reasoning)
        """
        try:
            # Try to extract JSON from the response
            # Look for JSON block in markdown code fence or plain JSON
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
            if not json_match:
                json_match = re.search(r'(\{.*?\})', response_text, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(1)
                json_str = self._clean_json_string(json_str)
                
                try:
                    result = json.loads(json_str)
                    
                    lamas_id = result.get('lamas_id')
                    confidence = float(result.get('confidence', 0.0))
                    reasoning = result.get('reasoning', 'No reasoning provided')
                    
                    # Convert lamas_id to string if it's not None
                    if lamas_id is not None and str(lamas_id).lower() != 'null' and str(lamas_id).lower() != 'none':
                        lamas_id = str(lamas_id)
                    else:
                        lamas_id = None
                    
                    return lamas_id, confidence, reasoning
                except json.JSONDecodeError:
                    pass # Fall through to other methods
            
            # Fallback: try to extract just a number if it looks like an ID (3-4 digits)
            # But be careful not to pick up numbers from the reasoning or prompt
            # Look for pattern "lamas_id": 123
            id_match = re.search(r'"lamas_id"\s*:\s*"?(\d+)"?', response_text)
            if id_match:
                return id_match.group(1), 0.6, "Extracted ID from partial JSON"

            # Last resort: if the response is VERY short and is just a number
            clean_text = response_text.strip()
            if re.match(r'^\d+$', clean_text):
                 return clean_text, 0.5, "Response was just a number"
            
            # No match found
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
        adjacent_streets: list
    ) -> Dict[str, Any]:
        """
        Use the local AI model to resolve a street match.
        
        Args:
            osm_id: OSM ID of the street
            osm_street_name: Street name from OSM
            city_name: City name
            lamas_candidates: List of candidate dictionaries
            adjacent_streets: List of adjacent street names
            
        Returns:
            Dictionary with 'lamas_id', 'confidence', 'reasoning', 'method'
        """
        if not self.is_available():
            return {
                'lamas_id': None,
                'confidence': 0.0,
                'reasoning': 'Local AI model not available',
                'method': 'local_ai_unavailable'
            }
        
        try:
            # Prepare the prompt
            prompt = self.prepare_prompt(
                osm_street_name,
                city_name,
                lamas_candidates,
                adjacent_streets
            )
            
            # Prepare inputs for the model (Text only)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt}
                    ]
                }
            ]
            
            # Process inputs
            text = self.processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            inputs = self.processor(
                text=[text],
                images=None, # No images
                return_tensors="pt",
                padding=True
            )
            
            # Move inputs to device
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Inference: Generation of the output
            with torch.no_grad():
                generated_ids = self.model.generate(
                    **inputs,
                    max_new_tokens=128,
                    do_sample=False  # Deterministic for consistency
                )
            
            generated_ids_trimmed = [
                out_ids[len(in_ids) :] for in_ids, out_ids in zip(inputs['input_ids'], generated_ids)
            ]
            
            # Decode response
            generated_text = self.processor.batch_decode(
                generated_ids_trimmed, # Decode trimmed IDs
                skip_special_tokens=True,
                clean_up_tokenization_spaces=False
            )[0]
            
            # Parse the response
            lamas_id, confidence, reasoning = self.parse_response(generated_text)
            
            print(f"    [DEBUG] Local AI Raw Response: {generated_text[:200]}...") # Log to console
            
            return {
                'lamas_id': lamas_id,
                'confidence': confidence,
                'reasoning': reasoning,
                'method': 'local_ai',
                'raw_response': generated_text[:500]
            }
            
        except Exception as e:
            print(f"Error during local AI resolution for OSM ID {osm_id}: {e}")
            return {
                'lamas_id': None,
                'confidence': 0.0,
                'reasoning': f'Error: {str(e)}',
                'method': 'local_ai_error'
            }


def get_local_ai_resolution(
    osm_id: str,
    osm_gdf: gpd.GeoDataFrame,
    lamas_df: pd.DataFrame,
    candidates_row: pd.Series,
    adjacency_map: dict,
    resolver: LocalAIResolver
) -> Dict[str, Any]:
    """
    Main function to get local AI resolution for a street.
    
    Args:
        osm_id: OSM ID of the street
        osm_gdf: GeoDataFrame with all OSM streets
        lamas_df: DataFrame with all LAMAS data
        candidates_row: Row from candidates DataFrame with LAMAS candidates
        adjacency_map: Dictionary mapping OSM IDs to adjacent IDs
        resolver: Initialized LocalAIResolver instance
        
    Returns:
        Dictionary with resolution results
    """
    # Get OSM street info
    osm_street = osm_gdf[osm_gdf['osm_id'] == osm_id]
    if osm_street.empty:
        return {'lamas_id': None, 'confidence': 0.0, 'reasoning': 'OSM street not found', 'method': 'error'}
    
    osm_street = osm_street.iloc[0]
    osm_street_name = osm_street.get('normalized_name', osm_street.get('osm_name', 'Unknown'))
    city_name = osm_street.get('city', 'Unknown')
    
    # Parse LAMAS candidates from the candidates_row
    lamas_candidates = []
    if pd.notna(candidates_row.get('all_candidates')):
        # Parse the candidate string
        candidate_lines = str(candidates_row['all_candidates']).split('\n')
        for line in candidate_lines:
            # Format: "ID: 123, Name: 'Street Name' (Score: 85.5)"
            id_match = re.search(r'ID:\s*(\d+)', line)
            name_match = re.search(r"Name:\s*['\"]([^'\"]+)['\"]", line)
            score_match = re.search(r'Score:\s*([\d.]+)', line)
            
            if id_match and name_match:
                lamas_candidates.append({
                    'id': id_match.group(1),
                    'name': name_match.group(1),
                    'score': float(score_match.group(1)) if score_match else 0.0
                })
    
    # Get adjacent street names
    adjacent_ids = adjacency_map.get(osm_id, [])
    adjacent_streets = []
    for adj_id in adjacent_ids[:10]:  # Limit to 10 for brevity
        adj_street = osm_gdf[osm_gdf['osm_id'] == adj_id]
        if not adj_street.empty:
            adj_name = adj_street.iloc[0].get('normalized_name', adj_street.iloc[0].get('osm_name'))
            if adj_name and pd.notna(adj_name):
                adjacent_streets.append(str(adj_name))
    
    # Call the AI resolver (without map image)
    result = resolver.resolve_street(
        osm_id,
        osm_street_name,
        city_name,
        lamas_candidates,
        adjacent_streets
    )
    
    return result


if __name__ == "__main__":
    # Test the local AI resolver
    print("Local AI Resolver module loaded successfully")
    print(f"Transformers available: {TRANSFORMERS_AVAILABLE}")
    
    if TRANSFORMERS_AVAILABLE:
        print("Attempting to initialize model...")
        resolver = LocalAIResolver()
        print(f"Model available: {resolver.is_available()}")
