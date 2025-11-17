# local_ai_matching.py
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
import re

# --- Global cache for the model and tokenizer ---
model = None
tokenizer = None
model_name = "google/gemma-2b-it"

def load_model():
    """Loads the Gemma model and tokenizer into the global cache."""
    global model, tokenizer
    if model is None or tokenizer is None:
        print(f"--- Loading local AI model: {model_name} ---")
        try:
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            model = AutoModelForCausalLM.from_pretrained(
                model_name,
                device_map="auto",  # Automatically use GPU if available
                torch_dtype=torch.bfloat16
            )
            print("--- Model loaded successfully. ---")
        except Exception as e:
            print(f"--- FATAL: Could not load local AI model. AI resolution will be disabled. Error: {e} ---")
            # Set to None to prevent retrying on every call
            model, tokenizer = None, None

def get_local_ai_resolution(prompt: str, osm_id: str) -> tuple[str, int]:
    """
    Uses a local language model to resolve ambiguous street name matches.
    Returns a tuple of (LAMAS_ID, confidence_score).
    """
    global model, tokenizer

    # Load the model on the first call
    if model is None or tokenizer is None:
        load_model()

    # If loading failed, skip AI resolution
    if model is None or tokenizer is None:
        return "None", 0

    print(f" -> Consulting Local AI for OSM ID: {osm_id}...")

    # Create the prompt with the required chat template
    chat = [
        {"role": "user", "content": prompt},
    ]
    formatted_prompt = tokenizer.apply_chat_template(chat, tokenize=False, add_generation_prompt=True)

    # Generate the response
    try:
        inputs = tokenizer.encode(formatted_prompt, add_special_tokens=False, return_tensors="pt")
        with torch.no_grad():
            outputs = model.generate(
                input_ids=inputs.to(model.device),
                max_new_tokens=25,
                do_sample=False,
            )

        response_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        model_response = response_text[len(formatted_prompt):].strip()

        # Extract ID and score using regex
        match = re.search(r"ID:\s*(\d+),\s*Score:\s*(\d+)", model_response)
        if match:
            lamas_id = match.group(1)
            score = int(match.group(2))
            return lamas_id, score
        else:
            # Fallback for simple ID response
            clean_text = ''.join(filter(str.isdigit, model_response))
            if clean_text:
                return clean_text, 75  # Default score for simple match
            return "None", 0

    except Exception as e:
        print(f"--- Error during local AI inference for {osm_id}: {e} ---")
        return "None", 0
