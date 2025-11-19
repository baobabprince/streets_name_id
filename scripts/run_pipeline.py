# scripts/run_pipeline.py
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.pipeline import run_pipeline

if __name__ == "__main__":
    place_arg = None
    force = False
    no_ai = False
    if len(sys.argv) > 1:
        place_arg = sys.argv[1]
    if "--refresh" in sys.argv:
        force = True
    if "--no-ai" in sys.argv:
        no_ai = True

    run_pipeline(place=place_arg, force_refresh=force, use_ai=(not no_ai))
