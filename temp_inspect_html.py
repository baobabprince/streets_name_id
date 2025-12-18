
import re
import os

def analyze_html(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # helper to print context around matches
    def print_matches(pattern, name):
        matches = re.findall(pattern, content)
        print(f"Found {len(matches)} {name}")
        if matches:
            print(f"Sample: {matches[:3]}")
            
    print(f"--- Analyzing {os.path.basename(filepath)} ---")
    
    # 1. Look for District
    # Pattern: "מחוז" usually followed by Hebrew words
    # Maybe inside a <p> or <div> or title
    print_matches(r'מחוז\s+[\u0590-\u05FF]+', "District mentions")

    # 2. Look for Stats
    # Maybe "X מתוך Y" (X out of Y)
    print_matches(r'\d+\s+מתוך\s+\d+', "X out of Y patterns")
    
    # Look for "matched" or "unmatched" counts
    print_matches(r'unmatched', "'unmatched' string")
    
    # Count list items in unmatched list?
    # <div class="unmatched-list"> ... <ul><li>...</li></ul>
    unmatched_div = re.search(r'<div class="unmatched-list">.*?</div>', content, re.DOTALL)
    if unmatched_div:
        unmatched_count = len(re.findall(r'<li', unmatched_div.group(0)))
        print(f"Counted {unmatched_count} unmatched items in list")
    else:
        print("No unmatched-list div found")
        
    # Count matched?
    # Matched streets might be rendered as SVG paths?
    # <path ... class="street-path" ...>
    matched_count = len(re.findall(r'class="street-path"', content))
    print(f"Counted {matched_count} 'street-path' elements")

if __name__ == "__main__":
    # Check a few
    files = ['HTML/אודים_roads.html', 'HTML/באר_יעקב_roads.html'] 
    for f in files:
        if os.path.exists(f):
            analyze_html(f)
        else:
            print(f"File {f} not found")
