
import json
import os
import glob

def inspect_data():
    if not os.path.exists('street_data.json'):
        print("street_data.json not found")
        return

    with open('street_data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Total entries in JSON: {len(data)}")
    
    settlements = {}
    
    for entry in data:
        s_raw = entry.get('settlement', 'Unknown')
        # Extract basic name (simplistic)
        s_name = s_raw.split(',')[0].strip() if ',' in s_raw else s_raw
        
        if s_name not in settlements:
            settlements[s_name] = {'total': 0, 'scores': [], 'raw_settlement': s_raw}
        
        settlements[s_name]['total'] += 1
        if 'score' in entry and entry['score'] is not None:
             settlements[s_name]['scores'].append(entry['score'])

    print(f"Total unique settlements in JSON: {len(settlements)}")
    
    # Check coverage against HTML files
    html_files = glob.glob('HTML/*_roads.html')
    html_settlements = set()
    for h in html_files:
        # filename is likely Name_roads.html
        basename = os.path.basename(h)
        name_part = basename.replace('_roads.html', '').replace('_', ' ')
        html_settlements.add(name_part)

    print(f"Total HTML files: {len(html_files)}")
    
    # Compare
    json_names = set(settlements.keys())
    # Note: JSON names might have spaces, HTML filenames use underscores which I mapped to spaces.
    # But there might be other diffs (e.g. quotes).
    
    common = 0
    for h_name in html_settlements:
        if h_name in json_names:
            common += 1
        else:
            # Try fuzzy or exact match?
            pass
            
    print(f"Settlements in both JSON and HTML (approx): {common}")
    
    # Sample district extraction
    print("\nSample District Extraction:")
    for name, info in list(settlements.items())[:5]:
        raw = info['raw_settlement']
        parts = raw.split(',')
        district = "Unknown"
        for p in parts:
            if 'מחוז' in p:
                district = p.strip()
        print(f"  {name}: {district} (Total Streets: {info['total']})")

if __name__ == "__main__":
    inspect_data()
