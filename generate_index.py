import os
import json
from collections import defaultdict
import re

def get_district_from_filename(filename):
    parts = filename.replace('.html', '').split('__')
    if len(parts) > 2:
        for part in parts:
            if 'מחוז' in part:
                return part.replace('_', ' ')
    return "Unknown"

def get_lamas_match_percentage(settlement_name):
    safe_settlement_name = re.sub(r'[^0-9A-Za-z_\-\u0590-\u05FF]', '_', settlement_name)
    json_path = os.path.join('data', f'diagnostic_summary_{safe_settlement_name}.json')

    if not os.path.exists(json_path):
        return "N/A"

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            diagnostics = json.load(f)
        
        unmatched_percentage_str = diagnostics.get("unmatched_lamas_percentage", "0%")
        unmatched_percentage = float(unmatched_percentage_str.strip('%'))
        matched_percentage = 100 - unmatched_percentage
        return f"{matched_percentage:.1f}%"

    except Exception as e:
        print(f"Could not process {json_path}: {e}")
        return "Error"

def main():
    html_dir = 'HTML/' # Still reading from HTML directory, but output will be text
    
    # --- Data for the page ---
    stats = {
        "Total street entries": "26,772",
        "Unique street names": "1,044",
        "Unique settlements": "25",
        "Streets with a match": "24,350",
        "Streets with no match": "2,422",
        "Average match score": "99.56"
    }

    # --- Get file list and group them ---
    if not os.path.exists(html_dir):
        print(f"Directory not found: {html_dir}")
        return

    file_list = [f for f in os.listdir(html_dir) if f.endswith('.html')]
    
    grouped_files = defaultdict(list)
    for filename in file_list:
        district = get_district_from_filename(filename)
        settlement_name = filename.split('__')[0].replace('_', ' ')
        match_percentage = get_lamas_match_percentage(settlement_name)
        grouped_files[district].append({
            'name': settlement_name,
            'path': os.path.join(html_dir, filename),
            'match_percentage': match_percentage
        })

    # --- Generate Text Report ---
    report_content = "Statistics\n"
    for label, value in stats.items():
        report_content += f"{label}\n{value}\n"

    report_content += "\nSettlement Reports by District\n"
    
    for district in sorted(grouped_files.keys()):
        report_content += f"{district}\n"
        for settlement in sorted(grouped_files[district], key=lambda x: x['name']):
            report_content += f'{settlement["name"]} ({settlement["match_percentage"]})\n'

    with open('index.txt', 'w', encoding='utf-8') as f:
        f.write(report_content)

    print("index.txt has been generated.")

if __name__ == '__main__':
    main()
