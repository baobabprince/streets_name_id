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
    safe_settlement_name = re.sub(r'[^0-9A-Za-z_\\-\\u0590-\\u05FF]', '_', settlement_name)
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
    html_dir = 'HTML/'
    
    # --- Data for the page ---
    project_purpose = """
    <p>This project aims to analyze and visualize street data for various settlements in Israel. It matches street names from geographic data sources (like OpenStreetMap) against a central database. The primary goal is to identify discrepancies, assess the quality of the data, and provide a visual representation of the matching results for each settlement.</p>
    <p>The logic involves processing street data for each settlement, attempting to match each street with a known record. A matching score is calculated, and the results are color-coded in the generated SVG maps. This index provides a central hub to access the visual reports and understand the overall data quality.</p>
    """
    
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

    # --- Generate HTML ---
    
    html_content = f"""
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Street Name Matching Project</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 0;
            background-color: #f4f4f9;
            color: #333;
        }}
        .container {{
            max-width: 960px;
            margin: 20px auto;
            padding: 20px;
            background-color: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        h1, h2, h3 {{
            color: #0056b3;
        }}
        h1 {{
            text-align: center;
            border-bottom: 2px solid #0056b3;
            padding-bottom: 10px;
        }}
        h2 {{
            border-bottom: 1px solid #ccc;
            padding-bottom: 5px;
        }}
        .section {{
            margin-bottom: 2em;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        .stat-item {{
            background-color: #eef7ff;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
            border-left: 5px solid #0056b3;
        }}
        .stat-item .label {{
            font-weight: bold;
            font-size: 1.1em;
        }}
        .stat-item .value {{
            font-size: 1.5em;
            color: #0056b3;
        }}
        .district-group {{
            margin-bottom: 1.5em;
        }}
        .settlement-list {{
            columns: 3;
            list-style: none;
            padding-right: 0;
        }}
        .settlement-list li {{
            margin-bottom: 8px;
            display: flex;
            justify-content: space-between;
        }}
        .settlement-list a {{
            text-decoration: none;
            color: #007bff;
            transition: color 0.2s;
        }}
        .settlement-list a:hover {{
            color: #0056b3;
            text-decoration: underline;
        }}
        .percentage {{
            color: #555;
            font-size: 0.9em;
            padding-right: 10px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Project for Matching and Verifying Street Names</h1>

        <div class="section">
            <h2>Project Purpose and Logic</h2>
            {project_purpose}
        </div>

        <div class="section">
            <h2>Main Statistics</h2>
            <div class="stats-grid">
    """
    for label, value in stats.items():
        html_content += f"""
                <div class="stat-item">
                    <div class="label">{{label}}</div>
                    <div class="value">{{value}}</div>
                </div>
        """
    html_content += """
            </div>
        </div>

        <div class="section">
            <h2>Settlement Reports by District</h2>
    """
    
    for district in sorted(grouped_files.keys()):
        html_content += f"""
            <div class="district-group">
                <h3>{{district}}</h3>
                <ul class="settlement-list">
        """
        for settlement in sorted(grouped_files[district], key=lambda x: x['name']):
            html_content += f'<li><a href="{{settlement[\'path\']}}">{{settlement["name"]}}</a> <span class="percentage">{{settlement["match_percentage"]}}</span></li>'
        html_content += """
                </ul>
            </div>
        """

    html_content += """
        </div>
    </div>
</body>
</html>
    """

    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html_content)

    print("index.html has been generated.")

if __name__ == '__main__':
    main()