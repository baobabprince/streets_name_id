import os
import re
import json
import glob
from collections import defaultdict

# --- Configuration ---
HTML_DIR = 'HTML'
OUTPUT_FILE = 'index.html'
STREET_DATA_FILE = 'street_data.json'

def load_district_map():
    """
    Load settlement -> district mapping from street_data.json.
    """
    mapping = {}
    if os.path.exists(STREET_DATA_FILE):
        try:
            with open(STREET_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for entry in data:
                    settlement_str = entry.get('settlement', '')
                    if not settlement_str:
                        continue
                    
                    # Extract name: "Name, District, ..."
                    parts = settlement_str.split(',')
                    name = parts[0].strip()
                    district = "Unknown District"
                    
                    for p in parts:
                        if 'מחוז' in p:
                            district = p.strip()
                            break
                    
                    # Add to mapping (prefer identified district over existing unknown)
                    if name not in mapping or mapping[name] == "Unknown District":
                        mapping[name] = district
        except Exception as e:
            print(f"Warning: Could not read {STREET_DATA_FILE}: {e}")
    return mapping

def parse_html_report(filepath):
    """
    Parse an HTML report to extract:
    - Settlement Name (from filename)
    - Matched Count (Unique street names)
    - Unmatched Count
    """
    filename = os.path.basename(filepath)
    # Filename format: Name_roads.html
    # Some names have underscores in them originally, but we assume the last part is _roads.html
    name_part = filename.replace('_roads.html', '')
    # Revert underscores to spaces for display, but keep in mind some keys might be different
    display_name = name_part.replace('_', ' ') 
    
    matched_names = set()
    unmatched_count = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 1. Extract Matched Names from data-tooltip
        # Format: data-tooltip="Name#10;..."
        # Regex to find data-tooltip value
        tooltips = re.findall(r'data-tooltip="([^"]+)"', content)
        for t in tooltips:
            # Extract name before first '#' or ';' or just take the whole thing if simple
            # Based on inspection: "הצאלון#10;..."
            street_name = t.split('#')[0].strip()
            if street_name:
                matched_names.add(street_name)
                
        # 2. Extract Unmatched Count from <li>
        # Start search from <div class="unmatched-list">
        unmatched_div_match = re.search(r'<div class="unmatched-list">(.*?)</div>', content, re.DOTALL)
        if unmatched_div_match:
            list_content = unmatched_div_match.group(1)
            unmatched_count = len(re.findall(r'<li', list_content))
            
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return None

    return {
        'name': display_name,
        'filename': filename,
        'matched': len(matched_names),
        'unmatched': unmatched_count,
        'total': len(matched_names) + unmatched_count
    }

def generate_index_html(reports, district_map):
    """
    Generate the HTML index page.
    """
    # Group by District
    grouped = defaultdict(list)
    
    # Statistics
    total_settlements = 0
    total_streets = 0
    total_matched = 0
    
    for r in reports:
        # Determine District
        # Try exact match or fuzzy match
        district = district_map.get(r['name'], "Unknown District")
        
        # If not found, try to strip common prefixes/suffixes if needed? 
        # For now, stick to direct lookup.
        
        grouped[district].append(r)
        
        total_settlements += 1
        total_streets += r['total']
        total_matched += r['matched']

    overall_accuracy = (total_matched / total_streets * 100) if total_streets > 0 else 0
    
    # HTML Template
    html = f"""<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Street Matching Reports Index</title>
    <style>
        :root {{
            --primary: #3b82f6;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text: #1e293b;
            --text-light: #64748b;
            --success: #22c55e;
            --warning: #f59e0b;
            --danger: #ef4444;
            --border: #e2e8f0;
        }}
        
        body {{
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            background-color: var(--bg);
            color: var(--text);
            margin: 0;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        header {{
            text-align: center;
            margin-bottom: 40px;
            padding: 40px 0;
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border-radius: 16px;
            color: white;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        }}
        
        h1 {{ margin: 0; font-size: 2.5rem; letter-spacing: -0.05em; }}
        .subtitle {{ color: #94a3b8; margin-top: 10px; font-size: 1.1rem; }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .stat-card {{
            background: var(--card-bg);
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            text-align: center;
            border: 1px solid var(--border);
        }}
        
        .stat-value {{ font-size: 2rem; font-weight: bold; color: var(--primary); }}
        .stat-label {{ color: var(--text-light); font-size: 0.9rem; }}
        
        .filters {{
            display: flex;
            gap: 10px;
            margin-bottom: 30px;
            flex-wrap: wrap;
            justify-content: center;
        }}
        
        .search-box {{
            padding: 10px 20px;
            border-radius: 999px;
            border: 1px solid var(--border);
            width: 300px;
            font-size: 1rem;
            outline: none;
        }}
        
        .district-section {{ margin-bottom: 40px; }}
        .district-title {{ 
            font-size: 1.5rem; 
            margin-bottom: 20px; 
            padding-right: 15px; 
            border-right: 4px solid var(--primary);
            color: var(--text);
        }}
        
        .reports-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
        }}
        
        .report-card {{
            background: var(--card-bg);
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            transition: transform 0.2s, box-shadow 0.2s;
            border: 1px solid var(--border);
            text-decoration: none;
            color: inherit;
            display: block;
            position: relative;
            overflow: hidden;
        }}
        
        .report-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
            border-color: var(--primary);
        }}
        
        .card-header {{ display: flex; justify-content: space-between; align-items: start; margin-bottom: 15px; }}
        .settlement-name {{ font-size: 1.2rem; font-weight: bold; }}
        .match-badge {{ 
            padding: 4px 12px; 
            border-radius: 99px; 
            font-size: 0.85rem; 
            font-weight: bold;
        }}
        
        .progress-container {{
            height: 8px;
            background-color: #f1f5f9;
            border-radius: 4px;
            overflow: hidden;
            margin: 15px 0;
        }}
        
        .progress-bar {{
            height: 100%;
            border-radius: 4px;
            transition: width 1s ease-in-out;
        }}
        
        .card-stats {{
            display: flex;
            justify-content: space-between;
            font-size: 0.9rem;
            color: var(--text-light);
        }}
        
        /* Utility classes for colors based on percentage */
        .color-high {{ background-color: var(--success); color: white; }}
        .color-med {{ background-color: var(--warning); color: white; }}
        .color-low {{ background-color: var(--danger); color: white; }}
        
        .bg-high {{ background-color: var(--success); }}
        .bg-med {{ background-color: var(--warning); }}
        .bg-low {{ background-color: var(--danger); }}
        
        .text-high {{ color: var(--success); }}
        .text-med {{ color: var(--warning); }}
        .text-low {{ color: var(--danger); }}

    </style>
</head>
<body>

<div class="container">
    <header>
        <h1>Street Matching Report Index</h1>
        <div class="subtitle">Visualizing matches between LAMAS and OSM data</div>
    </header>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{total_settlements}</div>
            <div class="stat-label">Settlements</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{total_streets:,}</div>
            <div class="stat-label">Total Streets</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{overall_accuracy:.1f}%</div>
            <div class="stat-label">Overall Match Rate</div>
        </div>
    </div>

    <div class="filters">
        <input type="text" class="search-box" id="searchInput" placeholder="Search settlement...">
    </div>

    <div id="grid-container">
"""
    
    # Sort districts (Unknown last)
    sorted_districts = sorted(grouped.keys())
    if "Unknown District" in sorted_districts:
        sorted_districts.remove("Unknown District")
        sorted_districts.append("Unknown District")
        
    for district in sorted_districts:
        items = grouped[district]
        # Sort items by name
        items.sort(key=lambda x: x['name'])
        
        html += f'<div class="district-section" data-district="{district}">'
        html += f'<div class="district-title">{district} ({len(items)})</div>'
        html += '<div class="reports-grid">'
        
        for r in items:
            total = r['total']
            matched = r['matched']
            percent = (matched / total * 100) if total > 0 else 0
            
            # Determine Color
            if percent >= 95:
                color_cls = "high"
            elif percent >= 80:
                color_cls = "med"
            else:
                color_cls = "low"
            
            html += f"""
            <a href="{HTML_DIR}/{r['filename']}" class="report-card" data-name="{r['name']}">
                <div class="card-header">
                    <div class="settlement-name">{r['name']}</div>
                    <div class="match-badge color-{color_cls}">{percent:.1f}%</div>
                </div>
                
                <div class="progress-container">
                    <div class="progress-bar bg-{color_cls}" style="width: {percent}%"></div>
                </div>
                
                <div class="card-stats">
                    <span>Matched: {matched}</span>
                    <span>Total: {total}</span>
                </div>
            </a>
            """
        
        html += '</div></div>' # Close grid and section

    html += """
    </div> <!-- grid-container -->
</div>

<script>
    const searchInput = document.getElementById('searchInput');
    const cards = document.querySelectorAll('.report-card');
    const sections = document.querySelectorAll('.district-section');

    searchInput.addEventListener('input', (e) => {
        const term = e.target.value.toLowerCase();
        
        cards.forEach(card => {
            const name = card.getAttribute('data-name').toLowerCase();
            const visible = name.includes(term);
            card.style.display = visible ? 'block' : 'none';
        });

        // Hide empty sections
        sections.forEach(section => {
            const visibleCards = section.querySelectorAll('.report-card[style="display: block"]');
            // Note: If style isn't set yet (initial load), it's visible. 
            // Better to check offsetParent or computed style if needed, but simple display check works usually.
            // Actually, we need to check if ANY card is visible.
            
            let hasVisible = false;
            section.querySelectorAll('.report-card').forEach(c => {
                 if (c.style.display !== 'none') hasVisible = true;
            });
            
            section.style.display = hasVisible ? 'block' : 'none';
        });
    });
</script>

</body>
</html>
"""

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"Generated {OUTPUT_FILE} with {total_settlements} settlements.")

def main():
    print("Loading district map...")
    district_map = load_district_map()
    
    print("Scanning HTML reports...")
    files = glob.glob(os.path.join(HTML_DIR, '*.html'))
    reports = []
    
    for i, f in enumerate(files):
        if i % 50 == 0:
            print(f"Processed {i}/{len(files)}...")
        r = parse_html_report(f)
        if r:
            reports.append(r)
            
    print("Generating Index...")
    generate_index_html(reports, district_map)

if __name__ == "__main__":
    main()
