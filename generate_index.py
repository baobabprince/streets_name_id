import os
import pandas as pd
import glob
import json
from pathlib import Path

# Configuration
DATA_DIR = 'data'
HTML_DIR = 'HTML'
OUTPUT_FILE = 'index.html'

def get_vital_statistics(csv_path):
    """
    Parses a diagnostic report CSV and calculates vital statistics.
    Returns a dictionary of stats.
    """
    try:
        df = pd.read_csv(csv_path)
        
        # Ensure we're working with strings for IDs and names
        if 'normalized_name' not in df.columns:
            return None
            
        # Total unique normalized streets
        total_streets = df['normalized_name'].dropna().nunique()
        
        # Matched streets (those with a final_LAMAS_id)
        # Check if column exists first
        if 'final_LAMAS_id' not in df.columns:
            matched_count = 0
        else:
            # Filter for valid IDs (not NaN, not 'None', not empty)
            valid_ids = df['final_LAMAS_id'].dropna().astype(str)
            valid_ids = valid_ids[~valid_ids.isin(['None', 'nan', ''])]
            
            # Get the subset of the dataframe with valid matches
            matched_df = df[df['final_LAMAS_id'].astype(str).isin(valid_ids)]
            matched_count = matched_df['normalized_name'].nunique()

        match_percentage = (matched_count / total_streets * 100) if total_streets > 0 else 0
        
        # Determine status tiers
        if match_percentage >= 95:
            tier = 'Excellent'
            color_class = 'status-excellent'
        elif match_percentage >= 80:
            tier = 'Good'
            color_class = 'status-good'
        elif match_percentage >= 50:
            tier = 'Fair'
            color_class = 'status-fair'
        else:
            tier = 'Poor'
            color_class = 'status-poor'

        return {
            'total_streets': total_streets,
            'matched_count': matched_count,
            'match_percentage': match_percentage,
            'tier': tier,
            'color_class': color_class
        }
    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return None

def generate_index_html():
    print("Generating index.html...")
    
    # Find all diagnostic reports
    report_files = glob.glob(os.path.join(DATA_DIR, 'diagnostic_report_*.csv'))
    
    settlements_data = []
    
    # Global Stats
    total_processed_settlements = 0
    global_total_streets = 0
    global_matched_streets = 0
    
    for report_path in report_files:
        filename = os.path.basename(report_path)
        # filename format: diagnostic_report_SETTLEMENT.csv
        settlement_name = filename.replace('diagnostic_report_', '').replace('.csv', '')
        
        # Calculate stats
        stats = get_vital_statistics(report_path)
        if not stats:
            continue
            
        total_processed_settlements += 1
        global_total_streets += stats['total_streets']
        global_matched_streets += stats['matched_count']
        
        # Check if HTML map exists
        safe_name = settlement_name 
        html_map_path = os.path.join(HTML_DIR, f"{safe_name}_roads.html")
        has_map = os.path.exists(html_map_path)
        
        settlements_data.append({
            'name': settlement_name.replace('_', ' '), # Display name
            'safe_name': safe_name,
            'stats': stats,
            'has_map': has_map,
            'map_link': f"{HTML_DIR}/{safe_name}_roads.html" if has_map else "#"
        })
    
    # Sort by match percentage (descending)
    settlements_data.sort(key=lambda x: x['stats']['match_percentage'], reverse=True)
    
    global_match_rate = (global_matched_streets / global_total_streets * 100) if global_total_streets > 0 else 0

    # HTML Template with format placeholders
    html_template = """
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Street Matcher Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Heebo:wght@300;400;700&family=Outfit:wght@300;500;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-color: #0f172a;
            --card-bg: #1e293b;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent: #3b82f6;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --glass-bg: rgba(30, 41, 59, 0.7);
            --glass-border: rgba(255, 255, 255, 0.1);
        }}

        body {{
            font-family: 'Heebo', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-primary);
            margin: 0;
            padding: 0;
            min-height: 100vh;
        }}

        .dashboard {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 40px 20px;
        }}

        /* Responsive Adjustments */
        @media (max-width: 768px) {{
            .dashboard {{
                padding: 20px 10px;
            }}
            h1 {{
                font-size: 2rem !important;
            }}
            .subtitle {{
                font-size: 1rem !important;
            }}
            .stat-value {{
                font-size: 1.8rem !important;
            }}
        }}

        /* Header Section */
        header {{
            text-align: center;
            margin-bottom: 60px;
            position: relative;
        }}

        h1 {{
            font-family: 'Outfit', sans-serif;
            font-size: 3.5rem;
            margin: 0 0 10px 0;
            background: linear-gradient(135deg, #60a5fa, #c084fc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
        }}

        .subtitle {{
            color: var(--text-secondary);
            font-size: 1.2rem;
            letter-spacing: 0.5px;
        }}

        /* Global Stats Cards */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 50px;
        }}

        .stat-card {{
            background: var(--card-bg);
            padding: 25px;
            border-radius: 16px;
            border: 1px solid var(--glass-border);
            text-align: center;
            transition: transform 0.3s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
        }}

        .stat-value {{
            font-size: 2.5rem;
            font-weight: 700;
            color: var(--text-primary);
            font-family: 'Outfit', sans-serif;
        }}

        .stat-label {{
            color: var(--text-secondary);
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-top: 5px;
        }}

        /* Search Filter */
        .search-container {{
            margin-bottom: 40px;
            position: relative;
        }}

        .search-input {{
            width: 100%;
            padding: 15px 25px;
            font-size: 1.1rem;
            background: var(--card-bg);
            border: 1px solid var(--glass-border);
            border-radius: 12px;
            color: var(--text-primary);
            outline: none;
            transition: all 0.3s;
            font-family: 'Heebo', sans-serif;
        }}

        .search-input:focus {{
            border-color: var(--accent);
            box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.2);
        }}

        /* Settlements Grid */
        .settlements-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 25px;
        }}

        .settlement-card {{
            background: var(--glass-bg);
            backdrop-filter: blur(10px);
            border: 1px solid var(--glass-border);
            border-radius: 16px;
            padding: 25px;
            position: relative;
            transition: all 0.3s ease;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }}

        .settlement-card:hover {{
            transform: translateY(-5px);
            border-color: rgba(255, 255, 255, 0.2);
            box-shadow: 0 10px 40px -10px rgba(0, 0, 0, 0.5);
        }}

        .card-header {{
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 20px;
        }}

        .settlement-name {{
            font-size: 1.4rem;
            font-weight: 700;
            margin: 0;
            line-height: 1.2;
        }}

        .badge {{
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: 600;
            text-transform: uppercase;
        }}

        .status-excellent {{ background: rgba(16, 185, 129, 0.2); color: #34d399; }}
        .status-good {{ background: rgba(59, 130, 246, 0.2); color: #60a5fa; }}
        .status-fair {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; }}
        .status-poor {{ background: rgba(239, 68, 68, 0.2); color: #f87171; }}

        .progress-container {{
            margin-top: 1rem;
        }}

        .progress-bar-bg {{
            height: 8px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 4px;
            overflow: hidden;
        }}

        .progress-bar-fill {{
            height: 100%;
            background: linear-gradient(90deg, var(--accent), #c084fc);
            border-radius: 4px;
            transition: width 1s ease-out;
        }}

        .match-stats {{
            display: flex;
            justify-content: space-between;
            color: var(--text-secondary);
            font-size: 0.9rem;
            margin-bottom: 5px;
        }}

        .vital-stats {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin-bottom: 25px;
            padding: 15px;
            background: rgba(0, 0, 0, 0.2);
            border-radius: 12px;
        }}

        .vital-item {{
            text-align: center;
        }}

        .vital-val {{
            display: block;
            font-size: 1.2rem;
            font-weight: 700;
            color: var(--text-primary);
        }}

        .vital-lbl {{
            font-size: 0.8rem;
            color: var(--text-secondary);
        }}

        .actions {{
            margin-top: auto;
            display: flex;
            gap: 10px;
        }}

        .btn {{
            flex: 1;
            padding: 12px;
            border-radius: 10px;
            text-align: center;
            text-decoration: none;
            font-weight: 600;
            transition: all 0.2s;
            font-size: 0.95rem;
            border: 1px solid transparent;
        }}

        .btn-primary {{
            background: var(--accent);
            color: white;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        }}

        .btn-primary:hover {{
            background: #2563eb;
            transform: translateY(-2px);
        }}

        .btn-disabled {{
            background: rgba(255, 255, 255, 0.05);
            color: rgba(255, 255, 255, 0.2);
            cursor: not-allowed;
            pointer-events: none;
        }}
        
        /* Loading animation */
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(20px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        .settlement-card {{
            animation: fadeIn 0.5s ease-out forwards;
        }}
    </style>
</head>
<body>

    <div class="dashboard">
        <header>
            <h1>Street Matcher Dashboard</h1>
            <div class="subtitle">ניתוח ומיפוי השוואתי של רחובות ישראל - OSM מול למ"ס</div>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{total_processed_settlements}</div>
                <div class="stat-label">יישובים עובדו</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{global_match_rate:.1f}%</div>
                <div class="stat-label">אחוז התאמה ארצי</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{global_total_streets:,}</div>
                <div class="stat-label">סה"כ רחובות נבדקו</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{global_matched_streets:,}</div>
                <div class="stat-label">רחובות זוהו ודאית</div>
            </div>
        </div>

        <div class="search-container">
            <input type="text" class="search-input" id="search" placeholder="חפש יישוב...">
        </div>

        <div class="settlements-grid" id="grid">
"""
    
    html_content = html_template.format(
        total_processed_settlements=total_processed_settlements,
        global_match_rate=global_match_rate,
        global_total_streets=global_total_streets,
        global_matched_streets=global_matched_streets
    )
    
    # Generate Cards
    for item in settlements_data:
        stats = item['stats']
        map_btn_class = "btn-primary" if item['has_map'] else "btn-disabled"
        map_btn_text = "צפה במפה" if item['has_map'] else "אין מפה"
        
        html_content += f"""
            <div class="settlement-card" data-name="{item['name']}">
                <div class="card-header">
                    <h3 class="settlement-name">{item['name']}</h3>
                    <span class="badge {stats['color_class']}">{stats['tier']}</span>
                </div>
                
                <div class="progress-container">
                    <div class="match-stats">
                        <span>אחוז התאמה</span>
                        <span>{stats['match_percentage']:.1f}%</span>
                    </div>
                    <div class="progress-bar-bg">
                        <div class="progress-bar-fill" style="width: {stats['match_percentage']}%"></div>
                    </div>
                </div>

                <div class="vital-stats">
                    <div class="vital-item">
                        <span class="vital-val">{stats['total_streets']}</span>
                        <span class="vital-lbl">רחובות</span>
                    </div>
                    <div class="vital-item">
                        <span class="vital-val">{stats['matched_count']}</span>
                        <span class="vital-lbl">זוהו</span>
                    </div>
                </div>

                <div class="actions">
                    <a href="{item['map_link']}" class="btn {map_btn_class}">{map_btn_text}</a>
                </div>
            </div>
"""

    html_content += """
        </div>
    </div>

    <script>
        document.getElementById('search').addEventListener('input', function(e) {
            const searchTerm = e.target.value.toLowerCase();
            const cards = document.querySelectorAll('.settlement-card');
            
            cards.forEach(card => {
                const name = card.getAttribute('data-name').toLowerCase();
                if (name.includes(searchTerm)) {
                    card.style.display = 'flex';
                } else {
                    card.style.display = 'none';
                }
            });
        });
    </script>
</body>
</html>
"""

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Successfully generated {OUTPUT_FILE} with {total_processed_settlements} settlements.")

if __name__ == "__main__":
    generate_index_html()