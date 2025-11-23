
import os
import re
import json

def parse_tooltip(tooltip):
    data = {
        'street_name': None,
        'score': None,
        'id': None,
        'status': None
    }
    parts = tooltip.split('#10;')
    data['street_name'] = parts[0]
    for part in parts[1:]:
        if 'ציון:' in part:
            try:
                data['score'] = float(part.split(':')[1].strip())
            except (ValueError, IndexError):
                pass
        elif 'מזהה:' in part:
            try:
                data['id'] = int(part.split(':')[1].strip())
            except (ValueError, IndexError):
                pass
        elif 'סטטוס:' in part:
            try:
                data['status'] = part.split(':')[1].strip()
            except IndexError:
                pass
    return data

def main():
    html_dir = 'HTML/'
    all_street_data = []

    h1_pattern = re.compile(r'<h1>מפת רחובות - (.*?)</h1>')
    path_pattern = re.compile(r'<path.*?data-tooltip="(.*?)".*?>')

    if not os.path.exists(html_dir):
        print(f"Directory not found: {html_dir}")
        return

    file_list = [f for f in os.listdir(html_dir) if f.endswith('.html')]

    for i, filename in enumerate(file_list):
        print(f"Processing file {i+1}/{len(file_list)}: {filename}")
        filepath = os.path.join(html_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        settlement_name = None
        h1_match = h1_pattern.search(content)
        if h1_match:
            settlement_name = h1_match.group(1)

        path_matches = path_pattern.findall(content)
        for tooltip in path_matches:
            street_data = parse_tooltip(tooltip)
            street_data['settlement'] = settlement_name
            street_data['source_file'] = filename
            all_street_data.append(street_data)

    output_filename = 'street_data.json'
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(all_street_data, f, ensure_ascii=False, indent=4)

    print(f"Data for {len(all_street_data)} streets from {len(file_list)} files has been written to {output_filename}")

if __name__ == '__main__':
    main()
