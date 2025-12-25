import requests

URL = "https://nominatim.openstreetmap.org/search"
queries = ['כפר רוזנאלד', 'כפר רוזנואלד', 'זרעית', 'Kfar Rosenwald']

for q in queries:
    print(f"--- Query: {q} ---")
    params = {
        'q': q,
        'format': 'json',
        'addressdetails': 1,
        'limit': 3,
        'accept-language': 'he,en'
    }
    headers = {'User-Agent': 'SettlementMatcherTest/1.0'}

    try:
        response = requests.get(URL, params=params, headers=headers)
        response.raise_for_status()
        results = response.json()
        if not results:
            print("  No results.")
        for i, r in enumerate(results):
            print(f"  Result {i+1}:")
            print(f"    Display Name: {r.get('display_name')}")
            print(f"    Type: {r.get('type')}")
            print(f"    Importance: {r.get('importance')}")
            print(f"    OSM ID: {r.get('osm_id')}")
            print(f"    Address: {r.get('address')}")
            print("-" * 10)
    except Exception as e:
        print(f"Error: {e}")
    print("\n")
