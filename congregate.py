import os
import sys
import json
import pandas as pd
import requests

def get_access_token():
    url = "https://api.box.com/oauth2/token"
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.environ.get('BOX_CLIENT_ID'),
        'client_secret': os.environ.get('BOX_CLIENT_SECRET'),
        'box_subject_type': 'enterprise',
        'box_subject_id': '1444288525'
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()['access_token']

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not target_id: 
        print("CRITICAL: No File ID received.")
        sys.exit(1)

    try:
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        # 1. Get exact filename from Box
        meta = requests.get(f"https://api.box.com/2.0/files/{target_id}", headers=headers).json()
        file_name = meta.get('name', 'Unknown.csv').lower()
        print(f"--- Processing File: {file_name} ---")

        # 2. Download Content
        content = requests.get(f"https://api.box.com/2.0/files/{target_id}/content", headers=headers)
        with open("temp.csv", "wb") as f: f.write(content.content)
        
        # 3. Load CSV and Standardize Headers (Lowercase + No Spaces)
        df = pd.read_csv("temp.csv")
        df.columns = [str(c).strip().lower() for c in df.columns]
        print(f"Detected Columns: {list(df.columns)}")

        # 4. Prepare fresh Data Structure (Overwrites legacy keys)
        data = {'claims': [], 'revenue': [], 'last_update': '', 'total_claims': 0, 'total_revenue': 0}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                try:
                    old_data = json.load(f)
                    # Keep existing data from the OTHER category so we don't wipe it
                    data['claims'] = old_data.get('claims', [])
                    data['revenue'] = old_data.get('revenue', [])
                except: pass

        # 5. Map Data using your specific headers (but lowercased)
        new_records = []
        if 'claim' in file_name:
            for _, row in df.iterrows():
                val = str(row.get('amount_billed', 0)).replace('$', '').replace(',', '')
                new_records.append({
                    "ID": str(row.get('claim_id', 'N/A')),
                    "Provider": str(row.get('provider_name', 'Unknown')),
                    "Amount": float(val or 0),
                    "Status": str(row.get('status', 'Pending'))
                })
            data['claims'] = new_records
            data['total_claims'] = sum(r['Amount'] for r in new_records)
        else:
            for _, row in df.iterrows():
                val = str(row.get('net_collected', 0)).replace('$', '').replace(',', '')
                new_records.append({
                    "ID": str(row.get('reference_num', 'N/A')),
                    "Doctor": str(row.get('doctor', 'Unknown')),
                    "Amount": float(val or 0)
                })
            data['revenue'] = new_records
            data['total_revenue'] = sum(r['Amount'] for r in new_records)

        # 6. Final Save
        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        data['last_file_processed'] = target_id

        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"SUCCESS: Captured {len(new_records)} records into {file_name}")

    except Exception as e:
        print(f"MAPPING ERROR: {str(e)}")
        sys.exit(1)

