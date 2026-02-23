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

def clean_val(val):
    return float(str(val).replace('$', '').replace(',', '').strip() or 0)

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not target_id: sys.exit(1)

    try:
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        meta = requests.get(f"https://api.box.com/2.0/files/{target_id}", headers=headers).json()
        file_name = meta.get('name', 'Unknown.csv')
        
        content = requests.get(f"https://api.box.com/2.0/files/{target_id}/content", headers=headers)
        with open("temp.csv", "wb") as f: f.write(content.content)
        
        df = pd.read_csv("temp.csv")
        category = 'claims' if 'claim' in file_name.lower() else 'revenue'
        
        data = {'claims': [], 'revenue': [], 'last_update': '', 'total_claims': 0, 'total_revenue': 0}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f: data = json.load(f)

        new_records = []
        if category == 'claims':
            # Map Claim_ID and Amount_Billed
            for _, row in df.iterrows():
                new_records.append({
                    "ID": str(row.get('Claim_ID', 'N/A')),
                    "Provider": str(row.get('Provider_Name', 'Unknown')),
                    "Amount": clean_val(row.get('Amount_Billed', 0)),
                    "Status": str(row.get('Status', 'Pending'))
                })
            data['total_claims'] = sum(r['Amount'] for r in new_records)
        else:
            # Map Reference_Num and Net_Collected
            for _, row in df.iterrows():
                new_records.append({
                    "ID": str(row.get('Reference_Num', 'N/A')),
                    "Doctor": str(row.get('Doctor', 'Unknown')),
                    "Amount": clean_val(row.get('Net_Collected', 0))
                })
            data['total_revenue'] = sum(r['Amount'] for r in new_records)

        data[category] = new_records
        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')

        with open('data.json', 'w') as f: json.dump(data, f, indent=4)
        print(f"SUCCESS: Processed {len(new_records)} {category} rows.")
            
    except Exception as e:
        print(f"FAILED: {str(e)}")
        sys.exit(1)
