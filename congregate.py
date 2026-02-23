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

def clean_currency(val):
    if pd.isna(val): return 0.0
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
        # Log columns for debugging in GitHub Actions
        print(f"File: {file_name} | Columns: {list(df.columns)}")
        
        category = 'claims' if 'claim' in file_name.lower() else 'revenue'
        
        # Load persistent state
        data = {'claims': [], 'revenue': [], 'last_update': '', 'stats': {}}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                try: data = json.load(f)
                except: pass

        if category == 'claims':
            # Mapping: Claim_ID, Provider_Name, Amount_Billed, Status
            data['claims'] = df.to_dict(orient='records')
            data['stats']['total_claims_value'] = df['Amount_Billed'].apply(clean_currency).sum()
        else:
            # Mapping: Reference_Num, Doctor, Gross_Charge, Net_Collected
            data['revenue'] = df.to_dict(orient='records')
            data['stats']['total_charges'] = df['Gross_Charge'].apply(clean_currency).sum()
            data['stats']['total_collected'] = df['Net_Collected'].apply(clean_currency).sum()

        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        data['last_file_id'] = target_id

        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        print(f"SUCCESS: Processed {file_name}")
            
    except Exception as e:
        print(f"MAPPING ERROR: {str(e)}")
        sys.exit(1)
