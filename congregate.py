import os
import sys
import json
import pandas as pd
import requests

def get_access_token():
    url = "https://api.box.com/oauth2/token"
    # Ensure this matches the Enterprise ID in your Box Account Info
    ent_id = '1444288525' 
    
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.environ.get('BOX_CLIENT_ID'),
        'client_secret': os.environ.get('BOX_CLIENT_SECRET'),
        'box_subject_type': 'enterprise',
        'box_subject_id': ent_id
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        print(f"TOKEN GENERATION FAILED: {response.text}")
        sys.exit(1)
    return response.json()['access_token']

def clean_curr(val):
    if pd.isna(val) or val == '': return 0.0
    return float(str(val).replace('$', '').replace(',', '').strip() or 0)

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not target_id: sys.exit(1)

    try:
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        # 1. Diagnostic Content Check
        dl_url = f"https://api.box.com/2.0/files/{target_id}/content"
        dl_res = requests.get(dl_url, headers=headers)
        
        if dl_res.status_code != 200:
            print(f"--- BOX ACCESS DENIED ({dl_res.status_code}) ---")
            print(f"ERROR DETAILS: {dl_res.text}")
            print("REMEDY: Reauthorize the app in the Box Admin Console (Apps > Custom Apps).")
            sys.exit(1)

        with open("temp.csv", "wb") as f: f.write(dl_res.content)
        
        # 2. Map exact headers from sample documents
        df = pd.read_csv("temp.csv")
        meta = requests.get(f"https://api.box.com/2.0/files/{target_id}", headers=headers).json()
        file_name = meta.get('name', 'file.csv').lower()
        
        data = {'claims': [], 'revenue': [], 'last_update': '', 'stats': {}}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                try: data = json.load(f)
                except: pass

        if 'claim' in file_name:
            # Matches headers: Claim_ID, Provider_Name, Amount_Billed, Status
            data['claims'] = df.to_dict(orient='records')
            data['stats']['total_claims_value'] = df['Amount_Billed'].apply(clean_curr).sum()
        else:
            # Matches headers: Gross_Charge, Net_Collected, Doctor
            data['revenue'] = df.to_dict(orient='records')
            data['stats']['total_charges'] = df['Gross_Charge'].apply(clean_curr).sum()
            data['stats']['total_collected'] = df['Net_Collected'].apply(clean_curr).sum()

        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        with open('data.json', 'w') as f: json.dump(data, f, indent=4)
        
        print(f"SUCCESS: Synced {file_name}")

    except Exception as e:
        print(f"PROCESSING ERROR: {str(e)}")
        sys.exit(1)
