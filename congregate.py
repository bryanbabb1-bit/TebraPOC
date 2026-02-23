import os
import sys
import json
import pandas as pd
import requests

def get_access_token():
    url = "https://api.box.com/oauth2/token"
    # Your verified Enterprise ID
    ent_id = '1444288525' 
    
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.environ.get('BOX_CLIENT_ID'),
        'client_secret': os.environ.get('BOX_CLIENT_SECRET'),
        'box_subject_type': 'enterprise',
        'box_subject_id': ent_id
    }
    res = requests.post(url, data=data)
    res.raise_for_status()
    return res.json()['access_token']

def clean_curr(val):
    if pd.isna(val) or val == '': return 0.0
    # Cleans symbols and commas to ensure math works
    return float(str(val).replace('$', '').replace(',', '').strip() or 0)

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not target_id: sys.exit(1)

    try:
        token = get_access_token()
        
        # THE FIX: Nested your Account ID in the As-User header
        headers = {
            'Authorization': f'Bearer {token}',
            'As-User': '49148430670' 
        }
        
        # 1. Download Content as Bryan
        dl_res = requests.get(f"https://api.box.com/2.0/files/{target_id}/content", headers=headers)
        
        if dl_res.status_code != 200:
            print(f"AS-USER DOWNLOAD FAILED: {dl_res.status_code}")
            print(f"Details: {dl_res.text}")
            sys.exit(1)

        with open("temp.csv", "wb") as f: f.write(dl_res.content)
        
        # 2. Process the CSV
        df = pd.read_csv("temp.csv")
        df.columns = [str(c).strip() for c in df.columns] # Remove accidental whitespace in headers
        
        meta = requests.get(f"https://api.box.com/2.0/files/{target_id}", headers=headers).json()
        file_name = meta.get('name', 'file.csv').lower()
        
        # 3. Build/Maintain JSON Structure
        data = {'claims': [], 'revenue': [], 'last_update': '', 'stats': {}}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                try: data = json.load(f)
                except: pass

        if 'claim' in file_name:
            # Mapping: Claim_ID, Provider_Name, Amount_Billed, Status
            data['claims'] = df.to_dict(orient='records')
            data['stats']['total_claims_value'] = df['Amount_Billed'].apply(clean_curr).sum()
        else:
            # Mapping: Gross_Charge, Net_Collected, Doctor
            data['revenue'] = df.to_dict(orient='records')
            data['stats']['total_charges'] = df['Gross_Charge'].apply(clean_curr).sum()
            data['stats']['total_collected'] = df['Net_Collected'].apply(clean_curr).sum()

        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"SUCCESS: Synced {len(df)} rows from {file_name} using As-User.")

    except Exception as e:
        print(f"MAPPING ERROR: {str(e)}")
        sys.exit(1)
