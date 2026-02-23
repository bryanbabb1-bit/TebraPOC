import os
import sys
import json
import pandas as pd
import requests

def get_access_token():
    url = "https://api.box.com/oauth2/token"
    ent_id = '1444288525' # Ensure this matches the ID in your Box Account Info
    
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.environ.get('BOX_CLIENT_ID'),
        'client_secret': os.environ.get('BOX_CLIENT_SECRET'),
        'box_subject_type': 'enterprise',
        'box_subject_id': ent_id
    }
    res = requests.post(url, data=data)
    if res.status_code != 200:
        print(f"AUTH FAILED: {res.text}")
        sys.exit(1)
    return res.json()['access_token']

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not target_id: sys.exit(1)

    try:
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        # --- DIAGNOSTIC: WHAT CAN I SEE? ---
        # We check the parent folder of the file to see if we are actually a collaborator
        meta = requests.get(f"https://api.box.com/2.0/files/{target_id}", headers=headers).json()
        parent_id = meta.get('parent', {}).get('id', '0')
        
        print(f"--- FOLDER CHECK (ID: {parent_id}) ---")
        folder_res = requests.get(f"https://api.box.com/2.0/folders/{parent_id}/items", headers=headers)
        if folder_res.status_code == 200:
            items = [i['name'] for i in folder_res.json().get('entries', [])]
            print(f"I CAN SEE THESE FILES: {items}")
        else:
            print(f"I CANNOT SEE THE FOLDER. Error: {folder_res.text}")
            print("REMEDY: Re-invite 'AutomationUser_2515661_EOliJ8y4OO@boxdevedition.com' to this folder.")

        # --- DOWNLOAD ATTEMPT ---
        dl_res = requests.get(f"https://api.box.com/2.0/files/{target_id}/content", headers=headers)
        if dl_res.status_code != 200:
            print(f"DOWNLOAD FAILED (403). Content: {dl_res.text}")
            sys.exit(1)

        with open("temp.csv", "wb") as f: f.write(dl_res.content)
        df = pd.read_csv("temp.csv")
        
        # Standardize headers to handle your samples
        df.columns = [str(c).strip() for c in df.columns]
        file_name = meta.get('name', 'file.csv').lower()
        
        data = {'claims': [], 'revenue': [], 'last_update': '', 'stats': {}}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f: data = json.load(f)

        if 'claim' in file_name:
            data['claims'] = df.to_dict(orient='records')
            data['stats']['total_claims_value'] = df['Amount_Billed'].sum()
        else:
            data['revenue'] = df.to_dict(orient='records')
            data['stats']['total_charges'] = df['Gross_Charge'].sum()
            data['stats']['total_collected'] = df['Net_Collected'].sum()

        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        with open('data.json', 'w') as f: json.dump(data, f, indent=4)
        print(f"SUCCESS: Synced {file_name}")

    except Exception as e:
        print(f"MAPPING ERROR: {str(e)}")
        sys.exit(1)
