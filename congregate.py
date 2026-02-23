import os
import json
import pandas as pd
import requests

def get_access_token():
    """Manually handles the CCG Handshake."""
    url = "https://api.box.com/oauth2/token"
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.environ.get('BOX_CLIENT_ID'),
        'client_secret': os.environ.get('BOX_CLIENT_SECRET'),
        'box_subject_type': 'enterprise',
        'box_subject_id': '1444288525'
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        raise Exception(f"Auth Failed: {response.status_code} - {response.text}")
    return response.json()['access_token']

def get_latest_file_id(folder_id, prefix, token):
    """Scans the folder for the newest file matching the prefix."""
    url = f"https://api.box.com/2.0/folders/{folder_id}/items"
    headers = {'Authorization': f'Bearer {token}'}
    params = {'fields': 'name,content_modified_at'}
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Folder Scan Failed: {response.status_code}")
    
    items = response.json().get('entries', [])
    # Filter for files matching the prefix
    matching_files = [
        i for i in items 
        if i['type'] == 'file' and i['name'].lower().startswith(prefix.lower())
    ]
    
    if not matching_files:
        return None, None
        
    # Sort by modification date (newest first)
    latest = sorted(matching_files, key=lambda x: x['content_modified_at'], reverse=True)[0]
    return latest['id'], latest['name']

def download_box_file(file_id, token, local_name):
    """Downloads file content via REST API."""
    url = f"https://api.box.com/2.0/files/{file_id}/content"
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Download Failed: {response.status_code}")
    
    with open(local_name, 'wb') as f:
        f.write(response.content)

def congregate_data():
    print("--- Starting Smart REST Sync ---")
    try:
        token = get_access_token()
        FOLDER_ID = '367459660638'
        
        # 1. Smart Scan for newest files
        claims_id, claims_name = get_latest_file_id(FOLDER_ID, 'Group_A_Claims', token)
        rev_id, rev_name = get_latest_file_id(FOLDER_ID, 'Group_B_Revenue', token)
        
        if not (claims_id and rev_id):
            print("ERROR: Could not find files. Ensure 'Tebra_Data_Bridge' is invited to the folder.")
            return

        print(f"Latest Claims: {claims_name}")
        print(f"Latest Revenue: {rev_name}")

        # 2. Download
        download_box_file(claims_id, token, 'claims.csv')
        download_box_file(rev_id, token, 'revenue.csv')

        # 3. Process
        df_a = pd.read_csv('claims.csv')
        df_b = pd.read_csv('revenue.csv')
        
        df_a = df_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
        df_b = df_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})
        
        combined = pd.concat([df_a, df_b], ignore_index=True)
        
        summary = {
            "total_revenue": float(combined['Amount'].sum()),
            "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
            "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %I:%M %p")
        }
        
        with open('data.json', 'w') as f:
            json.dump(summary, f, indent=4)
            
        print(f"SUCCESS: Dashboard Updated. New Total: ${summary['total_revenue']:,.2f}")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")

if __name__ == "__main__":
    congregate_data()
