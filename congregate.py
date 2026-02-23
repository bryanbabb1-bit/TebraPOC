import os
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
    if response.status_code != 200:
        raise Exception(f"Auth Failed: {response.text}")
    return response.json()['access_token']

def get_latest_file_id(folder_id, prefix, token):
    url = f"https://api.box.com/2.0/folders/{folder_id}/items"
    headers = {'Authorization': f'Bearer {token}'}
    params = {'fields': 'name,content_modified_at'}
    
    response = requests.get(url, headers=headers, params=params)
    items = response.json().get('entries', [])
    
    matching_files = [
        i for i in items 
        if i['type'] == 'file' and i['name'].lower().startswith(prefix.lower())
    ]
    
    if not matching_files:
        return None, None
        
    # Pick the one with the newest timestamp on Box
    latest = sorted(matching_files, key=lambda x: x['content_modified_at'], reverse=True)[0]
    return latest['id'], latest['name']

def congregate_data():
    print("--- FORCING SMART SYNC ---")
    try:
        token = get_access_token()
        FOLDER_ID = '367459660638'
        
        c_id, c_name = get_latest_file_id(FOLDER_ID, 'Group_A_Claims', token)
        r_id, r_name = get_latest_file_id(FOLDER_ID, 'Group_B_Revenue', token)
        
        # LOGGING FOR BRYAN: This will show up in your GitHub Actions console
        print(f"IDENTIFIED CLAIMS: {c_name} (ID: {c_id})")
        print(f"IDENTIFIED REVENUE: {r_name} (ID: {r_id})")

        # Download logic...
        url_base = "https://api.box.com/2.0/files/{}/content"
        headers = {'Authorization': f'Bearer {token}'}
        
        with open('claims.csv', 'wb') as f:
            f.write(requests.get(url_base.format(c_id), headers=headers).content)
        with open('revenue.csv', 'wb') as f:
            f.write(requests.get(url_base.format(r_id), headers=headers).content)

        df_a = pd.read_csv('claims.csv')
        df_b = pd.read_csv('revenue.csv')
        
        df_a = df_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
        df_b = df_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})
        
        combined = pd.concat([df_a, df_b], ignore_index=True)
        
        summary = {
            "total_revenue": float(combined['Amount'].sum()),
            "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
            "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %I:%M:%p"), # Added seconds for visibility
            "source_files": {"claims": c_name, "revenue": r_name}
        }
        
        with open('data.json', 'w') as f:
            json.dump(summary, f, indent=4)
            
        print(f"SUCCESS: Data saved. Newest timestamp: {summary['last_updated']}")

    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    congregate_data()
