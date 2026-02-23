import os
import json
import pandas as pd
import requests

def get_access_token():
    """Manually handles the CCG Handshake without an SDK."""
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

def download_box_file(file_id, access_token, local_name):
    """Downloads a file directly via REST API."""
    url = f"https://api.box.com/2.0/files/{file_id}/content"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Download Failed for {file_id}: {response.status_code}")
    
    with open(local_name, 'wb') as f:
        f.write(response.content)
    print(f"Successfully downloaded {local_name}")

def congregate_data():
    print("--- Starting REST API Data Sync ---")
    try:
        # 1. Get Token
        token = get_access_token()
        
        # 2. Static IDs provided by Bryan
        CLAIMS_ID = '2143561343275'
        REVENUE_ID = '2143561223806'
        
        # 3. Download
        download_box_file(CLAIMS_ID, token, 'claims.csv')
        download_box_file(REVENUE_ID, token, 'revenue.csv')
        
        # 4. Process with Pandas
        df_a = pd.read_csv('claims.csv')
        df_b = pd.read_csv('revenue.csv')
        
        # Map to unified names
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
            
        print(f"SUCCESS: Data congregated. Total: ${summary['total_revenue']:,.2f}")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")

if __name__ == "__main__":
    congregate_data()
