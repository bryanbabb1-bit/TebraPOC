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
    return response.json()['access_token']

def get_latest_file_id(folder_id, prefix, token):
    url = f"https://api.box.com/2.0/folders/{folder_id}/items"
    headers = {'Authorization': f'Bearer {token}'}
    params = {'fields': 'name,content_modified_at'}
    response = requests.get(url, headers=headers, params=params)
    items = response.json().get('entries', [])
    matching = [i for i in items if i['type'] == 'file' and i['name'].lower().startswith(prefix.lower())]
    if not matching: return None, None
    latest = sorted(matching, key=lambda x: x['content_modified_at'], reverse=True)[0]
    return latest['id'], latest['name']

def congregate_data():
    print("--- FIXING MAPPING ERROR ---")
    try:
        token = get_access_token()
        FOLDER_ID = '367459660638'
        
        c_id, c_name = get_latest_file_id(FOLDER_ID, 'Group_A_Claims', token)
        r_id, r_name = get_latest_file_id(FOLDER_ID, 'Group_B_Revenue', token)
        
        print(f"USING: {c_name} and {r_name}")

        url_base = "https://api.box.com/2.0/files/{}/content"
        headers = {'Authorization': f'Bearer {token}'}
        
        # Download
        c_data = requests.get(url_base.format(c_id), headers=headers).content
        r_data = requests.get(url_base.format(r_id), headers=headers).content
        
        with open('claims.csv', 'wb') as f: f.write(c_data)
        with open('revenue.csv', 'wb') as f: f.write(r_data)

        df_a = pd.read_csv('claims.csv')
        df_b = pd.read_csv('revenue.csv')

        # DYNAMIC MAPPING: Look for whatever column name Box provided
        # This prevents the 'Amount' error if the CSV header changed
        def map_cols(df):
            mapping = {
                'Provider_Name': 'Provider', 'Doctor': 'Provider',
                'Amount_Billed': 'Amount', 'Gross_Charge': 'Amount', 'Amount': 'Amount'
            }
            # Rename only if the column exists in this specific CSV
            return df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

        df_a = map_cols(df_a)
        df_b = map_cols(df_b)

        # FINAL CHECK: Ensure the 'Amount' column exists before summing
        if 'Amount' not in df_a.columns or 'Amount' not in df_b.columns:
            print(f"CRITICAL: 'Amount' column not found. Available in A: {df_a.columns.tolist()}")
            return

        combined = pd.concat([df_a, df_b], ignore_index=True)
        
        # Clean numeric data (remove $ or commas if they exist)
        combined['Amount'] = pd.to_numeric(combined['Amount'].replace(r'[\$,]', '', regex=True), errors='coerce').fillna(0)

        summary = {
            "total_revenue": float(combined['Amount'].sum()),
            "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
            "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %I:%M:%S %p"),
            "source_files": {"claims": c_name, "revenue": r_name}
        }
        
        with open('data.json', 'w') as f:
            json.dump(summary, f, indent=4)
            
        print(f"SUCCESS: Total Revenue is now ${summary['total_revenue']:,.2f}")

    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    congregate_data()
