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

def process_file(file_id):
    token = get_access_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # 1. Get File Metadata to see the name
    meta_url = f"https://api.box.com/2.0/files/{file_id}"
    meta = requests.get(meta_url, headers=headers).json()
    file_name = meta.get('name', 'Unknown')
    print(f"--- PROCESSING: {file_name} (ID: {file_id}) ---")

    # 2. Download Content
    dl_url = f"https://api.box.com/2.0/files/{file_id}/content"
    content = requests.get(dl_url, headers=headers)
    
    # Save locally for pandas to read
    with open("temp.csv", "wb") as f:
        f.write(content.content)
    
    return pd.read_csv("temp.csv")

if __name__ == "__main__":
    # Use the ID passed from the GitHub Action YAML
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    if not target_id:
        print("ERROR: No File ID provided by the webhook.")
        sys.exit(1)

    try:
        df = process_file(target_id)
        
        # Simple Logic: Update the JSON based on filename
        # You can expand this logic as we refine the dashboard
        data = {}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                data = json.load(f)
        
        # Convert dataframe to a simple dictionary for the dashboard
        data['latest_upload'] = df.head(10).to_dict(orient='records')
        data['last_file_processed'] = target_id
        
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
            
        print("SUCCESS: data.json updated.")
        
    except Exception as e:
        print(f"FAILED: {str(e)}")
        sys.exit(1)

