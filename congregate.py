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
    
    # Get Metadata for the filename
    meta = requests.get(f"https://api.box.com/2.0/files/{file_id}", headers=headers).json()
    file_name = meta.get('name', 'Unknown.csv')
    
    # Download Content
    content = requests.get(f"https://api.box.com/2.0/files/{file_id}/content", headers=headers)
    with open("temp.csv", "wb") as f:
        f.write(content.content)
    
    return pd.read_csv("temp.csv"), file_name

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not target_id: sys.exit(1)

    try:
        df, file_name = process_file(target_id)
        category = 'claims' if 'claim' in file_name.lower() else 'revenue'
        
        # Load or Init Data
        data = {'claims': [], 'revenue': [], 'last_update': ''}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                data = json.load(f)

        # Update and Save
        data[category] = df.to_dict(orient='records')
        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        print(f"SUCCESS: Updated {category} from {file_name}")
            
    except Exception as e:
        print(f"FAILED: {str(e)}")
        sys.exit(1)


