import pandas as pd
import json
import os
from boxsdk import JWTAuth, Client, CCGAuth

def get_box_client():
    # Uses the secrets you just saved in GitHub
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    
    # Authenticate via Client Credentials Grant (Free/Easy for Server-to-Server)
    auth = CCGAuth(client_id=client_id, client_secret=client_secret)
    return Client(auth)

def congregate_data():
    client = get_box_client()
    
    # 1. DOWNLOAD FROM BOX
    # Replace 'FILE_ID_A' with the actual ID from the Box URL for Group A's CSV
    file_ids = {
        'Group_A_Claims.csv': 'YOUR_BOX_FILE_ID_A', 
        'Group_B_Revenue.csv': 'YOUR_BOX_FILE_ID_B'
    }
    
    for filename, file_id in file_ids.items():
        with open(filename, 'wb') as f:
            client.file(file_id).download_to(f)
        print(f"Downloaded {filename} from Box.")

    # 2. THE CONGREGATION LOGIC (Same as before)
    group_a = pd.read_csv('Group_A_Claims.csv')
    group_b = pd.read_csv('Group_B_Revenue.csv')

    group_a_clean = group_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    group_b_clean = group_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    combined = pd.concat([group_a_clean, group_b_clean], ignore_index=True)

    # 3. JSON OUTPUT
    summary = {
        "total_revenue": float(combined['Amount'].sum()),
        "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    }

    with open('data.json', 'w') as f:
        json.dump(summary, f, indent=4)
    print("Dashboard data updated from Box feeds.")

if __name__ == "__main__":
    congregate_data()
