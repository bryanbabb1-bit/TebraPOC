import sys
import os

# --- PRE-FLIGHT DEBUGGING ---
print(f"Python Version: {sys.version}")
print(f"Current Directory: {os.getcwd()}")
print(f"System Path: {sys.path}")

try:
    from boxsdk import CCGAuth, Client
    print("SUCCESS: Box SDK imported.")
except ImportError as e:
    print(f"FAILURE: Could not import Box SDK. Error: {e}")
    # Try to find where it might be
    sys.exit(1)

import pandas as pd
import json

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    return Client(auth)

def get_latest_file_id(client, folder_id, name_prefix):
    print(f"Scanning Folder {folder_id} for '{name_prefix}'...")
    items = client.folder(folder_id).get_items()
    latest_file = None
    for item in items:
        if item.type == 'file' and item.name.lower().startswith(name_prefix.lower()):
            if latest_file is None or item.content_modified_at > latest_file.content_modified_at:
                latest_file = item
    if latest_file:
        print(f"  > Found: {latest_file.name}")
        return latest_file.id
    return None

def congregate_data():
    print("--- Starting Data Sync ---")
    client = get_box_client()
    FOLDER_ID = '367459660638' 

    # Find the files
    claims_id = get_latest_file_id(client, FOLDER_ID, 'Group_A_Claims')
    rev_id = get_latest_file_id(client, FOLDER_ID, 'Group_B_Revenue')

    if not claims_id or not rev_id:
        print("Missing required files in Box. Check naming conventions.")
        return

    # Download
    with open('claims.csv', 'wb') as f:
        client.file(claims_id).download_to(f)
    with open('revenue.csv', 'wb') as f:
        client.file(rev_id).download_to(f)

    # Process
    df_a = pd.read_csv('claims.csv')
    df_b = pd.read_csv('revenue.csv')

    df_a = df_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    df_b = df_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    combined = pd.concat([df_a, df_b], ignore_index=True)
    
    summary = {
        "total_revenue": float(combined['Amount'].sum()),
        "provider_breakdown": combined.groupby('Provider')['Amount'].sum().to_dict(),
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %I:%M %p")
    }

    with open('data.json', 'w') as f:
        json.dump(summary, f, indent=4)
    
    print("Sync Complete.")

if __name__ == "__main__":
    congregate_data()
