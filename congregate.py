import os
import json
import pandas as pd
from boxsdk import CCGAuth, Client

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    print(f"Connecting to Box Enterprise: {enterprise_id}...")
    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    return Client(auth)

def get_latest_file_id(client, folder_id, name_prefix):
    """Finds the ID of the newest file in a folder matching the prefix."""
    print(f"Scanning folder {folder_id} for '{name_prefix}' reports...")
    items = client.folder(folder_id).get_items()
    
    latest_item = None
    for item in items:
        if item.type == 'file' and item.name.lower().startswith(name_prefix.lower()):
            if latest_item is None or item.content_modified_at > latest_item.content_modified_at:
                latest_item = item
    
    if latest_item:
        print(f"  > Selected: {latest_item.name} (Modified: {latest_item.content_modified_at})")
        return latest_item.id
    return None

def congregate_data():
    print("--- Starting PractiSynergy POC Sync ---")
    client = get_box_client()
    REPORTS_FOLDER_ID = '367459660638'

    # 1. Identify newest files
    claims_file_id = get_latest_file_id(client, REPORTS_FOLDER_ID, 'Group_A_Claims')
    revenue_file_id = get_latest_file_id(client, REPORTS_FOLDER_ID, 'Group_B_Revenue')

    if not claims_file_id or not revenue_file_id:
        print("ERROR: Could not find both required files in Box.")
        return

    # 2. Download files
    print("Downloading data from Box...")
    with open('claims.csv', 'wb') as f:
        client.file(claims_file_id).download_to(f)
    with open('revenue.csv', 'wb') as f:
        client.file(revenue_file_id).download_to(f)

    # 3. Process Data
    print("Processing and aggregating data...")
    df_claims = pd.read_csv('claims.csv')
    df_revenue = pd.read_csv('revenue.csv')

    # Normalize columns (Rename to 'Provider' and 'Amount')
    df_claims = df_claims.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    df_revenue = df_revenue.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    combined = pd.concat([df_claims, df_revenue], ignore_index=True)
    
    # Calculate Summary
    summary = {
        "total_revenue": float(combined['Amount'].sum()),
        "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %I:%M %p")
    }

    # 4. Save to JSON
    with open('data.json', 'w') as f:
        json.dump(summary, f, indent=4)
    
    print(f"SUCCESS: Dashboard updated at {summary['last_updated']}")

if __name__ == "__main__":
    congregate_data()

