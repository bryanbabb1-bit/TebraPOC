import os
import json
import pandas as pd
from boxsdk import CCGAuth, Client

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    print(f"Authenticating for Enterprise: {enterprise_id}...")
    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    return Client(auth)

def get_latest_file_id(client, folder_id, name_prefix):
    """Finds the newest file in the folder starting with the prefix."""
    print(f"Scanning Folder {folder_id} for '{name_prefix}' reports...")
    items = client.folder(folder_id).get_items()
    
    latest_item = None
    for item in items:
        if item.type == 'file' and item.name.lower().startswith(name_prefix.lower()):
            # Using content_modified_at to find the true latest drop
            if latest_item is None or item.content_modified_at > latest_item.content_modified_at:
                latest_item = item
    
    if latest_item:
        print(f"  > Found Latest: {latest_item.name} (ID: {latest_item.id})")
        return latest_item.id
    return None

def congregate_data():
    print("--- Starting PractiSynergy Data Sync ---")
    client = get_box_client()
    REPORTS_FOLDER_ID = '367459660638'

    # Identify newest files dynamically
    claims_id = get_latest_file_id(client, REPORTS_FOLDER_ID, 'Group_A_Claims')
    revenue_id = get_latest_file_id(client, REPORTS_FOLDER_ID, 'Group_B_Revenue')

    if not (claims_id and revenue_id):
        print("ERROR: Missing files. Ensure the 'Tebra_Data_Bridge' automation user is invited to the folder.")
        return

    # Download
    print("Downloading files...")
    with open('claims.csv', 'wb') as f:
        client.file(claims_id).download_to(f)
    with open('revenue.csv', 'wb') as f:
        client.file(revenue_id).download_to(f)

    # Process and Aggregate
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
    
    print(f"SUCCESS: Dashboard updated at {summary['last_updated']}")

if __name__ == "__main__":
    congregate_data()
