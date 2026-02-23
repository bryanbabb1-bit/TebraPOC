import pandas as pd
import json
import os
from boxsdk import CCGAuth, Client

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    print("Authenticating with Stable Legacy SDK...")
    # This specific handshake is the industry standard for Box CCG
    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    return Client(auth)

def get_latest_file_id(client, folder_id, name_prefix):
    """Finds the newest file in a folder starting with a prefix."""
    print(f"Scanning Folder {folder_id} for newest '{name_prefix}' file...")
    
    # Get all items in the folder using stable legacy logic
    folder_items = client.folder(folder_id=folder_id).get_items()

    latest_file = None
    for item in folder_items:
        if item.type == 'file' and item.name.lower().startswith(name_prefix.lower()):
            if latest_file is None or item.content_modified_at > latest_file.content_modified_at:
                latest_file = item

    if latest_file:
        print(f"Found Latest: {latest_file.name} (ID: {latest_file.id})")
        return latest_file.id, latest_file.name
    return None, None

def congregate_data():
    print("--- Starting Smart Folder Sync ---")
    client = get_box_client()
    
    # Your Reports Folder ID
    FOLDER_REPORTS_ID = '367459660638' 

    targets = {
        'Group_A_Claims': FOLDER_REPORTS_ID,
        'Group_B_Revenue': FOLDER_REPORTS_ID
    }

    downloaded_files = []
    for prefix, folder_id in targets.items():
        file_id, real_name = get_latest_file_id(client, folder_id, prefix)
        
        if file_id:
            local_filename = f"{prefix}.csv"
            print(f"Downloading {real_name}...")
            
            with open(local_filename, 'wb') as f:
                client.file(file_id).download_to(f)
            
            downloaded_files.append(local_filename)
        else:
            print(f"CRITICAL: No file found starting with '{prefix}' in folder {folder_id}")

    if len(downloaded_files) < 2:
        print("Sync Aborted: Required files missing from Box.")
        return

    try:
        df_a = pd.read_csv('Group_A_Claims.csv')
        df_b = pd.read_csv('Group_B_Revenue.csv')

        # PractiSynergy Column Mapping
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
        
        print(f"SUCCESS: Dashboard updated via Legacy SDK.")

    except Exception as e:
        print(f"Error during data processing: {e}")

if __name__ == "__main__":
    congregate_data()
