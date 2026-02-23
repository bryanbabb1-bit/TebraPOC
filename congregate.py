import pandas as pd
import json
import os
import sys

# Force the script to look in the current folder for the Box libraries
sys.path.append(os.getcwd())

from box_sdk_gen import BoxClient, BoxCCGAuth, CCGConfig

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    print("Authenticating with Box...")
    # Using the configuration that matches the latest 2026 SDK install
    config = CCGConfig(
        client_id=client_id,
        client_secret=client_secret,
        box_subject_type="enterprise",
        box_subject_id=enterprise_id
    )
    auth = BoxCCGAuth(config)
    return BoxClient(auth)

def get_latest_file_id(client, folder_id, name_prefix):
    """Finds the newest file in a folder starting with a prefix."""
    print(f"Scanning Folder {folder_id} for newest '{name_prefix}' file...")
    
    # Modern SDK method to list items
    items = client.folders.get_folder_items(folder_id).entries

    latest_file = None
    for item in items:
        if item.type == 'file' and item.name.lower().startswith(name_prefix.lower()):
            if latest_file is None or item.modified_at > latest_file.modified_at:
                latest_file = item

    if latest_file:
        print(f"Found Latest: {latest_file.name} (ID: {latest_file.id})")
        return latest_file.id, latest_file.name
    return None, None

def congregate_data():
    print("--- Starting Smart Folder Sync ---")
    client = get_box_client()
    
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
            
            # Modern SDK download method
            file_content = client.files.download_file(file_id)
            with open(local_filename, 'wb') as f:
                f.write(file_content)
            
            downloaded_files.append(local_filename)
        else:
            print(f"CRITICAL: No file found starting with '{prefix}'")

    if len(downloaded_files) < 2:
        print("Sync Aborted: Required files missing from Box.")
        return

    try:
        df_a = pd.read_csv('Group_A_Claims.csv')
        df_b = pd.read_csv('Group_B_Revenue.csv')

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
        
        print(f"SUCCESS: Dashboard updated.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    congregate_data()
