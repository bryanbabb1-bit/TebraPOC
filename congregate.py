import pandas as pd
import json
import os
from boxsdk import CCGAuth, Client

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    print(f"Authenticating for Enterprise: {enterprise_id}")
    # The Legacy SDK handles the CCG handshake perfectly with just these 3 lines
    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    return Client(auth)

def get_latest_file_id(client, folder_id, name_prefix):
    """Finds the newest file in the folder starting with the prefix."""
    print(f"Scanning Folder {folder_id} for '{name_prefix}'...")
    
    # get_items() is the stable way to list folder contents
    items = client.folder(folder_id).get_items()

    latest_file = None
    for item in items:
        # Check if it's a file and matches the prefix (e.g., 'Group_A_Claims')
        if item.type == 'file' and item.name.lower().startswith(name_prefix.lower()):
            # Use content_modified_at to find the most recent drop
            if latest_file is None or item.content_modified_at > latest_file.content_modified_at:
                latest_file = item

    if latest_file:
        print(f"  > Found Latest: {latest_file.name} (Modified: {latest_file.content_modified_at})")
        return latest_file.id, latest_file.name
    return None, None

def congregate_data():
    print("--- Starting PractiSynergy Data Sync ---")
    try:
        client = get_box_client()
        FOLDER_ID = '367459660638' 

        # Define what we are looking for
        targets = {
            'Group_A_Claims': FOLDER_ID,
            'Group_B_Revenue': FOLDER_ID
        }

        downloaded = {}
        for prefix, f_id in targets.items():
            file_id, real_name = get_latest_file_id(client, f_id, prefix)
            if file_id:
                local_path = f"{prefix}.csv"
                with open(local_path, 'wb') as f:
                    client.file(file_id).download_to(f)
                downloaded[prefix] = local_path
            else:
                print(f"  ! Warning: No file found for {prefix}")

        if len(downloaded) < 2:
            print("Abort: Could not find both Claims and Revenue files.")
            return

        # Data Processing
        df_a = pd.read_csv(downloaded['Group_A_Claims'])
        df_b = pd.read_csv(downloaded['Group_B_Revenue'])

        # PractiSynergy Column Alignment
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
        
        print(f"SUCCESS: Dashboard data updated for PractiSynergy POC.")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    congregate_data()
