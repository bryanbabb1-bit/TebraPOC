import pandas as pd
import json
import os
from boxsdk import CCGAuth, Client

def congregate_data():
    # 1. Setup Box Connection
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    
    auth = CCGAuth(client_id=client_id, client_secret=client_secret)
    client = Client(auth)

    # 2. Define Box File IDs (Update these with your real IDs from Box)
    file_ids = {
        'Group_A_Claims.csv': 'YOUR_ID_HERE', 
        'Group_B_Revenue.csv': 'YOUR_ID_HERE'
    }
    
    # 3. Download files
    for filename, file_id in file_ids.items():
        print(f"Downloading {filename}...")
        with open(filename, 'wb') as f:
            client.file(file_id).download_to(f)

    # 4. Standardize and Merge
    df_a = pd.read_csv('Group_A_Claims.csv')
    df_b = pd.read_csv('Group_B_Revenue.csv')

    # Ensure these column names match what is actually inside your Box CSVs
    df_a = df_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    df_b = df_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    combined = pd.concat([df_a, df_b], ignore_index=True)

    # 5. Export JSON
    summary = {
        "total_revenue": float(combined['Amount'].sum()),
        "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    }

    with open('data.json', 'w') as f:
        json.dump(summary, f, indent=4)
    print("Dashboard refreshed successfully.")

if __name__ == "__main__":
    congregate_data()
