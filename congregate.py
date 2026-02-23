import os
import json
import pandas as pd
from boxsdk import CCGAuth, Client

def congregate_data():
    print("--- Starting Stable Data Sync ---")
    
    # 1. Setup Auth
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    client = Client(auth)

    # 2. Hard-coded File IDs (The IDs we know worked)
    # Update these if you have specific newer versions you want to pin
    CLAIMS_FILE_ID = '1770258164319' 
    REVENUE_FILE_ID = '1770265215758'

    # 3. Download
    print("Downloading files...")
    with open('claims.csv', 'wb') as f:
        client.file(CLAIMS_FILE_ID).download_to(f)
    with open('revenue.csv', 'wb') as f:
        client.file(REVENUE_FILE_ID).download_to(f)

    # 4. Process
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
    
    print("SUCCESS: Build is stable and data is updated.")

if __name__ == "__main__":
    congregate_data()
