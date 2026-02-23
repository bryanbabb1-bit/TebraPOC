import os
import json
import pandas as pd
from boxsdk import CCGAuth, Client

def congregate_data():
    print("--- Starting Fresh Data Sync ---")
    
    # 1. Credentials
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')
    enterprise_id = '1444288525'

    # 2. Authentication
    print("Connecting to Box...")
    auth = CCGAuth(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id
    )
    client = Client(auth)

    # 3. Static IDs Provided by Bryan
    CLAIMS_FILE_ID = '2143561343275' 
    REVENUE_FILE_ID = '2143561223806'

    # 4. Download and Merge
    print(f"Downloading Claims and Revenue files...")
    with open('claims.csv', 'wb') as f:
        client.file(CLAIMS_FILE_ID).download_to(f)
    with open('revenue.csv', 'wb') as f:
        client.file(REVENUE_FILE_ID).download_to(f)

    df_claims = pd.read_csv('claims.csv')
    df_revenue = pd.read_csv('revenue.csv')

    # PractiSynergy Mapping
    df_claims = df_claims.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    df_revenue = df_revenue.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    combined = pd.concat([df_claims, df_revenue], ignore_index=True)
    
    # Generate Output
    summary = {
        "total_revenue": float(combined['Amount'].sum()),
        "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %I:%M %p")
    }

    with open('data.json', 'w') as f:
        json.dump(summary, f, indent=4)
    
    print(f"SUCCESS: Sync complete for {summary['last_updated']}")

if __name__ == "__main__":
    congregate_data()

