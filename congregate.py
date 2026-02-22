import pandas as pd
import json
import os
import sys

# Force Python to look in the current directory for the installed boxsdk
sys.path.append(os.getcwd())

def get_box_client():
    client_id = os.environ.get('BOX_CLIENT_ID')
    client_secret = os.environ.get('BOX_CLIENT_SECRET')

    # Attempt to use the newer Box SDK generation logic first
    try:
        from box_sdk_gen import BoxClient, BoxCCGAuth, CCGConfig
        print("Authenticated using Modern Box SDK")
        config = CCGConfig(client_id=client_id, client_secret=client_secret)
        auth = BoxCCGAuth(config)
        return BoxClient(auth)
    except ImportError:
        # Fallback to the legacy SDK if the new one isn't found
        from boxsdk import CCGAuth, Client
        print("Authenticated using Legacy Box SDK")
        auth = CCGAuth(client_id=client_id, client_secret=client_secret)
        return Client(auth)

def congregate_data():
    print("--- Starting Box Data Sync ---")
    client = get_box_client()
    
    # --- STEP 1: DEFINE BOX FILE IDS ---
    # Replace these numbers with your actual Box File IDs from the URLs
    file_ids = {
        'Group_A_Claims.csv': '2143561343275', 
        'Group_B_Revenue.csv': '2143561223806'
    }
    
    # --- STEP 2: DOWNLOAD FILES FROM BOX ---
    for filename, file_id in file_ids.items():
        print(f"Downloading {filename} (ID: {file_id})...")
        try:
            # Modern SDK download method
            if hasattr(client, 'files'):
                file_content = client.files.download_file(file_id)
                with open(filename, 'wb') as f:
                    f.write(file_content)
            # Legacy SDK download method
            else:
                with open(filename, 'wb') as f:
                    client.file(file_id).download_to(f)
            print(f"Successfully downloaded {filename}")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")
            return

    # --- STEP 3: PROCESS AND MERGE DATA ---
    try:
        df_a = pd.read_csv('Group_A_Claims.csv')
        df_b = pd.read_csv('Group_B_Revenue.csv')

        # Standardizing columns for the "Full Picture"
        df_a = df_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
        df_b = df_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

        combined = pd.concat([df_a, df_b], ignore_index=True)

        # Create JSON summary for the dashboard
        summary = {
            "total_revenue": float(combined['Amount'].sum()),
            "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
            "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        }

        with open('data.json', 'w') as f:
            json.dump(summary, f, indent=4)
        
        print(f"--- Sync Complete ---")
        print(f"Total Revenue: ${summary['total_revenue']:,.2f}")

    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    congregate_data()

