import sys
import json
import os
import pandas as pd

def clean_curr(val):
    if pd.isna(val) or val == '': return 0.0
    return float(str(val).replace('$', '').replace(',', '').strip() or 0)

if __name__ == "__main__":
    file_name = sys.argv[1].lower() if len(sys.argv) > 1 else 'unknown.csv'

    try:
        # 1. Read the CSV text
        df = pd.read_csv("temp.csv")
        df.columns = [str(c).strip() for c in df.columns] 
        
        # --- DIAGNOSTICS ---
        print("--- DIAGNOSTICS ---")
        print(f"File Name Received: {file_name}")
        print(f"Columns Found: {list(df.columns)}")
        print("-------------------")
        
        # 2. Build/Maintain JSON Structure
        data = {'claims': [], 'revenue': [], 'last_update': '', 'stats': {}}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                try: data = json.load(f)
                except: pass

        # 3. Process based on filename
        if 'claim' in file_name:
            data['claims'] = df.to_dict(orient='records')
            data['stats']['total_claims_value'] = df['Amount_Billed'].apply(clean_curr).sum()
        else:
            data['revenue'] = df.to_dict(orient='records')
            data['stats']['total_charges'] = df['Gross_Charge'].apply(clean_curr).sum()
            data['stats']['total_collected'] = df['Net_Collected'].apply(clean_curr).sum()

        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"SUCCESS: Synced {len(df)} rows from {file_name} via Pipedream!")

    except Exception as e:
        print(f"PROCESS ERROR: {str(e)}")
        # Print columns even if it fails so we can debug
        try:
            print(f"AVAILABLE COLUMNS WERE: {list(df.columns)}")
        except:
            pass
        sys.exit(1)

