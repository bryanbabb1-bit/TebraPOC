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
        # 1. Read the CSV text & FIX NaNs
        df = pd.read_csv("temp.csv")
        df.columns = [str(c).strip() for c in df.columns] 
        df = df.fillna("") # THE FIX: Replaces illegal NaNs with safe empty strings
        
        # 2. Build/Maintain JSON Structure
        data = {'claims': [], 'revenue': [], 'last_update': '', 'stats': {}}
        if os.path.exists('data.json'):
            with open('data.json', 'r') as f:
                try: 
                    loaded = json.load(f)
                    data.update(loaded)
                except: pass
        
        if 'stats' not in data: data['stats'] = {}

        # 3. Smart Processing
        if 'Amount_Billed' in df.columns:
            print("Processing as CLAIMS...")
            data['claims'] = df.to_dict(orient='records')
            claims_total = df['Amount_Billed'].apply(clean_curr).sum()
            data['stats']['total_claims_value'] = claims_total
            data['total_claims'] = claims_total # Backwards compatibility for HTML
            
        elif 'Gross_Charge' in df.columns and 'Net_Collected' in df.columns:
            print("Processing as REVENUE...")
            data['revenue'] = df.to_dict(orient='records')
            
            gross = df['Gross_Charge'].apply(clean_curr).sum()
            net = df['Net_Collected'].apply(clean_curr).sum()
            
            data['stats']['total_charges'] = gross
            data['stats']['total_collected'] = net
            data['total_revenue'] = net # Backwards compatibility for HTML
            
        else:
            print("WARNING: Columns do not match known formats.")
            sys.exit(1)

        data['last_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %I:%M %p')
        
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"SUCCESS: Synced {len(df)} rows securely!")

    except Exception as e:
        print(f"PROCESS ERROR: {str(e)}")
        sys.exit(1)
