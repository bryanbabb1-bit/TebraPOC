import pandas as pd
import json
import os

def congregate_data():
    # 1. Print files for debugging
    print("Files currently in the repo:", os.listdir('.'))

    # 2. Define filenames (Ensure these match your GitHub filenames exactly)
    file_a = 'Group_A_Claims.csv'
    file_b = 'Group_B_Revenue.csv'

    # 3. Validation: Check if they exist
    if not os.path.exists(file_a) or not os.path.exists(file_b):
        print(f"CRITICAL ERROR: Missing {file_a} or {file_b}")
        return

    # 4. Read the data
    group_a = pd.read_csv(file_a)
    group_b = pd.read_csv(file_b)

    # 5. Standardization Logic
    # Mapping 'Provider_Name' and 'Doctor' to one 'Provider' column
    # Mapping 'Amount_Billed' and 'Gross_Charge' to 'Amount'
    group_a_clean = group_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    group_b_clean = group_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    # 6. Combine into the "Full Picture"
    combined = pd.concat([group_a_clean, group_b_clean], ignore_index=True)

    # 7. Create JSON output (Fixes the 'int64' error)
    # We convert all numbers to standard Python floats so JSON can read them
    summary = {
        "total_revenue": float(combined['Amount'].sum()),
        "provider_breakdown": {str(k): float(v) for k, v in combined.groupby('Provider')['Amount'].sum().to_dict().items()},
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    }

    # 8. Save to file
    with open('data.json', 'w') as f:
        json.dump(summary, f, indent=4)
        
    print("--- SUCCESS ---")
    print(f"Total Revenue Congregated: ${summary['total_revenue']:,.2f}")
    print("data.json has been created and updated.")

if __name__ == "__main__":
    congregate_data()
