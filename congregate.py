import pandas as pd
import json
import os

# For the POC, we assume files are in a local 'feeds' folder
# Later, we add the Box SDK to download them here automatically
def congregate_data():
    # 1. Load the fictitious files
    group_a = pd.read_csv('Group_A_Claims.csv')
    group_b = pd.read_csv('Group_B_Revenue.csv')

    # 2. Standardization Logic
    # We map 'Provider_Name' and 'Doctor' to one 'Provider' column
    group_a_clean = group_a.rename(columns={'Provider_Name': 'Provider', 'Amount_Billed': 'Amount'})
    group_b_clean = group_b.rename(columns={'Doctor': 'Provider', 'Gross_Charge': 'Amount'})

    # 3. Join the "Full Picture"
    # We combine them into a single dataset
    combined = pd.concat([group_a_clean, group_b_clean], ignore_index=True)

    # 4. Create JSON for the index.html
    # We calculate the KPI totals here so the browser doesn't have to
    summary = {
        "total_revenue": combined['Amount'].sum(),
        "provider_breakdown": combined.groupby('Provider')['Amount'].sum().to_dict(),
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    }

    with open('data.json', 'w') as f:
        json.dump(summary, f)

if __name__ == "__main__":
    congregate_data()