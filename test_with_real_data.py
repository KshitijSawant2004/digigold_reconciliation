#!/usr/bin/env python3
"""
Test reconciliation with real data from the data folder
"""

import pandas as pd
from app import reconcile_files

def test_real_data():
    """Test reconciliation with actual data files"""
    
    print("Testing DigiGold Reconciliation with Real Data")
    print("=" * 60)
    
    # File paths
    finfinity_file = "data/query_result_2026-01-22T09_06_32.412978294Z.xlsx"
    cashfree_file = "data/cashfree.csv"
    augmont_file = "data/augmont.csv"
    
    print(f"Loading files...")
    print(f"  - Finfinity: {finfinity_file} (XLSX)")
    print(f"  - Cashfree: {cashfree_file} (CSV)")
    print(f"  - Augmont: {augmont_file} (CSV)")
    
    # Load the actual data to check columns
    fin_df = pd.read_excel(finfinity_file)
    cf_df = pd.read_csv(cashfree_file)
    aug_df = pd.read_csv(augmont_file)
    
    print(f"\nData loaded:")
    print(f"  - Finfinity: {len(fin_df)} rows")
    print(f"  - Cashfree: {len(cf_df)} rows")
    print(f"  - Augmont: {len(aug_df)} rows")
    
    # Check required columns
    print(f"\nChecking required columns...")
    
    required_cols = {
        "Finfinity": ["Merchant Transaction ID", "Order Id", "Order Status"],
        "Cashfree": ["Order Id", "Transaction Status"],
        "Augmont": ["Merchant Transaction Id", "Transaction Status"]
    }
    
    # Check Finfinity
    fin_has = []
    for col in required_cols["Finfinity"]:
        if col in fin_df.columns:
            fin_has.append(f"✓ {col}")
        else:
            fin_has.append(f"✗ {col} (MISSING)")
    
    print(f"  Finfinity:")
    for status in fin_has:
        print(f"    {status}")
    
    # Check Cashfree
    cf_has = []
    for col in required_cols["Cashfree"]:
        if col in cf_df.columns:
            cf_has.append(f"✓ {col}")
        else:
            cf_has.append(f"✗ {col} (MISSING)")
    
    print(f"  Cashfree:")
    for status in cf_has:
        print(f"    {status}")
    
    # Check Augmont
    aug_has = []
    for col in required_cols["Augmont"]:
        if col in aug_df.columns:
            aug_has.append(f"✓ {col}")
        else:
            aug_has.append(f"✗ {col} (MISSING)")
    
    print(f"  Augmont:")
    for status in aug_has:
        print(f"    {status}")
    
    # Try reconciliation
    print(f"\n{'='*60}")
    print("Running reconciliation with CSV support...")
    
    try:
        # Pass file paths directly (app now supports CSV)
        result, error = reconcile_files(finfinity_file, cashfree_file, augmont_file)
        
        if error:
            print(f"ERROR: {error}")
            return False
        
        # Save result
        output_file = "real_data_reconciliation_output.xlsx"
        with open(output_file, "wb") as f:
            f.write(result.getvalue())
        
        print(f"✓ Reconciliation completed successfully!")
        print(f"✓ Output saved to: {output_file}")
        
        # Analyze the output
        result.seek(0)
        xl = pd.ExcelFile(result)
        
        print(f"\nOutput contains {len(xl.sheet_names)} sheets:")
        for sheet_name in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet_name)
            print(f"  - {sheet_name}: {len(df)} rows")
        
        print(f"\n{'='*60}")
        print("SUCCESS! CSV and XLSX files processed correctly!")
        return True
        
    except Exception as e:
        print(f"ERROR during reconciliation: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_real_data()
    exit(0 if success else 1)
