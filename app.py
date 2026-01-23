"""
DigiGold Reconciliation Tool - SIMPLE VERSION
Deployed on Vercel
"""

from flask import Flask, render_template, request, send_file
import pandas as pd
from io import BytesIO
import os

app = Flask(__name__, static_folder=None)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max

def clean_key(value):
    """Clean and normalize keys"""
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def normalize_finfinity_status(status):
    """Normalize Finfinity Order Status"""
    if pd.isna(status):
        return "Fail"
    status = str(status).strip().upper()
    if status in ["PAID", "ACTIVE"]:
        return "Success"
    return "Fail"


def normalize_cashfree_status(status):
    """Normalize Cashfree Transaction Status"""
    if pd.isna(status):
        return "Fail"
    status = str(status).strip().upper()
    if status == "SUCCESS":
        return "Success"
    return "Fail"


def normalize_augmont_status(status):
    """Normalize Augmont Transaction Status"""
    if pd.isna(status):
        return "Fail"
    status = str(status).strip().lower()
    if "not cancelled" in status or status == "not cancelled":
        return "Success"
    return "Fail"


def classify_mismatch(fin_result, aug_result, cf_result):
    """
    Classify based on status combination matrix.
    Returns the category name for the sheet.
    """
    # Convert to tuple for matching
    combo = (fin_result, aug_result, cf_result)
    
    # Matrix mapping: (Fin, Aug, CF) -> sheet_index
    matrix = [
        ("Success", "Fail", "Fail"),      # Index 0
        ("Fail", "Success", "Success"),   # Index 1
        ("Success", "Success", "Fail"),   # Index 2
        ("Fail", "Success", "Success"),   # Index 3 (duplicate of 1)
        ("Success", "Success", "Fail"),   # Index 4 (duplicate of 2)
        ("Fail", "Fail", "Success"),      # Index 5
        ("Success", "Fail", "Success"),   # Index 6
    ]
    
    # Find matching index
    for idx, pattern in enumerate(matrix):
        if combo == pattern:
            return idx
    
    return None  # No match


def create_status_analysis(ks_df, cashfree_df, augmont_df, ks_result):
    """
    Create status mismatch analysis for alarmed records only.
    Returns dict with sheets data and counts.
    """
    # Prepare working copies
    ks_working = ks_result.copy()
    
    # Normalize statuses in each dataset
    ks_working["Finfinity_Result"] = ks_working.get("Order Status", pd.Series()).apply(normalize_finfinity_status)
    
    # Match Cashfree data
    cashfree_match = cashfree_df.copy()
    cashfree_match["_ord_clean"] = cashfree_match.get("Order Id", pd.Series()).apply(clean_key)
    cashfree_match["Cashfree_Result"] = cashfree_match.get("Transaction Status", pd.Series()).apply(normalize_cashfree_status)
    
    # Match Augmont data
    augmont_match = augmont_df.copy()
    augmont_match["_mtx_clean"] = augmont_match.get("Merchant Transaction Id", pd.Series()).apply(clean_key)
    augmont_match["Augmont_Result"] = augmont_match.get("Transaction Status", pd.Series()).apply(normalize_augmont_status)
    
    # Merge status data into KS working copy
    ks_working["_ord_clean"] = ks_working.get("Order Id", pd.Series()).apply(clean_key)
    ks_working["_mtx_clean"] = ks_working.get("Merchant Transaction ID", pd.Series()).apply(clean_key)
    
    # Add Cashfree status
    cashfree_status = cashfree_match[["_ord_clean", "Cashfree_Result"]].drop_duplicates(subset=["_ord_clean"])
    ks_working = ks_working.merge(cashfree_status, on="_ord_clean", how="left")
    ks_working["Cashfree_Result"] = ks_working["Cashfree_Result"].fillna("Fail")
    
    # Add Augmont status
    augmont_status = augmont_match[["_mtx_clean", "Augmont_Result"]].drop_duplicates(subset=["_mtx_clean"])
    ks_working = ks_working.merge(augmont_status, on="_mtx_clean", how="left")
    ks_working["Augmont_Result"] = ks_working["Augmont_Result"].fillna("Fail")
    
    # Filter for alarmed records only (missing in Cashfree OR missing in Augmont)
    alarmed = ks_working[(ks_working["In Cashfree?"] == "NO") | (ks_working["In Augmont?"] == "NO")].copy()
    
    # Classify each alarmed record
    alarmed["Mismatch_Category"] = alarmed.apply(
        lambda row: classify_mismatch(row["Finfinity_Result"], row["Augmont_Result"], row["Cashfree_Result"]),
        axis=1
    )
    
    # Sheet names for each category
    sheet_names = [
        "FIN_SUCCESS_AUG_FAIL_CF_FAIL",
        "FIN_FAIL_AUG_PASS_CF_PASS",
        "FIN_SUCCESS_AUG_PASS_CF_FAIL",
        "FIN_FAIL_AUG_PASS_CF_PASS_2",
        "FIN_SUCCESS_AUG_PASS_CF_FAIL_2",
        "FIN_FAIL_AUG_FAIL_CF_PASS",
        "FIN_SUCCESS_AUG_FAIL_CF_PASS",
    ]
    
    # Organize records by category
    category_sheets = {}
    for idx in range(7):
        category_data = alarmed[alarmed["Mismatch_Category"] == idx].copy()
        if not category_data.empty:
            # Select relevant columns
            cols_to_keep = ["Order Id", "Merchant Transaction ID", "Order Status", 
                           "Finfinity_Result", "Cashfree_Result", "Augmont_Result"]
            available_cols = [col for col in cols_to_keep if col in category_data.columns]
            category_sheets[sheet_names[idx]] = category_data[available_cols]
    
    # Summary data
    total_records = len(ks_df)
    total_alarmed = len(alarmed)
    category_counts = []
    for idx, sheet_name in enumerate(sheet_names):
        count = len(alarmed[alarmed["Mismatch_Category"] == idx])
        category_counts.append({
            "Category": sheet_name,
            "Count": count
        })
    
    summary = pd.DataFrame({
        "Metric": [
            "Total Finfinity Records",
            "Total Alarmed Records"
        ] + [f"Records in {name}" for name in sheet_names],
        "Count": [total_records, total_alarmed] + [row["Count"] for row in category_counts]
    })
    
    return category_sheets, summary

def reconcile_files(ks_file, cashfree_file, augmont_file):
    """Main reconciliation logic"""
    # Read files
    ks_df = pd.read_excel(ks_file)
    cashfree_df = pd.read_excel(cashfree_file)
    augmont_df = pd.read_excel(augmont_file)
    
    # Validate columns
    if "Merchant Transaction ID" not in ks_df.columns:
        return None, "KS file needs 'Merchant Transaction ID' column"
    if "Order Id" not in ks_df.columns:
        return None, "KS file needs 'Order Id' column"
    if "Order Id" not in cashfree_df.columns:
        return None, "Cashfree file needs 'Order Id' column"
    if "Merchant Transaction Id" not in augmont_df.columns:
        return None, "Augmont file needs 'Merchant Transaction Id' column"
    
    # Create working copies
    ks_working = ks_df.copy()
    cashfree_working = cashfree_df.copy()
    augmont_working = augmont_df.copy()
    
    # === KS vs Augmont ===
    ks_working["_mtx_clean"] = ks_working["Merchant Transaction ID"].apply(clean_key)
    augmont_working["_mtx_clean"] = augmont_working["Merchant Transaction Id"].apply(clean_key)
    
    # Add "In Augmont?" column
    ks_working["In Augmont?"] = ks_working["_mtx_clean"].isin(augmont_working["_mtx_clean"]).map({True: "YES", False: "NO"})
    
    # Find missing
    augmont_missing = augmont_working[~augmont_working["_mtx_clean"].isin(ks_working["_mtx_clean"])].copy()
    
    # === KS vs Cashfree ===
    ks_working["_ord_clean"] = ks_working["Order Id"].apply(clean_key)
    cashfree_working["_ord_clean"] = cashfree_working["Order Id"].apply(clean_key)
    
    # Add "In Cashfree?" column
    ks_working["In Cashfree?"] = ks_working["_ord_clean"].isin(cashfree_working["_ord_clean"]).map({True: "YES", False: "NO"})
    
    # Find missing
    cashfree_missing = cashfree_working[~cashfree_working["_ord_clean"].isin(ks_working["_ord_clean"])].copy()
    
    # Clean up temp columns
    ks_result = ks_working.drop(columns=["_mtx_clean", "_ord_clean"])
    augmont_missing = augmont_missing.drop(columns=["_mtx_clean"])
    cashfree_missing = cashfree_missing.drop(columns=["_ord_clean"])
    
    # === Create Missing Sheet ===
    if not augmont_missing.empty:
        augmont_missing["Source"] = "Augmont"
    if not cashfree_missing.empty:
        cashfree_missing["Source"] = "Cashfree"
    
    missing_combined = pd.concat([augmont_missing, cashfree_missing], ignore_index=True)
    
    # === Create Summary ===
    summary = pd.DataFrame({
        "Metric": [
            "Total KS records",
            "KS in Augmont",
            "KS missing from Augmont",
            "KS in Cashfree",
            "KS missing from Cashfree",
            "Augmont missing from KS",
            "Cashfree missing from KS",
            "In Both Augmont & Cashfree"
        ],
        "Count": [
            len(ks_df),
            (ks_result["In Augmont?"] == "YES").sum(),
            (ks_result["In Augmont?"] == "NO").sum(),
            (ks_result["In Cashfree?"] == "YES").sum(),
            (ks_result["In Cashfree?"] == "NO").sum(),
            len(augmont_missing),
            len(cashfree_missing),
            ((ks_result["In Augmont?"] == "YES") & (ks_result["In Cashfree?"] == "YES")).sum()
        ]
    })
    
    # Write to Excel - COMBINED OUTPUT (original + status analysis)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Original sheets
        ks_result.to_excel(writer, sheet_name='KS Reconciliation', index=False)
        if not missing_combined.empty:
            missing_combined.to_excel(writer, sheet_name='Missing in KS', index=False)
        summary.to_excel(writer, sheet_name='Summary', index=False)
        
        # Status analysis sheets (ADD-ON)
        status_sheets, status_summary = create_status_analysis(ks_df, cashfree_df, augmont_df, ks_result)
        
        # Write category sheets
        for sheet_name, sheet_data in status_sheets.items():
            if not sheet_data.empty:
                sheet_data.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Excel 31 char limit
        
        # Write status summary (with different name to avoid conflict)
        status_summary.to_excel(writer, sheet_name='Status Summary', index=False)
    
    output.seek(0)
    return output, None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/reconcile', methods=['POST'])
def reconcile():
    try:
        # Get files
        ks_file = request.files.get('ks_file')
        cashfree_file = request.files.get('cashfree_file')
        augmont_file = request.files.get('augmont_file')
        
        if not all([ks_file, cashfree_file, augmont_file]):
            return {'error': 'Please upload all 3 files'}, 400
        
        # Check file extensions
        for f in [ks_file, cashfree_file, augmont_file]:
            if not f.filename.endswith('.xlsx'):
                return {'error': f'{f.filename} must be .xlsx file'}, 400
        
        # Process
        result, error = reconcile_files(ks_file, cashfree_file, augmont_file)
        
        if error:
            return {'error': error}, 400
        
        # Return combined report (includes original + status analysis sheets)
        return send_file(result, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True, download_name='reconciliation_report.xlsx')
    
    except Exception as e:
        return {'error': f'Error: {str(e)}'}, 500


if __name__ == '__main__':
    app.run(debug=False, host='127.0.0.1', port=5000)