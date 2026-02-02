"""
DigiGold Reconciliation Tool - Production Version
Supports local development and Vercel deployment
"""

from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
from io import BytesIO
import os
import traceback

app = Flask(__name__, static_folder=None)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max

# Production configuration
app.config['ENV'] = os.getenv('FLASK_ENV', 'production')
if app.config['ENV'] == 'development':
    app.config['DEBUG'] = True

def clean_key(value):
    """Clean and normalize keys"""
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def classify_by_decision_table(fin_status, cf_status, aug_status):
    """
    Apply master decision table logic to determine action required.
    Returns: (category_name, action_required, priority)
    """
    # Normalize statuses
    fin_status = str(fin_status).strip().upper() if not pd.isna(fin_status) else "MISSING"
    cf_status = str(cf_status).strip().upper() if not pd.isna(cf_status) else "MISSING"
    aug_status = str(aug_status).strip().lower() if not pd.isna(aug_status) else "missing"
    
    # Master Decision Table
    # Priority: 1=NO ACTION, 2=MONITOR, 3=INVESTIGATE, 4=CRITICAL
    
    # Fully successful transaction
    if fin_status in ["PAID", "ACTIVE"] and cf_status == "SUCCESS" and "not cancelled" in aug_status:
        return ("FULLY_RECONCILED", "NO ACTION", 1)
    
    # Money taken but order cancelled - REFUND REQUIRED
    if fin_status in ["PAID", "ACTIVE"] and cf_status == "SUCCESS" and "cancelled" in aug_status:
        return ("REFUND_REQUIRED", "REFUND REQUIRED", 4)
    
    # Payment done, internal update pending
    if fin_status == "PENDING" and cf_status == "SUCCESS" and "not cancelled" in aug_status:
        return ("SYNC_PENDING", "SYNC / MONITOR", 2)
    
    # Gateway success but internal failure
    if fin_status == "FAILED" and cf_status == "SUCCESS" and "not cancelled" in aug_status:
        return ("GATEWAY_SUCCESS_INTERNAL_FAIL", "INVESTIGATE", 3)
    
    # Payment failed - can be ignored
    if cf_status == "FAILED":
        return ("PAYMENT_FAILED", "IGNORE", 1)
    
    # User abandoned payment
    if "USER" in cf_status and "DROP" in cf_status:
        return ("USER_DROPPED", "IGNORE", 1)
    
    # Payment in progress
    if fin_status == "PENDING" and cf_status == "PENDING":
        return ("PAYMENT_IN_PROGRESS", "WAIT / RETRY", 2)
    
    # Order created but payment failed
    if fin_status == "ACTIVE" and cf_status == "FAILED":
        return ("ORDER_ACTIVE_PAYMENT_FAILED", "CANCEL ORDER", 3)
    
    # Inconsistent state - paid but payment failed
    if fin_status == "PAID" and cf_status == "FAILED":
        return ("INCONSISTENT_STATE", "INVESTIGATE", 4)
    
    # Payment success but order missing in Augmont
    if cf_status == "SUCCESS" and aug_status == "missing":
        return ("PAYMENT_SUCCESS_ORDER_MISSING", "INVESTIGATE / CREATE ORDER", 4)
    
    # Payment not confirmed
    if cf_status == "PENDING":
        return ("PAYMENT_NOT_CONFIRMED", "WAIT / RETRY", 2)
    
    # Internal failure
    if fin_status == "FAILED":
        return ("INTERNAL_FAILURE", "INVESTIGATE", 3)
    
    # Default case - needs investigation
    return ("UNCATEGORIZED", "INVESTIGATE", 3)


def reconcile_files(finfinity_file, cashfree_file, augmont_file):
    """Main reconciliation logic - creates single Excel workbook with all sheets"""
    # Read files
    finfinity_df = pd.read_excel(finfinity_file)
    cashfree_df = pd.read_excel(cashfree_file)
    augmont_df = pd.read_excel(augmont_file)

    # Validate columns
    if "Merchant Transaction ID" not in finfinity_df.columns:
        return None, "Finfinity file needs 'Merchant Transaction ID' column"
    if "Order Id" not in finfinity_df.columns:
        return None, "Finfinity file needs 'Order Id' column"
    if "Order Id" not in cashfree_df.columns:
        return None, "Cashfree file needs 'Order Id' column"
    if "Merchant Transaction Id" not in augmont_df.columns:
        return None, "Augmont file needs 'Merchant Transaction Id' column"

    # Create working copies
    fin_working = finfinity_df.copy()
    cf_working = cashfree_df.copy()
    aug_working = augmont_df.copy()

    # === Matching Logic ===
    fin_working["_ord_clean"] = fin_working["Order Id"].apply(clean_key)
    fin_working["_mtx_clean"] = fin_working["Merchant Transaction ID"].apply(clean_key)

    cf_working["_ord_clean"] = cf_working["Order Id"].apply(clean_key)
    aug_working["_mtx_clean"] = aug_working["Merchant Transaction Id"].apply(clean_key)

    # Add matching flags
    fin_working["In Cashfree?"] = fin_working["_ord_clean"].isin(cf_working["_ord_clean"]).map({True: "YES", False: "NO"})
    fin_working["In Augmont?"] = fin_working["_mtx_clean"].isin(aug_working["_mtx_clean"]).map({True: "YES", False: "NO"})

    # Create ALARMED_RECORDS sheet - Finfinity records missing from Cashfree OR Augmont
    alarmed_records = fin_working[(fin_working["In Cashfree?"] == "NO") | (fin_working["In Augmont?"] == "NO")].copy()
    alarmed_clean = alarmed_records.drop(columns=["_ord_clean", "_mtx_clean"])

    # === Apply Decision Table to Alarmed Records ===
    alarmed_with_status = alarmed_records.copy()

    # Add status columns from all three systems
    # Finfinity status
    if "Order Status" in finfinity_df.columns:
        fin_status_map = finfinity_df[["Merchant Transaction ID", "Order Status"]].drop_duplicates(subset=["Merchant Transaction ID"])
        fin_status_map["_mtx_clean"] = fin_status_map["Merchant Transaction ID"].apply(clean_key)
        alarmed_with_status = alarmed_with_status.merge(
            fin_status_map[["_mtx_clean", "Order Status"]].rename(columns={"Order Status": "Finfinity_Status"}),
            on="_mtx_clean", how="left"
        )
    else:
        alarmed_with_status["Finfinity_Status"] = "MISSING"

    # Cashfree status
    if "Transaction Status" in cashfree_df.columns:
        cf_status_map = cashfree_df[["Order Id", "Transaction Status"]].drop_duplicates(subset=["Order Id"])
        cf_status_map["_ord_clean"] = cf_status_map["Order Id"].apply(clean_key)
        alarmed_with_status = alarmed_with_status.merge(
            cf_status_map[["_ord_clean", "Transaction Status"]].rename(columns={"Transaction Status": "Cashfree_Status"}),
            on="_ord_clean", how="left"
        )
    else:
        alarmed_with_status["Cashfree_Status"] = "MISSING"

    # Augmont status
    if "Transaction Status" in augmont_df.columns:
        aug_status_map = augmont_df[["Merchant Transaction Id", "Transaction Status"]].drop_duplicates(subset=["Merchant Transaction Id"])
        aug_status_map["_mtx_clean"] = aug_status_map["Merchant Transaction Id"].apply(clean_key)
        alarmed_with_status = alarmed_with_status.merge(
            aug_status_map[["_mtx_clean", "Transaction Status"]].rename(columns={"Transaction Status": "Augmont_Status"}),
            on="_mtx_clean", how="left"
        )
    else:
        alarmed_with_status["Augmont_Status"] = "MISSING"

    # Apply decision table classification
    alarmed_with_status[["Decision_Category", "Action_Required", "Priority"]] = alarmed_with_status.apply(
        lambda row: pd.Series(classify_by_decision_table(
            row.get("Finfinity_Status", "MISSING"),
            row.get("Cashfree_Status", "MISSING"),
            row.get("Augmont_Status", "MISSING")
        )),
        axis=1
    )

    # Clean up temp columns
    alarmed_with_status = alarmed_with_status.drop(columns=["_ord_clean", "_mtx_clean"], errors='ignore')

    # === Group by Action Required ===
    action_categories = alarmed_with_status["Action_Required"].unique()
    action_categories_sorted = sorted([cat for cat in action_categories if cat], 
                                     key=lambda x: alarmed_with_status[alarmed_with_status["Action_Required"] == x]["Priority"].iloc[0],
                                     reverse=True)

    # === Create Complete Finfinity View ===
    # Merge all Finfinity records with statuses from all systems
    complete_finfinity = fin_working.copy()
    
    # Add Finfinity status
    if "Order Status" in finfinity_df.columns:
        complete_finfinity["Finfinity_Status"] = complete_finfinity["Order Status"]
    else:
        complete_finfinity["Finfinity_Status"] = "MISSING"
    
    # Add Cashfree status
    if "Transaction Status" in cashfree_df.columns:
        cf_status_map = cashfree_df[["Order Id", "Transaction Status"]].drop_duplicates(subset=["Order Id"])
        cf_status_map["_ord_clean"] = cf_status_map["Order Id"].apply(clean_key)
        complete_finfinity = complete_finfinity.merge(
            cf_status_map[["_ord_clean", "Transaction Status"]].rename(columns={"Transaction Status": "Cashfree_Status"}),
            on="_ord_clean", how="left"
        )
        complete_finfinity["Cashfree_Status"] = complete_finfinity["Cashfree_Status"].fillna("MISSING")
    else:
        complete_finfinity["Cashfree_Status"] = "MISSING"
    
    # Add Augmont status
    if "Transaction Status" in augmont_df.columns:
        aug_status_map = augmont_df[["Merchant Transaction Id", "Transaction Status"]].drop_duplicates(subset=["Merchant Transaction Id"])
        aug_status_map["_mtx_clean"] = aug_status_map["Merchant Transaction Id"].apply(clean_key)
        complete_finfinity = complete_finfinity.merge(
            aug_status_map[["_mtx_clean", "Transaction Status"]].rename(columns={"Transaction Status": "Augmont_Status"}),
            on="_mtx_clean", how="left"
        )
        complete_finfinity["Augmont_Status"] = complete_finfinity["Augmont_Status"].fillna("MISSING")
    else:
        complete_finfinity["Augmont_Status"] = "MISSING"
    
    # Apply decision table to ALL records
    complete_finfinity[["Decision_Category", "Action_Required", "Priority"]] = complete_finfinity.apply(
        lambda row: pd.Series(classify_by_decision_table(
            row.get("Finfinity_Status", "MISSING"),
            row.get("Cashfree_Status", "MISSING"),
            row.get("Augmont_Status", "MISSING")
        )),
        axis=1
    )
    
    # Clean up temp columns
    complete_finfinity = complete_finfinity.drop(columns=["_ord_clean", "_mtx_clean"], errors='ignore')

    # === Create SUMMARY Sheet ===
    total_finfinity = len(finfinity_df)
    total_alarmed = len(alarmed_records)
    total_reconciled = len(complete_finfinity[complete_finfinity["Action_Required"] == "NO ACTION"])

    # Count records by action category
    action_summary = complete_finfinity.groupby("Action_Required").agg({
        "Order Id": "count",
        "Priority": "first"
    }).reset_index()
    action_summary.columns = ["Action Required", "Count", "Priority"]
    action_summary = action_summary.sort_values("Priority", ascending=False)

    # Create overall summary
    summary_df = pd.DataFrame({
        "Metric": ["Total Finfinity Records", "Fully Reconciled", "Needs Review/Action"],
        "Count": [total_finfinity, total_reconciled, total_finfinity - total_reconciled]
    })

    # Create status combination column for grouping
    complete_finfinity["Status_Combination"] = (
        "FIN_" + complete_finfinity["Finfinity_Status"].astype(str) + 
        "_CF_" + complete_finfinity["Cashfree_Status"].astype(str) + 
        "_AUG_" + complete_finfinity["Augmont_Status"].astype(str)
    )
    
    # Get unique status combinations and sort by count (most frequent first)
    status_combo_counts = complete_finfinity.groupby("Status_Combination").size().reset_index(name='count')
    status_combo_counts = status_combo_counts.sort_values('count', ascending=False)
    status_combinations = status_combo_counts["Status_Combination"].tolist()

    # === Create Single Excel Workbook ===
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # 1. SUMMARY
        summary_df.to_excel(writer, sheet_name='SUMMARY', index=False)
        
        # 2. ACTION_SUMMARY - Breakdown by action required
        action_summary[["Action Required", "Count"]].to_excel(writer, sheet_name='ACTION_SUMMARY', index=False)
        
        # 3. STATUS_COMBINATION_SUMMARY - Breakdown by status combinations
        status_summary = complete_finfinity.groupby(["Finfinity_Status", "Cashfree_Status", "Augmont_Status"]).agg({
            "Order Id": "count"
        }).reset_index()
        status_summary.columns = ["Finfinity Status", "Cashfree Status", "Augmont Status", "Count"]
        status_summary = status_summary.sort_values("Count", ascending=False)
        status_summary.to_excel(writer, sheet_name='STATUS_COMBINATIONS', index=False)

        # 4. COMPLETE_FINFINITY - All Finfinity records with matching flags and statuses
        complete_finfinity_display = complete_finfinity.drop(columns=["Status_Combination"], errors='ignore')
        complete_finfinity_display.to_excel(writer, sheet_name='COMPLETE_FINFINITY', index=False)

        # 5. MISSING_IN_CASHFREE - Finfinity records not in Cashfree
        missing_cf = complete_finfinity[complete_finfinity["In Cashfree?"] == "NO"].copy()
        missing_cf = missing_cf.drop(columns=["Status_Combination"], errors='ignore')
        missing_cf.to_excel(writer, sheet_name='MISSING_IN_CASHFREE', index=False)

        # 6. MISSING_IN_AUGMONT - Finfinity records not in Augmont
        missing_aug = complete_finfinity[complete_finfinity["In Augmont?"] == "NO"].copy()
        missing_aug = missing_aug.drop(columns=["Status_Combination"], errors='ignore')
        missing_aug.to_excel(writer, sheet_name='MISSING_IN_AUGMONT', index=False)

        # 7. MISSING_IN_BOTH - Finfinity records missing in both Cashfree and Augmont
        missing_both = complete_finfinity[(complete_finfinity["In Cashfree?"] == "NO") & (complete_finfinity["In Augmont?"] == "NO")].copy()
        missing_both = missing_both.drop(columns=["Status_Combination"], errors='ignore')
        missing_both.to_excel(writer, sheet_name='MISSING_IN_BOTH', index=False)

        # 8-N. Status-combination-specific sheets (based on complete Finfinity data)
        for combo in status_combinations:
            combo_data = complete_finfinity[complete_finfinity["Status_Combination"] == combo].copy()
            combo_data = combo_data.drop(columns=["Status_Combination"], errors='ignore')
            
            # Create readable sheet name from combination (max 31 chars)
            sheet_name = combo[:31]
            
            if not combo_data.empty:
                combo_data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Additional raw data sheets at the end
        finfinity_df.to_excel(writer, sheet_name='RAW_FINFINITY', index=False)
        cashfree_df.to_excel(writer, sheet_name='RAW_CASHFREE', index=False)
        augmont_df.to_excel(writer, sheet_name='RAW_AUGMONT', index=False)

    output.seek(0)
    return output, None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/reconcile', methods=['POST'])
def reconcile():
    try:
        # Get files
        finfinity_file = request.files.get('finfinity_file')
        cashfree_file = request.files.get('cashfree_file')
        augmont_file = request.files.get('augmont_file')
        
        # Validation
        if not all([finfinity_file, cashfree_file, augmont_file]):
            return jsonify({'error': 'Please upload all 3 files'}), 400
        
        # Check if files have filenames
        if not all([f.filename for f in [finfinity_file, cashfree_file, augmont_file]]):
            return jsonify({'error': 'Invalid file upload'}), 400
        
        # Check file extensions
        for f in [finfinity_file, cashfree_file, augmont_file]:
            if not f.filename.endswith('.xlsx'):
                return jsonify({'error': f'{f.filename} must be .xlsx file'}), 400
        
        # Process reconciliation
        result, error = reconcile_files(finfinity_file, cashfree_file, augmont_file)
        
        if error:
            return jsonify({'error': error}), 400
        
        # Return single Excel workbook with all sheets
        return send_file(
            result, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True, 
            download_name='reconciliation_output.xlsx'
        )
    
    except Exception as e:
        # Log error for debugging (in production, use proper logging)
        print(f"Error during reconciliation: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint for monitoring"""
    return jsonify({'status': 'healthy', 'service': 'DigiGold Reconciliation'}), 200


if __name__ == '__main__':
    # Run in development mode locally
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    print(f"\n{'='*60}")
    print(f"DigiGold Reconciliation Tool")
    print(f"{'='*60}")
    print(f"Server running on: http://127.0.0.1:{port}")
    print(f"Environment: {'Development' if debug else 'Production'}")
    print(f"{'='*60}\n")
    app.run(debug=debug, host='0.0.0.0', port=port)