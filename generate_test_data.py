#!/usr/bin/env python3
"""
Generate comprehensive test datasets for DigiGold Reconciliation Tool
Creates diverse, randomized data covering all reconciliation scenarios
"""

import pandas as pd
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility (can change for different datasets)
random.seed(42)

def random_date(start_days_ago=60, end_days_ago=1):
    """Generate random date within range"""
    days_ago = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d %H:%M:%S')

def random_amount():
    """Generate random transaction amount"""
    return round(random.uniform(100, 50000), 2)

def random_gold_weight():
    """Generate random gold weight in grams"""
    return round(random.uniform(0.1, 10.0), 4)

# Generate 50 Finfinity records with diverse statuses
finfinity_data = []
order_statuses = ['PAID', 'ACTIVE', 'PENDING', 'FAILED', 'CANCELLED', 'PROCESSING', 'ON_HOLD']
transaction_types = ['BUY', 'SELL', 'TRANSFER']

for i in range(1, 51):
    order_id = f'ORD{i:04d}'
    merchant_txn_id = f'MTX{i:04d}'
    status = random.choice(order_statuses)
    
    finfinity_data.append({
        'Order Id': order_id,
        'Merchant Transaction ID': merchant_txn_id,
        'Order Status': status,
        'Transaction Type': random.choice(transaction_types),
        'Customer Name': f'Customer_{random.randint(1, 30)}',
        'Customer Email': f'customer{random.randint(1, 30)}@example.com',
        'Amount (INR)': random_amount(),
        'Gold Weight (g)': random_gold_weight(),
        'Order Date': random_date(),
        'Payment Method': random.choice(['UPI', 'Card', 'Net Banking', 'Wallet']),
        'Order Source': random.choice(['Mobile App', 'Website', 'API'])
    })

finfinity_df = pd.DataFrame(finfinity_data)

# Generate Cashfree records - intentionally missing some orders for mismatch testing
cashfree_data = []
cashfree_statuses = ['SUCCESS', 'FAILED', 'PENDING', 'USER_DROPPED', 'CANCELLED']

# Include 70% of Finfinity orders (35 out of 50)
included_orders = random.sample(range(1, 51), 35)

for i in included_orders:
    order_id = f'ORD{i:04d}'
    # Randomly align or misalign status with Finfinity
    if random.random() < 0.7:  # 70% match Finfinity success/fail pattern
        fin_status = finfinity_df[finfinity_df['Order Id'] == order_id]['Order Status'].values[0]
        if fin_status in ['PAID', 'ACTIVE']:
            cf_status = 'SUCCESS'
        else:
            cf_status = random.choice(['FAILED', 'PENDING', 'USER_DROPPED'])
    else:  # 30% intentional mismatch
        cf_status = random.choice(cashfree_statuses)
    
    cashfree_data.append({
        'Order Id': order_id,
        'Transaction Status': cf_status,
        'Payment Method': random.choice(['UPI', 'CARD', 'NETBANKING', 'WALLET']),
        'Amount': random_amount(),
        'Transaction Date': random_date(),
        'Payment Gateway': 'Cashfree',
        'Transaction ID': f'CF{random.randint(100000, 999999)}',
        'Bank Reference': f'BNK{random.randint(1000000, 9999999)}',
        'Settlement Status': random.choice(['SETTLED', 'PENDING', 'UNSETTLED'])
    })

# Add some extra Cashfree records not in Finfinity (orphan transactions)
for i in range(51, 56):
    order_id = f'ORD{i:04d}'
    cashfree_data.append({
        'Order Id': order_id,
        'Transaction Status': random.choice(cashfree_statuses),
        'Payment Method': random.choice(['UPI', 'CARD', 'NETBANKING', 'WALLET']),
        'Amount': random_amount(),
        'Transaction Date': random_date(),
        'Payment Gateway': 'Cashfree',
        'Transaction ID': f'CF{random.randint(100000, 999999)}',
        'Bank Reference': f'BNK{random.randint(1000000, 9999999)}',
        'Settlement Status': random.choice(['SETTLED', 'PENDING', 'UNSETTLED'])
    })

cashfree_df = pd.DataFrame(cashfree_data)

# Generate Augmont records - intentionally missing some transactions
augmont_data = []
augmont_statuses = ['not cancelled', 'cancelled', 'pending', 'failed']

# Include 75% of Finfinity transactions (37-38 out of 50)
included_txns = random.sample(range(1, 51), 38)

for i in included_txns:
    merchant_txn_id = f'MTX{i:04d}'
    # Randomly align or misalign status
    if random.random() < 0.75:  # 75% match Finfinity success/fail pattern
        fin_status = finfinity_df[finfinity_df['Merchant Transaction ID'] == merchant_txn_id]['Order Status'].values[0]
        if fin_status in ['PAID', 'ACTIVE']:
            aug_status = 'not cancelled'
        else:
            aug_status = random.choice(['cancelled', 'failed', 'pending'])
    else:  # 25% intentional mismatch
        aug_status = random.choice(augmont_statuses)
    
    augmont_data.append({
        'Merchant Transaction Id': merchant_txn_id,
        'Transaction Status': aug_status,
        'Gold Weight': random_gold_weight(),
        'Transaction Amount': random_amount(),
        'Transaction Date': random_date(),
        'Metal Type': random.choice(['Gold 24K', 'Gold 22K', 'Silver']),
        'Transaction Type': random.choice(['BUY', 'SELL']),
        'Augmont Order ID': f'AUG{random.randint(100000, 999999)}',
        'Block ID': f'BLK{random.randint(10000, 99999)}',
        'Vault Status': random.choice(['STORED', 'IN_TRANSIT', 'DELIVERED'])
    })

# Add some extra Augmont records not in Finfinity (orphan transactions)
for i in range(51, 57):
    merchant_txn_id = f'MTX{i:04d}'
    augmont_data.append({
        'Merchant Transaction Id': merchant_txn_id,
        'Transaction Status': random.choice(augmont_statuses),
        'Gold Weight': random_gold_weight(),
        'Transaction Amount': random_amount(),
        'Transaction Date': random_date(),
        'Metal Type': random.choice(['Gold 24K', 'Gold 22K', 'Silver']),
        'Transaction Type': random.choice(['BUY', 'SELL']),
        'Augmont Order ID': f'AUG{random.randint(100000, 999999)}',
        'Block ID': f'BLK{random.randint(10000, 99999)}',
        'Vault Status': random.choice(['STORED', 'IN_TRANSIT', 'DELIVERED'])
    })

augmont_df = pd.DataFrame(augmont_data)

# Save to Excel files
print("Generating test datasets...")
print("=" * 60)

finfinity_df.to_excel('test_finfinity.xlsx', index=False, engine='openpyxl')
print(f"✓ Created test_finfinity.xlsx with {len(finfinity_df)} Finfinity records")
print(f"  Status distribution: {finfinity_df['Order Status'].value_counts().to_dict()}")

cashfree_df.to_excel('test_cashfree.xlsx', index=False, engine='openpyxl')
print(f"\n✓ Created test_cashfree.xlsx with {len(cashfree_df)} Cashfree records")
print(f"  Status distribution: {cashfree_df['Transaction Status'].value_counts().to_dict()}")
print(f"  Records matching Finfinity: ~{len([i for i in included_orders if i <= 50])}")
print(f"  Orphan records (not in Finfinity): {len([i for i in range(51, 56)])}")

augmont_df.to_excel('test_augmont.xlsx', index=False, engine='openpyxl')
print(f"\n✓ Created test_augmont.xlsx with {len(augmont_df)} Augmont records")
print(f"  Status distribution: {augmont_df['Transaction Status'].value_counts().to_dict()}")
print(f"  Records matching Finfinity: ~{len([i for i in included_txns if i <= 50])}")
print(f"  Orphan records (not in Finfinity): {len([i for i in range(51, 57)])}")

print("\n" + "=" * 60)
print("Test Coverage:")
print(f"  Total Finfinity records: {len(finfinity_df)}")
print(f"  Missing from Cashfree: ~{50 - len([i for i in included_orders if i <= 50])} records")
print(f"  Missing from Augmont: ~{50 - len([i for i in included_txns if i <= 50])} records")
print(f"  Expected ALARMED records: Records missing from either system")
print(f"  Expected mismatch categories: Various Success/Fail combinations")

print("\n✓ Test datasets generated successfully!")
print("Run 'python test_reconciliation.py' to test the reconciliation logic")
