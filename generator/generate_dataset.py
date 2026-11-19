
# !pip install pandas numpy
import numpy as np
import pandas as pd
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================
OUT_DIR = Path("../data")
SEED = 42
N = 1_000_000

rng = np.random.default_rng(SEED)

# ============================================================
# 1. GENERATE ORDERS DATASET
# ============================================================

# Order IDs
order_ids = np.array(
    [f"ORD{i:09d}" for i in range(1, N + 1)],
    dtype=object
)

# Introduce duplicate order IDs (10,000 records)
dup_idx = rng.choice(N, size=10_000, replace=False)
order_ids[dup_idx] = order_ids[
    rng.integers(0, N, size=len(dup_idx))
]

# Customer IDs
customer_ids = np.array(
    [f"CUST{i:08d}" for i in rng.integers(1, 250_001, size=N)],
    dtype=object
)

# Missing customer IDs (20,000 records)
customer_ids[
    rng.choice(N, size=20_000, replace=False)
] = ""

# Order dates
start = np.datetime64("2023-01-01")
days = rng.integers(0, 1400, size=N)

dates = (
    start + days.astype("timedelta64[D]")
).astype("datetime64[D]").astype(str).astype(object)

# Invalid dates (5,000 records)
bad_date_idx = rng.choice(N, size=5_000, replace=False)

dates[bad_date_idx] = rng.choice(
    [
        "2023-02-30",
        "not-a-date",
        "31/13/2024",
        "2024-99-01"
    ],
    size=len(bad_date_idx)
)

# Product categories
categories = rng.choice(
    [
        "Electronics",
        "Clothing",
        "Home",
        "Beauty",
        "Sports",
        "Books",
        "Grocery"
    ],
    size=N
)

# Quantity
quantity = rng.integers(1, 11, size=N).astype(object)

# Invalid quantity values (3,000 records)
bad_qty_idx = rng.choice(N, size=3_000, replace=False)
quantity[bad_qty_idx] = "invalid"

# Unit prices
unit_price = np.round(
    rng.lognormal(mean=3.2, sigma=0.8, size=N),
    2
).astype(object)

# Negative prices (8,000 records)
neg_price_idx = rng.choice(N, size=8_000, replace=False)
unit_price[neg_price_idx] = -np.abs(
    np.asarray(unit_price[neg_price_idx], dtype=float)
)

# Invalid unit prices (3,000 records)
bad_price_idx = rng.choice(N, size=3_000, replace=False)
unit_price[bad_price_idx] = "N/A"

# Order statuses
order_status = rng.choice(
    ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"],
    size=N,
    p=[0.12, 0.18, 0.48, 0.12, 0.10]
)

# Countries
country = rng.choice(
    [
        "Bangladesh",
        "United States",
        "United Kingdom",
        "India",
        "Canada",
        "Australia",
        "Germany",
        "France",
        "Japan",
        "Singapore"
    ],
    size=N
)

# Create orders DataFrame
orders = pd.DataFrame({
    "order_id": order_ids,
    "customer_id": customer_ids,
    "order_date": dates,
    "category": categories,
    "quantity": quantity,
    "unit_price": unit_price,
    "order_status": order_status,
    "country": country
})

# Save orders CSV
orders_path = OUT_DIR / "orders.csv"
orders.to_csv(orders_path, index=False)

# ============================================================
# 2. GENERATE TRANSACTIONS DATASET
# ============================================================

# Transaction IDs
txn_ids = np.array(
    [f"TXN{i:09d}" for i in range(1, N + 1)],
    dtype=object
)

# Order IDs linked to transactions
txn_order_ids = np.array(
    [f"ORD{i:09d}" for i in rng.integers(1, N + 1, size=N)],
    dtype=object
)

# Orphan transaction order IDs (15,000 records)
orphan_idx = rng.choice(N, size=15_000, replace=False)

txn_order_ids[orphan_idx] = np.array(
    [f"ORPHAN{i:08d}" for i in range(len(orphan_idx))],
    dtype=object
)

# Payment methods
methods = rng.choice(
    [
        "CARD",
        "BANK_TRANSFER",
        "MOBILE_WALLET",
        "CASH",
        "PAYPAL"
    ],
    size=N,
    p=[0.42, 0.18, 0.22, 0.10, 0.08]
)

# Transaction amounts
amounts = np.round(
    rng.lognormal(mean=4.0, sigma=0.9, size=N),
    2
).astype(object)

# Invalid numeric values (5,000 records)
bad_amount_idx = rng.choice(N, size=5_000, replace=False)

amounts[bad_amount_idx] = rng.choice(
    ["invalid", "N/A", "unknown"],
    size=len(bad_amount_idx)
)

# Transaction statuses
txn_status = rng.choice(
    ["SUCCESS", "FAILED", "PENDING", "REFUNDED"],
    size=N,
    p=[0.78, 0.10, 0.08, 0.04]
).astype(object)

# Incorrect payment statuses (12,000 records)
bad_status_idx = rng.choice(N, size=12_000, replace=False)

txn_status[bad_status_idx] = rng.choice(
    ["SUCCES", "PAIDD", "UNKNOWN", "COMPLETEE"],
    size=len(bad_status_idx)
)

# Currencies
currencies = rng.choice(
    ["BDT", "USD", "GBP", "EUR", "INR", "CAD", "AUD", "JPY", "SGD"],
    size=N
)

# Transaction timestamps
base_ts = np.datetime64("2024-01-01T00:00:00")

seconds = rng.integers(
    0,
    900 * 24 * 60 * 60,
    size=N
)

txn_ts = (
    base_ts + seconds.astype("timedelta64[s]")
).astype("datetime64[s]").astype(str)

# Create transactions DataFrame
transactions = pd.DataFrame({
    "txn_id": txn_ids,
    "order_id": txn_order_ids,
    "payment_method": methods,
    "amount": amounts,
    "currency": currencies,
    "txn_status": txn_status,
    "txn_ts": txn_ts
})

# Save transactions CSV
transactions_path = OUT_DIR / "transactions.csv"
transactions.to_csv(transactions_path, index=False)

# ============================================================
# 3. SUMMARY
# ============================================================

print("Dataset generation completed successfully!")
print(f"Orders records: {len(orders):,}")
print(f"Transactions records: {len(transactions):,}")
print(f"Orders file: {orders_path.resolve()}")
print(f"Transactions file: {transactions_path.resolve()}")
print(f"Orders file size: {orders_path.stat().st_size / 1e6:.2f} MB")
print(f"Transactions file size: {transactions_path.stat().st_size / 1e6:.2f} MB")
