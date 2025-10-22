import json
import pandas as pd
import os

# -------------------------------------------------
# Config
# -------------------------------------------------
INPUT_FILE = "data/eob_raw_resources.json"
OUTPUT_DIR = "data"
CSV_OUT = "data/eob_part_d_clean.csv"
PARQUET_OUT = "data/eob_part_d_clean.parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------------
# 1. Load the raw JSON resources
# -------------------------------------------------
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    resources = json.load(f)

print(f"Loaded {len(resources)} raw ExplanationOfBenefit records")

# -------------------------------------------------
# 2. Normalize (flatten) JSON into a DataFrame
# -------------------------------------------------
df = pd.json_normalize(resources)
print("Shape after normalization:", df.shape)

# -------------------------------------------------
# 3. Basic column cleanup / rename
# -------------------------------------------------
df = df.rename(columns={
    "id": "claim_id",
    "patient.reference": "patient_ref",
    "billablePeriod.start": "period_start",
    "billablePeriod.end": "period_end",
    "type.text": "claim_type",
    "total[0].amount.value": "total_amount",
    "payment.amount.value": "payment_amount"
})

# -------------------------------------------------
# 4. Filter to Part D (Prescription Drug Event) claims
# -------------------------------------------------
# In Blue Button data, PDE claim IDs start with "pde"
df_partd = df[df["claim_id"].astype(str).str.startswith("pde", na=False)].copy()
print(f"Filtered to {len(df_partd)} Part D (PDE) claims")

# -------------------------------------------------
# 5. Select only columns useful for Part D research
# -------------------------------------------------
keep_cols = [
    "claim_id",
    "patient_ref",
    "status",
    "created",
    "period_start",
    "period_end",
    "claim_type",
    "total_amount",
    "payment_amount",
    "provider.reference",
    "facility.identifier.value",
    "subType.text",
    "type.coding",
    "item",               # nested drug line details
    "insurance",
    "benefitBalance"
]
df_partd = df_partd.loc[:, [c for c in keep_cols if c in df_partd.columns]]

# -------------------------------------------------
# 6. Convert numeric and date columns, and serialize JSON-like columns.
# note: df_partd["total_amount"] = pd.to_numeric(df_partd["total_amount"], errors="coerce") runs an error (why?), hence removed
# -------------------------------------------------

df_partd["payment_amount"] = pd.to_numeric(df_partd["payment_amount"], errors="coerce")
df_partd["period_start"] = pd.to_datetime(df_partd["period_start"], errors="coerce")
df_partd["period_end"] = pd.to_datetime(df_partd["period_end"], errors="coerce")
df_partd["created"] = pd.to_datetime(df_partd["created"], errors="coerce")

# Serialize JSON-like columns
json_columns = ["type.coding", "item", "insurance"]  # replace with your actual column names
for col in json_columns:
    df_partd[col] = df_partd[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)


# -------------------------------------------------
# 7. Save cleaned outputs 
# note: JSON formats are in python JSON formats, so we have ' instead of ". Keep this in mind if we plan to use JSON in database
# -------------------------------------------------
df_partd.to_csv(CSV_OUT, index=False)
df_partd.to_parquet(PARQUET_OUT, index=False)

print(f"Saved clean Part D CSV → {CSV_OUT}")
print(f"Saved clean Part D Parquet → {PARQUET_OUT}")
print("\nSample:")
print(df_partd.head(5))