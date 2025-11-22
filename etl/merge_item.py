import csv
from pathlib import Path
from typing import Any, Dict, List

ITEM_COL = "item"  # name of the original column to remove/replace
DROP_COLS = {"created", "sequence"}  # columns to drop from the *output*


def append_columns_replace_item(
    csv1_path: Path,
    csv2_path: Path,
    out_path: Path,
) -> None:
    """
    Read csv1 and csv2, remove the ITEM_COL from csv1, and insert all
    columns from csv2 at that column's index position in the final output.

    Additionally, drop any columns listed in DROP_COLS from the output.

    Assumptions:
    - csv1 and csv2 have the same number of data rows.
    - Rows correspond by position.
    """

    # Read csv1
    with csv1_path.open("r", newline="", encoding="utf-8") as f1:
        reader1 = csv.DictReader(f1)
        fieldnames1 = reader1.fieldnames or []
        rows1: List[Dict[str, Any]] = list(reader1)

    # Read csv2
    with csv2_path.open("r", newline="", encoding="utf-8") as f2:
        reader2 = csv.DictReader(f2)
        fieldnames2 = reader2.fieldnames or []
        rows2: List[Dict[str, Any]] = list(reader2)

    # Basic row count check
    if len(rows1) != len(rows2):
        raise ValueError(
            f"Row count mismatch: csv1 has {len(rows1)} rows, "
            f"csv2 has {len(rows2)} rows."
        )

    if ITEM_COL not in fieldnames1:
        raise ValueError(f"'{ITEM_COL}' column not found in csv1 header.")

    # Index of the original item column
    item_idx = fieldnames1.index(ITEM_COL)

    # Avoid duplicate column names: don't reinsert a column name that already
    # exists in csv1 (other than the item column we're removing).
    existing_cols_except_item = {c for c in fieldnames1 if c != ITEM_COL}
    insert_cols = [c for c in fieldnames2 if c not in existing_cols_except_item]

    # Build header before dropping created/sequence:
    combined_fields = (
        fieldnames1[:item_idx] + insert_cols + fieldnames1[item_idx + 1 :]
    )

    # Now drop the unwanted columns from the final header
    combined_fields = [c for c in combined_fields if c not in DROP_COLS]

    # Merge rows: copy csv1 row except ITEM_COL, then add csv2 columns,
    # but skip any DROP_COLS.
    merged_rows: List[Dict[str, Any]] = []
    for r1, r2 in zip(rows1, rows2):
        merged: Dict[str, Any] = {}

        # csv1 columns except ITEM_COL and DROP_COLS
        for col in fieldnames1:
            if col == ITEM_COL or col in DROP_COLS:
                continue
            merged[col] = r1.get(col, "")

        # csv2 columns (insert_cols) except DROP_COLS
        for col in insert_cols:
            if col in DROP_COLS:
                continue
            merged[col] = r2.get(col, "")

        merged_rows.append(merged)

    # Write output CSV
    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=combined_fields)
        writer.writeheader()
        for row in merged_rows:
            # Only write keys that are in combined_fields
            writer.writerow({k: row.get(k, "") for k in combined_fields})


if __name__ == "__main__":
    csv1 = Path("data/eob_part_d_clean_v2.csv")
    csv2 = Path("data/item_extracted.csv")
    out = Path("data/eob_part_d_final.csv")
    append_columns_replace_item(csv1, csv2, out)
