import csv
from pathlib import Path
from typing import Any, List, Dict

INPUT_CSV = Path("data/eob_part_d_clean.csv")
OUT_CSV = Path("data/eob_part_d_clean_v2.csv")


def is_null(value: Any) -> bool:
    """
    Decide whether a cell value should be considered 'null'.

    Currently treats empty strings and whitespace-only strings as null.
    Extend this if you want to treat 'NULL', 'NA', etc. as null too.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def drop_null_and_constant_columns(in_path: Path, out_path: Path) -> None:
    """
    - Read CSV file.
    - Drop columns that are entirely null/empty.
    - Drop columns where every value is identical (constant columns).
    - Never drop the column 'patient_ref'.
    - Write cleaned CSV.
    - Print diagnostics about initial, dropped, and kept columns.
    """
    with in_path.open("r", newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames

        # If the file is empty or has no header, just create an empty output file
        if not fieldnames:
            print("No header found in input CSV. Nothing to process.")
            with out_path.open("w", newline="", encoding="utf-8") as f_out:
                pass
            return

        rows: List[Dict[str, Any]] = list(reader)

    print(f"Initial columns ({len(fieldnames)}): {fieldnames}")

    # If there are no data rows, just keep the original header
    if not rows:
        print("No data rows found. Keeping original columns.")
        with out_path.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
        return

    # Track which columns are all-null and which are constant
    all_null: Dict[str, bool] = {col: True for col in fieldnames}
    constant: Dict[str, bool] = {col: True for col in fieldnames}
    first_value: Dict[str, Any] = {}

    # Scan rows
    for row_idx, row in enumerate(rows):
        for col in fieldnames:
            value = row.get(col, "")

            # Update all-null tracker
            if not is_null(value):
                all_null[col] = False

            # Update constant tracker
            if col not in first_value:
                # First row's raw value becomes baseline
                first_value[col] = value
            else:
                if value != first_value[col]:
                    constant[col] = False

    # Columns to drop:
    #  - entirely null
    #  - or constant (same value in every row)
    # BUT NEVER drop 'patient_ref'
    dropped_columns = [
        col for col in fieldnames
        if (all_null[col] or constant[col]) and col != "patient_ref"
    ]

    kept_fields = [col for col in fieldnames if col not in dropped_columns]

    # If somehow we would drop everything, keep original header as fallback
    if not kept_fields:
        print(
            "All columns are either null or constant. "
            "Keeping original columns as a fallback."
        )
        kept_fields = fieldnames
        dropped_columns = []

    print(f"Dropped columns ({len(dropped_columns)}): {dropped_columns}")
    print(f"Kept columns ({len(kept_fields)}): {kept_fields}")

    # Write out cleaned CSV
    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=kept_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in kept_fields})


if __name__ == "__main__":
    drop_null_and_constant_columns(INPUT_CSV, OUT_CSV)
