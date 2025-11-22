import csv
import json
from pathlib import Path
from typing import Any, List

INPUT_CSV = Path("data/eob_part_d_clean.csv")
OUT_CSV = Path("data/item_raw.csv")
OUT_JSON = Path("data/item_raw_resources.json")


def try_parse_json(s: str) -> Any:
    """Return parsed JSON if valid, otherwise return the original string."""
    if s is None:
        return None
    s_str = s.strip()
    if not s_str:
        return None
    try:
        return json.loads(s_str)
    except Exception:
        # return original string when parsing fails
        return s


def main():
    if not INPUT_CSV.exists():
        raise SystemExit(f"Input CSV not found: {INPUT_CSV}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    parsed_items: List[Any] = []
    row_count = 0
    missing_count = 0

    # Read CSV using csv.DictReader to avoid pandas
    with INPUT_CSV.open("r", encoding="utf-8", newline="") as fin, \
         OUT_CSV.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        # check that "item" column exists
        if "item" not in reader.fieldnames:
            raise SystemExit(f"'item' column not found in input CSV. Columns: {reader.fieldnames}")

        writer = csv.writer(fout)
        writer.writerow(["item"])  # header

        for row in reader:
            row_count += 1
            item_raw = row.get("item", None)
            if item_raw is None or item_raw == "":
                missing_count += 1
                writer.writerow([""])
                parsed_items.append(None)
                continue

            # write raw value to CSV (preserve as-is)
            writer.writerow([item_raw])

            # try to parse JSON so we have a convenient JSON file too
            parsed = try_parse_json(item_raw)
            parsed_items.append(parsed)

    # Save the JSON array of items (parsed when possible)
    with OUT_JSON.open("w", encoding="utf-8") as jf:
        json.dump(parsed_items, jf, ensure_ascii=False, indent=2)

    print(f"Done. Processed rows: {row_count}")
    print(f"Missing/empty 'item' entries: {missing_count}")
    print(f"Wrote CSV: {OUT_CSV}")
    print(f"Wrote JSON: {OUT_JSON}")


if __name__ == "__main__":
    main()