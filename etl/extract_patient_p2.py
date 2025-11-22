import json
import pandas as pd
import os
from typing import Any, Dict, List, Optional

# -------------------------------------------------
# Config
# -------------------------------------------------
INPUT_FILE = "data/patient_raw_resources.json"
OUTPUT_DIR = "data"
CSV_OUT = os.path.join(OUTPUT_DIR, "patient_clean.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------------
# Helpers for nested extraction
# -------------------------------------------------
def find_identifier_value(identifiers: Optional[List[Dict[str, Any]]], match_system_substr: str) -> Optional[str]:
    if not identifiers:
        return None
    for ident in identifiers:
        system = ident.get("system", "") or ""
        if match_system_substr in system:
            return ident.get("value")
    return None

def find_identifier_by_code(identifiers: Optional[List[Dict[str, Any]]], code: str) -> Optional[str]:
    """Search identifiers whose type.coding contains a coding with given v2-0203 code."""
    if not identifiers:
        return None
    for ident in identifiers:
        type_block = ident.get("type", {})
        codings = type_block.get("coding", []) if isinstance(type_block.get("coding", []), list) else []
        for c in codings:
            if c.get("code") == code:
                return ident.get("value")
    return None

def ext_value_by_url(exts: Optional[List[Dict[str, Any]]], url_substr: str) -> Any:
    """Return the raw value* field for the first extension whose url contains url_substr.
       value* can be valueCoding, valueCode, valueString, valueDate, etc.
    """
    if not exts:
        return None
    for e in exts:
        url = e.get("url", "") or ""
        if url_substr in url:
            # find any key that starts with 'value'
            for k, v in e.items():
                if k.startswith("value"):
                    return v
            # sometimes extension holds nested 'extension' (e.g., us-core-race)
            if "extension" in e and isinstance(e["extension"], list):
                # return the nested extension dict as json-friendly structure
                return e["extension"]
            return None
    return None

def ext_text_from_us_core_race(exts: Optional[List[Dict[str, Any]]]) -> Optional[str]:
    """Extract human readable race text from us-core-race extension structure."""
    if not exts:
        return None
    for e in exts:
        if e.get("url", "") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
            nested = e.get("extension", [])
            if isinstance(nested, list):
                for n in nested:
                    if n.get("url") == "text":
                        return n.get("valueString")
    return None

def first_name_block(names: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    if not names:
        return {}
    n = names[0]
    return {
        "name_use": n.get("use"),
        "family": n.get("family"),
        "given": " ".join(n.get("given", [])) if isinstance(n.get("given", []), list) else n.get("given")
    }

def first_address_block(addresses: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    if not addresses:
        return {}
    a = addresses[0]
    return {
        "address_state": a.get("state"),
        "postal_code": a.get("postalCode"),
        "city": a.get("city"),
        "line": " ".join(a.get("line", [])) if isinstance(a.get("line", []), list) else a.get("line")
    }

# -------------------------------------------------
# 1. Load the raw JSON resources
# -------------------------------------------------
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    resources = json.load(f)

print(f"Loaded {len(resources)} raw Patient records")

# -------------------------------------------------
# 2. Normalize (flatten) JSON into a DataFrame
# -------------------------------------------------
df = pd.json_normalize(resources)
print("Shape after normalization:", df.shape)

# -------------------------------------------------
# 3. Create extracted columns from nested structures
# -------------------------------------------------
# Basic top-level renames
df = df.rename(columns={
    "id": "patient_id",
    "meta.lastUpdated": "meta_lastUpdated",
    "gender": "gender",
    "birthDate": "birth_date",
    "deceasedBoolean": "deceased_boolean"
})

# Extract first name/family/given
name_blocks = df.get("name").apply(lambda x: first_name_block(x) if isinstance(x, list) else first_name_block(None))
df["name_use"] = name_blocks.apply(lambda nb: nb.get("name_use"))
df["family_name"] = name_blocks.apply(lambda nb: nb.get("family"))
df["given_names"] = name_blocks.apply(lambda nb: nb.get("given"))

# Extract address pieces
addr_blocks = df.get("address").apply(lambda x: first_address_block(x) if isinstance(x, list) else first_address_block(None))
df["address_state"] = addr_blocks.apply(lambda a: a.get("address_state"))
df["postal_code"] = addr_blocks.apply(lambda a: a.get("postal_code"))
df["city"] = addr_blocks.apply(lambda a: a.get("city"))
df["address_line"] = addr_blocks.apply(lambda a: a.get("line"))

# Extract common identifiers
df["bene_id"] = df.get("identifier").apply(lambda ids: find_identifier_value(ids, "bene_id"))
# MBI system is 'http://hl7.org/fhir/sid/us-mbi' - we can match on 'us-mbi'
df["mbi"] = df.get("identifier").apply(lambda ids: find_identifier_value(ids, "us-mbi"))
# also support searching by type code 'MB' (Member Number) or 'MC' (Medicare)
df["identifier_MB"] = df.get("identifier").apply(lambda ids: find_identifier_by_code(ids, "MB"))
df["identifier_MC"] = df.get("identifier").apply(lambda ids: find_identifier_by_code(ids, "MC"))

# Extract a few extensions by their urls (safe, tolerant)
df["us_core_sex_code"] = df.get("extension").apply(lambda exts: ext_value_by_url(exts, "us-core-sex"))
df["race_coding"] = df.get("extension").apply(lambda exts: ext_value_by_url(exts, "variables/race"))
df["us_core_race_text"] = df.get("extension").apply(lambda exts: ext_text_from_us_core_race(exts))
df["reference_year"] = df.get("extension").apply(lambda exts: ext_value_by_url(exts, "rfrnc_yr"))
df["dual_01"] = df.get("extension").apply(lambda exts: ext_value_by_url(exts, "dual_01"))
df["dual_02"] = df.get("extension").apply(lambda exts: ext_value_by_url(exts, "dual_02"))
df["dual_03"] = df.get("extension").apply(lambda exts: ext_value_by_url(exts, "dual_03"))

# -------------------------------------------------
# 4. Select / reorder columns we care about
# -------------------------------------------------
keep_cols = [
    "patient_id",
    "meta_lastUpdated",
    "bene_id",
    "mbi",
    "identifier_MB",
    "identifier_MC",
    "name_use",
    "family_name",
    "given_names",
    "gender",
    "birth_date",
    "deceased_boolean",
    "address_state",
    "city",
    "postal_code",
    "address_line",
    "us_core_sex_code",
    "race_coding",
    "us_core_race_text",
    "reference_year",
    "dual_01",
    "dual_02",
    "dual_03",
    # keep raw columns in case you need them later
    "identifier",
    "extension",
    "meta",
]

# Only keep those that actually exist in df
keep_cols = [c for c in keep_cols if c in df.columns]
df_out = df.loc[:, keep_cols].copy()

# -------------------------------------------------
# 5. Convert types and serialize JSON-like fields
# -------------------------------------------------
df_out["birth_date"] = pd.to_datetime(df_out["birth_date"], errors="coerce")
df_out["meta_lastUpdated"] = pd.to_datetime(df_out["meta_lastUpdated"], errors="coerce")

# Serialize identifier/extension/meta to JSON strings so CSV contains valid representations
for col in ["identifier", "extension", "meta"]:
    if col in df_out.columns:
        df_out[col] = df_out[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# For coding objects (e.g., race_coding) convert to a compact string if dict/list
def pretty_json_or_value(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return v

for col in ["us_core_sex_code", "race_coding", "reference_year", "dual_01", "dual_02", "dual_03"]:
    if col in df_out.columns:
        df_out[col] = df_out[col].apply(pretty_json_or_value)

# -------------------------------------------------
# 6. Save cleaned outputs
# -------------------------------------------------
df_out.to_csv(CSV_OUT, index=False)

print(f"Saved patient CSV → {CSV_OUT}")
print("\nSample rows:")
print(df_out.head(5).to_string(index=False))
