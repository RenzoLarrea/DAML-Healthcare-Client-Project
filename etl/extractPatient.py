import requests
import json
import time
from typing import Optional, List, Dict, Any

BASE_URL = "https://sandbox.bluebutton.cms.gov/v2/fhir/Patient/"
HEADERS = {
    "Accept": "application/json",
    "Authorization": "Bearer 3GVIib2tm86cv4GSkQn5SciXEiSh1O"  # replace with your token
}

def fetch_all_bundle_pages(
    url: str,
    headers: dict,
    first_count: int = 50,  
    max_pages: int = 1000
) -> List[Dict[str, Any]]:
    """
    Fetch all pages for a FHIR bundle endpoint by following server-provided 'next' links.
    Only retrieves JSON, does not flatten or transform.
    """
    bundles: List[Dict[str, Any]] = []
    next_url: Optional[str] = url
    params = {"_count": first_count}
    page = 0

    while next_url and page < max_pages:
        print(f"[fetch] page={page} GET {next_url}")
        resp = requests.get(
            next_url,
            headers=headers,
            params=params if page == 0 else None,
            timeout=30,
        )
        print(f"[fetch] status={resp.status_code} url={resp.url}")

        if resp.status_code != 200:
            print("[fetch] Non-200 response body (first 2000 chars):")
            print(resp.text[:2000])
            break

        bundle = resp.json()
        bundles.append(bundle)

        # Find next link for pagination
        next_link = None
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                next_link = link.get("url")
                break

        next_url = next_link
        page += 1
        time.sleep(0.05)  # polite pause between requests

    print(f"[fetch] completed: fetched {len(bundles)} page(s)")
    return bundles


def extract_resources_from_bundles(bundles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract the ExplanationOfBenefit 'resource' objects from FHIR bundles."""
    resources = []
    for bundle in bundles:
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                resources.append(resource)
    return resources


if __name__ == "__main__":
    bundles = fetch_all_bundle_pages(BASE_URL, HEADERS, first_count=50)
    if not bundles:
        print("No bundles fetched; check output above for errors.")
    else:
        print("Total bundles fetched:", len(bundles))
        total_hits = bundles[0].get("total", "unknown")
        print("Total hits (first bundle reports):", total_hits)

        # Extract raw ExplanationOfBenefit resources
        resources = extract_resources_from_bundles(bundles)
        print("Total Patient resources extracted:", len(resources))

        # ✅ Save raw JSON resources for clean reprocessing later
        with open("data/patient_raw_resources.json", "w", encoding="utf-8") as f:
            json.dump(resources, f, ensure_ascii=False, indent=2)

        print("Saved raw EOB JSON to data/patient_raw_resources.json")