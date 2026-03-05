from pathlib import Path
import pandas as pd
# Look for the data folder
DATA_DIR = Path(__file__).parent / "data"

missing = pd.read_csv(DATA_DIR / "download_02-13-2026.21_02_57.csv", encoding="utf-8-sig")

# Load the counties reference table, which contains county name, state
# abbreviation, and the lat/lng centroid for each county.
counties = pd.read_csv(DATA_DIR / "uscounties.csv")

# Fallback county lookup for records where County is NaN.
# Keyed by (city lowercase, state abbreviation) -> county name matching uscounties.csv spelling.
MISSING_COUNTY_HANDLING = {
    ("winston-salem", "NC"): "Forsyth",
    ("brooklyn",       "NY"): "Kings",
    ("chamblee",       "GA"): "DeKalb",
    ("virginia beach", "VA"): "Virginia Beach",
    ("greensboro",     "NC"): "Guilford",
    ("fort payne",     "AL"): "DeKalb",
    ("decatur",        "GA"): "DeKalb",
}

missing_county_mask = missing["County"].isna()
if missing_county_mask.any():
    missing.loc[missing_county_mask, "County"] = missing.loc[missing_county_mask].apply(
        lambda row: MISSING_COUNTY_HANDLING.get(
            (str(row["City"]).lower(), row["State"]), row["County"]
        ),
        axis=1,
    )

# This handles mismatched capitalization.
# For example, the missing persons data uses "LaRue" (KY) while the counties file uses "Larue".
# To avoid losing valid matches, lowercase both county name columns and
# join on the lowercase versions rather than the originals.
missing["_county_key"] = missing["County"].str.lower()
counties["_county_key"] = counties["county_ascii"].str.lower()

# Left join the data sets.
# Join on (normalized county name, state abbreviation) so that identically
# named counties in different states don't get mixed up (e.g., "Butler" county
# exists in multiple states). A left join keeps every missing persons record
# even if no county match is found — those rows will simply have NaN for lat/lng.
merged = missing.merge(
    counties[["_county_key", "state_id", "lat", "lng"]],
    left_on=["_county_key", "State"],
    right_on=["_county_key", "state_id"],
    how="left"
).drop(columns=["_county_key", "state_id"])  # drop helper columns used only for joining


# Report how many rows couldn't be matched. Investigation showed all unmatched
# rows either had no county in the source data (NaN) or a capitalization
# mismatch (the latter was fixed by the case-insensitive key above).
unmatched = merged["lat"].isna().sum()
print(f"Rows: {len(merged)} | Unmatched (no lat/lng): {unmatched}")

print("\nUnmatched rows:")
print(merged[merged["lat"].isna()][["Case Number", "City", "County", "State"]].to_string())


# Save output
merged.to_csv(DATA_DIR / "missing_persons_with_coords.csv", index=False)
print("\nSaved to missing_persons_with_coords.csv")