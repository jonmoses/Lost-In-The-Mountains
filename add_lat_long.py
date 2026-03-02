# ---------------------------------------------------------------------------
# Geocode Missing Persons Data by County
# ---------------------------------------------------------------------------
# Goal: The missing persons dataset (from the NCMEC download) contains a
# County and State column but no geographic coordinates. This script joins
# it against a US counties reference file to append a latitude and longitude
# column to each record, enabling map-based analysis.
#
# Input files:
#   - download_02-13-2026.21_02_57.csv  : NCMEC missing persons export
#   - uscounties.csv                    : US counties with lat/lng centroids
#
# Output:
#   - missing_persons_with_coords.csv   : original data + lat + lng columns
# ---------------------------------------------------------------------------

import pandas as pd

# Load the missing persons report. The file has a UTF-8 BOM (byte-order mark)
# at the start — encoding="utf-8-sig" strips it so column names read cleanly.
missing = pd.read_csv("download_02-13-2026.21_02_57.csv", encoding="utf-8-sig")

# Load the counties reference table, which contains county name, state
# abbreviation, and the lat/lng centroid for each county.
counties = pd.read_csv("uscounties.csv")

# ---------------------------------------------------------------------------
# Case-insensitive join key
# ---------------------------------------------------------------------------
# The two datasets don't always agree on capitalization — for example, the
# missing persons data uses "LaRue" (KY) while the counties file uses "Larue".
# To avoid losing valid matches, we lowercase both county name columns and
# join on the normalized versions rather than the originals.
missing["_county_key"] = missing["County"].str.lower()
counties["_county_key"] = counties["county_ascii"].str.lower()

# ---------------------------------------------------------------------------
# Left join: missing persons ← counties
# ---------------------------------------------------------------------------
# We join on (normalized county name, state abbreviation) so that identically
# named counties in different states don't get mixed up (e.g., "Butler" county
# exists in multiple states). A left join keeps every missing persons record
# even if no county match is found — those rows will simply have NaN for lat/lng.
merged = missing.merge(
    counties[["_county_key", "state_id", "lat", "lng"]],
    left_on=["_county_key", "State"],
    right_on=["_county_key", "state_id"],
    how="left"
).drop(columns=["_county_key", "state_id"])  # drop helper columns used only for joining

# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
# Report how many rows couldn't be matched. Investigation showed all unmatched
# rows either had no county in the source data (NaN) or a capitalization
# mismatch (the latter was fixed by the case-insensitive key above).
unmatched = merged["lat"].isna().sum()
print(f"Rows: {len(merged)} | Unmatched (no lat/lng): {unmatched}")

print("\nUnmatched rows:")
print(merged[merged["lat"].isna()][["Case Number", "City", "County", "State"]].to_string())

# ---------------------------------------------------------------------------
# Save output
# ---------------------------------------------------------------------------
merged.to_csv("missing_persons_with_coords.csv", index=False)
print("\nSaved to missing_persons_with_coords.csv")