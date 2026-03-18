import pandas as pd


# --- Required columns per location type ---

REQUIRED_STORE_COLS = ["name", "address", "lat", "lng", "ECC", "parentBranch", "PAR_LEVEL_Roll_Cart"]
REQUIRED_TRAILER_COLS = ["name", "parentBranch", "trailerLength", "trailerMake"]
VALID_TRAILER_LENGTHS = {28, 48, 53}


def get_dc_locations(pc):
    """
    Fetches all DC (Distribution Center) locations from the Wiliot platform.
    Returns a DataFrame with location_name and location_id for use as reference data.

    :param pc: An already-instantiated PlatformClient
    :return: DataFrame with columns: location_name, location_id
    """
    locations = pc.get_locations()
    dc_locations = []
    for loc in locations:
        labels = loc.get("labels", {})
        if labels.get("locationType") == "DC":
            dc_locations.append({
                "location_name": loc["name"],
                "location_id": loc["id"],
            })
    return pd.DataFrame(dc_locations)


def parse_upload(file, filename):
    """
    Parses an uploaded file (CSV or Excel) into a DataFrame.
    Normalizes column names by stripping whitespace.

    :param file: File-like object (e.g., Streamlit UploadedFile)
    :param filename: Original filename (used to detect format)
    :return: DataFrame
    """
    if filename.endswith(".csv"):
        df = pd.read_csv(file)
    elif filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(file)
    else:
        raise ValueError(f"Unsupported file format: {filename}. Use CSV or Excel (.xlsx).")

    df.columns = [col.strip() for col in df.columns]
    return df


def validate_store_data(df, dc_locations_df):
    """
    Validates a store DataFrame before creation.

    Checks:
    - All required columns exist
    - lat/lng are numeric
    - ECC is numeric
    - PAR_LEVEL_Roll_Cart is numeric
    - All parentBranch values match a known DC

    :param df: Store DataFrame to validate
    :param dc_locations_df: DC reference DataFrame (from get_dc_locations)
    :return: list of error strings (empty = valid)
    """
    errors = []

    # Check required columns
    missing_cols = [col for col in REQUIRED_STORE_COLS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return errors  # Can't validate further without the right columns

    # Check for empty DataFrame
    if len(df) == 0:
        errors.append("File contains no data rows.")
        return errors

    # Check numeric fields
    for col in ["lat", "lng"]:
        non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
        count = non_numeric.sum()
        if count:
            errors.append(f"{count} rows have non-numeric '{col}' values.")

    for col in ["ECC", "PAR_LEVEL_Roll_Cart"]:
        non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
        count = non_numeric.sum()
        if count:
            errors.append(f"{count} rows have non-numeric '{col}' values.")

    # Check parentBranch against DC list
    if len(dc_locations_df) > 0:
        known_dcs = set(dc_locations_df["location_name"].str.strip())
        upload_branches = set(df["parentBranch"].dropna().str.strip().unique())
        unmatched = upload_branches - known_dcs
        if unmatched:
            errors.append(f"parentBranch values not found in platform DCs: {sorted(unmatched)}")

    # Count rows with blanks in required fields
    blank_rows = 0
    for _, row in df.iterrows():
        for col in REQUIRED_STORE_COLS:
            if pd.isna(row.get(col)) or str(row.get(col)).strip() == "":
                blank_rows += 1
                break
    if blank_rows:
        errors.append(f"{blank_rows} rows have blank values in required fields (will be skipped during creation).")

    return errors


def validate_trailer_data(df, dc_locations_df):
    """
    Validates a trailer DataFrame before creation.

    Checks:
    - All required columns exist
    - trailerLength is numeric and in valid set (28, 48, 53)
    - All parentBranch values match a known DC

    :param df: Trailer DataFrame to validate
    :param dc_locations_df: DC reference DataFrame (from get_dc_locations)
    :return: list of error strings (empty = valid)
    """
    errors = []

    # Check required columns
    missing_cols = [col for col in REQUIRED_TRAILER_COLS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return errors

    # Check for empty DataFrame
    if len(df) == 0:
        errors.append("File contains no data rows.")
        return errors

    # Check trailerLength is numeric
    non_numeric = pd.to_numeric(df["trailerLength"], errors="coerce").isna() & df["trailerLength"].notna()
    count = non_numeric.sum()
    if count:
        errors.append(f"{count} rows have non-numeric 'trailerLength' values.")

    # Check trailerLength values are valid
    numeric_lengths = pd.to_numeric(df["trailerLength"], errors="coerce").dropna().astype(int)
    invalid_lengths = set(numeric_lengths.unique()) - VALID_TRAILER_LENGTHS
    if invalid_lengths:
        errors.append(f"Unexpected trailerLength values: {sorted(invalid_lengths)}. Expected: {sorted(VALID_TRAILER_LENGTHS)}")

    # Check parentBranch against DC list
    if len(dc_locations_df) > 0:
        known_dcs = set(dc_locations_df["location_name"].str.strip())
        upload_branches = set(df["parentBranch"].dropna().str.strip().unique())
        unmatched = upload_branches - known_dcs
        if unmatched:
            errors.append(f"parentBranch values not found in platform DCs: {sorted(unmatched)}")

    # Count rows with blanks in required fields
    blank_rows = 0
    for _, row in df.iterrows():
        for col in REQUIRED_TRAILER_COLS:
            if pd.isna(row.get(col)) or str(row.get(col)).strip() == "":
                blank_rows += 1
                break
    if blank_rows:
        errors.append(f"{blank_rows} rows have blank values in required fields (will be skipped during creation).")

    return errors
