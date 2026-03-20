import re
import time
import pandas as pd
from wiliot_api.platform.platform import LocationType, EntityType


def _retry_api_call(func, max_retries=3, base_delay=2):
    """Retry an API call with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt)
            print(f"  Retry {attempt + 1}/{max_retries} after {delay}s: {e}")
            time.sleep(delay)


def create_store_locations(pc, mdlz_stores, mdlz_dc_locations, on_progress=None):
    """
    Creates platform locations for stores from the mdlz_stores DataFrame.
    Each location gets labels: ECC, locationType, parentBranch, ASSOCIATE_DC, PAR_LEVEL_Roll_Cart.
    ASSOCIATE_DC is resolved by matching parentBranch to mdlz_dc_locations.location_name.

    :param pc: An already-instantiated PlatformClient
    :param mdlz_stores: DataFrame with columns: name, address, lat, lng, ECC,
                         parentBranch, PAR_LEVEL_Roll_Cart
    :param mdlz_dc_locations: DataFrame with columns: location_name, location_id
    :param on_progress: Optional callback(current, total, result_dict) for UI updates
    :return: DataFrame with creation results
    """
    mdlz_stores = mdlz_stores.copy()
    mdlz_stores = mdlz_stores.merge(
        mdlz_dc_locations[["location_name", "location_id"]],
        how="left",
        left_on="parentBranch",
        right_on="location_name",
    )
    mdlz_stores.rename(columns={"location_id": "ASSOCIATE_DC"}, inplace=True)

    unmatched = mdlz_stores["ASSOCIATE_DC"].isna().sum()
    if unmatched:
        print(f"WARNING: {unmatched} stores have no matching DC in mdlz_dc_locations\n")

    # Fetch existing locations to prevent duplicates (names and addresses)
    existing_locations = pc.get_locations()
    existing_names = {loc["name"] for loc in existing_locations}
    existing_addresses = {loc.get("address", "").strip().upper() for loc in existing_locations if loc.get("address")}
    print(f"Found {len(existing_names)} existing locations on platform")
    print(f"Found {len(mdlz_stores)} stores to process\n")

    REQUIRED_STORE_COLS = ["name", "address", "lat", "lng", "ECC", "parentBranch", "ASSOCIATE_DC", "PAR_LEVEL_Roll_Cart"]

    results = []
    skipped = 0
    processed = 0
    total = len(mdlz_stores)
    seen_names_in_file = set()
    seen_addresses_in_file = set()

    def _skip(row, name, reason):
        nonlocal skipped, processed
        skipped += 1
        processed += 1
        print(f"  SKIP: {name} — {reason}")
        result = {"ecc": str(row.get("ECC", "")), "store_name": name, "location_id": None, "status": "skipped", "reason": reason}
        results.append(result)
        if on_progress:
            on_progress(processed, total, result)

    for _, row in mdlz_stores.iterrows():
        raw_name = str(row.get("name", "")).strip()

        # Skip rows with missing required fields
        missing = [c for c in REQUIRED_STORE_COLS if pd.isna(row.get(c)) or str(row.get(c)).strip() == ""]
        if missing:
            _skip(row, raw_name or "unknown", f"missing {missing}")
            continue

        name = raw_name

        # Skip names with non-alphanumeric characters
        if not re.match(r"^[A-Za-z0-9 ]+$", name):
            _skip(row, name, "name contains special characters (only letters, numbers, spaces allowed)")
            continue

        # Skip duplicate names within the input file
        if name in seen_names_in_file:
            _skip(row, name, "duplicate name in input file")
            continue
        seen_names_in_file.add(name)

        address = str(row["address"]).strip()
        address_upper = address.upper()

        # Skip duplicate addresses within the input file
        if address_upper in seen_addresses_in_file:
            _skip(row, name, "duplicate address in input file")
            continue
        seen_addresses_in_file.add(address_upper)

        # Skip if address already exists on platform
        if address_upper in existing_addresses:
            _skip(row, name, f"address already exists on platform: {address}")
            continue

        lat = float(row["lat"])
        lng = float(row["lng"])
        ecc = str(int(row["ECC"]))
        parent_branch = str(row["parentBranch"]).strip()
        associate_dc = str(row["ASSOCIATE_DC"]).strip()
        par_level = str(int(row["PAR_LEVEL_Roll_Cart"]))

        # Skip if location name already exists on platform
        if name in existing_names:
            print(f"  SKIP (duplicate): {name} already exists on platform")
            processed += 1
            result = {"ecc": ecc, "store_name": name, "location_id": None, "status": "skipped", "reason": "name already exists on platform"}
            results.append(result)
            if on_progress:
                on_progress(processed, total, result)
            continue

        print(f"[{ecc}] Creating store location: {name}")

        result = None
        try:
            location = _retry_api_call(lambda: pc.create_location(
                location_type=LocationType.SITE,
                name=name,
                lat=lat,
                lng=lng,
                address=address,
                country="US",
            ))
            location_id = location["id"]
            print(f"  Location created: {location_id}")

            _retry_api_call(lambda: pc.set_keys_values_for_entities(
                entity_type=EntityType.LOCATION,
                entity_ids=[location_id],
                keys_values={
                    "ECC": ecc,
                    "siteType": "store",
                    "parentBranch": parent_branch,
                    "ASSOCIATE_DC": associate_dc,
                    "PAR_LEVEL_Roll_Cart": par_level,
                },
                overwrite_existing=True,
            ))
            print(f"  Labels set: ECC={ecc}, siteType=store, parentBranch={parent_branch}, ASSOCIATE_DC={associate_dc}, PAR_LEVEL_Roll_Cart={par_level}")

            result = {
                "ecc": ecc,
                "store_name": name,
                "location_id": location_id,
                "status": "success",
            }

        except Exception as e:
            print(f"  ERROR: {e}")
            result = {
                "ecc": ecc,
                "store_name": name,
                "location_id": None,
                "status": f"error: {e}",
            }

        results.append(result)
        processed += 1
        if on_progress:
            on_progress(processed, total, result)

        time.sleep(0.5)

    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    if skipped:
        print(f"\nSkipped {skipped} rows with missing required values.")
    print(f"Done. {success} succeeded, {failed} failed.")
    return pd.DataFrame(results)


def create_trailer_locations(pc, mdlz_trailers, mdlz_dc_locations, on_progress=None):
    """
    Creates TRANSPORTER locations for trailers from the mdlz_trailers DataFrame.
    Each location gets labels for parentBranch, ASSOCIATE_DC, trailerLength, and trailerMake.
    ASSOCIATE_DC is resolved by matching parentBranch to mdlz_dc_locations.location_name.
    Each location gets a zone with template_name based on trailerLength:
      - 28 -> Standard_truck_-_2_bridges
      - 48 or 53 -> Standard_truck

    :param pc: An already-instantiated PlatformClient
    :param mdlz_trailers: DataFrame with columns: name, parentBranch, trailerLength, trailerMake
    :param mdlz_dc_locations: DataFrame with columns: location_name, location_id
    :param on_progress: Optional callback(current, total, result_dict) for UI updates
    :return: DataFrame with creation results
    """
    mdlz_trailers = mdlz_trailers.copy()
    mdlz_trailers = mdlz_trailers.merge(
        mdlz_dc_locations[["location_name", "location_id"]],
        how="left",
        left_on="parentBranch",
        right_on="location_name",
    )
    mdlz_trailers.rename(columns={"location_id": "ASSOCIATE_DC"}, inplace=True)

    unmatched = mdlz_trailers["ASSOCIATE_DC"].isna().sum()
    if unmatched:
        print(f"WARNING: {unmatched} trailers have no matching DC in mdlz_dc_locations\n")

    # Fetch existing location names to prevent duplicates
    existing_locations = pc.get_locations()
    existing_names = {loc["name"] for loc in existing_locations}
    print(f"Found {len(existing_names)} existing locations on platform")

    TEMPLATE_MAP = {
        28: "Standard_truck_-_2_bridges",
        48: "Standard_truck",
        53: "Standard_truck",
    }

    print(f"Found {len(mdlz_trailers)} trailers to process\n")

    REQUIRED_TRAILER_COLS = ["name", "parentBranch", "trailerLength", "trailerMake", "ASSOCIATE_DC"]

    results = []
    skipped = 0
    processed = 0
    total = len(mdlz_trailers)
    seen_names_in_file = set()

    def _skip(row, name, reason):
        nonlocal skipped, processed
        skipped += 1
        processed += 1
        zone_name = name.replace("Truck", "", 1) if name.startswith("Truck") else name
        print(f"  SKIP: {name} — {reason}")
        result = {"unit_num": zone_name, "location_name": name, "location_id": None, "zone_id": None, "status": "skipped", "reason": reason}
        results.append(result)
        if on_progress:
            on_progress(processed, total, result)

    for _, row in mdlz_trailers.iterrows():
        raw_name = str(row.get("name", "")).strip()

        # Skip rows with missing required fields
        missing = [c for c in REQUIRED_TRAILER_COLS if pd.isna(row.get(c)) or str(row.get(c)).strip() == ""]
        if missing:
            _skip(row, raw_name or "unknown", f"missing {missing}")
            continue

        name = raw_name

        # Skip names with invalid characters (only letters, numbers, dashes, spaces)
        if not re.match(r"^[A-Za-z0-9\- ]+$", name):
            _skip(row, name, "name contains invalid characters (only letters, numbers, dashes, spaces allowed)")
            continue

        # Skip names missing the 'Truck' prefix
        if not name.startswith("Truck"):
            _skip(row, name, "name missing required 'Truck' prefix")
            continue

        # Skip non-alphabetic trailerMake
        trailer_make_raw = str(row["trailerMake"]).strip()
        if not re.match(r"^[A-Za-z ]+$", trailer_make_raw):
            _skip(row, name, f"trailerMake contains non-alphabetic characters: {trailer_make_raw}")
            continue

        # Skip duplicate names within the input file
        if name in seen_names_in_file:
            _skip(row, name, "duplicate name in input file")
            continue
        seen_names_in_file.add(name)

        parent_branch = str(row["parentBranch"]).strip()
        trailer_length = int(row["trailerLength"])
        trailer_make = trailer_make_raw
        associate_dc = str(row["ASSOCIATE_DC"]).strip()

        zone_name = name.replace("Truck", "", 1)
        template_name = TEMPLATE_MAP.get(trailer_length, "Standard_truck")

        # Skip if location name already exists on platform
        if name in existing_names:
            _skip(row, name, "name already exists on platform")
            continue

        print(f"[{zone_name}] Creating transporter: {name}")

        result = None
        try:
            location = _retry_api_call(lambda: pc.create_location(
                location_type=LocationType.TRANSPORTER,
                name=name,
            ))
            location_id = location["id"]
            print(f"  Location created: {location_id}")

            _retry_api_call(lambda: pc.set_keys_values_for_entities(
                entity_type=EntityType.LOCATION,
                entity_ids=[location_id],
                keys_values={
                    "parentBranch": parent_branch,
                    "ASSOCIATE_DC": associate_dc,
                    "trailerLength": str(trailer_length),
                    "trailerMake": trailer_make,
                },
                overwrite_existing=True,
            ))
            print(f"  Labels set: parentBranch={parent_branch}, ASSOCIATE_DC={associate_dc}, trailerLength={trailer_length}, trailerMake={trailer_make}")

            zone = _retry_api_call(lambda: pc.create_zone(name=zone_name, location_id=location_id))
            zone_id = zone["id"]
            print(f"  Zone created: {zone_id}")

            _retry_api_call(lambda: pc.set_keys_values_for_entities(
                entity_type=EntityType.ZONE,
                entity_ids=[zone_id],
                keys_values={"template_name": template_name},
                overwrite_existing=True,
            ))
            print(f"  Zone label set: template_name={template_name}")

            result = {
                "unit_num": zone_name,
                "location_name": name,
                "location_id": location_id,
                "zone_id": zone_id,
                "status": "success",
            }

        except Exception as e:
            print(f"  ERROR: {e}")
            result = {
                "unit_num": zone_name,
                "location_name": name,
                "location_id": None,
                "zone_id": None,
                "status": f"error: {e}",
            }

        results.append(result)
        processed += 1
        if on_progress:
            on_progress(processed, total, result)

        time.sleep(0.5)

    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    if skipped:
        print(f"\nSkipped {skipped} rows with missing required values.")
    print(f"Done. {success} succeeded, {failed} failed.")
    return pd.DataFrame(results)
