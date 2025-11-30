"""
PSE API Client Module

Functions for fetching data from the Polish Power System Operator (PSE) API.
Handles pagination, retry logic, and data processing.
"""

import requests
import json
import time
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import logging

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PSE_API_BASE_URL = "https://api.raporty.pse.pl/api/gen-jw"
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF_MULTIPLIER = 2
MIN_RETRY_DELAY = 1  # seconds
DEFAULT_PAGE_SIZE = 100000  # Max records per request
MAX_WARNING_LOGS = 5  # Max number of malformed datetime warnings to log

# Filter type constants
FILTER_TYPE_ALL = "Wszystkie dane"
FILTER_TYPE_BY_POWER_PLANT = "Według elektrowni"
FILTER_TYPE_BY_RESOURCE_CODE = "Według kodów jednostek"

# Power plant to resource code mapping
POWER_PLANT_TO_RESOURCES = {
    "Bełchatów": ["BEL 2-02", "BEL 2-03", "BEL 2-04", "BEL 2-05", "BEL 4-06", "BEL 4-07", "BEL 4-08", "BEL 4-09", "BEL 4-10", "BEL 4-11", "BEL 4-12", "BEL 4-14"],
    "Chorzów": ["CHZ21S01", "CHZ21S02"],
    "Dolna Odra": ["DOD 2-05", "DOD 4-07", "DOD 4-08", "DOD_2-06"],
    "EC Czechnica-2": ["CZN_1S01"],
    "EC Rzeszów": ["REC 1-01"],
    "EC Siekierki": ["WSIB1-07", "WSIB1-08", "WSIB1-09", "WSIB1-10"],
    "EC Stalowa Wola": ["STW42S12"],
    "EC Wrotków": ["LEC 1-01"],
    "EC Włocławek": ["WLC_2S01"],
    "EC Łódź-4": ["LD4 1-03"],
    "EC Żerań 2": ["WZE22-20", "WZE22S20"],
    "Gryfino": ["EGF_4S09", "EGF_4S10"],
    "Jaworzno 2 JWCD": ["JW2_4-07"],
    "Jaworzno 3": ["JW3 1-03", "JW3 2-01", "JW3 2-02", "JW3 2-04", "JW3 2-05", "JW3 2-06"],
    "Karolin 2": ["KAR 1-03", "KAR_1-02"],
    "Katowice": ["KAT 1-01"],
    "Kozienice 1": ["KOZ11S02", "KOZ11S06", "KOZ12S01", "KOZ12S03", "KOZ12S04", "KOZ12S05", "KOZ12S07", "KOZ12S08"],
    "Kozienice 2": ["KOZ24S09", "KOZ24S10", "KOZ24S11"],
    "Kraków Łęg": ["KLE 1-01", "KLE 1-02", "KLE 1-03", "KLE 1-04"],
    "Opole": ["OPL 1-01", "OPL 1-02", "OPL 4-03", "OPL 4-04", "OPL 4-05", "OPL 4-06"],
    "Ostrołęka B": ["OSB_1S03", "OSB_2S01", "OSB_2S02"],
    "Porąbka Żar": ["PZR 2-01", "PZR 2-02", "PZR 2-03", "PZR 2-04"],
    "Połaniec": ["POL_2S02", "POL_2S03", "POL_2S04", "POL_4S05", "POL_4S06", "POL_4S07"],
    "Połaniec 2-Pasywna": ["POL24S09"],
    "Pątnów 2": ["PAT24S09"],
    "Płock": ["PLO_4S01"],
    "Rybnik": ["RYB 2-05", "RYB 2-06", "RYB 4-07", "RYB 4-08"],
    "Siersza": ["SIA 1-01", "SIA 1-02"],
    "Skawina": ["SNA11S03", "SNA22S05", "SNA22S06"],
    "Turów": ["TUR 1-01", "TUR 2-02", "TUR 2-03", "TUR 2-04", "TUR 2-05", "TUR 2-06", "TUR 4-11"],
    "Wrocław": ["WROB1-02", "WROB1-03"],
    "Zielona Góra": ["ZGR22S01"],
    "Łagisza": ["LGA 4-10"],
    "Łaziska 3": ["LZA31-09", "LZA31-10", "LZA32-11", "LZA32-12"],
    "Żarnowiec": ["ZRN_4-01", "ZRN_4-02", "ZRN_4-03", "ZRN_4-04"],
}

# All resource codes (flattened from power plant mapping)
ALL_RESOURCE_CODES = [
    "BEL 2-02", "BEL 2-03", "BEL 2-04", "BEL 2-05", "BEL 4-06", "BEL 4-07", "BEL 4-08", "BEL 4-09",
    "BEL 4-10", "BEL 4-11", "BEL 4-12", "BEL 4-14", "CHZ21S01", "CHZ21S02", "CZN_1S01", "DOD 2-05",
    "DOD 4-07", "DOD 4-08", "DOD_2-06", "EGF_4S09", "EGF_4S10", "JW2_4-07", "JW3 1-03", "JW3 2-01",
    "JW3 2-02", "JW3 2-04", "JW3 2-05", "JW3 2-06", "KAR 1-03", "KAR_1-02", "KAT 1-01", "KLE 1-01",
    "KLE 1-02", "KLE 1-03", "KLE 1-04", "KOZ11S02", "KOZ11S06", "KOZ12S01", "KOZ12S03", "KOZ12S04",
    "KOZ12S05", "KOZ12S07", "KOZ12S08", "KOZ24S09", "KOZ24S10", "KOZ24S11", "LD4 1-03", "LEC 1-01",
    "LGA 4-10", "LZA31-09", "LZA31-10", "LZA32-11", "LZA32-12", "OPL 1-01", "OPL 1-02", "OPL 4-03",
    "OPL 4-04", "OPL 4-05", "OPL 4-06", "OSB_1S03", "OSB_2S01", "OSB_2S02", "PAT24S09", "PLO_4S01",
    "POL24S09", "POL_2S02", "POL_2S03", "POL_2S04", "POL_4S05", "POL_4S06", "POL_4S07", "PZR 2-01",
    "PZR 2-02", "PZR 2-03", "PZR 2-04", "REC 1-01", "RYB 2-05", "RYB 2-06", "RYB 4-07", "RYB 4-08",
    "SIA 1-01", "SIA 1-02", "SNA11S03", "SNA22S05", "SNA22S06", "STW42S12", "TUR 1-01", "TUR 2-02",
    "TUR 2-03", "TUR 2-04", "TUR 2-05", "TUR 2-06", "TUR 4-11", "WLC_2S01", "WROB1-02", "WROB1-03",
    "WSIB1-07", "WSIB1-08", "WSIB1-09", "WSIB1-10", "WZE22-20", "WZE22S20", "ZGR22S01", "ZRN_4-01",
    "ZRN_4-02", "ZRN_4-03", "ZRN_4-04"
]


# ============================================================================
# API FUNCTIONS
# ============================================================================

def fetch_pse_page(
    url: str,
    params: Optional[Dict[str, str]] = None,
    is_first_request: bool = True,
    retry_count: int = 0
) -> tuple[Optional[Dict[str, Any]], Optional[str], bool]:
    """
    Fetch a single page from PSE API with exponential backoff retry logic.
    
    Args:
        url: API endpoint URL
        params: Query parameters (only for first request)
        is_first_request: Whether this is the initial request or using nextLink
        retry_count: Current retry attempt number
    
    Returns:
        Tuple of (response_data, next_link_url, error_occurred)
        - response_data: The JSON response or None on error
        - next_link_url: URL for next page or None if no more pages or error
        - error_occurred: True if request failed after all retries, False otherwise
    """
    headers = {"Accept": "application/json"}
    
    try:
        if is_first_request and params:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
        else:
            # For subsequent requests, use the full nextLink URL (no params)
            response = requests.get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
        
        response.raise_for_status()
        data = response.json()
        
        records = data.get("value", [])
        next_link = data.get("nextLink", None)
        
        logger.info(f"Successfully fetched {len(records)} records")
        logger.info(f"Next link: {next_link}")
        return data, next_link, False
        
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        if retry_count < MAX_RETRIES:
            delay = MIN_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER ** retry_count)
            logger.warning(
                f"Request failed (attempt {retry_count + 1}/{MAX_RETRIES}): {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)
            return fetch_pse_page(url, params, is_first_request, retry_count + 1)
        else:
            logger.error(f"Max retries exceeded: {e}")
            return None, None, True


def fetch_all_pse_data(
    start_date: date,
    end_date: date,
    page_size: int = DEFAULT_PAGE_SIZE,
    progress_callback=None
) -> List[Dict[str, Any]]:
    """
    Fetch all pages of PSE data for a given date range using pagination.
    
    This implements the PSE API pagination spec:
    1. Make initial request with $filter, $orderby, $first parameters
    2. Extract value array (records) and nextLink (if present)
    3. For subsequent requests, use the full nextLink URL as-is
    4. Repeat until nextLink is not present in response
    
    Args:
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)
        page_size: Records per page (max 100000)
        progress_callback: Optional callback function(current_page, total_records) for progress updates
    
    Returns:
        List of all records from all pages
    """
    # OData filter: business_date ge 'YYYY-MM-DD' and business_date le 'YYYY-MM-DD'
    filter_param = (
        f"business_date ge '{start_date.isoformat()}' and "
        f"business_date le '{end_date.isoformat()}'"
    )
    
    # OData orderby: ensures consistent pagination across requests
    orderby_param = "business_date asc,resource_code asc,operating_mode asc,dtime_utc asc"
    
    # Initial request parameters
    params = {
        "$filter": filter_param,
        "$orderby": orderby_param,
        "$first": str(page_size)
    }
    
    all_records = []
    current_url = PSE_API_BASE_URL
    use_params = True
    page_count = 0
    
    while current_url:
        page_count += 1
        logger.info(f"Fetching page {page_count}...")
        
        # Fetch current page
        data, next_link, error_occurred = fetch_pse_page(
            current_url,
            params=params if use_params else None,
            is_first_request=use_params
        )
        
        if error_occurred:
            logger.error(f"Failed to fetch page {page_count}")
            break
        
        # Extract records from response
        records = data.get("value", [])
        all_records.extend(records)
        
        # Call progress callback if provided
        if progress_callback:
            progress_callback(page_count, len(all_records))
        
        logger.info(f"Page {page_count}: {len(records)} records (Total: {len(all_records)})")
        
        # Check for next page
        if next_link:
            current_url = next_link
            use_params = False  # nextLink already contains all parameters
        else:
            current_url = None  # No more pages
    
    logger.info(f"Completed: {len(all_records)} total records across {page_count} pages")
    return all_records


def calculate_time_coverage(
    all_data: List[Dict[str, Any]],
    start_date: datetime,
    end_date: datetime
) -> tuple[float, Optional[datetime], Optional[datetime]]:
    """
    Calculate time-series coverage progress based on dtime values in collected data.
    Assumes 15-minute intervals.
    
    Returns:
        Tuple of (progress_percentage, earliest_dtime, latest_dtime)
    """
    if not all_data:
        return 0.0, None, None
    
    dtime_strings = [
        item.get("dtime") or item.get("dtime_utc")
        for item in all_data
        if item.get("dtime") or item.get("dtime_utc")
    ]
    
    if not dtime_strings:
        return 0.0, None, None
    
    # Parse datetime strings, skipping any malformed ones
    dtime_objects = []
    skipped_count = 0
    
    for dt in dtime_strings:
        try:
            dtime_objects.append(datetime.strptime(dt, "%Y-%m-%d %H:%M:%S"))
        except (ValueError, TypeError) as e:
            skipped_count += 1
            if skipped_count <= MAX_WARNING_LOGS:  # Log first few errors only
                logger.warning(f"Skipping malformed dtime value '{dt}': {e}")
    
    if skipped_count > MAX_WARNING_LOGS:
        logger.warning(f"Skipped {skipped_count} total malformed dtime values")
    
    if not dtime_objects:
        logger.error("No valid dtime values found after parsing")
        return 0.0, None, None
    
    try:
        earliest = min(dtime_objects)
        latest = max(dtime_objects)
        
        # Calculate coverage
        total_span = (end_date - start_date).total_seconds()
        covered_span = (latest - start_date).total_seconds()
        progress = min(covered_span / total_span, 1.0) if total_span > 0 else 0.0
        
        return max(progress, 0.0), earliest, latest
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error calculating time coverage: {e}")
        return 0.0, None, None


def calculate_expected_intervals(
    start_date: date,
    end_date: date,
    filter_type: str = FILTER_TYPE_ALL,
    selected_power_plants: Optional[List[str]] = None,
    selected_resources: Optional[List[str]] = None
) -> int:
    """
    Calculate expected number of 15-minute measurements based on date range and filters.
    
    Args:
        start_date: Start date
        end_date: End date
        filter_type: Type of filter (use FILTER_TYPE_* constants)
        selected_power_plants: List of selected power plants (if filtering by power plant)
        selected_resources: List of selected resource codes (if filtering by resource code)
    
    Returns:
        Expected number of 15-minute measurements (time_intervals * num_resources)
    """
    # Calculate number of 15-minute intervals in the date range
    delta = end_date - start_date
    total_minutes = delta.total_seconds() / 60
    time_intervals = int(total_minutes / 15) + 1
    
    # Determine the number of resources based on filter type
    num_resources = 0
    
    if filter_type == FILTER_TYPE_ALL:
        # All resource codes
        num_resources = len(ALL_RESOURCE_CODES)
    elif filter_type == FILTER_TYPE_BY_POWER_PLANT and selected_power_plants:
        # Count resources from selected power plants
        resource_set = set()
        for plant in selected_power_plants:
            if plant in POWER_PLANT_TO_RESOURCES:
                resource_set.update(POWER_PLANT_TO_RESOURCES[plant])
        num_resources = len(resource_set)
    elif filter_type == FILTER_TYPE_BY_RESOURCE_CODE and selected_resources:
        # Direct count of selected resources
        num_resources = len(selected_resources)
    else:
        # Default case: if no filter is properly selected, assume all resources
        logger.warning(
            "calculate_expected_intervals: Malformed or incomplete filter parameters detected "
            "(filter_type=%r, selected_power_plants=%r, selected_resources=%r). "
            "Defaulting to all resources.",
            filter_type, selected_power_plants, selected_resources
        )
        num_resources = len(ALL_RESOURCE_CODES)
    
    # Expected measurements = time intervals * number of resources
    return time_intervals * num_resources
