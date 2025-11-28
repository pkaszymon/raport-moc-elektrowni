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
        
        if error_occurred or data is None:
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
    
    try:
        dtime_objects = [
            datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            for dt in dtime_strings
        ]
        
        earliest = min(dtime_objects)
        latest = max(dtime_objects)
        
        # Calculate coverage
        total_span = (end_date - start_date).total_seconds()
        covered_span = (latest - start_date).total_seconds()
        progress = min(covered_span / total_span, 1.0) if total_span > 0 else 0.0
        
        return max(progress, 0.0), earliest, latest
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error parsing dtime values: {e}")
        return 0.0, None, None


def calculate_expected_intervals(start_date: date, end_date: date) -> int:
    """Calculate expected number of 15-minute intervals in date range."""
    delta = end_date - start_date
    total_minutes = delta.total_seconds() / 60
    return int(total_minutes / 15) + 1
