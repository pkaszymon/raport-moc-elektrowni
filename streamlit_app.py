"""
PSE API Data Exporter - Streamlit Application

This app fetches time-series generator unit data from the Polish Power System Operator (PSE) API,
handles pagination using nextLink tokens, and exports data to Excel.

Key features:
- Efficient pagination using API-provided nextLink tokens
- Time-series progress tracking based on dtime coverage
- Caching to avoid redundant API calls
- Error handling with exponential backoff
- Excel export with in-memory buffering
- Real-time progress visualization
"""

import streamlit as st
import polars as pl
import io
from datetime import datetime, timedelta, date
import logging

# Import PSE API functions
from pse_api import (
    fetch_pse_page,
    calculate_time_coverage,
    calculate_expected_intervals,
    PSE_API_BASE_URL
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# STREAMLIT APP
# ============================================================================

def main():
    st.set_page_config(
        page_title="PSE Generator Data Exporter",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("⚡ PSE Generator Unit Data Exporter")
    st.markdown(
        """
        Fetch time-series generator unit data from the Polish Power System Operator (PSE) API
        and export to Excel. Supports efficient pagination with real-time progress tracking.
        """
    )
    
    # ========================================================================
    # SIDEBAR: Query Configuration
    # ========================================================================
    
    with st.sidebar:
        st.header("📋 Query Configuration")
        
        # Date range selection
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=date.today() - timedelta(days=7),
                help="Date range start (inclusive)"
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                value=date.today(),
                help="Date range end (inclusive)"
            )
        
        # Validate date range
        if start_date > end_date:
            st.error("Start date must be before end date")
            return
        
        # Page size configuration
        page_size = st.slider(
            "Records per Page",
            min_value=1000,
            max_value=100000,
            value=100000,
            step=10000,
            help="Higher values = fewer requests but higher memory usage"
        )
        
        # Advanced options
        with st.expander("⚙️ Advanced Options"):
            st.info(
                "**OData Filter Format:**\n"
                "businessdate ge YYYY-MM-DD and businessdate le YYYY-MM-DD\n\n"
                "**Sort Order (OrderBy):**\n"
                "businessdate asc,resourcecode asc,operatingmode asc,dtime_utc asc"
            )
            
            enable_cache = st.checkbox(
                "Enable Caching",
                value=True,
                help="Cache results to avoid refetching the same data"
            )
        
        # Reset button
        if st.button("🔄 Clear Session Data", width='stretch'):
            for key in ["all_data", "current_page", "next_link", "min_dtime", "max_dtime", "query_params"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.success("Session data cleared")
            st.rerun()
    
    # ========================================================================
    # Initialize Session State
    # ========================================================================
    
    if "all_data" not in st.session_state:
        st.session_state.all_data = []
    if "current_page" not in st.session_state:
        st.session_state.current_page = 0
    if "next_link" not in st.session_state:
        st.session_state.next_link = None
    if "min_dtime" not in st.session_state:
        st.session_state.min_dtime = None
    if "max_dtime" not in st.session_state:
        st.session_state.max_dtime = None
    if "query_params" not in st.session_state:
        st.session_state.query_params = None
    
    # ========================================================================
    # Main Content: Metrics & Controls
    # ========================================================================
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📊 Total Records",
            f"{len(st.session_state.all_data):,}",
            help="Total records fetched across all pages"
        )
    
    with col2:
        st.metric(
            "📄 Current Page",
            st.session_state.current_page,
            help="Number of API requests completed"
        )
    
    with col3:
        if st.session_state.min_dtime:
            min_dt = datetime.strptime(st.session_state.min_dtime, "%Y-%m-%d %H:%M:%S")
            days_back = (date.today() - min_dt.date()).days
            st.metric(
                "📅 Earliest Record",
                st.session_state.min_dtime.split()[0],
                f"{days_back} days ago" if days_back >= 0 else "future"
            )
        else:
            st.metric("📅 Earliest Record", "—", "No data yet")
    
    with col4:
        expected_intervals = calculate_expected_intervals(start_date, end_date)
        st.metric(
            "⏱️ Expected Intervals",
            f"{expected_intervals:,}",
            "15-min intervals in range"
        )
    
    # Progress tracking
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    # Query info
    st.info(
        f"**Query Period:** {start_date.isoformat()} → {end_date.isoformat()} "
        f"({(end_date - start_date).days + 1} days)"
    )

    # ========================================================================
    # Data Fetching Controls
    # ========================================================================

    col_fetch, col_export, col_info = st.columns([2, 2, 3])

    with col_fetch:
        # Check if query parameters have changed
        current_query = f"{start_date.isoformat()}_{end_date.isoformat()}_{page_size}"
        if st.session_state.query_params != current_query:
            # Reset if query changed
            st.session_state.all_data = []
            st.session_state.current_page = 0
            st.session_state.next_link = None
            st.session_state.min_dtime = None
            st.session_state.max_dtime = None
            st.session_state.query_params = current_query
        
        has_more_pages = st.session_state.current_page == 0 or st.session_state.next_link is not None
        
        if st.button(
            "📥 Fetch All Pages",
            width='stretch',
            type="primary"
        ):
            # Create placeholders for dynamic updates
            status_placeholder = st.empty()
            progress_bar = st.progress(0)
            
            continue_fetching = True
            
            while continue_fetching:
                # Determine if this is the first request
                is_first_request = st.session_state.current_page == 0
                
                status_placeholder.info(f"⏳ Fetching page {st.session_state.current_page + 1}...")
                
                if is_first_request:
                    # Build initial query parameters
                    filter_param = (
                        f"business_date ge '{start_date.isoformat()}' and "
                        f"business_date le '{end_date.isoformat()}'"
                    )
                    orderby_param = "business_date asc,resource_code asc,operating_mode asc,dtime_utc asc"
                    params = {
                        "$filter": filter_param,
                        "$orderby": orderby_param,
                        "$first": str(page_size)
                    }
                    url = PSE_API_BASE_URL
                else:
                    # Use the stored nextLink URL
                    url = st.session_state.next_link
                    params = None
                
                # Fetch single page
                data, next_link = fetch_pse_page(
                    url=url,
                    params=params,
                    is_first_request=is_first_request
                )
                
                logger.info(f"Next link after fetch: {next_link}")
                logger.info(f"Data keys: {data.keys() if data else 'No data'}")
                logger.info(f"Logical value of data: {bool(data)}")

                if data:
                    records = data.get("value", [])
                    st.session_state.all_data.extend(records)
                    st.session_state.next_link = next_link
                    st.session_state.current_page += 1

                    logger.info(f"Updated session state: current_page={st.session_state.current_page}, next_link={st.session_state.next_link}")

                    # Update dtime tracking
                    dtime_values = [
                        item.get("dtime") or item.get("dtime_utc")
                        for item in records
                        if item.get("dtime") or item.get("dtime_utc")
                    ]
                    
                    logger.info(f"dtime values count: {len(dtime_values)}")
                    
                    if dtime_values:
                        current_min = min(dtime_values)
                        current_max = max(dtime_values)
                        
                        # Update min/max across all pages
                        if st.session_state.min_dtime is None or current_min < st.session_state.min_dtime:
                            st.session_state.min_dtime = current_min
                        if st.session_state.max_dtime is None or current_max > st.session_state.max_dtime:
                            st.session_state.max_dtime = current_max

                    logger.info(f"Session min_dtime: {st.session_state.min_dtime}, max_dtime: {st.session_state.max_dtime}")

                    # Update progress bar based on time coverage
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    logger.info(f"Calculating time coverage between {start_dt} and {end_dt}")
                    progress_pct, _, _ = calculate_time_coverage(
                        st.session_state.all_data,
                        start_dt,
                        end_dt
                    )
                    logger.info(f"Progress percentage: {progress_pct*100:.1f}%")
                    progress_bar.progress(progress_pct)
                    
                    status_placeholder.success(
                        f"✅ Page {st.session_state.current_page}: {len(records):,} records | "
                        f"Total: {len(st.session_state.all_data):,} | Coverage: {progress_pct*100:.1f}%"
                    )
                    
                    # Check if we should continue
                    if not next_link:
                        status_placeholder.success(f"✓ Complete! Fetched {len(st.session_state.all_data):,} records across {st.session_state.current_page} pages")
                        continue_fetching = False
                    elif st.session_state.max_dtime:
                        logger.info(f"Checking if lastest dtime {st.session_state.max_dtime} reaches end date {end_date}")
                        latest_dt_obj = datetime.strptime(
                            st.session_state.max_dtime,
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if latest_dt_obj.date() >= end_date:
                            logger.info(f"Latest dtime {latest_dt_obj.date()} is greater than or equal to end date {end_date}")
                            progress_bar.progress(1.0)
                            status_placeholder.success(f"✓ Reached end date! {len(st.session_state.all_data):,} total records")
                            continue_fetching = False
                else:
                    status_placeholder.error("❌ Failed to fetch data from API")
                    continue_fetching = False
            
            st.rerun()
    
    with col_export:
        if st.session_state.all_data:
            # Convert to Polars DataFrame for efficient export
            df = pl.DataFrame(st.session_state.all_data)
            
            # Sort by dtime descending for better readability
            df = df.sort("dtime", descending=True)
            
            # Create Excel file in memory
            excel_buffer = io.BytesIO()
            df.write_excel(excel_buffer)
            excel_buffer.seek(0)
            
            file_name = f"pse_data_{start_date.isoformat()}_to_{end_date.isoformat()}.xlsx"
            
            st.download_button(
                "📊 Download as Excel",
                data=excel_buffer.getvalue(),
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width='stretch'
            )
        else:
            st.info("⬅️ Load data first to enable export")
    
    with col_info:
        if st.session_state.all_data:
            st.success(
                f"**Ready to export:** {len(st.session_state.all_data):,} records "
                f"across {st.session_state.current_page} page(s)"
            )
        else:
            st.info("**Status:** No data loaded yet")
    
    # ========================================================================
    # Data Preview & Statistics
    # ========================================================================
    
    if st.session_state.all_data:
        st.divider()
        
        col_preview, col_stats = st.columns([3, 1])
        
        with col_preview:
            st.subheader("📋 Data Preview (Latest 100 records)")
            
            df = pl.DataFrame(st.session_state.all_data)
            df = df.sort("dtime", descending=True)
            
            st.dataframe(
                df.head(100),
                width='stretch',
                height=400
            )
        
        with col_stats:
            st.subheader("📈 Statistics")
            
            # Basic statistics
            col_stat1, col_stat2 = st.columns(2)
            
            with col_stat1:
                st.metric(
                    "Power Plants",
                    df.select(pl.col("power_plant").n_unique()).item()
                )
                st.metric(
                    "Resources",
                    df.select(pl.col("resource_code").n_unique()).item()
                )
            
            with col_stat2:
                st.metric(
                    "Operating Modes",
                    df.select(pl.col("operating_mode").n_unique()).item()
                )
                st.metric(
                    "DataFrame Size",
                    f"{df.estimated_size('mb'):.1f} MB"
                )
            
            # Time span
            st.divider()
            st.write("**Time Coverage:**")
            if st.session_state.min_dtime and st.session_state.max_dtime:
                st.caption(f"From: {st.session_state.min_dtime}")
                st.caption(f"To: {st.session_state.max_dtime}")
                
                time_span = datetime.strptime(
                    st.session_state.max_dtime,
                    "%Y-%m-%d %H:%M:%S"
                ) - datetime.strptime(
                    st.session_state.min_dtime,
                    "%Y-%m-%d %H:%M:%S"
                )
                st.caption(f"Span: {time_span.days}d {time_span.seconds // 3600}h")


if __name__ == "__main__":
    main()
