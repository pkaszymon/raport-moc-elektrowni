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
    fetch_pse_data_with_auto_split,
    calculate_time_coverage,
    calculate_expected_intervals,
    PSE_API_BASE_URL,
    MAX_RETRIES,
    MAX_EXPECTED_ENTRIES,
    FILTER_TYPE_ALL,
    AGGREGATION_15_MIN,
    AGGREGATION_HOURLY,
    AGGREGATION_DAILY
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# SESSION STATE CONFIGURATION
# ============================================================================
# Define session state keys and their default values as a constant to ensure
# consistency across initialization and cleanup operations
SESSION_STATE_DEFAULTS = {
    "all_data": [],
    "current_page": 0,
    "next_link": None,
    "min_dtime": None,
    "max_dtime": None,
    "query_params": None,
    "current_progress": 0.0,
    "current_period": 0,
    "total_periods": 0
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def sanitize_filename(name: str, max_length: int = None) -> str:
    r"""
    Sanitize a string to be safe for use as a filename or Excel sheet name.
    
    Replaces characters that are invalid in filenames or Excel sheet names:
    / \ : * ? [ ]
    
    Args:
        name: The string to sanitize. Empty strings are allowed and will return
              an empty string after sanitization.
        max_length: Optional maximum length to truncate to (useful for Excel sheet names).
                    Note: Truncation uses simple string slicing [:max_length], which
                    operates on Unicode code points and will cleanly truncate at character
                    boundaries without corrupting multi-byte characters.
    
    Returns:
        Sanitized string safe for use as filename or sheet name. Returns empty string
        if input is empty.
    """
    sanitized = name
    for char in ['/', '\\', ':', '*', '?', '[', ']']:
        sanitized = sanitized.replace(char, '_')
    
    if max_length is not None:
        sanitized = sanitized[:max_length]
    
    return sanitized

def extract_year_expr() -> pl.Expr:
    """
    Create a Polars expression to extract the year from a 'dtime' column.
    
    Expects 'dtime' to be in ISO 8601 format (e.g., '2024-01-15 12:30:00').
    Extracts the first 4 characters which represent the year.
    
    Returns:
        Polars expression that extracts year from 'dtime' column
    """
    return pl.col("dtime").str.slice(0, 4).alias("year")


def extract_date_expr() -> pl.Expr:
    """
    Create a Polars expression to extract the date from a 'dtime' column.
    
    Expects 'dtime' to be in ISO 8601 format (e.g., '2024-01-15 12:30:00').
    Extracts the first 10 characters which represent the date (YYYY-MM-DD).
    
    Returns:
        Polars expression that extracts date from 'dtime' column
    """
    return pl.col("dtime").str.slice(0, 10).alias("date")


def format_hourly_period_expr() -> pl.Expr:
    """
    Create a Polars expression to format an hourly period range from 'dtime' column.
    
    Expects 'dtime' to be in ISO 8601 format (e.g., '2024-01-15 12:30:00').
    Extracts the hour and formats it as "HH:00 - HH:00" (e.g., "12:00 - 13:00").
    
    Returns:
        Polars expression that formats hourly period from 'dtime' column
    """
    return (pl.col("dtime").str.slice(11, 2).str.zfill(2) + ":00 - " +
            ((pl.col("dtime").str.slice(11, 2).cast(pl.Int32) + 1) % 24)
            .cast(pl.Utf8).str.zfill(2) + ":00").alias("period")


def format_daily_period_expr() -> pl.Expr:
    """
    Create a Polars expression for a daily period constant.
    
    Returns a constant "00:00-23:59" representing a full day period.
    
    Returns:
        Polars expression that creates a daily period constant
    """
    return pl.lit("00:00-23:59").alias("period")


def create_pivot_table(data_df: pl.DataFrame, value_column: str, agg_interval: str) -> pl.DataFrame:
    """
    Create a pivot table from the provided DataFrame, aggregating values as appropriate.

    Parameters:
        data_df (pl.DataFrame): The input data containing time-series values.
        value_column (str): The name of the column containing values to aggregate.
        agg_interval (str): The aggregation interval; one of AGGREGATION_15_MIN, AGGREGATION_HOURLY, or AGGREGATION_DAILY.

    Returns:
        pl.DataFrame: A pivot table sorted by 'date' and 'period', with resource codes as columns.

    Behavior:
        - If agg_interval == AGGREGATION_15_MIN, no aggregation is performed; the first value for each interval is used.
        - If agg_interval is AGGREGATION_HOURLY or AGGREGATION_DAILY, values are aggregated using the mean for each interval.
    """
    if agg_interval == AGGREGATION_15_MIN:
        # No aggregation for 15-minute intervals
        pivot = data_df.pivot(
            values=value_column,
            index=["date", "period"],
            on="resource_code",
            aggregate_function="first"
        )
    else:
        # Use mean for hourly and daily aggregations
        pivot = data_df.pivot(
            values=value_column,
            index=["date", "period"],
            on="resource_code",
            aggregate_function="mean"
        )
    # Sort by date and period
    return pivot.sort(["date", "period"])


# ============================================================================
# STREAMLIT APP
# ============================================================================

page_size = 100000  # Default page size for API requests

def main():
    st.set_page_config(
        page_title="Dane generatorów PSE",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("⚡ Pobieranie danych z PSE")
    st.markdown(
        """
        Aplikacja do pobierania danych o mocy elektrowni z systemu PSE i zapisywania ich do pliku Excel.
        """
    )
    
    # ========================================================================
    # Query Configuration
    # ========================================================================
    
    st.header("📋 Wybierz dane do pobrania")
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data początkowa",
            value=date.today() - timedelta(days=7),
            help="Od której daty pobrać dane"
        )
    with col2:
        end_date = st.date_input(
            "Data końcowa",
            value=date.today(),
            help="Do której daty pobrać dane"
        )
    
    # All data will be fetched without filtering (no need for filter variables)
    
    # Validate date range
    if start_date > end_date:
        st.error("Data początkowa musi być wcześniejsza niż data końcowa")
        return
    
    st.divider()
    
    # ========================================================================
    # SIDEBAR: Advanced Options and Session Management
    # ========================================================================
    
    with st.sidebar:
        st.header("⚙️ Ustawienia zaawansowane")
        
        enable_cache = st.checkbox(
            "Użyj cache",
            value=True,
            help="Zapobiega ponownemu pobieraniu tych samych danych. UWAGA: Wyłącz tę opcję, jeśli chcesz zawsze ponownie pobierać dane z PSE."
        )
        
        
        # Reset button
        if st.button("🔄 Wyczyść pobrane dane", use_container_width=True):
            for key in SESSION_STATE_DEFAULTS.keys():
                if key in st.session_state:
                    del st.session_state[key]
            st.success("Dane zostały wyczyszczone")
            st.rerun()

    # ========================================================================
    # Initialize Session State
    # ========================================================================
    
    for key, default_value in SESSION_STATE_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    # ========================================================================
    # Main Content: Metrics & Controls
    # ========================================================================
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📊 Liczba rekordów",
            f"{len(st.session_state.all_data):,}",
            help="Ile rekordów zostało pobranych"
        )
    
    with col2:
        if st.session_state.total_periods > 0:
            progress_text = f"{st.session_state.current_progress*100:.0f}%"
            if st.session_state.total_periods > 1:
                progress_text += f" ({st.session_state.current_period}/{st.session_state.total_periods})"
        else:
            progress_text = "—"
        
        st.metric(
            "📈 Postęp pobierania",
            progress_text,
            help="Postęp pobierania danych w procentach"
        )
    
    with col3:
        if st.session_state.min_dtime:
            min_dt = datetime.strptime(st.session_state.min_dtime, "%Y-%m-%d %H:%M:%S")
            days_back = (date.today() - min_dt.date()).days
            st.metric(
                "📅 Najwcześniejszy rekord",
                st.session_state.min_dtime.split()[0],
                f"{days_back} dni temu" if days_back >= 0 else "przyszłość"
            )
        else:
            st.metric("📅 Najwcześniejszy rekord", "—", "Brak danych")
    
    with col4:
        expected_intervals = calculate_expected_intervals(
            start_date,
            end_date,
            FILTER_TYPE_ALL,
            None,
            None
        )
        st.metric(
            "⏱️ Oczekiwane pomiary",
            f"{expected_intervals:,}",
            "pomiary co 15 min"
        )
    
    # Progress tracking
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    # Query info
    st.info(
        f"**Wybrany okres:** {start_date.isoformat()} → {end_date.isoformat()} "
        f"({(end_date - start_date).days + 1} dni)"
    )

    # ========================================================================
    # Data Fetching Controls
    # ========================================================================

    col_fetch, col_info = st.columns([2, 3])

    with col_fetch:
        # Check if query parameters have changed (only date range and page size)
        current_query = f"{start_date.isoformat()}_{end_date.isoformat()}_{page_size}"
        if st.session_state.query_params != current_query:
            # Reset if query changed
            for key, default_value in SESSION_STATE_DEFAULTS.items():
                if key != "query_params":  # Skip query_params as we set it to the new value below
                    st.session_state[key] = default_value
            st.session_state.query_params = current_query
        
        has_more_pages = st.session_state.current_page == 0 or st.session_state.next_link is not None
        
        if st.button(
            "📥 Pobierz dane",
            width='stretch',
            type="primary"
        ):
            # Create placeholders for dynamic updates
            status_placeholder = st.empty()
            progress_bar = st.progress(0)
            
            # Calculate expected entries to inform the user (always all data)
            expected_entries = calculate_expected_intervals(
                start_date,
                end_date,
                FILTER_TYPE_ALL,
                None,
                None
            )
            
            status_placeholder.info(
                f"⏳ Postęp: {0:.0f}% | "
                f"Pobrano: {0:,} rekordów"
            )
            # Define progress callback
            def update_progress(progress_percentage, total_records, current_period, total_periods):
                # Update session state
                st.session_state.current_progress = progress_percentage
                st.session_state.current_period = current_period
                st.session_state.total_periods = total_periods
                
                # Update progress bar
                progress_bar.progress(progress_percentage)
                

                status_placeholder.info(
                    f"⏳ Postęp: {progress_percentage*100:.0f}% | "
                    f"Pobrano: {total_records:,} rekordów"
                )
            
            try:
                # Fetch all data (no filtering)
                all_records = fetch_pse_data_with_auto_split(
                    start_date=start_date,
                    end_date=end_date,
                    filter_type=FILTER_TYPE_ALL,
                    selected_power_plants=None,
                    selected_resources=None,
                    page_size=page_size,
                    progress_callback=update_progress
                )
                
                # Store the data
                st.session_state.all_data = all_records
                
                # Update dtime tracking
                dtime_values = [
                    item.get("dtime") or item.get("dtime_utc")
                    for item in all_records
                    if item.get("dtime") or item.get("dtime_utc")
                ]
                
                if dtime_values:
                    st.session_state.min_dtime = min(dtime_values)
                    st.session_state.max_dtime = max(dtime_values)
                
                # Final progress update
                progress_bar.progress(1.0)
                
                if st.session_state.total_periods > 1:
                    status_placeholder.success(
                        f"✓ Ukończono! Pobrano {len(all_records):,} rekordów"
                    )
                else:
                    status_placeholder.success(
                        f"✓ Ukończono! Pobrano {len(all_records):,} rekordów"
                    )
                
            except Exception as e:
                logger.error(f"Error during data fetch: {e}", exc_info=True)
                status_placeholder.error(
                    f"❌ **Nie udało się pobrać danych**\n\n"
                    f"Błąd: {str(e)}\n\n"
                    "💡 **Spróbuj ponownie:** Kliknij przycisk 'Pobierz dane' aby ponowić próbę."
                )
            
            st.rerun()
    
    with col_info:
        if st.session_state.all_data:
            st.success(
                f"**Gotowe do zapisu:** {len(st.session_state.all_data):,} rekordów"
            )
        else:
            st.info("**Status:** Brak danych")
    
    # ========================================================================
    # Data Preview & Statistics
    # ========================================================================
    
    if st.session_state.all_data:
        st.divider()
        


        col_preview, col_stats = st.columns([3, 1])
        
        with col_preview:
            st.subheader("📋 Podgląd nieprzetworzonych danych (ostatnie 100 rekordów)")
            
            df = pl.DataFrame(st.session_state.all_data)
            df = df.sort("dtime", descending=True)
            
            st.dataframe(
                df.head(100),
                width='stretch',
                height=400
            )
        
        with col_stats:
            st.subheader("📈 Statystyki")

            # Basic statistics
            col_stat1, col_stat2 = st.columns(2)
            
            with col_stat1:
                st.metric(
                    "Elektrownie",
                    df.select(pl.col("power_plant").n_unique()).item()
                )
                st.metric(
                    "Jednostki",
                    df.select(pl.col("resource_code").n_unique()).item()
                )
            
            with col_stat2:
                st.metric(
                    "Tryby pracy",
                    df.select(pl.col("operating_mode").n_unique()).item()
                )
                st.metric(
                    "Rozmiar danych",
                    f"{df.estimated_size('mb'):.1f} MB"
                )
            
            # Time span
            st.divider()
            st.write("**Zakres czasowy:**")
            if st.session_state.min_dtime and st.session_state.max_dtime:
                st.caption(f"Od: {st.session_state.min_dtime}")
                st.caption(f"Do: {st.session_state.max_dtime}")
                
                time_span = datetime.strptime(
                    st.session_state.max_dtime,
                    "%Y-%m-%d %H:%M:%S"
                ) - datetime.strptime(
                    st.session_state.min_dtime,
                    "%Y-%m-%d %H:%M:%S"
                )
                st.caption(f"Okres: {time_span.days}d {time_span.seconds // 3600}h")
        
        # ========================================================================
        # Data Preview and Preparation
        # ========================================================================
        
        st.divider()
        st.subheader("📊 Podgląd i przygotowanie danych")
        
        # Determine if data spans multiple years
        df_with_year = df.with_columns([
            extract_year_expr()
        ])
        unique_years = df_with_year.select(pl.col("year").unique()).to_series().to_list()
        unique_years = sorted([y for y in unique_years if y is not None])
        has_multiple_years = len(unique_years) > 1
        
        # Aggregation options
        if has_multiple_years:
            col_agg, col_split = st.columns(2)
        else:
            col_agg = st.container()
        
        with col_agg:
            aggregation_interval = st.radio(
                "Interwał agregacji danych",
                options=[AGGREGATION_15_MIN, AGGREGATION_HOURLY, AGGREGATION_DAILY],
                index=1,  # Default to hourly
                horizontal=True,
                help="Wybierz interwał czasowy dla agregacji danych"
            )
        
        # Show year split option only if data spans multiple years
        if has_multiple_years:
            with col_split:
                split_by_year = st.checkbox(
                    "Podziel dane według roku",
                    value=False,
                    help="Podziel dane na osobne tabele dla każdego roku (np. Bełchatów 2023, Bełchatów 2024)"
                )
        else:
            split_by_year = False
        
        # Get unique power plants and resource codes
        unique_power_plants = df.select(pl.col("power_plant").unique()).to_series().to_list()
        unique_power_plants = sorted([pp for pp in unique_power_plants if pp is not None])
        
        unique_resource_codes = df.select(pl.col("resource_code").unique()).to_series().to_list()
        unique_resource_codes = sorted([rc for rc in unique_resource_codes if rc is not None])
        
        st.info(
            f"📊 Pobrane dane zawierają **{len(unique_power_plants)} elektrowni** "
            f"i **{len(unique_resource_codes)} jednostek wytwórczych**"
        )
        
        with st.spinner("Przygotowuję tabele dla każdej elektrowni..."):
            power_plant_pivot_tables = {}
        
        for power_plant in unique_power_plants:
            # Filter data for this power plant
            plant_df = df.filter(pl.col("power_plant") == power_plant)
            
            # Extract date from dtime
            plant_df = plant_df.with_columns([
                extract_date_expr(),
                extract_year_expr()
            ])
            
            # Determine grouping based on aggregation interval
            if aggregation_interval == AGGREGATION_15_MIN:
                # No aggregation - use original dtime
                plant_df = plant_df.with_columns([
                    pl.col("dtime").alias("period")
                ])
                time_label = "15-minutowy"
            elif aggregation_interval == AGGREGATION_HOURLY:
                # Hourly aggregation
                plant_df = plant_df.with_columns([
                    format_hourly_period_expr()
                ])
                time_label = "godzinowy"
            else:  # AGGREGATION_DAILY
                # Daily aggregation
                plant_df = plant_df.with_columns([
                    format_daily_period_expr()
                ])
                time_label = "dzienny"
            
            # Get unique resource codes for this power plant
            resource_codes = plant_df.select(pl.col("resource_code").unique()).to_series().to_list()
            resource_codes = sorted([rc for rc in resource_codes if rc is not None])
            
            # Check which value column exists
            available_cols = plant_df.columns
            value_col = None
            for possible_col in ["wartosc", "mw", "value", "capacity_mw", "generation_mw", "capacity"]:
                if possible_col in available_cols:
                    value_col = possible_col
                    break
            
            if value_col:
                if split_by_year:
                    # Split by year
                    unique_years = plant_df.select(pl.col("year").unique()).to_series().to_list()
                    unique_years = sorted([y for y in unique_years if y is not None])
                    
                    for year in unique_years:
                        year_df = plant_df.filter(pl.col("year") == year)
                        pivot_df = create_pivot_table(year_df, value_col, aggregation_interval)
                        
                        table_name = f"{power_plant} {year}"
                        power_plant_pivot_tables[table_name] = {
                            'data': pivot_df,
                            'aggregation': time_label,
                            'year': year
                        }
                else:
                    # No year split - all data together
                    pivot_df = create_pivot_table(plant_df, value_col, aggregation_interval)
                    
                    power_plant_pivot_tables[power_plant] = {
                        'data': pivot_df,
                        'aggregation': time_label,
                        'year': None
                    }
            else:
                st.warning(f"Nie znaleziono odpowiedniej kolumny z wartościami dla {power_plant}. Dostępne kolumny: {available_cols}")
        
        # Store in session state
        st.session_state.power_plant_pivot_tables = power_plant_pivot_tables
        
        st.success(f"✓ Utworzono {len(power_plant_pivot_tables)} tabel")
    
        # Display preview and tile panel
        if power_plant_pivot_tables:
            # Preview section
            st.divider()
            st.subheader("👁️ Podgląd danych")
            
            selected_plant = st.selectbox(
                "Wybierz tabelę do podglądu",
                options=list(power_plant_pivot_tables.keys()),
                help="Wybierz tabelę, aby zobaczyć jej dane"
            )
            
            if selected_plant:
                table_info = power_plant_pivot_tables[selected_plant]
                pivot_df = table_info['data']
                aggregation_label = table_info['aggregation']
                
                col_plant_info, col_plant_stats = st.columns([2, 1])
                
                with col_plant_info:
                    st.write(f"**Tabela:** `{selected_plant}`")
                    st.write(f"**Agregacja:** {aggregation_label}")
                    st.write(f"**Liczba wierszy:** {len(pivot_df):,}")
                
                with col_plant_stats:
                    # Number of resource code columns (excluding date and period)
                    resource_cols = [c for c in pivot_df.columns if c not in ["date", "period"]]
                    st.metric("Kolumn z danymi", len(resource_cols))
                    st.metric("Rozmiar tabeli", f"{pivot_df.estimated_size('mb'):.2f} MB")
                
                # Show preview
                st.write("**Podgląd (pierwsze 50 wierszy):**")
                st.caption(f"Dane zagregowane z interwałem: {aggregation_label}")
                st.dataframe(
                    pivot_df.head(50),
                    width='stretch',
                    height=400
                )

            st.subheader("📥 Pliki Excel dla poszczególnych elektrowni")
            
            # Tile panel for downloads inside an expander
            with st.expander("📥 Lista arkuszy—kliknij, aby rozwinąć"): 
                st.write("Kliknij przycisk przy wybranym arkuszu, aby pobrać go jako plik Excel.")

                # Search bar to filter sheets (case-insensitive)
                if 'download_search' not in st.session_state:
                    st.session_state['download_search'] = ''

                st.text_input(
                    "🔎 Szukaj arkusza",
                    key='download_search',
                    placeholder="🔎 Wpisz część nazwy elektrowni lub rok, np. Bełchatów 2024"
                )

                search_query = st.session_state.get('download_search', '')

                # Create tiles in a grid layout
                num_cols = 3
                tables_list = list(power_plant_pivot_tables.items())

                # Apply search filter
                if search_query:
                    q = search_query.strip().lower()
                    tables_list = [t for t in tables_list if q in t[0].lower()]

                if not tables_list:
                    st.info("Brak arkuszy pasujących do zapytania wyszukiwania.")

                for i in range(0, len(tables_list), num_cols):
                    cols = st.columns(num_cols)
                    for j in range(num_cols):
                        if i + j < len(tables_list):
                            table_name, table_info = tables_list[i + j]
                            pivot_df = table_info['data']
                            aggregation_label = table_info['aggregation']

                            with cols[j]:
                                # Create a card-like container
                                with st.container(border=True):
                                    st.write(f"**{table_name}**")
                                    st.caption(f"📊 {len(pivot_df):,} rekordów")
                                    st.caption(f"⏱️ Interwał: {aggregation_label}")

                                    # Export button
                                    output = io.BytesIO()
                                    pivot_df.write_excel(output)
                                    output.seek(0)

                                    safe_filename = sanitize_filename(table_name)
                                    st.download_button(
                                        label="💾 Pobierz Excel",
                                        data=output,
                                        file_name=f"{safe_filename}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"download_{table_name}",
                                        use_container_width=True
                                    )


            st.subheader("📦 Pobierz wszystkie arkusze")
            
            # Build dynamic mapping from downloaded data (optimized using group_by)
            dynamic_plant_to_resources = (
                df.group_by("power_plant")
                .agg(pl.col("resource_code").unique().alias("resources"))
                .with_columns([
                    pl.col("resources").list.sort()
                ])
            )
            
            # Filter selection for Excel export
            st.write("**🔍 Filtruj dane do eksportu:**")
            
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                export_filter_type = st.radio(
                    "Sposób filtrowania",
                    options=["Wszystkie dane", "Według elektrowni", "Według kodów jednostek"],
                    index=0,
                    horizontal=False,
                    help="Wybierz, które dane chcesz wyeksportować do pliku Excel",
                    key="export_filter_type"
                )
            
            with col_filter2:
                if export_filter_type == "Według elektrowni":
                    selected_export_plants = st.multiselect(
                        "Elektrownie do eksportu",
                        options=unique_power_plants,
                        default=unique_power_plants,
                        help="Wybierz elektrownie, które chcesz uwzględnić w pliku Excel",
                        key="selected_export_plants"
                    )
                elif export_filter_type == "Według kodów jednostek":
                    selected_export_resources = st.multiselect(
                        "Kody jednostek do eksportu",
                        options=unique_resource_codes,
                        default=unique_resource_codes,
                        help="Wybierz kody jednostek, które chcesz uwzględnić w pliku Excel",
                        key="selected_export_resources"
                    )

            if st.button("📦 Przygotuj wszystkie tabele jako jeden plik Excel", help="Utwórz plik Excel ze wszystkimi tabelami na osobnych arkuszach"):
                with st.spinner("Tworzę plik Excel ze wszystkimi tabelami..."):
                    import xlsxwriter
                    import numpy as np
                    
                    # Helper function to check if a table matches a power plant
                    def table_matches_plant(table_name: str, plant_name: str) -> bool:
                        """Check if table name corresponds to the given power plant."""
                        # Remove year suffix if present (e.g., "Bełchatów (2023)" -> "Bełchatów")
                        base_table_name = table_name.split(' (')[0] if ' (' in table_name else table_name
                        return base_table_name == plant_name

                    output_all = io.BytesIO()
                    workbook = xlsxwriter.Workbook(output_all, {'in_memory': True, 'nan_inf_to_errors': True})

                    # Determine which tables to include based on filter
                    tables_to_export = {}
                    
                    if export_filter_type == "Wszystkie dane":
                        # Export all tables
                        tables_to_export = power_plant_pivot_tables
                    elif export_filter_type == "Według elektrowni":
                        # Filter by selected power plants
                        for table_name, table_info in power_plant_pivot_tables.items():
                            if any(table_matches_plant(table_name, plant) for plant in selected_export_plants):
                                tables_to_export[table_name] = table_info
                    elif export_filter_type == "Według kodów jednostek":
                        # Filter by selected resource codes using dynamic mapping
                        # Convert dynamic mapping to dictionary format
                        plant_resource_dict = {
                            row["power_plant"]: row["resources"] 
                            for row in dynamic_plant_to_resources.to_dicts()
                        }
                        
                        # Find which power plants have the selected resource codes
                        plants_with_selected_resources = set()
                        for plant, resources in plant_resource_dict.items():
                            if any(rc in selected_export_resources for rc in resources):
                                plants_with_selected_resources.add(plant)
                        
                        # Include tables for those power plants
                        for table_name, table_info in power_plant_pivot_tables.items():
                            if any(table_matches_plant(table_name, plant) for plant in plants_with_selected_resources):
                                tables_to_export[table_name] = table_info
                    
                    for table_name, table_info in tables_to_export.items():
                        pivot_df = table_info['data']
                        # Sanitize sheet name (Excel has 31 char limit and some char restrictions)
                        sheet_name = sanitize_filename(table_name, max_length=31)

                        # Convert to pandas for xlsxwriter compatibility
                        pandas_df = pivot_df.to_pandas()

                        # Write to worksheet
                        worksheet = workbook.add_worksheet(sheet_name)

                        # Write headers
                        for col_num, col_name in enumerate(pandas_df.columns):
                            worksheet.write(0, col_num, col_name)

                        # Write data, handling NaN/Inf values
                        for row_num, row_data in enumerate(pandas_df.values, start=1):
                            for col_num, value in enumerate(row_data):
                                # Handle NaN and Inf values
                                if isinstance(value, (float, np.floating)):
                                    if np.isnan(value) or np.isinf(value):
                                        worksheet.write(row_num, col_num, None)  # Write empty cell
                                    else:
                                        worksheet.write(row_num, col_num, value)
                                else:
                                    worksheet.write(row_num, col_num, value)

                workbook.close()
                output_all.seek(0)
                st.session_state.excel_export = output_all.getvalue()
                file_size_mb = len(st.session_state.excel_export) / (1024 * 1024)
                st.success(f"✓ Przygotowano plik Excel z {len(tables_to_export)} arkuszami ({file_size_mb:.2f} MB)")

            if 'excel_export' in st.session_state:
                st.download_button(
                    label=f"💾 Pobierz wszystkie tabele (Excel)",
                    data=st.session_state.excel_export,
                    file_name=f"wszystkie_tabele_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Pobierz dane wszystkich tabel w jednym pliku Excel z wieloma arkuszami",
                    use_container_width=True
                )

if __name__ == "__main__":
    main()
