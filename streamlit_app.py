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
    PSE_API_BASE_URL,
    MAX_RETRIES,
    POWER_PLANT_TO_RESOURCES,
    ALL_RESOURCE_CODES,
    FILTER_TYPE_ALL,
    FILTER_TYPE_BY_POWER_PLANT,
    FILTER_TYPE_BY_RESOURCE_CODE
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# STREAMLIT APP
# ============================================================================

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
    
    # Filter selection - mutually exclusive
    st.subheader("🔍 Filtrowanie danych")
    
    filter_type = st.radio(
        "Wybierz sposób filtrowania",
        options=[FILTER_TYPE_ALL, FILTER_TYPE_BY_POWER_PLANT, FILTER_TYPE_BY_RESOURCE_CODE],
        index=0,
        horizontal=True,
        help="Wybierz sposób filtrowania danych - możesz pobrać wszystko, wybrać konkretne elektrownie lub jednostki wytwórcze"
    )
    
    selected_power_plants = []
    selected_resources = []
    
    if filter_type == FILTER_TYPE_BY_POWER_PLANT:
        # Power plant filter
        power_plants = [
            "Siersza", "Rybnik", "EC Włocławek", "Porąbka Żar", "EC Stalowa Wola", 
            "Kozienice 1", "Zielona Góra", "Gryfino", "Chorzów", "Łagisza", 
            "Dolna Odra", "Pątnów 2", "EC Żerań 2", "Połaniec 2-Pasywna", "Turów", 
            "Karolin 2", "EC Wrotków", "Jaworzno 3", "Jaworzno 2 JWCD", "Ostrołęka B", 
            "EC Rzeszów", "Połaniec", "EC Siekierki", "EC Łódź-4", "Płock", 
            "Skawina", "Żarnowiec", "Łaziska 3", "Opole", "EC Czechnica-2", 
            "Katowice", "Wrocław", "Kraków Łęg", "Bełchatów", "Kozienice 2"
        ]
        
        selected_power_plants = st.multiselect(
            "Elektrownie",
            options=sorted(power_plants),
            default=[],
            help="Wybierz elektrownie, dla których chcesz pobrać dane"
        )
    
    elif filter_type == FILTER_TYPE_BY_RESOURCE_CODE:
        # Resource code filter
        resource_codes = [
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
            "WSIB1-07", "WSIB1-08", "WSIB1-09", "WSIB1-10", "WZE22S20", "ZGR22S01", "ZRN_4-01", "ZRN_4-02",
            "ZRN_4-03", "ZRN_4-04"
        ]
        
        selected_resources = st.multiselect(
            "Kody jednostek wytwórczych",
            options=resource_codes,
            default=[],
            help="Wybierz konkretne jednostki wytwórcze"
        )
    
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
        
        
        
        # Page size configuration
        page_size = st.slider(
            "Rozmiar porcji danych",
            min_value=1000,
            max_value=100000,
            value=100000,
            step=10000,
            help="Ile rekordów pobrać za jednym razem. Maksimum to 100 000 - limit API PSE."
        )
        
        st.info(
            "ℹ️ **Dlaczego pobieramy dane partiami?** \n\n"
            "API PSE nie pozwala pobrać wszystkich danych naraz. "
            "Maksymalnie można pobrać 100 000 rekordów na raz. "
            "Dla większych okresów dane są pobierane w kilku \"porcjach\", "
            "co pozwala na pobranie nawet bardzo dużych zbiorów danych. "
            "Mniejsze porcje pozwalają śledzić postęp pobierania danych na bieżąco. 😊"
        )
        
        enable_cache = st.checkbox(
            "Zapamiętaj pobrane dane",
            value=True,
            help="Zapobiega ponownemu pobieraniu tych samych danych. UWAGA: Wyłącz tę opcję, jeśli chcesz zawsze ponownie pobierać dane z PSE."
        )
        
        
        # Reset button
        if st.button("🔄 Wyczyść pobrane dane", use_container_width=True):
            for key in ["all_data", "current_page", "next_link", "min_dtime", "max_dtime", "query_params"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.success("Dane zostały wyczyszczone")
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
            "📊 Liczba rekordów",
            f"{len(st.session_state.all_data):,}",
            help="Ile rekordów zostało pobranych"
        )
    
    with col2:
        st.metric(
            "📄 Pobrano w częściach",
            st.session_state.current_page,
            help="Ilość części na jakie dane zostały podzielone w celu ułatwienia pobierania"
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
            filter_type,
            selected_power_plants,
            selected_resources
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
        # Check if query parameters have changed
        selected_resources_str = ",".join(sorted(selected_resources)) if selected_resources else ""
        selected_power_plants_str = ",".join(sorted(selected_power_plants)) if selected_power_plants else ""
        current_query = f"{start_date.isoformat()}_{end_date.isoformat()}_{page_size}_{filter_type}_{selected_resources_str}_{selected_power_plants_str}"
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
            "📥 Pobierz dane",
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
                
                status_placeholder.info(f"⏳ Pobieram dane, część {st.session_state.current_page + 1}...")
                
                if is_first_request:
                    # Build initial query parameters
                    filter_param = (
                        f"business_date ge '{start_date.isoformat()}' and "
                        f"business_date le '{end_date.isoformat()}'"
                    )
                    
                    # Add power plant filter if specific power plants are selected
                    if selected_power_plants:
                        if len(selected_power_plants) == 1:
                            filter_param += f" and power_plant eq '{selected_power_plants[0]}'"
                        else:
                            # Build an 'or' condition for multiple power plants
                            plant_conditions = " or ".join([f"power_plant eq '{plant}'" for plant in selected_power_plants])
                            filter_param += f" and ({plant_conditions})"
                    
                    # Add resource code filter if specific resources are selected
                    elif selected_resources:
                        if len(selected_resources) == 1:
                            filter_param += f" and resource_code eq '{selected_resources[0]}'"
                        else:
                            # Build an 'or' condition for multiple resources
                            resource_conditions = " or ".join([f"resource_code eq '{code}'" for code in selected_resources])
                            filter_param += f" and ({resource_conditions})"
                    
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
                data, next_link, error_occurred = fetch_pse_page(
                    url=url,
                    params=params,
                    is_first_request=is_first_request
                )
                
                logger.info(f"Next link after fetch: {next_link}")
                logger.info(f"Data keys: {data.keys() if data else 'No data'}")
                logger.info(f"Logical value of data: {bool(data)}")
                logger.debug(f"Error occurred: {error_occurred}")

                if error_occurred:
                    # Request failed after all retries - show error and stop
                    status_placeholder.error(
                        f"❌ **Nie udało się pobrać danych**\n\n"
                        f"Żądanie nie powiodło się po {MAX_RETRIES} próbach. Możliwe przyczyny:\n"
                        "- Problem z połączeniem internetowym\n"
                        "- Serwer PSE nie odpowiada\n"
                        "- Przekroczono limit czasu połączenia\n\n"
                        "💡 **Spróbuj ponownie:** Kliknij przycisk 'Pobierz dane' aby ponowić próbę."
                    )
                    continue_fetching = False
                elif data:
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
                        f"✅ Część {st.session_state.current_page}: {len(records):,} rekordów | "
                        f"Łącznie: {len(st.session_state.all_data):,} | Pokrycie: {progress_pct*100:.1f}%"
                    )
                    
                    # Check if we should continue
                    if not next_link:
                        status_placeholder.success(f"✓ Ukończono! Pobrano {len(st.session_state.all_data):,} rekordów w {st.session_state.current_page} częściach")
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
                            status_placeholder.success(f"✓ Pobrano wszystkie dane! Łącznie {len(st.session_state.all_data):,} rekordów")
                            continue_fetching = False
                else:
                    # Unexpected case: data is None but no error occurred
                    status_placeholder.error(
                        "❌ **Nieoczekiwany błąd**\n\n"
                        "Wystąpił nieoczekiwany problem podczas pobierania danych.\n\n"
                        "💡 **Spróbuj ponownie:** Kliknij przycisk 'Pobierz dane' aby ponowić próbę."
                    )
                    continue_fetching = False
            
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
        # Data Aggregation by Power Plant
        # ========================================================================
        
        st.divider()
        st.subheader("🏭 Zestawienie danych według elektrowni")
        
        # Get unique power plants
        unique_power_plants = df.select(pl.col("power_plant").unique()).to_series().to_list()
        unique_power_plants = sorted([pp for pp in unique_power_plants if pp is not None])
        
        st.info(f"Znaleziono **{len(unique_power_plants)}** elektrowni")
        
        with st.spinner("Przygotowuję tabele dla każdej elektrowni..."):
            power_plant_pivot_tables = {}
        
        for power_plant in unique_power_plants:
            # Filter data for this power plant
            plant_df = df.filter(pl.col("power_plant") == power_plant)
            
            # Extract date and period from dtime
            # Assuming dtime format is "YYYY-MM-DD HH:MM:SS"
            plant_df = plant_df.with_columns([
                pl.col("dtime").str.slice(0, 10).alias("date"),
                (pl.col("dtime").str.slice(11, 2).str.zfill(2) + ":00 - " + 
                    ((pl.col("dtime").str.slice(11, 2).cast(pl.Int32) + 1) % 24)
                    .cast(pl.Utf8).str.zfill(2) + ":00").alias("hour")  # Format as "HH:00 - HH:00"
            ])
            
            # Get unique resource codes for this power plant
            resource_codes = plant_df.select(pl.col("resource_code").unique()).to_series().to_list()
            resource_codes = sorted([rc for rc in resource_codes if rc is not None])
            
            # Create pivot table: aggregate by date and hour, with resource_codes as columns
            # We'll use the 'wartosc' or other value field as the aggregated value
            # First, let's check which value column exists
            available_cols = plant_df.columns
            value_col = None
            for possible_col in ["wartosc", "mw", "value", "capacity_mw", "generation_mw", "capacity"]:
                if possible_col in available_cols:
                    value_col = possible_col
                    break
            
            if value_col:
                # Pivot: rows are (date, hour), columns are resource_codes
                # Use mean to aggregate the 4 fifteen-minute intervals into 1 hour
                pivot_df = plant_df.pivot(
                    values=value_col,
                    index=["date", "hour"],
                    columns="resource_code",
                    aggregate_function="mean"  # Calculate arithmetic mean for hourly aggregation
                )
                
                # Sort by date and hour
                pivot_df = pivot_df.sort(["date", "hour"])
                
                power_plant_pivot_tables[power_plant] = pivot_df
            else:
                st.warning(f"Nie znaleziono odpowiedniej kolumny z wartościami dla {power_plant}. Dostępne kolumny: {available_cols}")
        
        # Store in session state
        st.session_state.power_plant_pivot_tables = power_plant_pivot_tables
        
        st.success(f"✓ Utworzono tabele dla {len(power_plant_pivot_tables)} elektrowni")
    
        # Display pivot tables
        if power_plant_pivot_tables:
            selected_plant = st.selectbox(
                "Wybierz elektrownię do podglądu",
                options=list(power_plant_pivot_tables.keys()),
                help="Wybierz elektrownię, aby zobaczyć jej dane"
            )
            
            if selected_plant:
                pivot_df = power_plant_pivot_tables[selected_plant]
                
                col_plant_info, col_plant_stats = st.columns([2, 1])
                
                with col_plant_info:
                    st.write(f"**Elektrownia:** `{selected_plant}`")
                    st.write(f"**Liczba wierszy (data-godzina):** {len(pivot_df):,}")
                
                with col_plant_stats:
                    # Number of resource code columns (excluding date and hour)
                    resource_cols = [c for c in pivot_df.columns if c not in ["date", "hour"]]
                    st.metric("Kolumn z danymi", len(resource_cols))
                    st.metric("Rozmiar tabeli", f"{pivot_df.estimated_size('mb'):.2f} MB")
                
                # Show preview
                st.write("**Podgląd (pierwsze 50 wierszy):**")
                st.caption("Dane zagregowane godzinowo - średnia z pomiarów 15-minutowych")
                st.dataframe(
                    pivot_df.head(50),
                    width='stretch',
                    height=400
                )
                
                # Export single power plant table
                st.divider()
                st.subheader("📥 Pobierz dane")
                
                col_export1, col_export2 = st.columns(2)
                
                with col_export1:
                    selected_plant = st.selectbox(
                        "Wybierz elektrownię do pobrania",
                        options=list(power_plant_pivot_tables.keys()),
                        help="Wybierz elektrownię, aby pobrać jej dane jako Excel"
                    )
                    # Export single table as Excel
                    output = io.BytesIO()
                    pivot_df.write_excel(output)
                    output.seek(0)
                    
                    st.download_button(
                        label=f"💾 Pobierz {selected_plant} (Excel)",
                        data=output,
                        file_name=f"{selected_plant}_dane.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        help="Pobierz dane tej elektrowni jako plik Excel"
                    )
                
                with col_export2:
                    # Export all tables as single Excel with multiple sheets
                    if len(power_plant_pivot_tables.keys()) > 1:
                        if st.button("📦 Przygotuj wszystkie elektrownie do pobrania (Excel)", help="Utwórz plik Excel ze wszystkimi elektrowniami na osobnych arkuszach"):
                            with st.spinner("Tworzę plik Excel ze wszystkimi elektrowniami..."):
                                # Use xlsxwriter to create multi-sheet Excel file
                                import xlsxwriter
                                import numpy as np
                                
                                output_all = io.BytesIO()
                                workbook = xlsxwriter.Workbook(output_all, {'in_memory': True, 'nan_inf_to_errors': True})
                                
                                for plant_name, plant_pivot_df in power_plant_pivot_tables.items():
                                    # Sanitize sheet name (Excel has 31 char limit and some char restrictions)
                                    sheet_name = plant_name[:31].replace("/", "_").replace("\\", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("[", "_").replace("]", "_")
                                    
                                    # Convert to pandas for xlsxwriter compatibility
                                    pandas_df = plant_pivot_df.to_pandas()
                                    
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
                                st.success(f"✓ Plik Excel gotowy z {len(power_plant_pivot_tables)} arkuszami")
                            if 'excel_export' in st.session_state:
                                st.download_button(
                                    label=f"💾 Pobierz wszystkie elektrownie (Excel)",
                                    data=st.session_state.excel_export,
                                    file_name=f"wszystkie_elektrownie_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    help="Pobierz dane wszystkich elektrowni w jednym pliku Excel z wieloma arkuszami"
                                )

if __name__ == "__main__":
    main()
