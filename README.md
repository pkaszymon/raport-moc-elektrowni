# ⚡ PSE Power Plant Data Exporter

A Streamlit web application for fetching and exporting time-series power generation data from the Polish Power System Operator (PSE) API. The application provides an intuitive interface for downloading generator unit data and exporting it to Excel format with advanced filtering and aggregation capabilities.

## 📋 Features

- **Data Fetching**: Retrieve historical power generation data from PSE API
- **Efficient Pagination**: Handles large datasets using API-provided pagination tokens
- **Smart Filtering**: Filter data by:
  - Date range
  - Specific power plants
  - Individual generator unit codes
- **Progress Tracking**: Real-time progress visualization based on time coverage
- **Data Aggregation**: Automatic hourly aggregation from 15-minute intervals
- **Excel Export**: Export data with multiple options:
  - Individual power plant data
  - All power plants in a single Excel file with multiple sheets
- **Data Caching**: Optional caching to avoid redundant API calls
- **Error Handling**: Automatic retry with exponential backoff for failed requests
- **Polish UI**: User interface in Polish language

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/pkaszymon/raport-moc-elektrowni.git
   cd raport-moc-elektrowni
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## 💻 Usage

### Running the Application

Start the Streamlit application:

```bash
streamlit run streamlit_app.py
```

The application will open in your default web browser at `http://localhost:8501`.

### Using the Application

1. **Select Date Range**: Choose the start and end dates for the data you want to fetch
2. **Choose Filter Type**: Select one of three filtering options:
   - **All Data**: Fetch all available data
   - **By Power Plant**: Select specific power plants from a list of 35+ facilities
   - **By Unit Codes**: Select specific generator units using their codes
3. **Configure Settings** (in sidebar):
   - Adjust page size (batch size for API requests, max 100,000 records)
   - Enable/disable data caching
4. **Fetch Data**: Click the "Pobierz dane" (Fetch Data) button to start downloading
5. **Preview Data**: View raw data and statistics in the application
6. **Export to Excel**: Download data for individual power plants or all plants combined

## 📊 Data Format

The application fetches data from the PSE API endpoint: `https://api.raporty.pse.pl/api/gen-jw`

### Data Fields

The data includes information about generator units with 15-minute intervals:
- `business_date`: Business date
- `dtime` / `dtime_utc`: Timestamp of the measurement
- `resource_code`: Generator unit code
- `power_plant`: Power plant name
- `operating_mode`: Operating mode
- Various power generation values

### Aggregation

Data is automatically aggregated to hourly intervals by calculating the arithmetic mean of the four 15-minute measurements within each hour.

## 🏭 Supported Power Plants

The application includes a pre-configured list of 35+ Polish power plants, including:
- Bełchatów
- Turów
- Kozienice
- Opole
- Połaniec
- And many more...

## 🔧 Configuration

### API Settings

You can modify the following settings in `pse_api.py`:
- `REQUEST_TIMEOUT`: API request timeout (default: 60 seconds)
- `MAX_RETRIES`: Maximum retry attempts (default: 3)
- `RETRY_BACKOFF_MULTIPLIER`: Exponential backoff multiplier (default: 2)
- `DEFAULT_PAGE_SIZE`: Default records per page (default: 100,000)

## 📦 Dependencies

- `streamlit>=1.51.0` - Web application framework
- `polars>=1.35.2` - Fast DataFrame library for data processing
- `requests>=2.32.5` - HTTP library for API calls
- `openpyxl>=3.1.5` - Excel file handling
- `xlsxwriter>=3.2.9` - Excel file creation
- `pytest>=7.4.0` - Testing framework

## 🧪 Testing

The project includes automated tests for the new label detection functionality.

### Running Tests

To run the tests:

```bash
pytest test_pse_api.py -v
```

Or run all tests in the project:

```bash
pytest -v
```

### Test Coverage

The test suite covers:
- Empty data handling
- Known labels (no false positives)
- New power plant detection
- New resource codes for existing plants
- Mixed scenarios (new plants + new codes)
- Edge cases (duplicates, missing fields, sorting)

## 🔐 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 Notes

- The PSE API has a maximum limit of 100,000 records per request
- Data is fetched in batches to handle large date ranges efficiently
- The application uses caching by default to minimize redundant API calls
- All timestamps are in UTC format

## 🐛 Troubleshooting

### Connection Issues
If you encounter connection problems:
- Check your internet connection
- Verify the PSE API is accessible
- Try reducing the page size in advanced settings

### Memory Issues
For very large datasets:
- Reduce the date range
- Use filtering to limit the data scope
- Process data in smaller batches

## 📞 Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/pkaszymon/raport-moc-elektrowni).
