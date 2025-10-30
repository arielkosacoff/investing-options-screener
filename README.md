# Put Options Screener - Execution Instructions

Automated screening system for identifying cash-secured put selling opportunities using PostgreSQL database.

## Prerequisites

- **Python 3.8+**
- **PostgreSQL 12+** installed and running
- **Git** (for cloning repository)

## Installation

### 1. Clone Repository & Install Dependencies

```bash
cd investing-options-screener
pip3 install -r requirements.txt
```

### 2. Configure Database

Create `.env` file with your PostgreSQL credentials:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=postgres
DB_PASSWORD=your_password
DB_DATABASE=investing_options_screener
DB_LOGGING=false
```

### 3. Initialize Database

```bash
chmod +x setup_database.sh
./setup_database.sh
```

This creates the database and all required tables.

## Running the Application

### Start Web Interface

Option 1: Using the quick start script (recommended)
```bash
chmod +x start.sh
./start.sh
```

Option 2: Direct Python execution
```bash
python3 web_app.py
```

The browser will open automatically at [http://localhost:5001](http://localhost:5001)

## First-Time Setup Workflow

Run these steps in order (use the web UI buttons):

1. **Populate Stocks** (~1-2 min)
   - Fetches up to 2000 top US stocks by market cap (yfinance screener)
   - Falls back to S&P 500 + NASDAQ 100 if screener unavailable
   - No metadata fetching - just stores symbols
   - All metadata populated during price sync (step 2)
   - Adds market ETFs (SPY, QQQ, IWM)

2. **Update Prices** (~30-60 min first time, ~5-15 min incremental)
   - Downloads 1 year of historical price data (first time)
   - **Re-fetches last day** to ensure final closing prices (incremental)
   - Updates ticker metadata: name, sector, industry, market_cap, shares_outstanding, earnings_date
   - **Saves fundamental metrics:** PE ratio, beta, dividend yield
   - Subsequent runs are incremental (much faster)
   - **Shows ETA:** Real-time progress with time remaining

3. **Calculate Metrics** (~10-20 min, 3-5x faster than before)
   - **Optimized single-pass:** Fetches price data once per ticker
   - Computes 52W percentile, ATR, volume metrics
   - Uses pandas-ta for technical indicators
   - **Shows ETA:** Real-time progress with time remaining

4. **Execute Screener** (~10-20 min)
   - Filters stocks and analyzes options chains
   - Displays opportunities in results table
   - **Shows ETA:** Real-time progress with time remaining

## Daily Usage

After initial setup, only run:

1. **Update Prices** (~5-10 min)
   - Fetches only new data since last sync

2. **Calculate Metrics** (~10-20 min)
   - Updates metrics with latest prices

3. **Execute Screener** (~10-20 min)
   - Finds current opportunities

## Monitoring Progress

- Progress bars show real-time status during operations
- "Data Status" section displays last sync dates
- Check `screener.log` for detailed logs

## Configuration

Click "View/Edit Config" button to see current settings.

Default configuration:
- Stock 52W percentile: ≤ 20% (bottom of range)
- PE ratio: 5-20 (must be < sector & market PE)
- Market cap: ≥ $1B
- Avg volume: ≥ $10M/day
- Target DTE: 30 days (±7 days tolerance)
- Put strike: 10% OTM
- Min annualized yield: 36%
- Target premium: $10,000
- ATR threshold: 3% (for lateral trend detection)

## Troubleshooting

### Database Connection Error
```bash
# Check PostgreSQL is running
pg_isready

# Check if database exists
psql -l | grep investing
```

### No Results Found
- Verify "Data Status" shows recent sync dates
- Check configuration isn't too restrictive
- Review `screener.log` for errors

### Slow Performance
- First-time operations are slow (lots of API calls)
- Subsequent updates are incremental and faster
- Consider reducing universe size if needed

## Manual Database Operations

### View Sync Status
```bash
psql -d investing_options_screener -c "SELECT * FROM sync_status;"
```

### Check Ticker Count
```bash
psql -d investing_options_screener -c "SELECT COUNT(*) FROM tickers;"
```

### View Recent Results
```bash
psql -d investing_options_screener -c "
  SELECT t.symbol, s.stock_price, s.annualized_yield
  FROM screening_results s
  JOIN tickers t ON t.id = s.ticker_id
  ORDER BY s.annualized_yield DESC
  LIMIT 10;
"
```

## Stopping the Application

Press `Ctrl+C` in the terminal running `web_app.py`

## Backup & Maintenance

### Backup Database
```bash
pg_dump investing_options_screener > backup_$(date +%Y%m%d).sql
```

### Restore Database
```bash
psql investing_options_screener < backup_20251030.sql
```

### Clean Old Data
A cleanup function (`cleanup_old_data()`) exists in database.py to remove old screening results, prices, and metrics (default: 90 days retention). Currently this must be run manually if needed. Prices and metrics are retained indefinitely by default.

## File Locations

- **Configuration:** Stored in database (migrated from `config.json`)
- **Logs:** `screener.log` (cleared on each app start)
- **Database Backups:** Create manually as needed

## Support

For issues or questions:
1. Check `screener.log` for error details
2. Verify database connection and sync status
3. Review CLAUDE.md for technical architecture details

## Updates

To update the application:

```bash
git pull
pip3 install -r requirements.txt --upgrade
# Restart web_app.py
```

Database schema migrations (if needed) will be handled automatically on application start.
