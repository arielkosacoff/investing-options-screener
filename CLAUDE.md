# Investing Options Screener - AI Agent Instructions

## Project Overview

Automated options screening system using PostgreSQL database for persistent storage, with web-based interface for data management and screening.

## Architecture

### Database Layer (`database.py`)
- **ORM:** SQLAlchemy with PostgreSQL
- **Connection:** Configured via `.env` file
- **Tables:** markets, sectors, industries, tickers, ticker_prices, ticker_metrics, config, sync_status, screening_results
- **Industry Usage:** Industries stored for description/display only, NOT used in relative strength analysis
- **Relative Strength:** Uses sector and market ETFs only (not industry ETFs)

### Data Management Modules

1. **`data_population.py`** - Stock Universe Population
   - Fetches up to 2000 top US stocks by market cap using yfinance screener
   - Fallback to S&P 500 + NASDAQ 100 if screener fails
   - No yfinance API calls - just stores ticker symbols
   - All metadata populated during price sync (step 2)
   - Preserves existing FK relationships (upsert-only, never delete)
   - Populates market ETFs (SPY, QQQ, IWM)

2. **`price_sync.py`** - Price Data Synchronization
   - **Re-fetches last day** on incremental sync (ensures final closing data)
   - Stores adjusted prices (handles splits automatically)
   - Updates ticker metadata: name, sector, industry, market_cap, shares_outstanding, earnings_date
   - **Saves fundamental metrics** (PE, beta, dividend_yield) to ticker_metrics table
   - Uses calendar API for earnings dates (fixes timezone bug)
   - Batch processing with ETA-enabled progress callbacks

3. **`metrics_calculation.py`** - Technical Metrics Only
   - **NO yfinance imports** - all data from database
   - **Single-pass optimization:** Fetches price data ONCE per ticker, reuses for all calculations
   - Uses pandas-ta for ATR calculation
   - Calculates: 52W percentile, ATR%, avg volume, days to earnings
   - **3-5x faster** than previous implementation
   - EAV (Entity-Attribute-Value) storage pattern in `ticker_metrics`

4. **`put_screener.py`** - Options Screening Engine
   - Reads data from database (no live API calls except for options chains)
   - Multi-stage filtering: 52W position, relative strength, fundamentals, options premium
   - Stores results in `screening_results` table

### Web Application (`web_app.py`)

**Framework:** Flask with background threading for long operations

**Key Endpoints:**
- `GET /api/sync-status` - Last sync dates
- `GET /api/progress` - Real-time progress of background operations
- `POST /api/populate-stocks` - Initialize stock universe
- `POST /api/sync-prices` - Update price data
- `POST /api/calculate-metrics` - Calculate all metrics
- `POST /api/execute-screener` - Run screening
- `GET /api/results` - Fetch latest screening results
- `GET /api/config` - Configuration management

**Progress Tracking:**
- Polls `/api/progress` every 2 seconds during operations
- Global `progress_state` dict tracks current operation
- **ETA calculation:** Shows elapsed time and estimated time remaining
- ETA updates dynamically based on average processing speed

### Frontend (`templates/index.html`)

**UI Workflow:**
1. Display last sync status (prices/metrics dates)
2. Four workflow buttons (populate → sync → calculate → screen)
3. Real-time progress bar with current ticker/status/ETA
4. **ETA display:** Shows "Xm Ys remaining" during operations
5. Results table with comprehensive columns
6. Configuration viewer

## Configuration System

- **Storage:** Database `config` table (key-value with data_type)
- **Migration:** Auto-migrates `config.json` to database on first run
- **Access:** Via web UI or API endpoints

## Key Design Patterns

1. **Upsert Operations:** All data updates use upsert (insert or update) to preserve relationships
2. **Incremental Sync:** Price sync from last date (re-fetches for final data), metrics sync from last calc date
3. **Background Operations:** Long-running tasks use daemon threads with ETA-enabled progress callbacks
4. **EAV Metrics:** Flexible metric storage allows adding new calculations without schema changes
5. **Single-Pass Processing:** Fetch data once, reuse for multiple calculations (metrics optimization)
6. **Separation of Concerns:** Fundamentals fetched during price sync, technical metrics calculated separately

## Data Sources

- **Tickers:** yfinance screener (top stocks by market cap), fallback to Wikipedia S&P 500/NASDAQ 100
- **Price/Fundamental Data:** yfinance (adjusted prices, info dict)
- **Sector/Industry Taxonomy:** yfinance Sector/Industry API
- **Options Chains:** yfinance options API (real-time for screening)
- **Earnings Dates:** yfinance earnings_dates (best-effort, may be None)

## Workflow

### Initial Setup
1. Run `./setup_database.sh` to create PostgreSQL database
2. Start web app: `python3 web_app.py`
3. Click "Populate Stocks" (adds ~2000 tickers)
4. Click "Update Prices" (fetches 1 year history)
5. Click "Calculate Metrics" (computes all metrics)
6. Click "Execute Screener" (filters and finds opportunities)

### Daily Usage
1. Click "Update Prices" (incremental, only new days)
2. Click "Calculate Metrics" (updates with latest data)
3. Click "Execute Screener" (find opportunities)

## Screening Strategy

**Target:** Weak stock in strong sector/market with quality fundamentals and high option premium

**Filters (configurable):**
- Stock 52W percentile below threshold (default: bottom 20%)
- Relative strength: Stock < Sector < Market
- PE ratio within range and below sector/market PE
- Minimum market cap and volume
- Put option near target DTE with minimum annualized yield
- Optional: Lateral trend detection via ATR threshold

## Technical Details

- **Python:** 3.8+
- **Database:** PostgreSQL 12+
- **Dependencies:** See `requirements.txt`
- **Logging:** `screener.log` (cleared on app start)

## File Structure

```
├── database.py              # ORM models and DB functions
├── data_population.py       # Ticker population module
├── price_sync.py            # Price synchronization module
├── metrics_calculation.py   # Metrics calculation module
├── put_screener.py          # Screening engine
├── web_app.py               # Flask web application
├── setup_database.sh        # Database initialization script
├── start.sh                 # Quick start script (web UI launcher)
├── requirements.txt         # Python dependencies
├── .env                     # Database credentials
├── screener.log             # Application log
└── templates/
    └── index.html           # Web UI
```

## Important Notes for AI Agents

1. **Preserve FK relationships** - Only upsert, never delete tickers during sync
2. **Check sync status** - Verify last sync dates before operations
3. **Handle None values** - Earnings dates and some metrics may be None
4. **Adjusted prices only** - yfinance auto-adjusts for splits
5. **Progress callbacks** - Always provide for UI updates
6. **Background threads** - Use daemon threads for long operations
7. **EAV flexibility** - New metrics don't require schema migrations

## Common Issues

### Database Connection Failed
- Check PostgreSQL is running
- Verify `.env` credentials
- Ensure database exists

### Slow Price Sync
- First sync fetches 1 year (slow)
- Subsequent syncs are incremental (fast)

### Missing Metrics
- Ensure prices synced first
- Check logs for specific ticker errors
- Some tickers may lack sufficient data

### Screening Returns Zero Results
- Check configuration values
- Verify metrics calculated for current date
- Review sync_status table
