#!/usr/bin/env python3
"""
Web UI for Put Option Screener (Database Version)
Provides interactive interface for data management and screening with PostgreSQL backend.
"""

import json
import os
import time
import webbrowser
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from threading import Timer, Thread
from typing import Dict, Optional

from flask import Flask, render_template, jsonify, request

from database import (
    init_db,
    get_db,
    get_all_config,
    set_config,
    get_config,
    get_sync_status,
    get_latest_screening_results,
    Ticker
)
from data_population import populate_stocks
from price_sync import sync_all_prices
from metrics_calculation import calculate_all_metrics
from put_screener import screen_all_stocks

# Clean log file on startup
log_file = Path('screener.log')
if log_file.exists():
    log_file.unlink()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('screener.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global progress state for background operations
progress_state = {
    'active': False,
    'operation': None,
    'current': 0,
    'total': 0,
    'ticker': '',
    'status': '',
    'message': '',
    'last_updated': None,
    'start_time': None,       # Track start time for ETA
    'eta_seconds': None,      # Estimated time remaining
    'time_elapsed': None      # Time elapsed so far
}


# ==============================================================================
# CONFIG MIGRATION FROM JSON TO DATABASE
# ==============================================================================

def migrate_config_from_json():
    """Migrate config.json to database on first run"""
    config_file = Path('config.json')

    if not config_file.exists():
        logger.info("No config.json found, using database defaults")
        return

    db = get_db()

    try:
        # Check if config already migrated
        existing_config = get_all_config(db)

        if existing_config:
            logger.info("Config already exists in database, skipping migration")
            return

        # Load from JSON
        with open(config_file, 'r') as f:
            json_config = json.load(f)

        # Migrate to database
        config_mapping = {
            'STOCK_52W_PERCENTILE_MAX': ('float', 'Maximum 52-week percentile for stock'),
            'PE_RATIO_MIN': ('int', 'Minimum P/E ratio'),
            'PE_RATIO_MAX': ('int', 'Maximum P/E ratio'),
            'MARKET_CAP_MIN_MILLIONS': ('int', 'Minimum market cap in millions'),
            'AVG_VOLUME_USD_MIN_MILLIONS': ('int', 'Minimum average volume in millions USD'),
            'TARGET_DTE': ('int', 'Target days to expiration'),
            'DTE_TOLERANCE': ('int', 'DTE tolerance in days'),
            'PUT_STRIKE_DISCOUNT': ('float', 'Put strike discount (OTM %)'),
            'MIN_ANNUALIZED_PREMIUM_YIELD': ('float', 'Minimum annualized premium yield'),
            'TARGET_PREMIUM_THOUSANDS': ('int', 'Target premium in thousands'),
            'LATERAL_TREND_ATR_THRESHOLD': ('float', 'ATR threshold for lateral trend'),
            'STOCK_UNIVERSE': ('string', 'Stock universe (SP500, NASDAQ100, RUSSELL1000, CUSTOM)')
        }

        for key, value in json_config.items():
            if key in config_mapping:
                data_type, description = config_mapping[key]
                set_config(db, key, value, data_type, description)

        logger.info(f"Migrated {len(json_config)} config values from JSON to database")

        # Backup JSON file
        backup_file = config_file.with_suffix('.json.backup')
        config_file.rename(backup_file)
        logger.info(f"Backed up config.json to {backup_file}")

    except Exception as e:
        logger.error(f"Error migrating config: {e}")

    finally:
        db.close()


# ==============================================================================
# ROUTES
# ==============================================================================

@app.route('/')
def index():
    """Main page."""
    return render_template('index.html')


@app.route('/api/sync-status')
def api_sync_status():
    """Get sync status for prices and metrics"""
    db = get_db()

    try:
        stocks_status = get_sync_status(db, 'stocks')

        return jsonify({
            'success': True,
            'last_price_sync': stocks_status.last_price_sync.isoformat() if stocks_status and stocks_status.last_price_sync else None,
            'last_metrics_calc': stocks_status.last_metrics_calc.isoformat() if stocks_status and stocks_status.last_metrics_calc else None,
            'today': date.today().isoformat()
        })

    finally:
        db.close()


@app.route('/api/progress')
def api_progress():
    """Get current progress of background operation"""
    return jsonify({
        'active': progress_state['active'],
        'operation': progress_state['operation'],
        'current': progress_state['current'],
        'total': progress_state['total'],
        'ticker': progress_state['ticker'],
        'status': progress_state['status'],
        'message': progress_state['message'],
        'last_updated': progress_state['last_updated'],
        'time_elapsed': progress_state['time_elapsed'],
        'eta_seconds': progress_state['eta_seconds']
    })


@app.route('/api/populate-stocks', methods=['POST'])
def api_populate_stocks():
    """Populate stocks from S&P 500, NASDAQ 100, Russell 1000"""
    if progress_state['active']:
        return jsonify({
            'success': False,
            'message': 'Another operation is in progress'
        }), 409

    def progress_callback(current, total, ticker, status):
        import time

        # Track start time on first item
        if current == 1:
            progress_state['start_time'] = time.time()

        # Calculate ETA
        if progress_state['start_time'] and current > 0:
            elapsed = time.time() - progress_state['start_time']
            avg_time_per_item = elapsed / current
            remaining_items = total - current
            eta_seconds = int(avg_time_per_item * remaining_items)

            progress_state['time_elapsed'] = int(elapsed)
            progress_state['eta_seconds'] = eta_seconds

        progress_state['active'] = True
        progress_state['operation'] = 'populate_stocks'
        progress_state['current'] = current
        progress_state['total'] = total
        progress_state['ticker'] = ticker
        progress_state['status'] = status
        progress_state['message'] = f'Processing {ticker}...'
        progress_state['last_updated'] = datetime.now().isoformat()

    def run_population():
        try:
            logger.info("Starting stock population...")
            results = populate_stocks(max_count=2000, progress_callback=progress_callback)

            progress_state['message'] = f"Complete! Added: {results['added']}, Skipped: {results['skipped']}, Failed: {results['failed']}"
            logger.info(progress_state['message'])

        except Exception as e:
            progress_state['message'] = f"Error: {str(e)}"
            logger.error(f"Population error: {e}")

        finally:
            progress_state['active'] = False
            progress_state['start_time'] = None
            progress_state['eta_seconds'] = None
            progress_state['time_elapsed'] = None

    # Run in background thread
    thread = Thread(target=run_population)
    thread.daemon = True
    thread.start()

    return jsonify({
        'success': True,
        'message': 'Stock population started. Check progress endpoint for updates.'
    })


@app.route('/api/sync-prices', methods=['POST'])
def api_sync_prices():
    """Sync prices from last sync date forward"""
    if progress_state['active']:
        return jsonify({
            'success': False,
            'message': 'Another operation is in progress'
        }), 409

    data = request.get_json() or {}
    force_full = data.get('force_full', False)

    def progress_callback(current, total, ticker, status, last_date):
        import time

        # Track start time on first item
        if current == 1:
            progress_state['start_time'] = time.time()

        # Calculate ETA
        if progress_state['start_time'] and current > 0:
            elapsed = time.time() - progress_state['start_time']
            avg_time_per_item = elapsed / current
            remaining_items = total - current
            eta_seconds = int(avg_time_per_item * remaining_items)

            progress_state['time_elapsed'] = int(elapsed)
            progress_state['eta_seconds'] = eta_seconds

        progress_state['active'] = True
        progress_state['operation'] = 'sync_prices'
        progress_state['current'] = current
        progress_state['total'] = total
        progress_state['ticker'] = ticker
        progress_state['status'] = status
        progress_state['message'] = f"Last date: {last_date}" if last_date else ''
        progress_state['last_updated'] = datetime.now().isoformat()

    def run_sync():
        try:
            logger.info(f"Starting price sync (force_full={force_full})...")
            results = sync_all_prices(progress_callback=progress_callback, force_full_sync=force_full)

            progress_state['message'] = f"Complete! Success: {results['success_count']}, Failed: {results['failed_count']}, Days added: {results['total_days_added']}"
            logger.info(progress_state['message'])

        except Exception as e:
            progress_state['message'] = f"Error: {str(e)}"
            logger.error(f"Sync error: {e}")

        finally:
            progress_state['active'] = False
            progress_state['start_time'] = None
            progress_state['eta_seconds'] = None
            progress_state['time_elapsed'] = None

    # Run in background thread
    thread = Thread(target=run_sync)
    thread.daemon = True
    thread.start()

    return jsonify({
        'success': True,
        'message': 'Price sync started. Check progress endpoint for updates.'
    })


@app.route('/api/calculate-metrics', methods=['POST'])
def api_calculate_metrics():
    """Calculate metrics for all tickers"""
    if progress_state['active']:
        return jsonify({
            'success': False,
            'message': 'Another operation is in progress'
        }), 409

    def progress_callback(current, total, ticker, status):
        import time

        # Track start time on first item
        if current == 1:
            progress_state['start_time'] = time.time()

        # Calculate ETA
        if progress_state['start_time'] and current > 0:
            elapsed = time.time() - progress_state['start_time']
            avg_time_per_item = elapsed / current
            remaining_items = total - current
            eta_seconds = int(avg_time_per_item * remaining_items)

            progress_state['time_elapsed'] = int(elapsed)
            progress_state['eta_seconds'] = eta_seconds

        progress_state['active'] = True
        progress_state['operation'] = 'calculate_metrics'
        progress_state['current'] = current
        progress_state['total'] = total
        progress_state['ticker'] = ticker
        progress_state['status'] = status
        progress_state['message'] = f'Processing {ticker}...'
        progress_state['last_updated'] = datetime.now().isoformat()

    def run_calculation():
        try:
            logger.info("Starting metrics calculation...")
            results = calculate_all_metrics(progress_callback=progress_callback)

            progress_state['message'] = f"Complete! Success: {results['success_count']}, Failed: {results['failed_count']}, Total metrics: {results['total_metrics']}"
            logger.info(progress_state['message'])

        except Exception as e:
            progress_state['message'] = f"Error: {str(e)}"
            logger.error(f"Calculation error: {e}")

        finally:
            progress_state['active'] = False
            progress_state['start_time'] = None
            progress_state['eta_seconds'] = None
            progress_state['time_elapsed'] = None

    # Run in background thread
    thread = Thread(target=run_calculation)
    thread.daemon = True
    thread.start()

    return jsonify({
        'success': True,
        'message': 'Metrics calculation started. Check progress endpoint for updates.'
    })


@app.route('/api/execute-screener', methods=['POST'])
def api_execute_screener():
    """Execute put options screener"""
    if progress_state['active']:
        return jsonify({
            'success': False,
            'message': 'Another operation is in progress'
        }), 409

    def progress_callback(current, total, ticker, status, passed_count):
        import time

        # Track start time on first item
        if current == 1:
            progress_state['start_time'] = time.time()

        # Calculate ETA
        if progress_state['start_time'] and current > 0:
            elapsed = time.time() - progress_state['start_time']
            avg_time_per_item = elapsed / current
            remaining_items = total - current
            eta_seconds = int(avg_time_per_item * remaining_items)

            progress_state['time_elapsed'] = int(elapsed)
            progress_state['eta_seconds'] = eta_seconds

        progress_state['active'] = True
        progress_state['operation'] = 'execute_screener'
        progress_state['current'] = current
        progress_state['total'] = total
        progress_state['ticker'] = ticker
        progress_state['status'] = status
        progress_state['message'] = f"Passed: {passed_count}"
        progress_state['last_updated'] = datetime.now().isoformat()

    def run_screener():
        try:
            logger.info("Starting screening...")
            results = screen_all_stocks(progress_callback=progress_callback)

            progress_state['message'] = f"Complete! Passed: {results['passed_count']}, Failed: {results['failed_count']}"
            logger.info(progress_state['message'])

        except Exception as e:
            progress_state['message'] = f"Error: {str(e)}"
            logger.error(f"Screener error: {e}")

        finally:
            progress_state['active'] = False
            progress_state['start_time'] = None
            progress_state['eta_seconds'] = None
            progress_state['time_elapsed'] = None

    # Run in background thread
    thread = Thread(target=run_screener)
    thread.daemon = True
    thread.start()

    return jsonify({
        'success': True,
        'message': 'Screener started. Check progress endpoint for updates.'
    })


@app.route('/api/results')
def api_results():
    """Get screening results (optionally filtered by date)"""
    from database import get_price_history, get_screening_results_by_date

    # Get optional date parameter
    date_param = request.args.get('date', None)

    db = get_db()

    try:
        # Get results by date if specified, otherwise get latest
        if date_param:
            try:
                query_date = datetime.strptime(date_param, '%Y-%m-%d').date()
                results = get_screening_results_by_date(db, query_date)
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid date format. Use YYYY-MM-DD'
                }), 400
        else:
            results = get_latest_screening_results(db, days=1)

        results_list = []
        for r in results:
            # Get ticker symbol
            ticker = db.query(Ticker).filter_by(id=r.ticker_id).first()

            # Calculate price changes dynamically
            price_change_1d = None
            price_change_5d = None

            if ticker:
                try:
                    # Get price history for the last 7 days (to calculate 5-day change)
                    end_date = r.screening_date
                    start_date = end_date - timedelta(days=7)
                    prices = get_price_history(db, ticker.id, start_date, end_date)

                    if prices and len(prices) > 0:
                        current_price = float(r.stock_price)

                        # Sort by date descending
                        prices_sorted = sorted(prices, key=lambda p: p.date, reverse=True)

                        # 1-day change (compare to yesterday)
                        if len(prices_sorted) >= 2:
                            yesterday_price = float(prices_sorted[1].close)
                            if yesterday_price > 0:
                                price_change_1d = ((current_price - yesterday_price) / yesterday_price)

                        # 5-day change (compare to 5 days ago)
                        if len(prices_sorted) >= 6:
                            five_days_ago_price = float(prices_sorted[5].close)
                            if five_days_ago_price > 0:
                                price_change_5d = ((current_price - five_days_ago_price) / five_days_ago_price)
                    else:
                        logger.debug(f"{ticker.symbol}: No price history found for date range {start_date} to {end_date}")
                except Exception as e:
                    logger.error(f"Error calculating price changes for {ticker.symbol}: {e}", exc_info=True)

            results_list.append({
                'Ticker': ticker.symbol if ticker else 'N/A',
                'Name': ticker.name if ticker else 'N/A',
                'Price': float(r.stock_price) if r.stock_price else 0,
                'Price_Change_1D': price_change_1d,
                'Price_Change_5D': price_change_5d,
                'Industry': r.industry,
                'Sector': r.sector,
                'Sector_ETF': r.sector_etf,
                'Stock_52W_Pct': float(r.stock_52w_pct) if r.stock_52w_pct else 0,
                '52W_High': float(r.week_52_high) if r.week_52_high else 0,
                '52W_Low': float(r.week_52_low) if r.week_52_low else 0,
                'Dist_High_Pct': float(r.dist_high_pct) if r.dist_high_pct else 0,
                'Dist_Low_Pct': float(r.dist_low_pct) if r.dist_low_pct else 0,
                'Sector_52W_Pct': float(r.sector_52w_pct) if r.sector_52w_pct else 0,
                'PE_Ratio': float(r.pe_ratio) if r.pe_ratio else 0,
                'Sector_PE': float(r.sector_pe) if r.sector_pe else 0,
                'Market_Cap': r.market_cap_millions,
                'Avg_Volume': float(r.avg_volume_millions) if r.avg_volume_millions else 0,
                'ATR_Pct': float(r.atr_pct) if r.atr_pct else 0,
                'Is_Lateral': r.is_lateral,
                'Strike': float(r.put_strike) if r.put_strike else 0,
                'DTE': r.dte,
                'Bid': float(r.bid) if r.bid else 0,
                'Ask': float(r.ask) if r.ask else 0,
                'Spread': float(r.spread) if r.spread else 0,
                'Premium': float(r.premium) if r.premium else 0,
                'Yield': float(r.annualized_yield) if r.annualized_yield else 0,
                'Contracts': r.contracts_needed,
                'Days_to_Earnings': r.days_to_earnings if r.days_to_earnings else 'N/A',
                'Created_At': r.created_at.strftime('%Y-%m-%d %H:%M:%S') if r.created_at else 'N/A',
                'Chart_Link': r.chart_link,
                'Options_Link': r.options_link
            })

        return jsonify({
            'success': True,
            'results': results_list,
            'count': len(results_list)
        })

    except Exception as e:
        logger.error(f"Error fetching results: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

    finally:
        db.close()


@app.route('/api/config')
def api_get_config():
    """Get current configuration from database"""
    db = get_db()

    try:
        config = get_all_config(db)

        # Add defaults if not set
        defaults = {
            'STOCK_52W_PERCENTILE_MAX': 0.20,
            'PE_RATIO_MIN': 5,
            'PE_RATIO_MAX': 20,
            'MARKET_CAP_MIN_MILLIONS': 1000,
            'AVG_VOLUME_USD_MIN_MILLIONS': 10,
            'TARGET_DTE': 30,
            'DTE_TOLERANCE': 7,
            'PUT_STRIKE_DISCOUNT': 0.10,
            'MIN_ANNUALIZED_PREMIUM_YIELD': 0.36,
            'TARGET_PREMIUM_THOUSANDS': 10,
            'LATERAL_TREND_ATR_THRESHOLD': 0.03
        }

        for key, default_value in defaults.items():
            if key not in config:
                config[key] = default_value

        return jsonify(config)

    finally:
        db.close()


@app.route('/api/config', methods=['POST'])
def api_update_config():
    """Update configuration in database"""
    db = get_db()

    try:
        new_config = request.get_json()

        if not new_config:
            return jsonify({'success': False, 'error': 'No configuration provided'}), 400

        # Update each config value
        config_types = {
            'STOCK_52W_PERCENTILE_MAX': 'float',
            'PE_RATIO_MIN': 'int',
            'PE_RATIO_MAX': 'int',
            'MARKET_CAP_MIN_MILLIONS': 'int',
            'AVG_VOLUME_USD_MIN_MILLIONS': 'int',
            'TARGET_DTE': 'int',
            'DTE_TOLERANCE': 'int',
            'PUT_STRIKE_DISCOUNT': 'float',
            'MIN_ANNUALIZED_PREMIUM_YIELD': 'float',
            'TARGET_PREMIUM_THOUSANDS': 'int',
            'LATERAL_TREND_ATR_THRESHOLD': 'float'
        }

        for key, value in new_config.items():
            if key in config_types:
                set_config(db, key, value, config_types[key])

        return jsonify({
            'success': True,
            'message': 'Configuration updated successfully.'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

    finally:
        db.close()


def open_browser():
    """Open browser after short delay."""
    webbrowser.open('http://localhost:5001')


if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("Put Option Screener Web UI (Database Version)")
    print("=" * 80)

    # Initialize database
    print("\nInitializing database...")
    init_db()

    # Migrate config from JSON if needed
    print("Checking for config migration...")
    migrate_config_from_json()

    print(f"\nStarting server at http://localhost:5001")
    print("Opening browser automatically...")
    print("Auto-reload enabled - server will restart on file changes")
    print("\nPress Ctrl+C to stop the server\n")
    print("=" * 80 + "\n")

    # Only open browser on first run (not on reloader restart)
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        Timer(1.5, open_browser).start()

    # Enable reloader with extra files to watch
    extra_files = [
        'database.py',
        'data_population.py',
        'price_sync.py',
        'metrics_calculation.py',
        'put_screener_db.py',
        'templates/index.html',
        'static/css/style.css',
        'static/js/app.js'
    ]

    app.run(debug=True, host='0.0.0.0', port=5001, use_reloader=True, extra_files=extra_files)
