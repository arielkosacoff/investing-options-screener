"""
Price synchronization module for fetching and storing price data.
Handles incremental updates from last sync date and stock splits.
"""

import logging
import time
from datetime import datetime, date, timedelta
from typing import Optional, Callable, Dict
import yfinance as yf
import pandas as pd

from database import (
    get_db,
    Ticker,
    get_all_tickers,
    get_latest_price_date,
    upsert_price,
    get_sync_status,
    update_price_sync
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# PRICE SYNC FUNCTIONS
# ==============================================================================

def sync_ticker_prices(
    ticker: Ticker,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> Dict[str, any]:
    """
    Sync price data for a single ticker.
    Fetches from start_date to end_date (or today if not specified).
    yfinance returns adjusted prices by default, handling splits automatically.

    Returns:
        Dict with status: {success: bool, days_added: int, message: str}
    """
    try:
        # Default date range
        if end_date is None:
            end_date = date.today()

        if start_date is None:
            # If no start date, fetch 1 year of data
            start_date = end_date - timedelta(days=365)

        # Fetch data from yfinance
        yf_ticker = yf.Ticker(ticker.symbol)

        # Add 1 day to end_date for yfinance (exclusive end)
        hist = yf_ticker.history(
            start=start_date,
            end=end_date + timedelta(days=1),
            auto_adjust=True  # Use adjusted prices (handles splits)
        )

        if hist.empty:
            return {
                'success': False,
                'days_added': 0,
                'message': 'No data returned from yfinance'
            }

        # Store prices in database
        db = get_db()
        days_added = 0

        try:
            for dt, row in hist.iterrows():
                price_date = dt.date()

                price_data = {
                    'open': float(row['Open']) if pd.notna(row['Open']) else None,
                    'high': float(row['High']) if pd.notna(row['High']) else None,
                    'low': float(row['Low']) if pd.notna(row['Low']) else None,
                    'close': float(row['Close']) if pd.notna(row['Close']) else None,
                    'volume': int(row['Volume']) if pd.notna(row['Volume']) else None
                }

                upsert_price(db, ticker.id, price_date, price_data)
                days_added += 1

            # Update ticker metadata while we have the yf_ticker object
            # This includes: sector, industry, market_cap, earnings_date, and name
            try:
                db_ticker = db.query(Ticker).filter_by(id=ticker.id).first()
                if db_ticker:
                    # Fetch info for metadata (done once per ticker, not per day)
                    info = yf_ticker.info
                    if info:
                        # Update name if it's still just the symbol
                        if db_ticker.name == db_ticker.symbol:
                            db_ticker.name = info.get('longName', info.get('shortName', db_ticker.symbol))

                        # Update market cap
                        if info.get('marketCap'):
                            db_ticker.market_cap = info.get('marketCap')

                        # Update sector/industry if not set
                        if not db_ticker.sector_id and info.get('sectorKey'):
                            from data_population import populate_sector_industry
                            sector_id, industry_id = populate_sector_industry(
                                db,
                                info.get('sectorKey'),
                                info.get('industryKey')
                            )
                            db_ticker.sector_id = sector_id
                            db_ticker.industry_id = industry_id

                    # Update shares outstanding
                    if info.get('sharesOutstanding'):
                        db_ticker.shares_outstanding = info.get('sharesOutstanding')

                    # Update earnings date using calendar (fixes timezone bug)
                    calendar = yf_ticker.calendar
                    if calendar and 'Earnings Date' in calendar:
                        earnings_list = calendar['Earnings Date']
                        if isinstance(earnings_list, list) and len(earnings_list) > 0:
                            today = date.today()
                            future = [d for d in earnings_list if d >= today]
                            if future:
                                db_ticker.next_earnings_date = future[0]

                    # Save fundamental metrics (as metrics, not ticker columns)
                    from database import upsert_metric

                    if info.get('trailingPE'):
                        upsert_metric(db, ticker.id, date.today(), 'pe_ratio', float(info.get('trailingPE')))

                    if info.get('forwardPE'):
                        upsert_metric(db, ticker.id, date.today(), 'forward_pe', float(info.get('forwardPE')))

                    if info.get('beta'):
                        upsert_metric(db, ticker.id, date.today(), 'beta', float(info.get('beta')))

                    if info.get('dividendYield'):
                        upsert_metric(db, ticker.id, date.today(), 'dividend_yield', float(info.get('dividendYield')))

            except Exception as e:
                logger.warning(f"{ticker.symbol}: Could not update metadata: {e}")

            db.commit()

            return {
                'success': True,
                'days_added': days_added,
                'message': f'Synced {days_added} days'
            }

        finally:
            db.close()

    except Exception as e:
        logger.error(f"{ticker.symbol}: Error syncing prices - {e}")
        return {
            'success': False,
            'days_added': 0,
            'message': str(e)
        }


def sync_all_prices(
    progress_callback: Optional[Callable[[int, int, str, str, date], None]] = None,
    force_full_sync: bool = False
) -> Dict[str, any]:
    """
    Sync prices for all tickers in database.
    By default, syncs from last_price_sync date forward (incremental).
    If force_full_sync=True, syncs last 1 year of data for all tickers.

    Args:
        progress_callback: Function(current, total, ticker, status, last_date) for progress tracking
        force_full_sync: If True, sync last 1 year regardless of last sync date

    Returns:
        Dict with statistics: {success_count, failed_count, total, last_sync_date}
    """
    db = get_db()
    stats = {
        'success_count': 0,
        'failed_count': 0,
        'total': 0,
        'total_days_added': 0,
        'last_sync_date': None
    }

    try:
        # Get all tickers (including ETFs for sector/market analysis)
        tickers = get_all_tickers(db, exclude_etfs=False)
        stats['total'] = len(tickers)

        logger.info(f"Starting price sync for {stats['total']} tickers...")

        # Determine start date for sync
        if force_full_sync:
            # Full sync: get last 1 year
            default_start_date = date.today() - timedelta(days=365)
            logger.info(f"Force full sync: fetching from {default_start_date}")
        else:
            # Incremental sync: check last sync date
            sync_status = get_sync_status(db, 'stocks')

            if sync_status and sync_status.last_price_sync:
                # Start from last sync date (re-fetch to get final closing data)
                default_start_date = sync_status.last_price_sync
                logger.info(f"Incremental sync: fetching from {default_start_date} (re-fetching last day for final data)")
            else:
                # First sync: get last 1 year
                default_start_date = date.today() - timedelta(days=365)
                logger.info(f"First sync: fetching from {default_start_date}")

        end_date = date.today()

        # Check if sync is needed
        if not force_full_sync and default_start_date > end_date:
            logger.info("Prices are already up to date. No sync needed.")
            stats['last_sync_date'] = default_start_date - timedelta(days=1)
            return stats

        # Process each ticker
        for idx, ticker in enumerate(tickers, 1):
            try:
                # Determine start date for this ticker
                latest_price_date = get_latest_price_date(db, ticker.id)

                if latest_price_date and not force_full_sync:
                    # Start from latest price date (re-fetch for final close)
                    start_date = latest_price_date
                else:
                    start_date = default_start_date

                # Skip if no new data to fetch
                if start_date > end_date:
                    logger.debug(f"{ticker.symbol}: Already up to date")
                    stats['success_count'] += 1

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'up_to_date', latest_price_date or start_date)

                    continue

                # Progress callback
                if progress_callback:
                    progress_callback(idx, stats['total'], ticker.symbol, 'syncing', start_date)

                # Sync prices
                result = sync_ticker_prices(ticker, start_date, end_date)

                if result['success']:
                    stats['success_count'] += 1
                    stats['total_days_added'] += result['days_added']

                    logger.info(f"{ticker.symbol}: {result['message']} ({stats['success_count']}/{stats['total']})")

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'success', end_date)
                else:
                    stats['failed_count'] += 1
                    logger.warning(f"{ticker.symbol}: {result['message']}")

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'failed', start_date)

                # Rate limiting
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"{ticker.symbol}: Error during sync - {e}")
                stats['failed_count'] += 1

                if progress_callback:
                    progress_callback(idx, stats['total'], ticker.symbol, 'error', default_start_date)

        # Update sync status
        update_price_sync(db, 'stocks', end_date)
        stats['last_sync_date'] = end_date

        logger.info(f"Price sync complete: {stats['success_count']} successful, {stats['failed_count']} failed, {stats['total_days_added']} total days added")

        return stats

    finally:
        db.close()


def sync_single_ticker_by_symbol(
    symbol: str,
    days_back: int = 365
) -> Dict[str, any]:
    """
    Sync prices for a single ticker by symbol.
    Useful for testing or manual updates.

    Args:
        symbol: Ticker symbol
        days_back: Number of days back to fetch

    Returns:
        Dict with status
    """
    db = get_db()

    try:
        ticker = db.query(Ticker).filter_by(symbol=symbol).first()

        if not ticker:
            return {
                'success': False,
                'message': f'Ticker {symbol} not found in database'
            }

        start_date = date.today() - timedelta(days=days_back)
        end_date = date.today()

        result = sync_ticker_prices(ticker, start_date, end_date)

        return result

    finally:
        db.close()


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def check_price_data_coverage(symbol: str = None) -> Dict[str, any]:
    """
    Check price data coverage for a ticker or all tickers.

    Args:
        symbol: Optional ticker symbol. If None, checks all tickers.

    Returns:
        Dict with coverage info
    """
    db = get_db()

    try:
        if symbol:
            ticker = db.query(Ticker).filter_by(symbol=symbol).first()
            if not ticker:
                return {'error': f'Ticker {symbol} not found'}

            latest_date = get_latest_price_date(db, ticker.id)

            return {
                'symbol': symbol,
                'latest_date': latest_date,
                'days_behind': (date.today() - latest_date).days if latest_date else None
            }
        else:
            # Check all tickers
            tickers = get_all_tickers(db, exclude_etfs=False)
            coverage = []

            for ticker in tickers:
                latest_date = get_latest_price_date(db, ticker.id)
                days_behind = (date.today() - latest_date).days if latest_date else None

                coverage.append({
                    'symbol': ticker.symbol,
                    'latest_date': latest_date,
                    'days_behind': days_behind
                })

            # Summary stats
            with_data = [c for c in coverage if c['latest_date'] is not None]
            up_to_date = [c for c in with_data if c['days_behind'] == 0]

            return {
                'total_tickers': len(coverage),
                'with_data': len(with_data),
                'up_to_date': len(up_to_date),
                'coverage': coverage
            }

    finally:
        db.close()


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        symbol = sys.argv[1].upper()
        print(f"Syncing prices for {symbol}...")
        result = sync_single_ticker_by_symbol(symbol, days_back=365)
        print(f"Result: {result}")
    else:
        print("Starting full price sync...")
        results = sync_all_prices(force_full_sync=False)
        print(f"\nResults:")
        print(f"  Success: {results['success_count']}")
        print(f"  Failed: {results['failed_count']}")
        print(f"  Total: {results['total']}")
        print(f"  Days added: {results['total_days_added']}")
        print(f"  Last sync: {results['last_sync_date']}")
