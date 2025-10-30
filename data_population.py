"""
Data population module for fetching and storing tickers, sectors, and industries.
Fetches top stocks by market cap using pandas and Wikipedia tables.
"""

import logging
import time
from datetime import datetime, date
from typing import List, Dict, Optional, Callable
import yfinance as yf
import pandas as pd

from database import (
    get_db,
    Market,
    Sector,
    Industry,
    Ticker,
    upsert_ticker,
    upsert_sector,
    upsert_industry,
    get_sector_by_key,
    get_industry_by_key
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# TICKER LIST FETCHING
# ==============================================================================

def get_combined_ticker_list(max_count: int = 2000) -> List[str]:
    """
    Fetch top US stocks by market cap using yfinance screener.
    Falls back to S&P 500 + NASDAQ 100 if screener fails.

    Args:
        max_count: Maximum number of tickers to return (default 2000)

    Returns:
        List of ticker symbols
    """
    logger.info(f"Fetching top {max_count} US stocks by market cap using yfinance screener...")

    all_tickers = []

    # Try using yfinance screener first (most reliable for large cap stocks)
    try:
        from yfinance import EquityQuery
        import yfinance as yf

        # Create query for US stocks sorted by market cap
        query = EquityQuery('and', [
            EquityQuery('eq', ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 1000000000])  # Market cap >= $1B
        ])

        # Screen in batches (Yahoo limits to 250 per request)
        batch_size = 250
        for offset in range(0, max_count, batch_size):
            size = min(batch_size, max_count - offset)

            response = yf.screen(
                query,
                offset=offset,
                size=size,
                sortField='intradaymarketcap',
                sortAsc=False  # Descending (largest first)
            )

            if response and 'quotes' in response:
                batch_tickers = [quote['symbol'] for quote in response['quotes'] if 'symbol' in quote]
                all_tickers.extend(batch_tickers)
                logger.info(f"Fetched {len(batch_tickers)} tickers (offset {offset})")

                # If we got fewer than requested, we've reached the end
                if len(batch_tickers) < size:
                    break
            else:
                logger.warning(f"No results from screener at offset {offset}")
                break

            # Small delay between requests
            time.sleep(0.5)

        if all_tickers:
            logger.info(f"Successfully fetched {len(all_tickers)} tickers via yfinance screener")
            return all_tickers

    except Exception as e:
        logger.warning(f"yfinance screener failed: {e}, falling back to Wikipedia")

    # Fallback to Wikipedia if screener fails
    logger.info("Using fallback: Fetching from S&P 500 and NASDAQ-100...")

    # Fetch S&P 500
    try:
        sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp500_tables = pd.read_html(sp500_url)
        sp500_df = sp500_tables[0]

        if 'Symbol' in sp500_df.columns:
            sp500_tickers = sp500_df['Symbol'].tolist()
            all_tickers.extend(sp500_tickers)
            logger.info(f"Fetched {len(sp500_tickers)} S&P 500 tickers")

    except Exception as e:
        logger.error(f"Error fetching S&P 500 tickers: {e}")

    # Fetch NASDAQ 100
    try:
        nasdaq_url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        nasdaq_tables = pd.read_html(nasdaq_url)

        nasdaq_df = None
        for table in nasdaq_tables:
            if 'Ticker' in table.columns or 'Symbol' in table.columns:
                nasdaq_df = table
                break

        if nasdaq_df is not None:
            ticker_col = 'Ticker' if 'Ticker' in nasdaq_df.columns else 'Symbol'
            nasdaq_tickers = nasdaq_df[ticker_col].tolist()
            all_tickers.extend(nasdaq_tickers)
            logger.info(f"Fetched {len(nasdaq_tickers)} NASDAQ-100 tickers")

    except Exception as e:
        logger.error(f"Error fetching NASDAQ-100 tickers: {e}")

    # Remove duplicates
    all_tickers = list(set(all_tickers))

    logger.info(f"Total unique tickers: {len(all_tickers)}")

    return all_tickers


# ==============================================================================
# TICKER METADATA FETCHING
# ==============================================================================

def fetch_ticker_info(symbol: str) -> Optional[Dict]:
    """
    Fetch minimal ticker information from yfinance.
    FAST MODE: Only validates ticker exists, doesn't fetch metadata.
    Metadata (sector, industry, market_cap) will be populated during price sync.

    Returns dict with name only, or None if ticker invalid.
    """
    try:
        ticker = yf.Ticker(symbol)

        # Use fast_info for quick validation (no full API call needed)
        # This just checks if the ticker exists
        try:
            # Try to access a fast property - if it fails, ticker is invalid
            _ = ticker.fast_info.last_price

            # Return minimal info - just use symbol as name for now
            # Real name will be fetched during price sync
            return {
                'name': symbol,
                'sector_key': None,
                'industry_key': None,
                'market_cap': None
            }
        except Exception:
            logger.warning(f"{symbol}: Invalid ticker or no data available")
            return None

    except Exception as e:
        logger.warning(f"{symbol}: Error validating ticker - {e}")
        return None


# ==============================================================================
# SECTOR/INDUSTRY POPULATION
# ==============================================================================

# Standard sector ETF mapping (yfinance returns Yahoo indices, not ETF tickers)
SECTOR_ETF_MAP = {
    'technology': 'XLK',
    'financial-services': 'XLF',
    'healthcare': 'XLV',
    'energy': 'XLE',
    'industrials': 'XLI',
    'consumer-cyclical': 'XLY',
    'consumer-defensive': 'XLP',
    'utilities': 'XLU',
    'real-estate': 'XLRE',
    'basic-materials': 'XLB',
    'communication-services': 'XLC'
}

def populate_sector_industry(db, sector_key: str, industry_key: str) -> tuple[Optional[int], Optional[int]]:
    """
    Populate or retrieve sector and industry.
    Returns (sector_id, industry_id)
    """
    sector_id = None
    industry_id = None

    # Handle sector
    if sector_key:
        sector_db = get_sector_by_key(db, sector_key)

        if not sector_db:
            try:
                # Fetch sector info from yfinance
                sector_yf = yf.Sector(sector_key)
                sector_name = sector_yf.name

                # Use our ETF mapping instead of yfinance symbol (which returns Yahoo indices)
                sector_symbol = SECTOR_ETF_MAP.get(sector_key)

                sector_db = upsert_sector(db, sector_key, sector_name, sector_symbol)
                logger.info(f"Added sector: {sector_name} ({sector_key}) -> ETF: {sector_symbol}")
            except Exception as e:
                logger.warning(f"Could not fetch sector {sector_key}: {e}")
                # Create with minimal info and ETF from mapping
                sector_symbol = SECTOR_ETF_MAP.get(sector_key)
                sector_db = upsert_sector(db, sector_key, sector_key.title(), sector_symbol)

        sector_id = sector_db.id

    # Handle industry
    if industry_key and sector_id:
        industry_db = get_industry_by_key(db, industry_key)

        if not industry_db:
            try:
                # Fetch industry info from yfinance
                industry_yf = yf.Industry(industry_key)
                industry_name = industry_yf.name

                industry_db = upsert_industry(db, industry_key, industry_name, sector_id)
                logger.info(f"Added industry: {industry_name} ({industry_key})")
            except Exception as e:
                logger.warning(f"Could not fetch industry {industry_key}: {e}")
                # Create with minimal info
                industry_db = upsert_industry(db, industry_key, industry_key.replace('-', ' ').title(), sector_id)

        industry_id = industry_db.id

    return sector_id, industry_id


# ==============================================================================
# MAIN POPULATION FUNCTION
# ==============================================================================

def populate_stocks(
    max_count: int = 2000,
    progress_callback: Optional[Callable[[int, int, str, str], None]] = None
) -> Dict[str, any]:
    """
    Populate stocks in database from S&P 500 and NASDAQ 100.
    Only adds new tickers, preserves existing ones.

    Args:
        max_count: Maximum number of tickers to process (currently gets all from indices)
        progress_callback: Function(current, total, ticker, status) for progress tracking

    Returns:
        Dict with statistics: {added, updated, failed, total}
    """
    db = get_db()
    stats = {
        'added': 0,
        'updated': 0,
        'skipped': 0,
        'failed': 0,
        'total': 0
    }

    try:
        # Get SP500 market reference
        market = db.query(Market).filter_by(key='sp500').first()
        if not market:
            logger.error("SP500 market not found in database. Run database initialization first.")
            return stats

        # Fetch ticker list
        ticker_list = get_combined_ticker_list(max_count)
        stats['total'] = len(ticker_list)

        logger.info(f"Starting population of {stats['total']} tickers...")

        # Add market ETFs first
        etf_symbols = {
            'SPY': ('sp500', True, False),
            'QQQ': ('nasdaq100', True, False),
            'IWM': ('russell1000', True, False),
        }

        for etf_symbol, (market_key, is_market, is_sector) in etf_symbols.items():
            market_ref = db.query(Market).filter_by(key=market_key).first()
            if market_ref:
                existing = db.query(Ticker).filter_by(symbol=etf_symbol).first()
                if not existing:
                    upsert_ticker(db, etf_symbol, {
                        'name': etf_symbol,
                        'market_id': market_ref.id,
                        'is_market_etf': is_market,
                        'is_sector_etf': is_sector
                    })
                    logger.info(f"Added market ETF: {etf_symbol}")

        # Process each ticker
        for idx, symbol in enumerate(ticker_list, 1):
            try:
                if progress_callback:
                    progress_callback(idx, stats['total'], symbol, 'processing')

                # Check if ticker already exists
                existing_ticker = db.query(Ticker).filter_by(symbol=symbol).first()

                if existing_ticker:
                    # Skip existing tickers to preserve FK relationships
                    stats['skipped'] += 1
                    logger.debug(f"{symbol}: Already exists, skipping")

                    if progress_callback:
                        progress_callback(idx, stats['total'], symbol, 'skipped')

                    continue

                # Create ticker with minimal data
                # All metadata will be populated during price sync (step 2)
                ticker_data = {
                    'name': symbol,  # Will be updated during price sync
                    'industry_id': None,  # Will be populated during price sync (for description only)
                    'sector_id': None,    # Will be populated during price sync (for relative strength)
                    'market_id': market.id,
                    'market_cap': None,   # Will be populated during price sync
                    'next_earnings_date': None,  # Will be fetched during price sync
                    'is_sector_etf': False,
                    'is_market_etf': False
                }

                upsert_ticker(db, symbol, ticker_data)
                stats['added'] += 1

                logger.info(f"{symbol}: Added ({stats['added']}/{stats['total']})")

                if progress_callback:
                    progress_callback(idx, stats['total'], symbol, 'added')

            except Exception as e:
                logger.error(f"{symbol}: Error processing - {e}")
                stats['failed'] += 1

                if progress_callback:
                    progress_callback(idx, stats['total'], symbol, 'error')

        # Now add sector ETFs (for relative strength analysis)
        logger.info("Adding sector ETFs...")
        sectors = db.query(Sector).all()
        for sector in sectors:
            if sector.symbol:
                existing = db.query(Ticker).filter_by(symbol=sector.symbol).first()
                if not existing:
                    upsert_ticker(db, sector.symbol, {
                        'name': f"{sector.name} ETF",
                        'sector_id': sector.id,
                        'market_id': market.id,
                        'is_sector_etf': True,
                        'is_market_etf': False
                    })
                    logger.info(f"Added sector ETF: {sector.symbol}")

        logger.info(f"Population complete: {stats['added']} added, {stats['skipped']} skipped, {stats['failed']} failed")

        return stats

    finally:
        db.close()


if __name__ == '__main__':
    print("Starting stock population...")
    results = populate_stocks(max_count=2000)
    print(f"\nResults:")
    print(f"  Added: {results['added']}")
    print(f"  Updated: {results['updated']}")
    print(f"  Skipped: {results['skipped']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Total: {results['total']}")
