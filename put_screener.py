#!/usr/bin/env python3
"""
Put Option Screener (Database Version)
Screens stocks for optimal cash-secured put selling opportunities using database-stored data.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Callable
import yfinance as yf
import pandas as pd

from database import (
    get_db,
    Ticker,
    get_all_tickers,
    get_all_metrics,
    get_config,
    get_all_config,
    save_screening_result,
    get_latest_price_date
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# SCREENING FUNCTIONS
# ==============================================================================

def get_screening_config(db) -> Dict:
    """Get screening configuration from database"""
    config = get_all_config(db)

    # Set defaults if not in database
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

    return config


def screen_ticker(
    ticker: Ticker,
    screening_date: date,
    config: Dict
) -> Optional[List[Dict]]:
    """
    Screen a single ticker against all criteria.
    Returns list of dicts (one per qualifying option) if passed, None if failed any criteria.
    Each dict represents one expiration date that meets all screening criteria.
    """
    db = get_db()

    try:
        # Get latest price date for this ticker
        latest_price = get_latest_price_date(db, ticker.id)
        if not latest_price or latest_price < screening_date - timedelta(days=5):
            logger.debug(f"{ticker.symbol}: No recent price data")
            return None

        # Use latest available data if screening_date is in future
        data_date = min(screening_date, latest_price)

        # Get all metrics for this ticker
        metrics = get_all_metrics(db, ticker.id, data_date)

        if not metrics:
            logger.debug(f"{ticker.symbol}: No metrics calculated")
            return None

        # Required metrics from ticker_metrics table
        required_metrics = ['52w_pct', '52w_high', '52w_low', 'atr_pct', 'avg_volume_usd', 'pe_ratio']
        for metric in required_metrics:
            if metric not in metrics:
                logger.debug(f"{ticker.symbol}: Missing metric {metric}")
                return None

        stock_52w_pct = metrics['52w_pct']
        stock_pe = metrics['pe_ratio']
        avg_volume_usd = metrics['avg_volume_usd']
        atr_pct = metrics['atr_pct']
        week_52_high = metrics['52w_high']
        week_52_low = metrics['52w_low']

        # Market cap is stored in the tickers table, not metrics
        market_cap = ticker.market_cap
        if not market_cap:
            logger.debug(f"{ticker.symbol}: Missing market cap")
            return None

        # Get current price
        current_price = metrics.get('close', None)
        if not current_price:
            # Fetch from latest price if not in metrics
            from database import get_price_history
            prices = get_price_history(db, ticker.id, data_date, data_date)
            if not prices:
                return None
            current_price = float(prices[0].close)

        # FILTER 1: 52-week percentile
        if stock_52w_pct > config['STOCK_52W_PERCENTILE_MAX']:
            logger.debug(f"{ticker.symbol}: Failed 52W% ({stock_52w_pct:.2%})")
            return None

        # FILTER 2: PE Ratio
        if stock_pe < config['PE_RATIO_MIN'] or stock_pe > config['PE_RATIO_MAX']:
            logger.debug(f"{ticker.symbol}: Failed PE ({stock_pe:.2f})")
            return None

        # FILTER 3: Market Cap
        market_cap_millions = market_cap / 1_000_000
        if market_cap_millions < config['MARKET_CAP_MIN_MILLIONS']:
            logger.debug(f"{ticker.symbol}: Failed market cap (${market_cap_millions:.0f}M)")
            return None

        # FILTER 4: Average Volume
        avg_volume_millions = avg_volume_usd / 1_000_000
        if avg_volume_millions < config['AVG_VOLUME_USD_MIN_MILLIONS']:
            logger.debug(f"{ticker.symbol}: Failed volume (${avg_volume_millions:.0f}M)")
            return None

        # Get sector metrics
        sector_52w_pct = None
        sector_pe = None

        if ticker.sector:
            # Get the sector ETF ticker by symbol
            from database import get_ticker
            sector_ticker = get_ticker(db, ticker.sector.symbol)
            if sector_ticker:
                sector_metrics = get_all_metrics(db, sector_ticker.id, data_date)
                if sector_metrics:
                    sector_52w_pct = sector_metrics.get('52w_pct')
                    sector_pe = sector_metrics.get('pe_ratio')

        # FILTER 5: Relative Strength (Stock < Sector)
        if sector_52w_pct and stock_52w_pct >= sector_52w_pct:
            logger.debug(f"{ticker.symbol}: Failed relative strength vs sector")
            return None

        # FILTER 6: PE Ratio Comparison vs Sector
        if sector_pe and stock_pe >= sector_pe:
            logger.debug(f"{ticker.symbol}: Failed PE vs sector")
            return None

        # FILTER 7: Lateral Trend (Optional)
        is_lateral = atr_pct < config['LATERAL_TREND_ATR_THRESHOLD']

        # FILTER 8: Options Premium Analysis
        try:
            yf_ticker = yf.Ticker(ticker.symbol)
            expirations = yf_ticker.options

            if not expirations:
                logger.debug(f"{ticker.symbol}: No options available")
                return None

            # Find ALL expirations within TARGET_DTE tolerance
            target_date = date.today() + timedelta(days=config['TARGET_DTE'])
            tolerance = timedelta(days=config['DTE_TOLERANCE'])

            qualifying_puts = []

            for exp_str in expirations:
                exp_date = pd.to_datetime(exp_str).date()
                dte = (exp_date - date.today()).days

                if abs(exp_date - target_date) <= tolerance:
                    # Get put options
                    options = yf_ticker.option_chain(exp_str)
                    puts = options.puts

                    # Target strike (10% OTM - BELOW current price)
                    target_strike = current_price * (1 - config['PUT_STRIKE_DISCOUNT'])

                    # Filter puts to only include strikes below current price
                    puts_below = puts[puts['strike'] < current_price]

                    if puts_below.empty:
                        continue

                    # Find closest strike below current price
                    puts_below['strike_diff'] = abs(puts_below['strike'] - target_strike)
                    closest_put = puts_below.loc[puts_below['strike_diff'].idxmin()]

                    # Calculate metrics (convert numpy types to Python native)
                    bid = float(closest_put['bid'])
                    ask = float(closest_put['ask'])
                    premium = (bid + ask) / 2
                    strike = float(closest_put['strike'])

                    # Annualized yield
                    if dte > 0 and strike > 0:
                        annual_yield = (premium / strike) * (365 / dte)

                        if annual_yield >= config['MIN_ANNUALIZED_PREMIUM_YIELD']:
                            qualifying_puts.append({
                                'strike': strike,
                                'bid': bid,
                                'ask': ask,
                                'premium': premium,
                                'dte': dte,
                                'annual_yield': annual_yield,
                                'spread': ask - bid,
                                'expiration': exp_date
                            })

            if not qualifying_puts:
                logger.debug(f"{ticker.symbol}: No puts meeting premium criteria")
                return None

        except Exception as e:
            logger.warning(f"{ticker.symbol}: Error fetching options - {e}")
            return None

        # Calculate additional metrics
        dist_high_pct = ((week_52_high - current_price) / current_price) if current_price > 0 else 0
        dist_low_pct = ((current_price - week_52_low) / current_price) if current_price > 0 else 0

        # Days to earnings (convert to int if present)
        days_to_earnings = metrics.get('days_to_earnings')
        if days_to_earnings is not None:
            days_to_earnings = int(days_to_earnings)

        # Target premium for contracts calculation
        target_premium = config['TARGET_PREMIUM_THOUSANDS'] * 1000

        # Build results list - one entry per qualifying option
        results = []
        for put_option in qualifying_puts:
            # Calculate contracts needed based on stock price (capital required if assigned)
            # Each contract controls 100 shares, so capital per contract = stock_price * 100
            contracts_needed = int(target_premium / (current_price * 100)) if current_price > 0 else 0

            result = {
                'ticker_id': ticker.id,
                'screening_date': screening_date,
                'stock_price': float(current_price),
                'industry': ticker.industry.name if ticker.industry else None,
                'sector': ticker.sector.name if ticker.sector else None,
                'sector_etf': ticker.sector.symbol if ticker.sector else None,
                'stock_52w_pct': float(stock_52w_pct),
                'week_52_high': float(week_52_high),
                'week_52_low': float(week_52_low),
                'dist_high_pct': float(dist_high_pct),
                'dist_low_pct': float(dist_low_pct),
                'sector_52w_pct': float(sector_52w_pct) if sector_52w_pct is not None else None,
                'pe_ratio': float(stock_pe),
                'sector_pe': float(sector_pe) if sector_pe is not None else None,
                'market_cap_millions': int(market_cap_millions),
                'avg_volume_millions': float(avg_volume_millions),
                'atr_pct': float(atr_pct),
                'is_lateral': bool(is_lateral),
                'put_strike': float(put_option['strike']),
                'dte': int(put_option['dte']),
                'bid': float(put_option['bid']),
                'ask': float(put_option['ask']),
                'spread': float(put_option['spread']),
                'premium': float(put_option['premium']),
                'annualized_yield': float(put_option['annual_yield']),
                'contracts_needed': int(contracts_needed),
                'days_to_earnings': days_to_earnings,
                'chart_link': f'https://finance.yahoo.com/quote/{ticker.symbol}',
                'options_link': f'https://finance.yahoo.com/quote/{ticker.symbol}/options'
            }
            results.append(result)

        return results

    except Exception as e:
        logger.error(f"{ticker.symbol}: Error during screening - {e}")
        return None

    finally:
        db.close()


def screen_all_stocks(
    screening_date: Optional[date] = None,
    progress_callback: Optional[Callable[[int, int, str, str, int], None]] = None
) -> Dict[str, any]:
    """
    Screen all stocks in database.

    Args:
        screening_date: Date to screen for (default: today)
        progress_callback: Function(current, total, ticker, status, passed_count) for progress tracking

    Returns:
        Dict with statistics: {success_count, failed_count, total, results}
    """
    if screening_date is None:
        screening_date = date.today()

    # Generate single timestamp for this screening run
    screening_run_timestamp = datetime.utcnow()

    db = get_db()
    stats = {
        'passed_count': 0,
        'failed_count': 0,
        'total': 0,
        'results': []
    }

    try:
        # Get configuration
        config = get_screening_config(db)

        # Get all non-ETF tickers
        tickers = get_all_tickers(db, exclude_etfs=True)
        stats['total'] = len(tickers)

        logger.info(f"Starting screening of {stats['total']} tickers...")

        # Process each ticker
        for idx, ticker in enumerate(tickers, 1):
            try:
                if progress_callback:
                    progress_callback(idx, stats['total'], ticker.symbol, 'screening', stats['passed_count'])

                # Screen ticker - returns list of results (one per qualifying option)
                results = screen_ticker(ticker, screening_date, config)

                if results:
                    # Save all results to database with same timestamp
                    for result in results:
                        result['created_at'] = screening_run_timestamp
                        save_screening_result(db, result)
                        stats['results'].append(result)

                    stats['passed_count'] += len(results)

                    logger.info(f"{ticker.symbol}: PASSED screening with {len(results)} option(s) ({stats['passed_count']} total opportunities)")

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'passed', stats['passed_count'])
                else:
                    stats['failed_count'] += 1

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'failed', stats['passed_count'])

            except Exception as e:
                logger.error(f"{ticker.symbol}: Error during screening - {e}")
                stats['failed_count'] += 1

                if progress_callback:
                    progress_callback(idx, stats['total'], ticker.symbol, 'error', stats['passed_count'])

        logger.info(f"Screening complete: {stats['passed_count']} passed, {stats['failed_count']} failed out of {stats['total']}")

        return stats

    finally:
        db.close()


if __name__ == '__main__':
    print("Starting put options screener...")
    results = screen_all_stocks()
    print(f"\nResults:")
    print(f"  Passed: {results['passed_count']}")
    print(f"  Failed: {results['failed_count']}")
    print(f"  Total: {results['total']}")

    if results['results']:
        print(f"\n{len(results['results'])} opportunities found!")
        for r in results['results'][:10]:  # Show first 10
            print(f"  {r['ticker_id']}: ${r['stock_price']:.2f} - {r['annualized_yield']:.1%} yield")
