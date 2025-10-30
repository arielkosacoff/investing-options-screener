"""
Metrics calculation module using pandas-ta for technical indicators.
Calculates 52W percentile, ATR, volume, etc.
NO yfinance imports - all data from database.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, Callable, Dict, List
import pandas as pd
import numpy as np

try:
    import pandas_ta as ta
except ImportError:
    ta = None
    logging.warning("pandas-ta not installed. Some metrics may not be available.")

from database import (
    get_db,
    Ticker,
    get_all_tickers,
    get_price_history,
    get_latest_price_date,
    upsert_metric,
    get_sync_status,
    update_metrics_calc
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# OPTIMIZED METRIC CALCULATION FUNCTIONS
# All functions receive DataFrame to avoid redundant DB queries
# ==============================================================================

def calculate_52week_metrics(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate 52-week metrics from DataFrame.
    No DB access - pure calculation.

    Args:
        df: DataFrame with columns: high, low, close (indexed by date)

    Returns:
        Dict with: 52w_high, 52w_low, 52w_pct
    """
    if df.empty or len(df) < 50:
        return {}

    try:
        week_52_high = df['high'].max()
        week_52_low = df['low'].min()
        current_price = df['close'].iloc[-1]

        # Calculate percentile
        if week_52_high == week_52_low:
            week_52_pct = 0.5
        else:
            week_52_pct = (current_price - week_52_low) / (week_52_high - week_52_low)

        return {
            '52w_high': float(week_52_high),
            '52w_low': float(week_52_low),
            '52w_pct': float(week_52_pct)
        }
    except Exception as e:
        logger.warning(f"Error calculating 52W metrics: {e}")
        return {}


def calculate_atr_metrics(df: pd.DataFrame, period: int = 20) -> Dict[str, float]:
    """
    Calculate ATR (Average True Range) metrics using pandas-ta.
    No DB access - pure calculation.

    Args:
        df: DataFrame with columns: high, low, close (indexed by date)
        period: ATR period (default 20 days)

    Returns:
        Dict with: atr, atr_pct
    """
    if df.empty or len(df) < period:
        return {}

    try:
        # Calculate ATR using pandas-ta if available
        if ta:
            atr_series = ta.atr(df['high'], df['low'], df['close'], length=period)
            if atr_series is not None and not atr_series.empty:
                atr = float(atr_series.iloc[-1])
            else:
                return {}
        else:
            # Manual ATR calculation
            df_copy = df.copy()
            df_copy['prev_close'] = df_copy['close'].shift(1)
            df_copy['tr1'] = df_copy['high'] - df_copy['low']
            df_copy['tr2'] = abs(df_copy['high'] - df_copy['prev_close'])
            df_copy['tr3'] = abs(df_copy['low'] - df_copy['prev_close'])
            df_copy['true_range'] = df_copy[['tr1', 'tr2', 'tr3']].max(axis=1)

            # ATR is simple moving average of true range
            atr = df_copy['true_range'].tail(period).mean()

        current_price = df['close'].iloc[-1]

        if current_price > 0:
            atr_pct = atr / current_price
        else:
            atr_pct = 0

        return {
            'atr': float(atr),
            'atr_pct': float(atr_pct)
        }
    except Exception as e:
        logger.warning(f"Error calculating ATR metrics: {e}")
        return {}


def calculate_volume_metrics(df: pd.DataFrame, period: int = 20) -> Dict[str, float]:
    """
    Calculate average volume metrics from DataFrame.
    No DB access - pure calculation.

    Args:
        df: DataFrame with columns: volume, close (indexed by date)
        period: Averaging period (default 20 days)

    Returns:
        Dict with: avg_volume, avg_volume_usd
    """
    if df.empty or len(df) < period:
        return {}

    try:
        # Calculate average volume
        avg_volume = df['volume'].tail(period).mean()

        # Calculate average USD volume
        df_copy = df.copy()
        df_copy['volume_usd'] = df_copy['volume'] * df_copy['close']
        avg_volume_usd = df_copy['volume_usd'].tail(period).mean()

        return {
            'avg_volume': float(avg_volume),
            'avg_volume_usd': float(avg_volume_usd)
        }
    except Exception as e:
        logger.warning(f"Error calculating volume metrics: {e}")
        return {}


def calculate_days_to_earnings(ticker: Ticker) -> Optional[int]:
    """
    Calculate days until next earnings date.
    Reads from ticker.next_earnings_date (populated during price sync).

    Returns:
        Number of days to earnings, or None if not available
    """
    if not ticker.next_earnings_date:
        return None

    today = date.today()

    if ticker.next_earnings_date < today:
        return None

    days = (ticker.next_earnings_date - today).days

    return days


# ==============================================================================
# MAIN CALCULATION FUNCTION (OPTIMIZED)
# ==============================================================================

def calculate_ticker_metrics(
    ticker: Ticker,
    target_date: Optional[date] = None
) -> Dict[str, any]:
    """
    Calculate all metrics for a ticker on a specific date.
    OPTIMIZED: Fetches price data ONCE and reuses for all calculations.

    Args:
        ticker: Ticker object
        target_date: Date to calculate for (default: today)

    Returns:
        Dict with status: {success: bool, metrics_calculated: int, message: str}
    """
    if target_date is None:
        target_date = date.today()

    db = get_db()
    metrics_calculated = 0

    try:
        # Check if we have price data
        latest_price_date = get_latest_price_date(db, ticker.id)

        if not latest_price_date:
            return {
                'success': False,
                'metrics_calculated': 0,
                'message': 'No price data available'
            }

        # Can't calculate metrics for future dates
        if target_date > latest_price_date:
            target_date = latest_price_date

        # OPTIMIZATION: Fetch price data ONCE (1 year for 52W calculation)
        start_date = target_date - timedelta(days=365)
        prices = get_price_history(db, ticker.id, start_date, target_date)

        if not prices or len(prices) < 20:
            return {
                'success': False,
                'metrics_calculated': 0,
                'message': 'Insufficient price history'
            }

        # OPTIMIZATION: Convert to DataFrame ONCE
        df = pd.DataFrame([{
            'date': p.date,
            'high': float(p.high) if p.high else None,
            'low': float(p.low) if p.low else None,
            'close': float(p.close) if p.close else None,
            'volume': float(p.volume) if p.volume else 0
        } for p in prices])

        df = df.set_index('date').sort_index()
        df = df.dropna(subset=['high', 'low', 'close'])

        if df.empty:
            return {
                'success': False,
                'metrics_calculated': 0,
                'message': 'No valid price data'
            }

        # Calculate ALL metrics from same DataFrame (NO redundant DB queries)

        # 52-week metrics
        week_52_metrics = calculate_52week_metrics(df)
        for key, value in week_52_metrics.items():
            upsert_metric(db, ticker.id, target_date, key, value)
            metrics_calculated += 1

        # ATR metrics
        atr_metrics = calculate_atr_metrics(df, period=20)
        for key, value in atr_metrics.items():
            upsert_metric(db, ticker.id, target_date, key, value)
            metrics_calculated += 1

        # Volume metrics
        volume_metrics = calculate_volume_metrics(df, period=20)
        for key, value in volume_metrics.items():
            upsert_metric(db, ticker.id, target_date, key, value)
            metrics_calculated += 1

        # Days to earnings (from ticker table, no API call)
        days_to_earnings = calculate_days_to_earnings(ticker)
        if days_to_earnings is not None:
            upsert_metric(db, ticker.id, target_date, 'days_to_earnings', days_to_earnings)
            metrics_calculated += 1

        db.commit()

        return {
            'success': True,
            'metrics_calculated': metrics_calculated,
            'message': f'Calculated {metrics_calculated} metrics'
        }

    except Exception as e:
        logger.error(f"{ticker.symbol}: Error calculating metrics - {e}")
        return {
            'success': False,
            'metrics_calculated': 0,
            'message': str(e)
        }
    finally:
        db.close()


def calculate_all_metrics(
    target_date: Optional[date] = None,
    progress_callback: Optional[Callable[[int, int, str, str], None]] = None
) -> Dict[str, any]:
    """
    Calculate metrics for all tickers.

    Args:
        target_date: Date to calculate for (default: today)
        progress_callback: Function(current, total, ticker, status) for progress tracking

    Returns:
        Dict with statistics: {success_count, failed_count, total, total_metrics}
    """
    if target_date is None:
        target_date = date.today()

    db = get_db()
    stats = {
        'success_count': 0,
        'failed_count': 0,
        'total': 0,
        'total_metrics': 0,
        'last_calc_date': None
    }

    try:
        # Get all tickers (including ETFs for analysis)
        tickers = get_all_tickers(db, exclude_etfs=False)
        stats['total'] = len(tickers)

        logger.info(f"Starting metrics calculation for {stats['total']} tickers on {target_date}...")

        # Process each ticker
        for idx, ticker in enumerate(tickers, 1):
            try:
                if progress_callback:
                    progress_callback(idx, stats['total'], ticker.symbol, 'calculating')

                # Calculate metrics
                result = calculate_ticker_metrics(ticker, target_date)

                if result['success']:
                    stats['success_count'] += 1
                    stats['total_metrics'] += result['metrics_calculated']

                    logger.info(f"{ticker.symbol}: {result['message']} ({stats['success_count']}/{stats['total']})")

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'success')
                else:
                    stats['failed_count'] += 1
                    logger.warning(f"{ticker.symbol}: {result['message']}")

                    if progress_callback:
                        progress_callback(idx, stats['total'], ticker.symbol, 'failed')

            except Exception as e:
                logger.error(f"{ticker.symbol}: Error during calculation - {e}")
                stats['failed_count'] += 1

                if progress_callback:
                    progress_callback(idx, stats['total'], ticker.symbol, 'error')

        # Update sync status
        update_metrics_calc(db, 'stocks', target_date)
        stats['last_calc_date'] = target_date

        logger.info(f"Metrics calculation complete: {stats['success_count']} successful, {stats['failed_count']} failed, {stats['total_metrics']} total metrics")

        return stats

    finally:
        db.close()


def calculate_single_ticker_by_symbol(
    symbol: str,
    target_date: Optional[date] = None
) -> Dict[str, any]:
    """
    Calculate metrics for a single ticker by symbol.

    Args:
        symbol: Ticker symbol
        target_date: Date to calculate for (default: today)

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

        result = calculate_ticker_metrics(ticker, target_date)

        return result

    finally:
        db.close()


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        symbol = sys.argv[1].upper()
        print(f"Calculating metrics for {symbol}...")
        result = calculate_single_ticker_by_symbol(symbol)
        print(f"Result: {result}")
    else:
        print("Starting full metrics calculation...")
        results = calculate_all_metrics()
        print(f"\nResults:")
        print(f"  Success: {results['success_count']}")
        print(f"  Failed: {results['failed_count']}")
        print(f"  Total: {results['total']}")
        print(f"  Total metrics: {results['total_metrics']}")
        print(f"  Last calc date: {results['last_calc_date']}")
