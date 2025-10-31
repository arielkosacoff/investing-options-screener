"""
Database module using SQLAlchemy ORM for PostgreSQL
Provides persistent storage for stocks, prices, metrics, sectors, industries, and screening results.
"""

import os
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from decimal import Decimal

from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date, Boolean, BigInteger, DateTime, Text, ForeignKey, Index, func, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

engine = create_engine(
    DATABASE_URL,
    echo=os.getenv('DB_LOGGING', 'false').lower() == 'true',
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ==============================================================================
# DATABASE MODELS
# ==============================================================================

class Market(Base):
    """Market/Index definitions (e.g., S&P 500)"""
    __tablename__ = 'markets'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(50), nullable=False, unique=True, index=True)  # 'sp500', 'nasdaq100', 'russell1000'
    name = Column(String(100), nullable=False)  # 'S&P 500', 'NASDAQ 100'
    symbol = Column(String(10), nullable=False)  # 'SPY', 'QQQ', 'IWM'
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    tickers = relationship('Ticker', back_populates='market')


class Sector(Base):
    """Sector definitions from yfinance (e.g., Technology)"""
    __tablename__ = 'sectors'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), nullable=False, unique=True, index=True)  # 'technology' from yfinance
    name = Column(String(200), nullable=False)  # 'Technology'
    symbol = Column(String(10))  # 'XLK' (sector ETF)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    industries = relationship('Industry', back_populates='sector')
    tickers = relationship('Ticker', back_populates='sector')


class Industry(Base):
    """Industry definitions from yfinance (e.g., Software - Infrastructure)"""
    __tablename__ = 'industries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), nullable=False, unique=True, index=True)  # 'software-infrastructure'
    name = Column(String(200), nullable=False)  # 'Software - Infrastructure'
    sector_id = Column(Integer, ForeignKey('sectors.id'), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    sector = relationship('Sector', back_populates='industries')
    tickers = relationship('Ticker', back_populates='industry')


class Ticker(Base):
    """Stock tickers with metadata"""
    __tablename__ = 'tickers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, unique=True, index=True)
    name = Column(String(200))
    industry_id = Column(Integer, ForeignKey('industries.id'), index=True)  # For description only
    sector_id = Column(Integer, ForeignKey('sectors.id'), index=True)       # For relative strength analysis
    market_id = Column(Integer, ForeignKey('markets.id'), index=True)       # For relative strength analysis
    is_sector_etf = Column(Boolean, default=False)
    is_market_etf = Column(Boolean, default=False)
    next_earnings_date = Column(Date, nullable=True)
    market_cap = Column(BigInteger)
    shares_outstanding = Column(BigInteger)  # For PE ratio calculation
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    industry = relationship('Industry', back_populates='tickers')
    sector = relationship('Sector', back_populates='tickers')
    market = relationship('Market', back_populates='tickers')
    prices = relationship('TickerPrice', back_populates='ticker', cascade='all, delete-orphan')
    metrics = relationship('TickerMetric', back_populates='ticker', cascade='all, delete-orphan')
    screening_results = relationship('ScreeningResult', back_populates='ticker', cascade='all, delete-orphan')


class TickerPrice(Base):
    """Daily OHLCV price data for tickers"""
    __tablename__ = 'ticker_prices'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey('tickers.id'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    open = Column(Numeric(12, 4))
    high = Column(Numeric(12, 4))
    low = Column(Numeric(12, 4))
    close = Column(Numeric(12, 4))
    volume = Column(BigInteger)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    ticker = relationship('Ticker', back_populates='prices')

    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uq_ticker_price_date'),
        Index('idx_ticker_date', 'ticker_id', 'date'),
    )


class TickerMetric(Base):
    """Calculated metrics for tickers (EAV pattern)"""
    __tablename__ = 'ticker_metrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey('tickers.id'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    metric_key = Column(String(100), nullable=False, index=True)  # '52w_high', '52w_low', '52w_pct', 'atr_pct', 'pe_ratio', 'avg_volume', etc.
    metric_value = Column(Numeric(20, 6))
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    ticker = relationship('Ticker', back_populates='metrics')

    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', 'metric_key', name='uq_ticker_metric'),
        Index('idx_ticker_metric', 'ticker_id', 'date', 'metric_key'),
    )


class Config(Base):
    """Application configuration (key-value store)"""
    __tablename__ = 'config'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), nullable=False, unique=True, index=True)
    value = Column(Text, nullable=False)
    data_type = Column(String(20), nullable=False)  # 'float', 'int', 'string', 'bool'
    description = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SyncStatus(Base):
    """Track last sync dates for different entity types"""
    __tablename__ = 'sync_status'

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String(50), nullable=False, unique=True, index=True)  # 'stocks', 'sectors', 'industries', 'market'
    last_price_sync = Column(Date, nullable=True)
    last_metrics_calc = Column(Date, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ScreeningResult(Base):
    """Historical screening results"""
    __tablename__ = 'screening_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey('tickers.id'), nullable=False, index=True)
    screening_date = Column(Date, nullable=False, index=True, default=date.today)

    # Stock info
    stock_price = Column(Numeric(12, 4))
    industry = Column(String(200))
    sector = Column(String(200))
    sector_etf = Column(String(10))

    # 52-week positioning
    stock_52w_pct = Column(Numeric(5, 4))
    week_52_high = Column(Numeric(12, 4))
    week_52_low = Column(Numeric(12, 4))
    dist_high_pct = Column(Numeric(6, 4))
    dist_low_pct = Column(Numeric(6, 4))

    # Relative strength
    sector_52w_pct = Column(Numeric(5, 4))
    market_52w_pct = Column(Numeric(5, 4))

    # Valuation
    pe_ratio = Column(Numeric(10, 2))
    sector_pe = Column(Numeric(10, 2))
    market_pe = Column(Numeric(10, 2))
    market_cap_millions = Column(Integer)

    # Liquidity & volatility
    avg_volume_millions = Column(Numeric(10, 2))
    atr_pct = Column(Numeric(6, 4))
    is_lateral = Column(Boolean)

    # Options details
    put_strike = Column(Numeric(12, 4))
    dte = Column(Integer)
    bid = Column(Numeric(10, 2))
    ask = Column(Numeric(10, 2))
    spread = Column(Numeric(10, 2))
    premium = Column(Numeric(10, 2))
    annualized_yield = Column(Numeric(6, 4))
    contracts_needed = Column(Integer)

    # Earnings
    days_to_earnings = Column(Integer, nullable=True)

    # Links
    chart_link = Column(String(500))
    options_link = Column(String(500))

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    ticker = relationship('Ticker', back_populates='screening_results')

    __table_args__ = (
        Index('idx_screening_date_yield', 'screening_date', 'annualized_yield'),
    )


# ==============================================================================
# DATABASE FUNCTIONS
# ==============================================================================

def init_db():
    """Initialize database - create all tables"""
    Base.metadata.create_all(bind=engine)

    # Initialize default markets if not exists
    db = SessionLocal()
    try:
        if db.query(Market).count() == 0:
            markets = [
                Market(key='sp500', name='S&P 500', symbol='SPY'),
                Market(key='nasdaq100', name='NASDAQ 100', symbol='QQQ'),
                Market(key='russell1000', name='Russell 1000', symbol='IWM'),
            ]
            db.add_all(markets)
            db.commit()
    finally:
        db.close()


def get_db() -> Session:
    """Get database session"""
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.close()
        raise


# ==============================================================================
# CONFIG FUNCTIONS
# ==============================================================================

def get_config(db: Session, key: str, default: Any = None) -> Any:
    """Get configuration value"""
    config = db.query(Config).filter_by(key=key).first()
    if not config:
        return default

    # Parse based on data type
    if config.data_type == 'int':
        return int(config.value)
    elif config.data_type == 'float':
        return float(config.value)
    elif config.data_type == 'bool':
        return config.value.lower() in ('true', '1', 'yes')
    else:
        return config.value


def set_config(db: Session, key: str, value: Any, data_type: str, description: str = None):
    """Set configuration value"""
    config = db.query(Config).filter_by(key=key).first()

    value_str = str(value)

    if config:
        config.value = value_str
        config.data_type = data_type
        if description:
            config.description = description
        config.updated_at = datetime.utcnow()
    else:
        config = Config(
            key=key,
            value=value_str,
            data_type=data_type,
            description=description
        )
        db.add(config)

    db.commit()
    return config


def get_all_config(db: Session) -> Dict[str, Any]:
    """Get all configuration as dictionary"""
    configs = db.query(Config).all()
    result = {}
    for config in configs:
        if config.data_type == 'int':
            result[config.key] = int(config.value)
        elif config.data_type == 'float':
            result[config.key] = float(config.value)
        elif config.data_type == 'bool':
            result[config.key] = config.value.lower() in ('true', '1', 'yes')
        else:
            result[config.key] = config.value
    return result


# ==============================================================================
# SYNC STATUS FUNCTIONS
# ==============================================================================

def get_sync_status(db: Session, entity_type: str) -> Optional[SyncStatus]:
    """Get sync status for entity type"""
    return db.query(SyncStatus).filter_by(entity_type=entity_type).first()


def update_price_sync(db: Session, entity_type: str, sync_date: date):
    """Update last price sync date"""
    status = db.query(SyncStatus).filter_by(entity_type=entity_type).first()

    if status:
        status.last_price_sync = sync_date
        status.updated_at = datetime.utcnow()
    else:
        status = SyncStatus(entity_type=entity_type, last_price_sync=sync_date)
        db.add(status)

    db.commit()
    return status


def update_metrics_calc(db: Session, entity_type: str, calc_date: date):
    """Update last metrics calculation date"""
    status = db.query(SyncStatus).filter_by(entity_type=entity_type).first()

    if status:
        status.last_metrics_calc = calc_date
        status.updated_at = datetime.utcnow()
    else:
        status = SyncStatus(entity_type=entity_type, last_metrics_calc=calc_date)
        db.add(status)

    db.commit()
    return status


# ==============================================================================
# TICKER FUNCTIONS
# ==============================================================================

def upsert_ticker(db: Session, symbol: str, data: Dict[str, Any]) -> Ticker:
    """Insert or update ticker"""
    ticker = db.query(Ticker).filter_by(symbol=symbol).first()

    if ticker:
        for key, value in data.items():
            setattr(ticker, key, value)
        ticker.updated_at = datetime.utcnow()
    else:
        ticker = Ticker(symbol=symbol, **data)
        db.add(ticker)

    db.commit()
    db.refresh(ticker)
    return ticker


def get_ticker(db: Session, symbol: str) -> Optional[Ticker]:
    """Get ticker by symbol"""
    return db.query(Ticker).filter_by(symbol=symbol).first()


def get_all_tickers(db: Session, exclude_etfs: bool = False) -> List[Ticker]:
    """Get all tickers, optionally excluding ETFs"""
    query = db.query(Ticker)
    if exclude_etfs:
        query = query.filter(
            Ticker.is_sector_etf == False,
            Ticker.is_market_etf == False
        )
    return query.all()


# ==============================================================================
# SECTOR/INDUSTRY FUNCTIONS
# ==============================================================================

def upsert_sector(db: Session, key: str, name: str, symbol: str = None) -> Sector:
    """Insert or update sector"""
    sector = db.query(Sector).filter_by(key=key).first()

    if sector:
        sector.name = name
        if symbol:
            sector.symbol = symbol
        sector.updated_at = datetime.utcnow()
    else:
        sector = Sector(key=key, name=name, symbol=symbol)
        db.add(sector)

    db.commit()
    db.refresh(sector)
    return sector


def upsert_industry(db: Session, key: str, name: str, sector_id: int) -> Industry:
    """Insert or update industry"""
    industry = db.query(Industry).filter_by(key=key).first()

    if industry:
        industry.name = name
        industry.sector_id = sector_id
        industry.updated_at = datetime.utcnow()
    else:
        industry = Industry(key=key, name=name, sector_id=sector_id)
        db.add(industry)

    db.commit()
    db.refresh(industry)
    return industry


def get_sector_by_key(db: Session, key: str) -> Optional[Sector]:
    """Get sector by key"""
    return db.query(Sector).filter_by(key=key).first()


def get_industry_by_key(db: Session, key: str) -> Optional[Industry]:
    """Get industry by key"""
    return db.query(Industry).filter_by(key=key).first()


# ==============================================================================
# PRICE FUNCTIONS
# ==============================================================================

def upsert_price(db: Session, ticker_id: int, date_val: date, data: Dict[str, Any]) -> TickerPrice:
    """Insert or update price data"""
    price = db.query(TickerPrice).filter_by(ticker_id=ticker_id, date=date_val).first()

    if price:
        for key, value in data.items():
            setattr(price, key, value)
        price.updated_at = datetime.utcnow()
    else:
        price = TickerPrice(ticker_id=ticker_id, date=date_val, **data)
        db.add(price)

    db.commit()
    return price


def get_latest_price_date(db: Session, ticker_id: int) -> Optional[date]:
    """Get the latest price date for a ticker"""
    result = db.query(func.max(TickerPrice.date)).filter_by(ticker_id=ticker_id).scalar()
    return result


def get_price_history(db: Session, ticker_id: int, start_date: date = None, end_date: date = None) -> List[TickerPrice]:
    """Get price history for ticker"""
    query = db.query(TickerPrice).filter_by(ticker_id=ticker_id)

    if start_date:
        query = query.filter(TickerPrice.date >= start_date)
    if end_date:
        query = query.filter(TickerPrice.date <= end_date)

    return query.order_by(TickerPrice.date).all()


# ==============================================================================
# METRIC FUNCTIONS
# ==============================================================================

def upsert_metric(db: Session, ticker_id: int, date_val: date, metric_key: str, metric_value: float) -> TickerMetric:
    """Insert or update metric"""
    metric = db.query(TickerMetric).filter_by(
        ticker_id=ticker_id,
        date=date_val,
        metric_key=metric_key
    ).first()

    if metric:
        metric.metric_value = Decimal(str(metric_value))
    else:
        metric = TickerMetric(
            ticker_id=ticker_id,
            date=date_val,
            metric_key=metric_key,
            metric_value=Decimal(str(metric_value))
        )
        db.add(metric)

    db.commit()
    return metric


def get_metric(db: Session, ticker_id: int, date_val: date, metric_key: str) -> Optional[float]:
    """Get specific metric value"""
    metric = db.query(TickerMetric).filter_by(
        ticker_id=ticker_id,
        date=date_val,
        metric_key=metric_key
    ).first()

    return float(metric.metric_value) if metric else None


def get_all_metrics(db: Session, ticker_id: int, date_val: date) -> Dict[str, float]:
    """Get all metrics for ticker on specific date"""
    metrics = db.query(TickerMetric).filter_by(
        ticker_id=ticker_id,
        date=date_val
    ).all()

    return {m.metric_key: float(m.metric_value) for m in metrics}


# ==============================================================================
# SCREENING RESULTS FUNCTIONS
# ==============================================================================

def save_screening_result(db: Session, data: dict) -> ScreeningResult:
    """Save screening result"""
    result = ScreeningResult(**data)
    db.add(result)
    db.commit()
    return result


def get_latest_screening_results(db: Session, days: int = 1) -> List[ScreeningResult]:
    """Get screening results from the most recent screening run only"""
    from sqlalchemy import func

    # Get the most recent created_at timestamp
    latest_run = db.query(
        func.max(ScreeningResult.created_at)
    ).scalar()

    if not latest_run:
        return []

    # Get all results from that specific screening run
    results = db.query(ScreeningResult).filter(
        ScreeningResult.created_at == latest_run
    ).order_by(
        ScreeningResult.annualized_yield.desc()
    ).all()

    return results


def get_screening_results_by_date(db: Session, date_val: date) -> List[ScreeningResult]:
    """Get all screening results for a specific date"""
    results = db.query(ScreeningResult).filter_by(
        screening_date=date_val
    ).order_by(
        ScreeningResult.annualized_yield.desc()
    ).all()

    return results


# ==============================================================================
# CLEANUP FUNCTIONS
# ==============================================================================

def cleanup_old_data(db: Session, days_to_keep: int = 90):
    """Clean up old data from database"""
    from datetime import timedelta
    cutoff_date = date.today() - timedelta(days=days_to_keep)

    # Delete old prices
    db.query(TickerPrice).filter(TickerPrice.date < cutoff_date).delete()

    # Delete old metrics
    db.query(TickerMetric).filter(TickerMetric.date < cutoff_date).delete()

    # Delete old screening results
    db.query(ScreeningResult).filter(ScreeningResult.screening_date < cutoff_date).delete()

    db.commit()


if __name__ == '__main__':
    print("Initializing database...")
    init_db()
    print("Database initialized successfully!")
    print(f"Tables created: {', '.join([table.name for table in Base.metadata.tables.values()])}")
