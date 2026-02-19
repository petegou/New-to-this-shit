"""
Price data module with synthetic data fallback for demo purposes.
In production, this would use live Yahoo Finance data.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Base prices for common ETFs (approximate 2018 starting prices)
BASE_PRICES = {
    # US Equity
    "SPY": 268.0, "QQQ": 165.0, "IWM": 150.0, "IWF": 145.0, "IWB": 165.0,
    "IWD": 125.0, "IWR": 55.0, "SPHB": 45.0, "SPLV": 48.0, "MTUM": 105.0,
    "QUAL": 85.0, "SPHD": 40.0,

    # Sectors
    "XLK": 68.0, "XLF": 28.0, "XLE": 72.0, "XLV": 82.0, "XLI": 75.0,
    "XLY": 105.0, "XLP": 55.0, "XLC": 48.0, "XLB": 58.0, "XLRE": 32.0,
    "XLU": 52.0,

    # International
    "EEM": 47.0, "EWZ": 42.0, "FXI": 46.0, "EWA": 22.0, "INDA": 34.0,
    "EZU": 42.0, "ACWX": 48.0, "DXJ": 58.0, "EWU": 33.0, "EWC": 28.0,
    "GNR": 32.0,

    # Fixed Income
    "TLT": 122.0, "IEF": 104.0, "SHY": 84.0, "SHV": 110.0, "AGG": 108.0,
    "TIP": 112.0, "STIP": 100.0, "LQD": 118.0, "HYG": 86.0, "EMB": 112.0,
    "EMLC": 32.0, "BNDX": 55.0, "BWX": 27.0, "MBB": 105.0, "BKLN": 23.0,
    "PFF": 38.0, "CWB": 48.0, "BIZD": 18.0, "BILS": 100.0,

    # Commodities & Alternatives
    "GLD": 125.0, "SLV": 15.0, "USO": 12.0, "PDBC": 16.0, "PDBA": 22.0,
    "DBB": 17.0, "UUP": 24.0,

    # Currency
    "FXE": 115.0, "FXB": 128.0, "FXY": 88.0, "FXA": 76.0, "FXC": 78.0,
    "FXF": 98.0,

    # Other
    "SRUUF": 10.0, "BIGPX": 12.0, "XBT": 10.0,
}

# Annualized volatility estimates by asset type
VOLATILITY = {
    # Equity - higher vol
    "SPY": 0.16, "QQQ": 0.22, "IWM": 0.22, "XLK": 0.24, "XLF": 0.22,
    "XLE": 0.30, "EEM": 0.24, "EWZ": 0.35, "SPHB": 0.28, "XLC": 0.22,

    # Bonds - lower vol
    "TLT": 0.15, "IEF": 0.08, "SHY": 0.02, "SHV": 0.01, "AGG": 0.04,
    "TIP": 0.06, "LQD": 0.08, "HYG": 0.10,

    # Commodities - higher vol
    "GLD": 0.15, "SLV": 0.25, "USO": 0.40, "PDBC": 0.20, "DBB": 0.22,

    # Default
    "default": 0.20
}

# Regime-based expected returns (annualized)
REGIME_RETURNS = {
    "GOLDILOCKS": {
        "SPY": 0.18, "QQQ": 0.25, "XLK": 0.28, "IWF": 0.22,
        "TLT": -0.05, "GLD": 0.02, "XLE": 0.10,
        "default_equity": 0.15, "default_bond": -0.02, "default": 0.08
    },
    "REFLATION": {
        "SPY": 0.12, "XLE": 0.25, "XLF": 0.20, "XLB": 0.18, "EEM": 0.18,
        "TLT": -0.08, "GLD": 0.10, "PDBC": 0.15,
        "default_equity": 0.12, "default_bond": -0.04, "default": 0.08
    },
    "INFLATION": {
        "SPY": -0.05, "QQQ": -0.12, "XLK": -0.15,
        "XLE": 0.15, "GLD": 0.12, "PDBC": 0.18, "TIP": 0.04,
        "TLT": -0.15, "SHY": 0.02,
        "default_equity": -0.08, "default_bond": -0.05, "default": 0.02
    },
    "DEFLATION": {
        "SPY": -0.20, "QQQ": -0.25, "EEM": -0.30,
        "TLT": 0.25, "IEF": 0.15, "AGG": 0.08, "SHY": 0.03, "SHV": 0.02,
        "GLD": 0.08, "UUP": 0.05,
        "default_equity": -0.15, "default_bond": 0.10, "default": -0.05
    }
}

# Asset type classification
ASSET_TYPES = {
    "equity": ["SPY", "QQQ", "IWM", "IWF", "IWB", "IWD", "IWR", "SPHB", "SPLV",
               "MTUM", "QUAL", "SPHD", "XLK", "XLF", "XLE", "XLV", "XLI", "XLY",
               "XLP", "XLC", "XLB", "XLRE", "XLU", "EEM", "EWZ", "FXI", "EWA",
               "INDA", "EZU", "ACWX", "DXJ", "EWU", "EWC", "GNR"],
    "bond": ["TLT", "IEF", "SHY", "SHV", "AGG", "TIP", "STIP", "LQD", "HYG",
             "EMB", "EMLC", "BNDX", "BWX", "MBB", "BKLN", "PFF", "CWB", "BIZD", "BILS"],
    "commodity": ["GLD", "SLV", "USO", "PDBC", "PDBA", "DBB"],
    "currency": ["UUP", "FXE", "FXB", "FXY", "FXA", "FXC", "FXF"]
}


def get_asset_type(ticker: str) -> str:
    """Determine asset type for a ticker"""
    for asset_type, tickers in ASSET_TYPES.items():
        if ticker in tickers:
            return asset_type
    return "equity"  # Default


def get_expected_return(ticker: str, regime: str) -> float:
    """Get expected return for ticker in given regime"""
    regime_data = REGIME_RETURNS.get(regime, REGIME_RETURNS["GOLDILOCKS"])

    if ticker in regime_data:
        return regime_data[ticker]

    asset_type = get_asset_type(ticker)
    if asset_type == "equity":
        return regime_data.get("default_equity", 0.10)
    elif asset_type == "bond":
        return regime_data.get("default_bond", 0.02)
    else:
        return regime_data.get("default", 0.05)


def get_volatility(ticker: str) -> float:
    """Get volatility for ticker"""
    return VOLATILITY.get(ticker, VOLATILITY["default"])


def generate_synthetic_prices(
    tickers: List[str],
    start_date: datetime,
    end_date: datetime,
    regime_data: Dict[datetime, str]
) -> pd.DataFrame:
    """
    Generate synthetic but realistic price data based on regime.

    Args:
        tickers: List of tickers to generate
        start_date: Start date
        end_date: End date
        regime_data: Dict mapping date to regime name

    Returns:
        DataFrame with date index and ticker columns
    """
    # Generate date range (business days only)
    dates = pd.date_range(start=start_date, end=end_date, freq='B')

    # Initialize with base prices
    price_data = {}

    for ticker in tickers:
        base_price = BASE_PRICES.get(ticker, 50.0)
        vol = get_volatility(ticker)
        daily_vol = vol / np.sqrt(252)

        prices = [base_price]
        current_price = base_price

        for i in range(1, len(dates)):
            date = dates[i].to_pydatetime()

            # Get regime for this date (find closest)
            regime = "GOLDILOCKS"  # Default
            for d in sorted(regime_data.keys(), reverse=True):
                if d <= date:
                    regime = regime_data[d]
                    break

            # Get expected daily return
            annual_return = get_expected_return(ticker, regime)
            daily_return = annual_return / 252

            # Add random component
            random_return = np.random.normal(daily_return, daily_vol)

            # Apply return
            current_price = current_price * (1 + random_return)
            current_price = max(current_price, 0.01)  # Floor at $0.01

            prices.append(current_price)

        price_data[ticker] = prices

    df = pd.DataFrame(price_data, index=dates)
    return df


class PriceDataProvider:
    """Provides price data, using synthetic data as fallback"""

    def __init__(self, regime_timeline: Dict[datetime, str]):
        self.regime_timeline = regime_timeline
        self.price_cache: Optional[pd.DataFrame] = None

    def get_prices(
        self,
        tickers: List[str],
        start_date: datetime,
        end_date: datetime,
        use_synthetic: bool = True
    ) -> pd.DataFrame:
        """
        Get price data for tickers over date range.
        Falls back to synthetic data if real data unavailable.
        """
        if use_synthetic:
            print(f"Generating synthetic price data for {len(tickers)} tickers...")
            self.price_cache = generate_synthetic_prices(
                tickers, start_date, end_date, self.regime_timeline
            )
            return self.price_cache

        # Try yfinance first
        try:
            import yfinance as yf
            print(f"Fetching real price data for {len(tickers)} tickers...")

            data = yf.download(
                tickers,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                progress=False
            )

            if data.empty:
                raise ValueError("No data returned from yfinance")

            if len(tickers) > 1:
                self.price_cache = data['Adj Close']
            else:
                self.price_cache = data['Adj Close'].to_frame()
                self.price_cache.columns = tickers

            return self.price_cache

        except Exception as e:
            print(f"yfinance failed ({e}), falling back to synthetic data...")
            return self.get_prices(tickers, start_date, end_date, use_synthetic=True)

    def get_price(self, ticker: str, date: datetime) -> Optional[float]:
        """Get price for a single ticker on a date"""
        if self.price_cache is None or ticker not in self.price_cache.columns:
            return None

        try:
            idx = self.price_cache.index.get_indexer([date], method='ffill')[0]
            if idx >= 0:
                price = self.price_cache.iloc[idx][ticker]
                if pd.notna(price):
                    return float(price)
        except:
            pass

        return None


if __name__ == "__main__":
    # Test synthetic data generation
    from datetime import datetime

    tickers = ["SPY", "QQQ", "TLT", "GLD", "XLE"]
    start = datetime(2020, 1, 1)
    end = datetime(2023, 12, 31)

    # Sample regime data
    regimes = {
        datetime(2020, 1, 1): "GOLDILOCKS",
        datetime(2020, 3, 1): "DEFLATION",
        datetime(2020, 6, 1): "REFLATION",
        datetime(2021, 11, 1): "INFLATION",
        datetime(2022, 12, 1): "GOLDILOCKS",
    }

    df = generate_synthetic_prices(tickers, start, end, regimes)
    print(f"Generated {len(df)} days of price data")
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nLast 5 rows:")
    print(df.tail())
    print(f"\nPrice ranges:")
    print(df.describe().loc[['min', 'max', 'mean']])
