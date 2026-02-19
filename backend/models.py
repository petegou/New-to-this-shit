"""Data models for PGRB Backtesting"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum

class Regime(Enum):
    GOLDILOCKS = "GOLDILOCKS"
    REFLATION = "REFLATION"
    INFLATION = "INFLATION"
    DEFLATION = "DEFLATION"

class RiskRegime(Enum):
    RISK_ON = "RISK ON"
    RISK_OFF = "RISK OFF"

# Regime colors for frontend
REGIME_COLORS = {
    "GOLDILOCKS": "#22c55e",  # Green
    "REFLATION": "#3b82f6",   # Blue
    "INFLATION": "#f97316",   # Orange
    "DEFLATION": "#ef4444",   # Red
}

@dataclass
class GridDataRow:
    """Single row of GRID data from 42 Macro Excel"""
    date: datetime
    sum_confirming_markets: int
    goldilocks_confirming: int
    reflation_confirming: int
    inflation_confirming: int
    deflation_confirming: int
    market_regime: str
    risk_regime: str
    vams: Dict[str, int]  # {ticker: -2/0/+2}

@dataclass
class RiskProfile:
    """Risk profile with allocations per regime"""
    id: str
    name: str
    allocations: Dict[str, Dict[str, float]]  # {regime: {ticker: weight}}

@dataclass
class Position:
    """Current position in a security"""
    ticker: str
    shares: float
    avg_cost: float
    current_price: float

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def total_return(self) -> float:
        if self.avg_cost == 0:
            return 0
        return (self.current_price - self.avg_cost) / self.avg_cost

@dataclass
class Trade:
    """Record of a trade execution"""
    date: datetime
    action: str  # "BUY" or "SELL"
    ticker: str
    shares: float
    price: float
    value: float
    regime: str
    reason: str

@dataclass
class EquityCurvePoint:
    """Single point on equity curve"""
    date: datetime
    portfolio_value: float
    benchmark_value: float
    regime: str
    cash_value: float = 0

@dataclass
class DrawdownEvent:
    """A drawdown period"""
    drawdown_pct: float
    start_date: datetime
    low_date: datetime
    end_date: Optional[datetime]
    length_days: int
    recovery_days: Optional[int]

@dataclass
class RegimeStat:
    """Statistics for a single regime"""
    regime: str
    days: int
    pct_time: float
    total_return: float
    num_trades: int

@dataclass
class BacktestConfig:
    """Configuration for running a backtest"""
    name: str
    risk_profile_id: str
    start_date: datetime
    end_date: datetime
    starting_value: float = 100000.0
    benchmark_ticker: str = "SPY"

@dataclass
class BacktestResults:
    """Complete results from a backtest run"""
    config: BacktestConfig

    # Summary stats
    starting_value: float
    ending_value: float
    total_return: float
    annualized_return: float
    benchmark_total_return: float
    benchmark_annualized_return: float

    # Risk metrics
    max_drawdown: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    std_dev_annualized: float
    beta: float
    alpha: float
    information_ratio: float
    upside_capture: float
    downside_capture: float
    positive_months_pct: float

    # Time series
    equity_curve: List[EquityCurvePoint] = field(default_factory=list)
    drawdown_series: List[dict] = field(default_factory=list)

    # Monthly returns grid
    monthly_returns: Dict[str, Dict[str, float]] = field(default_factory=dict)
    benchmark_monthly_returns: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # Trailing returns
    trailing_returns: Dict[str, float] = field(default_factory=dict)
    benchmark_trailing_returns: Dict[str, float] = field(default_factory=dict)

    # Top drawdowns
    top_drawdowns: List[DrawdownEvent] = field(default_factory=list)

    # Holdings at end
    final_holdings: List[dict] = field(default_factory=list)

    # Regime analysis
    regime_stats: List[RegimeStat] = field(default_factory=list)
    regime_timeline: List[dict] = field(default_factory=list)

    # Trades
    trades: List[Trade] = field(default_factory=list)
    total_trades: int = 0

# Default risk profiles
DEFAULT_PROFILES = {
    "aggressive": RiskProfile(
        id="aggressive",
        name="Aggressive",
        allocations={
            "GOLDILOCKS": {
                "XLK": 0.15, "QQQ": 0.15, "SPHB": 0.10, "XLC": 0.10,
                "IWF": 0.10, "EWZ": 0.05, "EEM": 0.05, "FXI": 0.05,
                "EWA": 0.05, "INDA": 0.05, "GLD": 0.05, "SPY": 0.10
            },
            "REFLATION": {
                "XLE": 0.15, "XLF": 0.15, "XLI": 0.10, "XLB": 0.10,
                "EWZ": 0.10, "EEM": 0.10, "GNR": 0.10, "PDBC": 0.10,
                "GLD": 0.05, "SPY": 0.05
            },
            "INFLATION": {
                "XLE": 0.20, "GLD": 0.20, "PDBC": 0.15, "TIP": 0.10,
                "SHY": 0.15, "USO": 0.10, "XLU": 0.10
            },
            "DEFLATION": {
                "TLT": 0.30, "IEF": 0.20, "AGG": 0.15, "SHY": 0.15,
                "GLD": 0.10, "UUP": 0.10
            }
        }
    ),
    "moderate": RiskProfile(
        id="moderate",
        name="Moderate",
        allocations={
            "GOLDILOCKS": {
                "SPY": 0.30, "QQQ": 0.15, "XLK": 0.10, "IWF": 0.10,
                "EEM": 0.05, "GLD": 0.05, "AGG": 0.15, "TIP": 0.10
            },
            "REFLATION": {
                "SPY": 0.25, "XLE": 0.10, "XLF": 0.10, "XLI": 0.10,
                "EEM": 0.10, "GLD": 0.10, "AGG": 0.15, "TIP": 0.10
            },
            "INFLATION": {
                "XLE": 0.15, "GLD": 0.20, "TIP": 0.20, "SHY": 0.20,
                "AGG": 0.15, "XLU": 0.10
            },
            "DEFLATION": {
                "TLT": 0.25, "IEF": 0.20, "AGG": 0.20, "SHY": 0.20,
                "GLD": 0.15
            }
        }
    ),
    "conservative": RiskProfile(
        id="conservative",
        name="Conservative",
        allocations={
            "GOLDILOCKS": {
                "SPY": 0.25, "AGG": 0.30, "TIP": 0.15, "GLD": 0.10,
                "SHY": 0.10, "IEF": 0.10
            },
            "REFLATION": {
                "SPY": 0.20, "AGG": 0.30, "TIP": 0.20, "GLD": 0.15,
                "XLE": 0.05, "SHY": 0.10
            },
            "INFLATION": {
                "GLD": 0.20, "TIP": 0.25, "SHY": 0.25, "AGG": 0.20,
                "XLE": 0.10
            },
            "DEFLATION": {
                "TLT": 0.25, "IEF": 0.25, "AGG": 0.20, "SHY": 0.20,
                "GLD": 0.10
            }
        }
    )
}
