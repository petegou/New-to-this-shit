"""Core Backtesting Engine for PGRB"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from models import (
    GridDataRow, RiskProfile, Position, Trade, EquityCurvePoint,
    DrawdownEvent, RegimeStat, BacktestConfig, BacktestResults,
    DEFAULT_PROFILES
)
from price_data import PriceDataProvider


class BacktestEngine:
    """
    Backtesting engine implementing 42 Macro regime-based strategy.

    Key rules:
    1. Start in REFLATION regime
    2. Regime change requires: sum > 59, highest confirming, +2 spread over opposing
    3. Risk Off transition: 25% to SHV
    4. Deflation special rule: cash returns when XLK VAMS >= 0
    """

    OPPOSING_REGIMES = {
        "GOLDILOCKS": "DEFLATION",
        "DEFLATION": "GOLDILOCKS",
        "REFLATION": "INFLATION",
        "INFLATION": "REFLATION"
    }

    RISK_ON_REGIMES = {"GOLDILOCKS", "REFLATION"}
    RISK_OFF_REGIMES = {"INFLATION", "DEFLATION"}

    def __init__(self, grid_data: List[GridDataRow], risk_profile: RiskProfile):
        self.grid_data = {row.date: row for row in grid_data}
        self.dates = sorted(self.grid_data.keys())
        self.risk_profile = risk_profile
        self.price_provider: Optional[PriceDataProvider] = None

        # State
        self.current_regime = "REFLATION"
        self.positions: Dict[str, Position] = {}
        self.cash = 0.0
        self.shv_position = 0.0
        self.trades: List[Trade] = []
        self.equity_curve: List[EquityCurvePoint] = []

    def _get_required_tickers(self) -> List[str]:
        """Get all tickers needed for the backtest"""
        tickers = set()
        for regime_alloc in self.risk_profile.allocations.values():
            tickers.update(regime_alloc.keys())
        tickers.add("SHV")
        # Filter out crypto
        tickers = {t for t in tickers if t not in ["Bitcoin", "Ethereum"]}
        return list(tickers)

    def _build_regime_timeline(self) -> Dict[datetime, str]:
        """Build regime timeline from grid data"""
        timeline = {}
        for date in self.dates:
            row = self.grid_data[date]
            timeline[date] = row.market_regime
        return timeline

    def _get_price(self, ticker: str, date: datetime) -> Optional[float]:
        """Get price for a ticker on a given date"""
        if self.price_provider is None:
            return None
        return self.price_provider.get_price(ticker, date)

    def _check_regime_change(self, row: GridDataRow) -> Optional[str]:
        """Check if regime change conditions are met."""
        if row.sum_confirming_markets <= 59:
            return None

        confirmings = {
            "GOLDILOCKS": row.goldilocks_confirming,
            "REFLATION": row.reflation_confirming,
            "INFLATION": row.inflation_confirming,
            "DEFLATION": row.deflation_confirming
        }

        candidate = max(confirmings, key=confirmings.get)

        if candidate == self.current_regime:
            return None

        opposing = self.OPPOSING_REGIMES[candidate]
        spread = confirmings[candidate] - confirmings[opposing]

        if spread > 2:
            return candidate

        return None

    def _is_risk_on(self, regime: str) -> bool:
        return regime in self.RISK_ON_REGIMES

    def _calculate_portfolio_value(self, date: datetime) -> float:
        """Calculate total portfolio value"""
        total = self.cash

        if self.shv_position > 0:
            shv_price = self._get_price("SHV", date) or 110.0
            total += self.shv_position * shv_price

        for ticker, position in self.positions.items():
            price = self._get_price(ticker, date)
            if price:
                position.current_price = price
                total += position.market_value

        return total

    def _execute_trade(self, date: datetime, action: str, ticker: str,
                       shares: float, price: float, reason: str):
        """Record and execute a trade"""
        value = shares * price

        trade = Trade(
            date=date,
            action=action,
            ticker=ticker,
            shares=abs(shares),
            price=price,
            value=abs(value),
            regime=self.current_regime,
            reason=reason
        )
        self.trades.append(trade)

        if action == "BUY":
            if ticker in self.positions:
                old_pos = self.positions[ticker]
                total_shares = old_pos.shares + shares
                total_cost = (old_pos.shares * old_pos.avg_cost) + (shares * price)
                avg_cost = total_cost / total_shares if total_shares > 0 else price
                self.positions[ticker] = Position(ticker, total_shares, avg_cost, price)
            else:
                self.positions[ticker] = Position(ticker, shares, price, price)
            self.cash -= value
        else:
            if ticker in self.positions:
                self.positions[ticker].shares -= shares
                if self.positions[ticker].shares <= 0.001:
                    del self.positions[ticker]
            self.cash += value

    def _liquidate_all(self, date: datetime, reason: str):
        """Liquidate all positions"""
        for ticker, position in list(self.positions.items()):
            price = self._get_price(ticker, date) or position.current_price
            self._execute_trade(date, "SELL", ticker, position.shares, price, reason)

        if self.shv_position > 0:
            shv_price = self._get_price("SHV", date) or 110.0
            self.cash += self.shv_position * shv_price
            self.shv_position = 0

    def _allocate_to_regime(self, date: datetime, regime: str, capital: float, reason: str):
        """Allocate capital according to regime's allocation"""
        allocations = self.risk_profile.allocations.get(regime, {})

        for ticker, weight in allocations.items():
            if ticker in ["Bitcoin", "Ethereum"]:
                continue

            target_value = capital * weight
            price = self._get_price(ticker, date)

            if price and price > 0:
                shares = target_value / price
                if shares > 0.001:
                    self._execute_trade(date, "BUY", ticker, shares, price, reason)

    def _handle_risk_off_transition(self, date: datetime, new_regime: str):
        """Handle transition to Risk Off regime - move 25% to SHV"""
        portfolio_value = self._calculate_portfolio_value(date)
        self._liquidate_all(date, f"Regime change to {new_regime}")

        cash_reserve = portfolio_value * 0.25
        shv_price = self._get_price("SHV", date) or 110.0
        self.shv_position = cash_reserve / shv_price

        equity_capital = portfolio_value * 0.75
        self.cash = equity_capital
        self._allocate_to_regime(date, new_regime, equity_capital, f"Allocated to {new_regime}")

    def _handle_risk_on_transition(self, date: datetime, new_regime: str):
        """Handle transition to Risk On regime - return all cash"""
        portfolio_value = self._calculate_portfolio_value(date)
        self._liquidate_all(date, f"Regime change to {new_regime}")
        self._allocate_to_regime(date, new_regime, self.cash, f"Allocated to {new_regime}")

    def _check_xlk_vams_exit(self, row: GridDataRow, date: datetime) -> bool:
        """Check if XLK VAMS improved (exit cash rule for Deflation)"""
        xlk_vams = row.vams.get("XLK", -2)
        return xlk_vams >= 0 and self.shv_position > 0

    def run(self, config: BacktestConfig) -> BacktestResults:
        """Run the backtest"""
        print(f"Starting backtest: {config.name}")
        print(f"Period: {config.start_date.strftime('%Y-%m-%d')} to {config.end_date.strftime('%Y-%m-%d')}")

        # Build regime timeline for price simulation
        regime_timeline = self._build_regime_timeline()

        # Setup price provider
        tickers = self._get_required_tickers()
        tickers.append(config.benchmark_ticker)
        tickers = list(set(tickers))

        self.price_provider = PriceDataProvider(regime_timeline)
        self.price_provider.get_prices(
            tickers,
            config.start_date - timedelta(days=10),
            config.end_date + timedelta(days=5)
        )

        # Initialize
        self.cash = config.starting_value
        self.current_regime = "REFLATION"
        self.positions = {}
        self.shv_position = 0
        self.trades = []
        self.equity_curve = []

        # Filter dates to backtest period
        backtest_dates = [d for d in self.dates
                         if config.start_date <= d <= config.end_date]

        if not backtest_dates:
            raise ValueError("No data available for the specified date range")

        # Initial allocation
        first_date = backtest_dates[0]
        self._allocate_to_regime(
            first_date,
            self.current_regime,
            self.cash,
            "Initial allocation to REFLATION"
        )

        # Store initial benchmark price
        initial_benchmark_price = self._get_price(config.benchmark_ticker, first_date) or 100

        regime_periods = []
        current_regime_start = first_date

        # Main backtest loop
        for date in backtest_dates:
            row = self.grid_data.get(date)
            if not row:
                continue

            # Update position prices
            for ticker, position in self.positions.items():
                price = self._get_price(ticker, date)
                if price:
                    position.current_price = price

            # Check for regime change
            new_regime = self._check_regime_change(row)

            if new_regime:
                regime_periods.append({
                    "regime": self.current_regime,
                    "start": current_regime_start.isoformat(),
                    "end": date.isoformat()
                })
                current_regime_start = date

                old_risk_on = self._is_risk_on(self.current_regime)
                new_risk_on = self._is_risk_on(new_regime)

                print(f"{date.strftime('%Y-%m-%d')}: Regime change {self.current_regime} -> {new_regime}")

                if old_risk_on and not new_risk_on:
                    self._handle_risk_off_transition(date, new_regime)
                elif not old_risk_on and new_risk_on:
                    self._handle_risk_on_transition(date, new_regime)
                else:
                    portfolio_value = self._calculate_portfolio_value(date)
                    self._liquidate_all(date, f"Regime change to {new_regime}")
                    self._allocate_to_regime(date, new_regime, self.cash, f"Allocated to {new_regime}")

                self.current_regime = new_regime

            # Check XLK VAMS exit rule (only in Deflation)
            elif self.current_regime == "DEFLATION" and self._check_xlk_vams_exit(row, date):
                print(f"{date.strftime('%Y-%m-%d')}: XLK VAMS improved, returning cash to portfolio")
                if self.shv_position > 0:
                    shv_price = self._get_price("SHV", date) or 110.0
                    returned_cash = self.shv_position * shv_price
                    self.shv_position = 0

                    total_equity = sum(p.market_value for p in self.positions.values())
                    if total_equity > 0:
                        for ticker, position in list(self.positions.items()):
                            weight = position.market_value / total_equity
                            additional = returned_cash * weight
                            price = self._get_price(ticker, date)
                            if price:
                                shares = additional / price
                                self._execute_trade(date, "BUY", ticker, shares, price,
                                                    "XLK VAMS improved - reinvesting cash")

            # Record equity curve point
            portfolio_value = self._calculate_portfolio_value(date)
            shv_value = self.shv_position * (self._get_price("SHV", date) or 110.0)

            benchmark_price = self._get_price(config.benchmark_ticker, date) or initial_benchmark_price
            benchmark_value = config.starting_value * (benchmark_price / initial_benchmark_price)

            self.equity_curve.append(EquityCurvePoint(
                date=date,
                portfolio_value=portfolio_value,
                benchmark_value=benchmark_value,
                regime=self.current_regime,
                cash_value=shv_value
            ))

        # Final regime period
        regime_periods.append({
            "regime": self.current_regime,
            "start": current_regime_start.isoformat(),
            "end": backtest_dates[-1].isoformat()
        })

        return self._calculate_results(config, regime_periods)

    def _calculate_results(self, config: BacktestConfig,
                           regime_periods: List[dict]) -> BacktestResults:
        """Calculate all backtest metrics"""
        if not self.equity_curve:
            raise ValueError("No equity curve data")

        # Basic returns
        starting = config.starting_value
        ending = self.equity_curve[-1].portfolio_value
        total_return = (ending - starting) / starting

        days = len(self.equity_curve)
        years = days / 252

        annualized_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0

        # Benchmark returns
        benchmark_start = self.equity_curve[0].benchmark_value
        benchmark_end = self.equity_curve[-1].benchmark_value
        benchmark_total_return = (benchmark_end - benchmark_start) / benchmark_start
        benchmark_annualized = (1 + benchmark_total_return) ** (1 / years) - 1 if years > 0 else 0

        # Daily returns for risk metrics
        portfolio_values = [p.portfolio_value for p in self.equity_curve]
        benchmark_values = [p.benchmark_value for p in self.equity_curve]

        daily_returns = pd.Series(portfolio_values).pct_change().dropna()
        benchmark_daily = pd.Series(benchmark_values).pct_change().dropna()

        # Risk metrics
        std_dev = daily_returns.std() * np.sqrt(252) if len(daily_returns) > 0 else 0.15
        downside_returns = daily_returns[daily_returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0.10

        risk_free_rate = 0.02
        excess_return = annualized_return - risk_free_rate

        sharpe = excess_return / std_dev if std_dev > 0 else 0
        sortino = excess_return / downside_std if downside_std > 0 else 0

        # Max drawdown
        peak = portfolio_values[0]
        max_dd = 0
        drawdown_series = []

        for i, value in enumerate(portfolio_values):
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            max_dd = max(max_dd, dd)
            drawdown_series.append({
                "date": self.equity_curve[i].date.isoformat(),
                "drawdown": -dd
            })

        calmar = annualized_return / max_dd if max_dd > 0 else 0

        # Beta and Alpha
        if len(daily_returns) > 0 and len(benchmark_daily) > 0:
            min_len = min(len(daily_returns), len(benchmark_daily))
            aligned = pd.DataFrame({
                'port': daily_returns.values[:min_len],
                'bench': benchmark_daily.values[:min_len]
            }).dropna()
            if len(aligned) > 1:
                cov = aligned.cov().iloc[0, 1]
                var = aligned['bench'].var()
                beta = cov / var if var > 0 else 1
                alpha = annualized_return - (risk_free_rate + beta * (benchmark_annualized - risk_free_rate))
            else:
                beta, alpha = 1, 0
        else:
            beta, alpha = 1, 0

        # Information ratio
        min_len = min(len(daily_returns), len(benchmark_daily))
        if min_len > 0:
            tracking_diff = daily_returns.values[:min_len] - benchmark_daily.values[:min_len]
            tracking_error = np.std(tracking_diff) * np.sqrt(252) if len(tracking_diff) > 0 else 0.01
            info_ratio = (annualized_return - benchmark_annualized) / tracking_error if tracking_error > 0 else 0
        else:
            info_ratio = 0

        # Capture ratios
        min_len = min(len(daily_returns), len(benchmark_daily))
        port_aligned = daily_returns.values[:min_len]
        bench_aligned = benchmark_daily.values[:min_len]

        up_mask = bench_aligned > 0
        down_mask = bench_aligned < 0

        upside_capture = (np.mean(port_aligned[up_mask]) / np.mean(bench_aligned[up_mask]) * 100) if up_mask.sum() > 0 and np.mean(bench_aligned[up_mask]) != 0 else 100
        downside_capture = (np.mean(port_aligned[down_mask]) / np.mean(bench_aligned[down_mask]) * 100) if down_mask.sum() > 0 and np.mean(bench_aligned[down_mask]) != 0 else 100

        # Monthly returns
        monthly_returns = {}
        benchmark_monthly = {}
        dates = [p.date for p in self.equity_curve]

        month_start_val = portfolio_values[0]
        bench_month_start = benchmark_values[0]
        prev_month = dates[0].strftime('%Y-%m')

        for i, (d, val, bench) in enumerate(zip(dates, portfolio_values, benchmark_values)):
            year = str(d.year)
            month = d.strftime('%b')
            curr_month = d.strftime('%Y-%m')

            if curr_month != prev_month or i == len(dates) - 1:
                if year not in monthly_returns:
                    monthly_returns[year] = {}
                    benchmark_monthly[year] = {}

                prev_date = dates[i-1] if i > 0 else dates[0]
                prev_year = str(prev_date.year)
                prev_month_name = prev_date.strftime('%b')

                if prev_year not in monthly_returns:
                    monthly_returns[prev_year] = {}
                    benchmark_monthly[prev_year] = {}

                port_ret = (portfolio_values[i-1] - month_start_val) / month_start_val if month_start_val > 0 else 0
                bench_ret = (benchmark_values[i-1] - bench_month_start) / bench_month_start if bench_month_start > 0 else 0

                monthly_returns[prev_year][prev_month_name] = port_ret
                benchmark_monthly[prev_year][prev_month_name] = bench_ret

                month_start_val = val
                bench_month_start = bench
                prev_month = curr_month

        # Positive months
        all_monthly = [r for yr in monthly_returns.values() for r in yr.values()]
        positive_months = sum(1 for r in all_monthly if r > 0)
        positive_months_pct = positive_months / len(all_monthly) * 100 if all_monthly else 50

        # Trailing returns
        trailing = {}
        benchmark_trailing = {}

        if days >= 2:
            trailing['1D'] = (portfolio_values[-1] - portfolio_values[-2]) / portfolio_values[-2]
            benchmark_trailing['1D'] = (benchmark_values[-1] - benchmark_values[-2]) / benchmark_values[-2]

        for period, td in [('1W', 5), ('1M', 21), ('3M', 63), ('6M', 126), ('1Y', 252), ('3Y', 756), ('5Y', 1260)]:
            if days > td:
                trailing[period] = (portfolio_values[-1] - portfolio_values[-td-1]) / portfolio_values[-td-1]
                benchmark_trailing[period] = (benchmark_values[-1] - benchmark_values[-td-1]) / benchmark_values[-td-1]

        trailing['YTD'] = total_return
        benchmark_trailing['YTD'] = benchmark_total_return

        # Regime stats
        regime_stats = []
        regime_counts = defaultdict(lambda: {"days": 0, "trades": 0, "returns": []})

        for period in regime_periods:
            regime = period['regime']
            start = datetime.fromisoformat(period['start'])
            end = datetime.fromisoformat(period['end'])

            period_days = [p for p in self.equity_curve if start <= p.date <= end]
            if period_days:
                regime_counts[regime]['days'] += len(period_days)
                start_val = period_days[0].portfolio_value
                end_val = period_days[-1].portfolio_value
                if start_val > 0:
                    regime_counts[regime]['returns'].append((end_val - start_val) / start_val)

        for trade in self.trades:
            regime_counts[trade.regime]['trades'] += 1

        total_days = len(self.equity_curve)
        for regime, data in regime_counts.items():
            total_ret = sum(data['returns']) if data['returns'] else 0
            regime_stats.append(RegimeStat(
                regime=regime,
                days=data['days'],
                pct_time=data['days'] / total_days * 100 if total_days > 0 else 0,
                total_return=total_ret,
                num_trades=data['trades']
            ))

        # Final holdings
        final_holdings = []
        total_value = self._calculate_portfolio_value(self.equity_curve[-1].date)

        for ticker, pos in self.positions.items():
            final_holdings.append({
                "ticker": ticker,
                "shares": pos.shares,
                "price": pos.current_price,
                "value": pos.market_value,
                "weight": pos.market_value / total_value if total_value > 0 else 0,
                "return": pos.total_return
            })

        # Top drawdowns
        top_drawdowns = self._find_top_drawdowns(portfolio_values, dates)

        return BacktestResults(
            config=config,
            starting_value=starting,
            ending_value=ending,
            total_return=total_return,
            annualized_return=annualized_return,
            benchmark_total_return=benchmark_total_return,
            benchmark_annualized_return=benchmark_annualized,
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            std_dev_annualized=std_dev,
            beta=beta,
            alpha=alpha,
            information_ratio=info_ratio,
            upside_capture=upside_capture,
            downside_capture=downside_capture,
            positive_months_pct=positive_months_pct,
            equity_curve=self.equity_curve,
            drawdown_series=drawdown_series,
            monthly_returns=monthly_returns,
            benchmark_monthly_returns=benchmark_monthly,
            trailing_returns=trailing,
            benchmark_trailing_returns=benchmark_trailing,
            top_drawdowns=top_drawdowns,
            final_holdings=final_holdings,
            regime_stats=regime_stats,
            regime_timeline=regime_periods,
            trades=self.trades,
            total_trades=len(self.trades)
        )

    def _find_top_drawdowns(self, values: List[float], dates: List[datetime],
                            n: int = 5) -> List[DrawdownEvent]:
        """Find the top n drawdown events"""
        drawdowns = []
        peak = values[0]
        peak_idx = 0
        in_drawdown = False
        dd_start_idx = 0
        dd_low_idx = 0
        dd_low = float('inf')

        for i, val in enumerate(values):
            if val >= peak:
                if in_drawdown:
                    dd_pct = (peak - dd_low) / peak
                    drawdowns.append(DrawdownEvent(
                        drawdown_pct=dd_pct,
                        start_date=dates[dd_start_idx],
                        low_date=dates[dd_low_idx],
                        end_date=dates[i],
                        length_days=i - dd_start_idx,
                        recovery_days=i - dd_low_idx
                    ))
                    in_drawdown = False
                peak = val
                peak_idx = i
            else:
                if not in_drawdown:
                    in_drawdown = True
                    dd_start_idx = peak_idx
                    dd_low = val
                    dd_low_idx = i
                elif val < dd_low:
                    dd_low = val
                    dd_low_idx = i

        if in_drawdown:
            dd_pct = (peak - dd_low) / peak
            drawdowns.append(DrawdownEvent(
                drawdown_pct=dd_pct,
                start_date=dates[dd_start_idx],
                low_date=dates[dd_low_idx],
                end_date=None,
                length_days=len(values) - dd_start_idx,
                recovery_days=None
            ))

        drawdowns.sort(key=lambda x: x.drawdown_pct, reverse=True)
        return drawdowns[:n]


if __name__ == "__main__":
    from parser import parse_42macro_excel

    rows, tickers, summary = parse_42macro_excel(
        "/sessions/exciting-clever-lamport/mnt/uploads/Macro Regime Outlook (1).xlsx"
    )

    config = BacktestConfig(
        name="Test Backtest",
        risk_profile_id="aggressive",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2023, 12, 31),
        starting_value=100000,
        benchmark_ticker="SPY"
    )

    engine = BacktestEngine(rows, DEFAULT_PROFILES["aggressive"])
    results = engine.run(config)

    print(f"\n=== RESULTS ===")
    print(f"Starting Value: ${results.starting_value:,.2f}")
    print(f"Ending Value: ${results.ending_value:,.2f}")
    print(f"Total Return: {results.total_return:.2%}")
    print(f"Annualized Return: {results.annualized_return:.2%}")
    print(f"Benchmark Return: {results.benchmark_total_return:.2%}")
    print(f"Benchmark Ann. Return: {results.benchmark_annualized_return:.2%}")
    print(f"Sharpe Ratio: {results.sharpe_ratio:.2f}")
    print(f"Sortino Ratio: {results.sortino_ratio:.2f}")
    print(f"Max Drawdown: {results.max_drawdown:.2%}")
    print(f"Total Trades: {results.total_trades}")
    print(f"\n=== REGIME STATS ===")
    for rs in results.regime_stats:
        print(f"{rs.regime}: {rs.days} days ({rs.pct_time:.1f}%), Return: {rs.total_return:.2%}, Trades: {rs.num_trades}")
