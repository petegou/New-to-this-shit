"""
Seed data generator for the Rankings feature.
Creates realistic synthetic fund data for 80+ funds across multiple categories.
"""
import random
from datetime import date, datetime
from sqlalchemy.orm import Session

from ranking_models import FundCategory, Fund, FundScore
from ranking_calculator import RankingCalculator


# =====================================================================
# CATEGORIES
# =====================================================================

CATEGORIES = [
    # US Equity
    ("Large Cap Value", "US Equity", 1),
    ("Large Cap Blend", "US Equity", 2),
    ("Large Cap Growth", "US Equity", 3),
    ("Mid Cap Value", "US Equity", 4),
    ("Mid Cap Blend", "US Equity", 5),
    ("Mid Cap Growth", "US Equity", 6),
    ("Small Cap Value", "US Equity", 7),
    ("Small Cap Blend", "US Equity", 8),
    ("Small Cap Growth", "US Equity", 9),
    # International Equity
    ("Developed International - Large Cap", "International Equity", 10),
    ("Developed International - Small/Mid Cap", "International Equity", 11),
    ("Emerging Markets - Large Cap", "International Equity", 12),
    ("Emerging Markets - Small/Mid Cap", "International Equity", 13),
    ("Region Specific (Europe, Asia Pacific, Latin America)", "International Equity", 14),
    # Fixed Income
    ("US Aggregate Bond", "Fixed Income", 15),
    ("US Government Bond", "Fixed Income", 16),
    ("US Corporate Bond", "Fixed Income", 17),
    ("US High Yield Bond", "Fixed Income", 18),
    ("International Bond", "Fixed Income", 19),
    ("Emerging Markets Bond", "Fixed Income", 20),
    ("TIPS / Inflation Protected", "Fixed Income", 21),
    ("Short-Term Bond", "Fixed Income", 22),
    ("Municipal Bond", "Fixed Income", 23),
    # Alternatives
    ("Commodities", "Alternatives", 24),
    ("Real Estate / REITs", "Alternatives", 25),
    ("Multi-Alternative", "Alternatives", 26),
    # Other
    ("Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", "Other", 27),
    ("Crypto / Digital Assets", "Other", 28),
    ("Target Date", "Other", 29),
    ("Money Market", "Other", 30),
]


# =====================================================================
# FUND DEFINITIONS WITH REALISTIC DATA RANGES
# =====================================================================

def _r(low, high, decimals=4):
    """Generate a random float in range."""
    return round(random.uniform(low, high), decimals)


def _gen_equity_fund(ticker, name, fund_type, category_name, age, large_cap=True):
    """Generate realistic equity fund data."""
    has_5yr = age >= 5
    has_10yr = age >= 10

    beta_base = _r(0.75, 1.25) if large_cap else _r(0.85, 1.45)
    expense = _r(0.0003, 0.0075) if fund_type == "ETF" else _r(0.005, 0.015)
    mcap = _r(500, 2000) if large_cap else _r(100, 800)
    yld = _r(0.005, 0.035) if large_cap else _r(0.002, 0.025)
    turnover = _r(0.03, 0.95)
    pe = _r(14, 32)
    pb = _r(1.5, 5.5)

    fund = {
        "ticker": ticker,
        "name": name,
        "fund_type": fund_type,
        "category_name": category_name,
        "fund_age_years": age,
        "net_expense_ratio": expense,
        "turnover": turnover,
        "market_cap": mcap,
        "yield_pct": yld,
        "pe_ratio": pe,
        "pb_ratio": pb,
        "beta_3yr": beta_base,
        "r_squared_3yr": _r(80, 99),
        "up_capture_3yr": _r(0.85, 1.20),
        "down_capture_3yr": _r(0.80, 1.15),
        "sharpe_ratio_3yr": _r(0.2, 1.8),
        "tracking_error_3yr": _r(1.0, 8.0),
        "sortino_ratio_3yr": _r(0.3, 2.5),
        "treynor_ratio_3yr": _r(0.02, 0.15),
        "information_ratio_3yr": _r(-0.5, 1.0),
        "kurtosis_3yr": _r(-1.0, 3.0),
        "max_drawdown_3yr": _r(5, 30),
        "skewness_3yr": _r(-1.5, 0.5),
        "alpha_3yr": _r(-3, 5),
        "return_qtd": _r(-0.05, 0.08),
        "return_ytd": _r(-0.10, 0.20),
        "return_1yr": _r(-0.05, 0.30),
        "bm_return_1yr": _r(0.05, 0.18),
        "batting_avg_3yr": _r(0.25, 0.65),
        "data_as_of_date": date(2025, 1, 31),
    }

    if age >= 3:
        fund["return_3yr"] = _r(0.02, 0.18)
        fund["bm_return_3yr"] = _r(0.05, 0.14)

    if has_5yr:
        fund["beta_5yr"] = beta_base + _r(-0.15, 0.15)
        fund["r_squared_5yr"] = fund["r_squared_3yr"] + _r(-5, 5)
        fund["up_capture_5yr"] = fund["up_capture_3yr"] + _r(-0.05, 0.05)
        fund["down_capture_5yr"] = fund["down_capture_3yr"] + _r(-0.05, 0.05)
        fund["sharpe_ratio_5yr"] = fund["sharpe_ratio_3yr"] + _r(-0.3, 0.3)
        fund["tracking_error_5yr"] = fund["tracking_error_3yr"] + _r(-1, 1)
        fund["sortino_ratio_5yr"] = fund["sortino_ratio_3yr"] + _r(-0.3, 0.3)
        fund["treynor_ratio_5yr"] = fund["treynor_ratio_3yr"] + _r(-0.03, 0.03)
        fund["information_ratio_5yr"] = fund["information_ratio_3yr"] + _r(-0.2, 0.2)
        fund["kurtosis_5yr"] = fund["kurtosis_3yr"] + _r(-0.5, 0.5)
        fund["max_drawdown_5yr"] = fund["max_drawdown_3yr"] + _r(0, 10)
        fund["skewness_5yr"] = fund["skewness_3yr"] + _r(-0.3, 0.3)
        fund["alpha_5yr"] = fund["alpha_3yr"] + _r(-1.5, 1.5)
        fund["return_5yr"] = _r(0.04, 0.16)
        fund["bm_return_5yr"] = _r(0.06, 0.12)
        fund["batting_avg_5yr"] = _r(0.25, 0.60)

    if has_10yr:
        fund["return_10yr"] = _r(0.06, 0.14)
        fund["bm_return_10yr"] = _r(0.07, 0.11)

    return fund


def _gen_bond_fund(ticker, name, fund_type, category_name, age):
    """Generate realistic fixed income fund data."""
    has_5yr = age >= 5
    has_10yr = age >= 10

    beta_base = _r(0.0, 0.6)
    expense = _r(0.0003, 0.006) if fund_type == "ETF" else _r(0.003, 0.01)

    fund = {
        "ticker": ticker,
        "name": name,
        "fund_type": fund_type,
        "category_name": category_name,
        "fund_age_years": age,
        "net_expense_ratio": expense,
        "turnover": _r(0.10, 0.80),
        "market_cap": _r(50, 500),
        "yield_pct": _r(0.02, 0.065),
        "pe_ratio": None,
        "pb_ratio": None,
        "beta_3yr": beta_base,
        "r_squared_3yr": _r(40, 95),
        "up_capture_3yr": _r(0.10, 0.60),
        "down_capture_3yr": _r(0.05, 0.50),
        "sharpe_ratio_3yr": _r(-0.3, 1.2),
        "tracking_error_3yr": _r(0.5, 5.0),
        "sortino_ratio_3yr": _r(0.1, 1.5),
        "treynor_ratio_3yr": _r(0.01, 0.08),
        "information_ratio_3yr": _r(-0.3, 0.8),
        "kurtosis_3yr": _r(-0.5, 2.0),
        "max_drawdown_3yr": _r(2, 18),
        "skewness_3yr": _r(-1.0, 0.3),
        "alpha_3yr": _r(-2, 3),
        "return_qtd": _r(-0.03, 0.04),
        "return_ytd": _r(-0.05, 0.08),
        "return_1yr": _r(-0.02, 0.12),
        "bm_return_1yr": _r(0.01, 0.06),
        "batting_avg_3yr": _r(0.30, 0.55),
        "data_as_of_date": date(2025, 1, 31),
    }

    if age >= 3:
        fund["return_3yr"] = _r(-0.01, 0.06)
        fund["bm_return_3yr"] = _r(0.01, 0.04)

    if has_5yr:
        fund["beta_5yr"] = beta_base + _r(-0.1, 0.1)
        fund["r_squared_5yr"] = fund["r_squared_3yr"] + _r(-5, 5)
        fund["up_capture_5yr"] = fund["up_capture_3yr"] + _r(-0.05, 0.05)
        fund["down_capture_5yr"] = fund["down_capture_3yr"] + _r(-0.05, 0.05)
        fund["sharpe_ratio_5yr"] = fund["sharpe_ratio_3yr"] + _r(-0.2, 0.2)
        fund["tracking_error_5yr"] = fund["tracking_error_3yr"] + _r(-0.5, 0.5)
        fund["sortino_ratio_5yr"] = fund["sortino_ratio_3yr"] + _r(-0.2, 0.2)
        fund["treynor_ratio_5yr"] = fund["treynor_ratio_3yr"] + _r(-0.02, 0.02)
        fund["information_ratio_5yr"] = fund["information_ratio_3yr"] + _r(-0.15, 0.15)
        fund["kurtosis_5yr"] = fund["kurtosis_3yr"] + _r(-0.3, 0.3)
        fund["max_drawdown_5yr"] = fund["max_drawdown_3yr"] + _r(0, 5)
        fund["skewness_5yr"] = fund["skewness_3yr"] + _r(-0.2, 0.2)
        fund["alpha_5yr"] = fund["alpha_3yr"] + _r(-1, 1)
        fund["return_5yr"] = _r(0.00, 0.05)
        fund["bm_return_5yr"] = _r(0.01, 0.03)
        fund["batting_avg_5yr"] = _r(0.30, 0.55)

    if has_10yr:
        fund["return_10yr"] = _r(0.01, 0.04)
        fund["bm_return_10yr"] = _r(0.015, 0.035)

    return fund


# =====================================================================
# ALL FUNDS TO SEED
# =====================================================================

def get_all_seed_funds():
    """Return list of all seed fund dicts."""
    random.seed(42)  # Reproducible data

    funds = []

    # --- Large Cap Blend ETFs ---
    funds.append(_gen_equity_fund("SPY", "SPDR S&P 500 ETF Trust", "ETF", "Large Cap Blend", 30))
    funds.append(_gen_equity_fund("IVV", "iShares Core S&P 500 ETF", "ETF", "Large Cap Blend", 24))
    funds.append(_gen_equity_fund("VOO", "Vanguard S&P 500 ETF", "ETF", "Large Cap Blend", 14))
    funds.append(_gen_equity_fund("QUAL", "iShares MSCI USA Quality Factor ETF", "ETF", "Large Cap Blend", 11))
    funds.append(_gen_equity_fund("MTUM", "iShares MSCI USA Momentum Factor ETF", "ETF", "Large Cap Blend", 11))
    funds.append(_gen_equity_fund("SPLV", "Invesco S&P 500 Low Volatility ETF", "ETF", "Large Cap Blend", 13))

    # --- Large Cap Growth ETFs ---
    funds.append(_gen_equity_fund("QQQ", "Invesco QQQ Trust", "ETF", "Large Cap Growth", 25))
    funds.append(_gen_equity_fund("VUG", "Vanguard Growth ETF", "ETF", "Large Cap Growth", 20))
    funds.append(_gen_equity_fund("IWF", "iShares Russell 1000 Growth ETF", "ETF", "Large Cap Growth", 24))
    funds.append(_gen_equity_fund("SCHG", "Schwab U.S. Large-Cap Growth ETF", "ETF", "Large Cap Growth", 14))
    funds.append(_gen_equity_fund("MGK", "Vanguard Mega Cap Growth ETF", "ETF", "Large Cap Growth", 17))

    # --- Large Cap Value ETFs ---
    funds.append(_gen_equity_fund("VTV", "Vanguard Value ETF", "ETF", "Large Cap Value", 20))
    funds.append(_gen_equity_fund("IWD", "iShares Russell 1000 Value ETF", "ETF", "Large Cap Value", 24))
    funds.append(_gen_equity_fund("SCHV", "Schwab U.S. Large-Cap Value ETF", "ETF", "Large Cap Value", 14))
    funds.append(_gen_equity_fund("DVY", "iShares Select Dividend ETF", "ETF", "Large Cap Value", 18))
    funds.append(_gen_equity_fund("VYM", "Vanguard High Dividend Yield ETF", "ETF", "Large Cap Value", 18))

    # --- Mid Cap ETFs ---
    funds.append(_gen_equity_fund("IWR", "iShares Russell Mid-Cap ETF", "ETF", "Mid Cap Blend", 23, large_cap=False))
    funds.append(_gen_equity_fund("MDY", "SPDR S&P MidCap 400 ETF Trust", "ETF", "Mid Cap Blend", 28, large_cap=False))
    funds.append(_gen_equity_fund("VO", "Vanguard Mid-Cap ETF", "ETF", "Mid Cap Blend", 20, large_cap=False))
    funds.append(_gen_equity_fund("IJH", "iShares Core S&P Mid-Cap ETF", "ETF", "Mid Cap Blend", 24, large_cap=False))
    funds.append(_gen_equity_fund("IWP", "iShares Russell Mid-Cap Growth ETF", "ETF", "Mid Cap Growth", 23, large_cap=False))
    funds.append(_gen_equity_fund("IWS", "iShares Russell Mid-Cap Value ETF", "ETF", "Mid Cap Value", 23, large_cap=False))

    # --- Small Cap ETFs ---
    funds.append(_gen_equity_fund("IWM", "iShares Russell 2000 ETF", "ETF", "Small Cap Blend", 24, large_cap=False))
    funds.append(_gen_equity_fund("IJR", "iShares Core S&P Small-Cap ETF", "ETF", "Small Cap Blend", 24, large_cap=False))
    funds.append(_gen_equity_fund("VB", "Vanguard Small-Cap ETF", "ETF", "Small Cap Blend", 20, large_cap=False))
    funds.append(_gen_equity_fund("IWN", "iShares Russell 2000 Value ETF", "ETF", "Small Cap Value", 24, large_cap=False))
    funds.append(_gen_equity_fund("IWO", "iShares Russell 2000 Growth ETF", "ETF", "Small Cap Growth", 24, large_cap=False))
    funds.append(_gen_equity_fund("SCHA", "Schwab U.S. Small-Cap ETF", "ETF", "Small Cap Blend", 14, large_cap=False))

    # --- International ETFs ---
    funds.append(_gen_equity_fund("EFA", "iShares MSCI EAFE ETF", "ETF", "Developed International - Large Cap", 22))
    funds.append(_gen_equity_fund("VEA", "Vanguard FTSE Developed Markets ETF", "ETF", "Developed International - Large Cap", 17))
    funds.append(_gen_equity_fund("IEFA", "iShares Core MSCI EAFE ETF", "ETF", "Developed International - Large Cap", 12))
    funds.append(_gen_equity_fund("SCZ", "iShares MSCI EAFE Small-Cap ETF", "ETF", "Developed International - Small/Mid Cap", 17, large_cap=False))
    funds.append(_gen_equity_fund("VSS", "Vanguard FTSE All-World ex-US Small-Cap ETF", "ETF", "Developed International - Small/Mid Cap", 16, large_cap=False))
    funds.append(_gen_equity_fund("EEM", "iShares MSCI Emerging Markets ETF", "ETF", "Emerging Markets - Large Cap", 22, large_cap=False))
    funds.append(_gen_equity_fund("VWO", "Vanguard FTSE Emerging Markets ETF", "ETF", "Emerging Markets - Large Cap", 19, large_cap=False))
    funds.append(_gen_equity_fund("IEMG", "iShares Core MSCI Emerging Markets ETF", "ETF", "Emerging Markets - Large Cap", 12, large_cap=False))
    funds.append(_gen_equity_fund("ACWX", "iShares MSCI ACWI ex U.S. ETF", "ETF", "Developed International - Large Cap", 17))
    funds.append(_gen_equity_fund("EWZ", "iShares MSCI Brazil ETF", "ETF", "Region Specific (Europe, Asia Pacific, Latin America)", 24, large_cap=False))
    funds.append(_gen_equity_fund("EWJ", "iShares MSCI Japan ETF", "ETF", "Region Specific (Europe, Asia Pacific, Latin America)", 28))
    funds.append(_gen_equity_fund("EWG", "iShares MSCI Germany ETF", "ETF", "Region Specific (Europe, Asia Pacific, Latin America)", 28))
    funds.append(_gen_equity_fund("FXI", "iShares China Large-Cap ETF", "ETF", "Emerging Markets - Large Cap", 20, large_cap=False))

    # --- Fixed Income ETFs ---
    funds.append(_gen_bond_fund("AGG", "iShares Core U.S. Aggregate Bond ETF", "ETF", "US Aggregate Bond", 22))
    funds.append(_gen_bond_fund("BND", "Vanguard Total Bond Market ETF", "ETF", "US Aggregate Bond", 17))
    funds.append(_gen_bond_fund("TLT", "iShares 20+ Year Treasury Bond ETF", "ETF", "US Government Bond", 22))
    funds.append(_gen_bond_fund("IEF", "iShares 7-10 Year Treasury Bond ETF", "ETF", "US Government Bond", 22))
    funds.append(_gen_bond_fund("SHY", "iShares 1-3 Year Treasury Bond ETF", "ETF", "Short-Term Bond", 22))
    funds.append(_gen_bond_fund("LQD", "iShares iBoxx Investment Grade Corporate Bond ETF", "ETF", "US Corporate Bond", 22))
    funds.append(_gen_bond_fund("HYG", "iShares iBoxx High Yield Corporate Bond ETF", "ETF", "US High Yield Bond", 17))
    funds.append(_gen_bond_fund("TIP", "iShares TIPS Bond ETF", "ETF", "TIPS / Inflation Protected", 22))
    funds.append(_gen_bond_fund("MUB", "iShares National Muni Bond ETF", "ETF", "Municipal Bond", 17))
    funds.append(_gen_bond_fund("EMB", "iShares JP Morgan USD Emerging Markets Bond ETF", "ETF", "Emerging Markets Bond", 17))
    funds.append(_gen_bond_fund("BNDX", "Vanguard Total International Bond ETF", "ETF", "International Bond", 11))
    funds.append(_gen_bond_fund("BSV", "Vanguard Short-Term Bond ETF", "ETF", "Short-Term Bond", 17))
    funds.append(_gen_bond_fund("VCSH", "Vanguard Short-Term Corporate Bond ETF", "ETF", "US Corporate Bond", 14))

    # --- Sector ETFs ---
    funds.append(_gen_equity_fund("XLK", "Technology Select Sector SPDR Fund", "ETF", "Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", 26))
    funds.append(_gen_equity_fund("XLF", "Financial Select Sector SPDR Fund", "ETF", "Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", 26))
    funds.append(_gen_equity_fund("XLE", "Energy Select Sector SPDR Fund", "ETF", "Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", 26))
    funds.append(_gen_equity_fund("XLV", "Health Care Select Sector SPDR Fund", "ETF", "Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", 26))
    funds.append(_gen_equity_fund("XLU", "Utilities Select Sector SPDR Fund", "ETF", "Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", 26))
    funds.append(_gen_equity_fund("XLI", "Industrial Select Sector SPDR Fund", "ETF", "Sector Funds (Technology, Healthcare, Energy, Financials, etc.)", 26))
    funds.append(_gen_equity_fund("XLRE", "Real Estate Select Sector SPDR Fund", "ETF", "Real Estate / REITs", 9))
    funds.append(_gen_equity_fund("VNQ", "Vanguard Real Estate ETF", "ETF", "Real Estate / REITs", 20))

    # --- Commodity ETFs ---
    funds.append(_gen_bond_fund("GLD", "SPDR Gold Shares", "ETF", "Commodities", 20))
    funds.append(_gen_bond_fund("IAU", "iShares Gold Trust", "ETF", "Commodities", 19))
    funds.append(_gen_bond_fund("DBC", "Invesco DB Commodity Index Tracking Fund", "ETF", "Commodities", 18))

    # --- Mutual Funds ---
    funds.append(_gen_equity_fund("VFIAX", "Vanguard 500 Index Fund Admiral", "Mutual Fund", "Large Cap Blend", 24))
    funds.append(_gen_equity_fund("FXAIX", "Fidelity 500 Index Fund", "Mutual Fund", "Large Cap Blend", 35))
    funds.append(_gen_equity_fund("VIGAX", "Vanguard Growth Index Fund Admiral", "Mutual Fund", "Large Cap Growth", 24))
    funds.append(_gen_equity_fund("VVIAX", "Vanguard Value Index Fund Admiral", "Mutual Fund", "Large Cap Value", 24))
    funds.append(_gen_equity_fund("VEXAX", "Vanguard Extended Market Index Fund Admiral", "Mutual Fund", "Mid Cap Blend", 24, large_cap=False))
    funds.append(_gen_equity_fund("VTIAX", "Vanguard Total International Stock Index Fund Admiral", "Mutual Fund", "Developed International - Large Cap", 14))
    funds.append(_gen_bond_fund("VBTLX", "Vanguard Total Bond Market Index Fund Admiral", "Mutual Fund", "US Aggregate Bond", 24))
    funds.append(_gen_bond_fund("PTTAX", "PIMCO Total Return Fund A", "Mutual Fund", "US Aggregate Bond", 35))
    funds.append(_gen_equity_fund("FCNTX", "Fidelity Contrafund", "Mutual Fund", "Large Cap Growth", 35))
    funds.append(_gen_equity_fund("DODGX", "Dodge & Cox Stock Fund", "Mutual Fund", "Large Cap Value", 35))

    return funds


# =====================================================================
# SEEDING FUNCTIONS
# =====================================================================

def seed_categories(db: Session):
    """Insert all fund categories. Returns dict mapping name -> id."""
    existing = db.query(FundCategory).count()
    if existing > 0:
        # Return existing mapping
        cats = db.query(FundCategory).all()
        return {c.name: c.id for c in cats}

    category_map = {}
    for name, parent, order in CATEGORIES:
        cat = FundCategory(name=name, parent_category=parent, display_order=order)
        db.add(cat)
        db.flush()
        category_map[name] = cat.id

    db.commit()
    return category_map


def seed_funds(db: Session):
    """Seed the database with sample fund data and calculate all scores."""
    # First seed categories
    category_map = seed_categories(db)

    # Check if funds already exist
    existing_count = db.query(Fund).count()
    if existing_count > 0:
        print(f"Database already has {existing_count} funds. Skipping seed.")
        return

    # Get all seed fund data
    all_funds = get_all_seed_funds()
    calculator = RankingCalculator()

    fund_records = []
    for fund_data in all_funds:
        cat_name = fund_data.pop("category_name", None)
        cat_id = category_map.get(cat_name)

        # Create the Fund record
        fund = Fund(
            ticker=fund_data["ticker"],
            name=fund_data["name"],
            fund_type=fund_data["fund_type"],
            category_id=cat_id,
            fund_age_years=fund_data.get("fund_age_years"),
            net_expense_ratio=fund_data.get("net_expense_ratio"),
            turnover=fund_data.get("turnover"),
            market_cap=fund_data.get("market_cap"),
            yield_pct=fund_data.get("yield_pct"),
            pe_ratio=fund_data.get("pe_ratio"),
            pb_ratio=fund_data.get("pb_ratio"),
            beta_3yr=fund_data.get("beta_3yr"),
            r_squared_3yr=fund_data.get("r_squared_3yr"),
            up_capture_3yr=fund_data.get("up_capture_3yr"),
            down_capture_3yr=fund_data.get("down_capture_3yr"),
            sharpe_ratio_3yr=fund_data.get("sharpe_ratio_3yr"),
            tracking_error_3yr=fund_data.get("tracking_error_3yr"),
            sortino_ratio_3yr=fund_data.get("sortino_ratio_3yr"),
            treynor_ratio_3yr=fund_data.get("treynor_ratio_3yr"),
            information_ratio_3yr=fund_data.get("information_ratio_3yr"),
            kurtosis_3yr=fund_data.get("kurtosis_3yr"),
            max_drawdown_3yr=fund_data.get("max_drawdown_3yr"),
            skewness_3yr=fund_data.get("skewness_3yr"),
            alpha_3yr=fund_data.get("alpha_3yr"),
            beta_5yr=fund_data.get("beta_5yr"),
            r_squared_5yr=fund_data.get("r_squared_5yr"),
            up_capture_5yr=fund_data.get("up_capture_5yr"),
            down_capture_5yr=fund_data.get("down_capture_5yr"),
            sharpe_ratio_5yr=fund_data.get("sharpe_ratio_5yr"),
            tracking_error_5yr=fund_data.get("tracking_error_5yr"),
            sortino_ratio_5yr=fund_data.get("sortino_ratio_5yr"),
            treynor_ratio_5yr=fund_data.get("treynor_ratio_5yr"),
            information_ratio_5yr=fund_data.get("information_ratio_5yr"),
            kurtosis_5yr=fund_data.get("kurtosis_5yr"),
            max_drawdown_5yr=fund_data.get("max_drawdown_5yr"),
            skewness_5yr=fund_data.get("skewness_5yr"),
            alpha_5yr=fund_data.get("alpha_5yr"),
            return_qtd=fund_data.get("return_qtd"),
            return_ytd=fund_data.get("return_ytd"),
            return_1yr=fund_data.get("return_1yr"),
            return_3yr=fund_data.get("return_3yr"),
            return_5yr=fund_data.get("return_5yr"),
            return_10yr=fund_data.get("return_10yr"),
            bm_return_1yr=fund_data.get("bm_return_1yr"),
            bm_return_3yr=fund_data.get("bm_return_3yr"),
            bm_return_5yr=fund_data.get("bm_return_5yr"),
            bm_return_10yr=fund_data.get("bm_return_10yr"),
            batting_avg_3yr=fund_data.get("batting_avg_3yr"),
            batting_avg_5yr=fund_data.get("batting_avg_5yr"),
            data_as_of_date=fund_data.get("data_as_of_date"),
        )
        db.add(fund)
        db.flush()

        # Prepare data dict for calculator (include parent_category)
        cat = db.query(FundCategory).filter(FundCategory.id == cat_id).first()
        calc_data = dict(fund_data)
        calc_data["parent_category"] = cat.parent_category if cat else None
        calc_data["category_id"] = cat_id

        # Calculate scores
        scores = calculator.calculate_all_scores(calc_data)
        scores["fund_id"] = fund.id
        scores["category_id"] = cat_id  # For ranking later

        fund_records.append(scores)

    # Rank all funds
    calculator.rank_funds(fund_records)

    # Save scores
    for score_data in fund_records:
        fund_id = score_data.pop("fund_id")
        cat_id = score_data.pop("category_id", None)
        score = FundScore(fund_id=fund_id, **score_data)
        db.add(score)

    db.commit()
    print(f"Seeded {len(all_funds)} funds with scores and rankings.")
