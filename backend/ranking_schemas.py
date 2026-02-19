"""
Pydantic schemas for the Rankings API.
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime


# ---------- Category Schemas ----------

class CategoryOut(BaseModel):
    id: int
    name: str
    parent_category: Optional[str] = None
    display_order: Optional[int] = None
    fund_count: int = 0

    class Config:
        from_attributes = True


# ---------- Fund Schemas ----------

class FundBase(BaseModel):
    ticker: str
    name: Optional[str] = None
    fund_type: Optional[str] = None
    category_id: Optional[int] = None

    inception_date: Optional[date] = None
    fund_age_years: Optional[float] = None

    net_expense_ratio: Optional[float] = None
    turnover: Optional[float] = None
    market_cap: Optional[float] = None
    yield_pct: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None

    beta_3yr: Optional[float] = None
    r_squared_3yr: Optional[float] = None
    up_capture_3yr: Optional[float] = None
    down_capture_3yr: Optional[float] = None
    sharpe_ratio_3yr: Optional[float] = None
    tracking_error_3yr: Optional[float] = None
    sortino_ratio_3yr: Optional[float] = None
    treynor_ratio_3yr: Optional[float] = None
    information_ratio_3yr: Optional[float] = None
    kurtosis_3yr: Optional[float] = None
    max_drawdown_3yr: Optional[float] = None
    skewness_3yr: Optional[float] = None
    alpha_3yr: Optional[float] = None

    beta_5yr: Optional[float] = None
    r_squared_5yr: Optional[float] = None
    up_capture_5yr: Optional[float] = None
    down_capture_5yr: Optional[float] = None
    sharpe_ratio_5yr: Optional[float] = None
    tracking_error_5yr: Optional[float] = None
    sortino_ratio_5yr: Optional[float] = None
    treynor_ratio_5yr: Optional[float] = None
    information_ratio_5yr: Optional[float] = None
    kurtosis_5yr: Optional[float] = None
    max_drawdown_5yr: Optional[float] = None
    skewness_5yr: Optional[float] = None
    alpha_5yr: Optional[float] = None

    return_qtd: Optional[float] = None
    return_ytd: Optional[float] = None
    return_1yr: Optional[float] = None
    return_3yr: Optional[float] = None
    return_5yr: Optional[float] = None
    return_10yr: Optional[float] = None

    bm_return_1yr: Optional[float] = None
    bm_return_3yr: Optional[float] = None
    bm_return_5yr: Optional[float] = None
    bm_return_10yr: Optional[float] = None

    batting_avg_3yr: Optional[float] = None
    batting_avg_5yr: Optional[float] = None

    data_as_of_date: Optional[date] = None


class FundCreate(FundBase):
    pass


class FundUpdate(FundBase):
    pass


class ScoreOut(BaseModel):
    beta_score: Optional[float] = None
    r_squared_score: Optional[float] = None
    up_capture_score: Optional[float] = None
    down_capture_score: Optional[float] = None
    sharpe_score: Optional[float] = None
    tracking_error_score: Optional[float] = None
    sortino_score: Optional[float] = None
    treynor_score: Optional[float] = None
    info_ratio_score: Optional[float] = None
    kurtosis_score: Optional[float] = None
    drawdown_score: Optional[float] = None
    skewness_score: Optional[float] = None
    risk_score: Optional[float] = None

    alpha_score: Optional[float] = None
    yield_score: Optional[float] = None
    relative_return_score: Optional[float] = None
    price_score: Optional[float] = None
    fee_score: Optional[float] = None
    return_score: Optional[float] = None

    total_rr_score: Optional[float] = None

    market_cap_score: Optional[float] = None
    turnover_score: Optional[float] = None

    total_gpa_score: Optional[float] = None

    category_rank: Optional[int] = None
    global_rank: Optional[int] = None

    class Config:
        from_attributes = True


class FundOut(BaseModel):
    id: int
    ticker: str
    name: Optional[str] = None
    fund_type: Optional[str] = None
    category_id: Optional[int] = None
    category_name: Optional[str] = None
    parent_category: Optional[str] = None
    fund_age_years: Optional[float] = None
    net_expense_ratio: Optional[float] = None
    turnover: Optional[float] = None
    market_cap: Optional[float] = None
    yield_pct: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    data_as_of_date: Optional[date] = None

    class Config:
        from_attributes = True


class FundWithScores(FundOut):
    scores: Optional[ScoreOut] = None


class FundDetailOut(FundBase):
    id: int
    category_name: Optional[str] = None
    parent_category: Optional[str] = None
    scores: Optional[ScoreOut] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ---------- Rankings Schemas ----------

class RankedFundOut(BaseModel):
    rank: int
    ticker: str
    name: Optional[str] = None
    fund_type: Optional[str] = None
    category_name: Optional[str] = None
    parent_category: Optional[str] = None
    category_id: Optional[int] = None
    total_gpa_score: Optional[float] = None
    risk_score: Optional[float] = None
    return_score: Optional[float] = None
    total_rr_score: Optional[float] = None
    category_rank: Optional[int] = None
    global_rank: Optional[int] = None

    # All sub-scores for detail view
    scores: Optional[ScoreOut] = None

    # Fund characteristics for detail view
    fund_age_years: Optional[float] = None
    net_expense_ratio: Optional[float] = None
    turnover: Optional[float] = None
    market_cap: Optional[float] = None
    yield_pct: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None


class RankingsResponse(BaseModel):
    funds: List[RankedFundOut]
    total: int
    page: int
    limit: int
    category: Optional[str] = None
    data_as_of: Optional[date] = None


class CategorySummaryOut(BaseModel):
    category_name: str
    fund_count: int
    avg_gpa: Optional[float] = None
    highest_gpa: Optional[float] = None
    highest_gpa_ticker: Optional[str] = None
    lowest_gpa: Optional[float] = None
    avg_expense: Optional[float] = None
    avg_risk_score: Optional[float] = None


class UploadSummary(BaseModel):
    added: int
    updated: int
    errors: int
    error_details: List[str] = []


class RecalculateResponse(BaseModel):
    success: bool
    funds_calculated: int
    message: str
