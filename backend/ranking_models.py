"""
SQLAlchemy models for the Rankings feature.
Tables: fund_categories, funds, fund_scores
"""
from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from database import Base


class FundCategory(Base):
    __tablename__ = "fund_categories"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    parent_category = Column(String(50))
    display_order = Column(Integer)

    funds = relationship("Fund", back_populates="category")


class Fund(Base):
    __tablename__ = "funds"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, unique=True, index=True)
    name = Column(String(255))
    fund_type = Column(String(20))
    category_id = Column(Integer, ForeignKey("fund_categories.id"))

    # Age / availability
    inception_date = Column(Date, nullable=True)
    fund_age_years = Column(Numeric(5, 2))

    # Fund characteristics
    net_expense_ratio = Column(Numeric(6, 4))
    turnover = Column(Numeric(6, 4))
    market_cap = Column(Numeric(12, 2))
    yield_pct = Column(Numeric(6, 4))
    pe_ratio = Column(Numeric(8, 2))
    pb_ratio = Column(Numeric(8, 2))

    # 3-Year Risk Metrics
    beta_3yr = Column(Numeric(8, 4))
    r_squared_3yr = Column(Numeric(8, 4))
    up_capture_3yr = Column(Numeric(8, 4))
    down_capture_3yr = Column(Numeric(8, 4))
    sharpe_ratio_3yr = Column(Numeric(8, 4))
    tracking_error_3yr = Column(Numeric(8, 4))
    sortino_ratio_3yr = Column(Numeric(8, 4))
    treynor_ratio_3yr = Column(Numeric(8, 4))
    information_ratio_3yr = Column(Numeric(8, 4))
    kurtosis_3yr = Column(Numeric(8, 4))
    max_drawdown_3yr = Column(Numeric(8, 4))
    skewness_3yr = Column(Numeric(8, 4))
    alpha_3yr = Column(Numeric(8, 4))

    # 5-Year Risk Metrics
    beta_5yr = Column(Numeric(8, 4))
    r_squared_5yr = Column(Numeric(8, 4))
    up_capture_5yr = Column(Numeric(8, 4))
    down_capture_5yr = Column(Numeric(8, 4))
    sharpe_ratio_5yr = Column(Numeric(8, 4))
    tracking_error_5yr = Column(Numeric(8, 4))
    sortino_ratio_5yr = Column(Numeric(8, 4))
    treynor_ratio_5yr = Column(Numeric(8, 4))
    information_ratio_5yr = Column(Numeric(8, 4))
    kurtosis_5yr = Column(Numeric(8, 4))
    max_drawdown_5yr = Column(Numeric(8, 4))
    skewness_5yr = Column(Numeric(8, 4))
    alpha_5yr = Column(Numeric(8, 4))

    # Return Data
    return_qtd = Column(Numeric(8, 4))
    return_ytd = Column(Numeric(8, 4))
    return_1yr = Column(Numeric(8, 4))
    return_3yr = Column(Numeric(8, 4))
    return_5yr = Column(Numeric(8, 4))
    return_10yr = Column(Numeric(8, 4))

    # Benchmark Return Data
    bm_return_1yr = Column(Numeric(8, 4))
    bm_return_3yr = Column(Numeric(8, 4))
    bm_return_5yr = Column(Numeric(8, 4))
    bm_return_10yr = Column(Numeric(8, 4))

    # Batting Average
    batting_avg_3yr = Column(Numeric(6, 4))
    batting_avg_5yr = Column(Numeric(6, 4))

    # Metadata
    data_as_of_date = Column(Date, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships
    category = relationship("FundCategory", back_populates="funds")
    scores = relationship("FundScore", back_populates="fund", uselist=False,
                          cascade="all, delete-orphan")


class FundScore(Base):
    __tablename__ = "fund_scores"

    id = Column(Integer, primary_key=True, index=True)
    fund_id = Column(Integer, ForeignKey("funds.id", ondelete="CASCADE"), unique=True)

    # Individual Risk Sub-Scores
    beta_score = Column(Numeric(10, 4))
    r_squared_score = Column(Numeric(10, 4))
    up_capture_score = Column(Numeric(10, 4))
    down_capture_score = Column(Numeric(10, 4))
    sharpe_score = Column(Numeric(10, 4))
    tracking_error_score = Column(Numeric(10, 4))
    sortino_score = Column(Numeric(10, 4))
    treynor_score = Column(Numeric(10, 4))
    info_ratio_score = Column(Numeric(10, 4))
    kurtosis_score = Column(Numeric(10, 4))
    drawdown_score = Column(Numeric(10, 4))
    skewness_score = Column(Numeric(10, 4))

    # Aggregated Risk Score
    risk_score = Column(Numeric(10, 4))

    # Individual Return Sub-Scores
    alpha_score = Column(Numeric(10, 4))
    yield_score = Column(Numeric(10, 4))
    relative_return_score = Column(Numeric(10, 4))
    price_score = Column(Numeric(10, 4))
    fee_score = Column(Numeric(10, 4))

    # Aggregated Return Score
    return_score = Column(Numeric(10, 4))

    # Total RR Score
    total_rr_score = Column(Numeric(10, 4))

    # Adjustment Scores
    market_cap_score = Column(Numeric(10, 4))
    turnover_score = Column(Numeric(10, 4))

    # Final GPA Score
    total_gpa_score = Column(Numeric(10, 4))

    # Rankings
    category_rank = Column(Integer)
    global_rank = Column(Integer)

    calculated_at = Column(DateTime, server_default=func.now())

    # Relationship
    fund = relationship("Fund", back_populates="scores")
