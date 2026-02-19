"""
FastAPI Backend for PGRB Portfolio Backtesting
"""
import os
import sys
import json
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser import parse_42macro_excel, get_data_preview
from engine import BacktestEngine
from models import (
    GridDataRow, RiskProfile, BacktestConfig, BacktestResults,
    DEFAULT_PROFILES, REGIME_COLORS
)

# Rankings feature imports
try:
    from database import init_db, SessionLocal
    from rankings_router import router as rankings_router
    from seed_fund_data import seed_funds, seed_categories
    RANKINGS_AVAILABLE = True
except Exception as e:
    print(f"Rankings feature not available: {e}")
    RANKINGS_AVAILABLE = False

app = FastAPI(title="PGRB Portfolio Backtesting API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage (would be database in production)
grid_data: List[GridDataRow] = []
data_summary: Dict = {}
backtests: Dict[str, Dict] = {}
risk_profiles: Dict[str, RiskProfile] = dict(DEFAULT_PROFILES)

# Models for API
class BacktestRequest(BaseModel):
    name: str
    risk_profile_id: str
    start_date: str  # YYYY-MM-DD
    end_date: str
    starting_value: float = 100000.0
    benchmark_ticker: str = "SPY"

class RiskProfileRequest(BaseModel):
    name: str
    allocations: Dict[str, Dict[str, float]]

# Helper functions
def results_to_dict(results: BacktestResults) -> Dict:
    """Convert BacktestResults to JSON-serializable dict"""
    return {
        "config": {
            "name": results.config.name,
            "risk_profile_id": results.config.risk_profile_id,
            "start_date": results.config.start_date.isoformat(),
            "end_date": results.config.end_date.isoformat(),
            "starting_value": results.config.starting_value,
            "benchmark_ticker": results.config.benchmark_ticker,
        },
        "summary": {
            "starting_value": results.starting_value,
            "ending_value": results.ending_value,
            "total_return": results.total_return,
            "annualized_return": results.annualized_return,
            "benchmark_total_return": results.benchmark_total_return,
            "benchmark_annualized_return": results.benchmark_annualized_return,
        },
        "risk_metrics": {
            "max_drawdown": results.max_drawdown,
            "sharpe_ratio": results.sharpe_ratio,
            "sortino_ratio": results.sortino_ratio,
            "calmar_ratio": results.calmar_ratio,
            "std_dev_annualized": results.std_dev_annualized,
            "beta": results.beta,
            "alpha": results.alpha,
            "information_ratio": results.information_ratio,
            "upside_capture": results.upside_capture,
            "downside_capture": results.downside_capture,
            "positive_months_pct": results.positive_months_pct,
        },
        "equity_curve": [
            {
                "date": p.date.isoformat(),
                "portfolio_value": p.portfolio_value,
                "benchmark_value": p.benchmark_value,
                "regime": p.regime,
                "cash_value": p.cash_value
            }
            for p in results.equity_curve
        ],
        "drawdown_series": results.drawdown_series,
        "monthly_returns": results.monthly_returns,
        "benchmark_monthly_returns": results.benchmark_monthly_returns,
        "trailing_returns": results.trailing_returns,
        "benchmark_trailing_returns": results.benchmark_trailing_returns,
        "top_drawdowns": [
            {
                "drawdown_pct": dd.drawdown_pct,
                "start_date": dd.start_date.isoformat(),
                "low_date": dd.low_date.isoformat(),
                "end_date": dd.end_date.isoformat() if dd.end_date else None,
                "length_days": dd.length_days,
                "recovery_days": dd.recovery_days
            }
            for dd in results.top_drawdowns
        ],
        "final_holdings": results.final_holdings,
        "regime_stats": [
            {
                "regime": rs.regime,
                "days": rs.days,
                "pct_time": rs.pct_time,
                "total_return": rs.total_return,
                "num_trades": rs.num_trades,
                "color": REGIME_COLORS.get(rs.regime, "#888888")
            }
            for rs in results.regime_stats
        ],
        "regime_timeline": results.regime_timeline,
        "trades": [
            {
                "date": t.date.isoformat(),
                "action": t.action,
                "ticker": t.ticker,
                "shares": t.shares,
                "price": t.price,
                "value": t.value,
                "regime": t.regime,
                "reason": t.reason
            }
            for t in results.trades
        ],
        "total_trades": results.total_trades
    }

# Routes

@app.get("/")
async def root():
    return {"message": "PGRB Portfolio Backtesting API", "version": "1.0.0"}

@app.get("/api/health")
async def health():
    return {"status": "healthy", "data_loaded": len(grid_data) > 0}

# Data Management
@app.post("/api/data/upload")
async def upload_data(file: UploadFile = File(...)):
    """Upload and parse 42 Macro Excel file"""
    global grid_data, data_summary

    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")

    try:
        # Save uploaded file temporarily
        temp_path = f"/tmp/{uuid.uuid4()}.xlsx"
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)

        # Parse the file
        rows, tickers, summary = parse_42macro_excel(temp_path)

        # Store in memory
        grid_data = rows
        data_summary = summary

        # Cleanup
        os.remove(temp_path)

        return {
            "success": True,
            "message": f"Successfully loaded {len(rows)} trading days",
            "summary": summary
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/data/summary")
async def get_data_summary():
    """Get summary of loaded data"""
    if not grid_data:
        raise HTTPException(status_code=404, detail="No data loaded. Please upload a file first.")
    return data_summary

@app.get("/api/data/preview")
async def get_preview(n: int = 20):
    """Get preview of recent data"""
    if not grid_data:
        raise HTTPException(status_code=404, detail="No data loaded")
    return get_data_preview(grid_data, n)

@app.get("/api/data/regimes")
async def get_regime_distribution():
    """Get regime distribution summary"""
    if not grid_data:
        raise HTTPException(status_code=404, detail="No data loaded")

    counts = {}
    for row in grid_data:
        counts[row.market_regime] = counts.get(row.market_regime, 0) + 1

    return {
        "distribution": counts,
        "colors": REGIME_COLORS
    }

# Risk Profiles
@app.get("/api/risk-profiles")
async def list_risk_profiles():
    """List all risk profiles"""
    return [
        {
            "id": p.id,
            "name": p.name,
            "allocations": p.allocations
        }
        for p in risk_profiles.values()
    ]

@app.get("/api/risk-profiles/{profile_id}")
async def get_risk_profile(profile_id: str):
    """Get a specific risk profile"""
    if profile_id not in risk_profiles:
        raise HTTPException(status_code=404, detail="Risk profile not found")
    p = risk_profiles[profile_id]
    return {"id": p.id, "name": p.name, "allocations": p.allocations}

@app.post("/api/risk-profiles")
async def create_risk_profile(profile: RiskProfileRequest):
    """Create a new risk profile"""
    profile_id = str(uuid.uuid4())[:8]
    new_profile = RiskProfile(
        id=profile_id,
        name=profile.name,
        allocations=profile.allocations
    )
    risk_profiles[profile_id] = new_profile
    return {"id": profile_id, "name": new_profile.name}

@app.delete("/api/risk-profiles/{profile_id}")
async def delete_risk_profile(profile_id: str):
    """Delete a risk profile"""
    if profile_id in ["aggressive", "moderate", "conservative"]:
        raise HTTPException(status_code=400, detail="Cannot delete default profiles")
    if profile_id not in risk_profiles:
        raise HTTPException(status_code=404, detail="Risk profile not found")
    del risk_profiles[profile_id]
    return {"success": True}

# Backtests
@app.post("/api/backtest")
async def run_backtest(request: BacktestRequest):
    """Run a new backtest"""
    if not grid_data:
        raise HTTPException(status_code=400, detail="No data loaded. Please upload a file first.")

    if request.risk_profile_id not in risk_profiles:
        raise HTTPException(status_code=400, detail=f"Risk profile '{request.risk_profile_id}' not found")

    try:
        # Parse dates
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d")

        # Create config
        config = BacktestConfig(
            name=request.name,
            risk_profile_id=request.risk_profile_id,
            start_date=start_date,
            end_date=end_date,
            starting_value=request.starting_value,
            benchmark_ticker=request.benchmark_ticker
        )

        # Run backtest
        profile = risk_profiles[request.risk_profile_id]
        engine = BacktestEngine(grid_data, profile)
        results = engine.run(config)

        # Store results
        backtest_id = str(uuid.uuid4())[:8]
        backtests[backtest_id] = {
            "id": backtest_id,
            "created_at": datetime.now().isoformat(),
            "results": results_to_dict(results)
        }

        return {
            "success": True,
            "backtest_id": backtest_id,
            "summary": {
                "total_return": results.total_return,
                "annualized_return": results.annualized_return,
                "sharpe_ratio": results.sharpe_ratio,
                "max_drawdown": results.max_drawdown,
                "total_trades": results.total_trades
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/backtest/{backtest_id}")
async def get_backtest(backtest_id: str):
    """Get backtest results"""
    if backtest_id not in backtests:
        raise HTTPException(status_code=404, detail="Backtest not found")
    return backtests[backtest_id]

@app.get("/api/backtests")
async def list_backtests():
    """List all backtests"""
    return [
        {
            "id": bt["id"],
            "name": bt["results"]["config"]["name"],
            "created_at": bt["created_at"],
            "total_return": bt["results"]["summary"]["total_return"],
            "sharpe_ratio": bt["results"]["risk_metrics"]["sharpe_ratio"]
        }
        for bt in backtests.values()
    ]

# Ticker validation
@app.get("/api/validate-ticker")
async def validate_ticker(ticker: str):
    """Validate if a ticker exists in the data"""
    if not grid_data:
        raise HTTPException(status_code=400, detail="No data loaded")

    # Check if ticker exists in VAMS data
    tickers_in_data = set()
    for row in grid_data[:1]:  # Just check first row
        tickers_in_data = set(row.vams.keys())
        break

    return {
        "ticker": ticker,
        "valid": ticker in tickers_in_data,
        "available_tickers": sorted(list(tickers_in_data))
    }

# Include rankings router if available
if RANKINGS_AVAILABLE:
    app.include_router(rankings_router)

# Auto-load data if file exists
@app.on_event("startup")
async def startup_event():
    global grid_data, data_summary

    # Check if we have the uploaded file
    default_file = "/sessions/exciting-clever-lamport/mnt/uploads/Macro Regime Outlook (1).xlsx"
    if os.path.exists(default_file):
        try:
            rows, tickers, summary = parse_42macro_excel(default_file)
            grid_data = rows
            data_summary = summary
            print(f"Auto-loaded {len(rows)} rows from default file")
        except Exception as e:
            print(f"Failed to auto-load data: {e}")

    # Initialize Rankings database
    if RANKINGS_AVAILABLE:
        try:
            init_db()
            db = SessionLocal()
            try:
                seed_funds(db)
            finally:
                db.close()
            print("Rankings database initialized with seed data")
        except Exception as e:
            print(f"Rankings database initialization failed: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
