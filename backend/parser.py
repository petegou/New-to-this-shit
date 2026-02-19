"""Parser for 42 Macro Excel files"""
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple
from models import GridDataRow

def parse_42macro_excel(file_path: str) -> Tuple[List[GridDataRow], List[str], Dict]:
    """
    Parse 42 Macro Excel file into structured data.

    Args:
        file_path: Path to the Excel file

    Returns:
        Tuple of (list of GridDataRow, list of tickers, summary dict)
    """
    # Read with header on row 1 (0-indexed)
    df = pd.read_excel(file_path, sheet_name=0, header=1)

    # Extract tickers from columns 11-80
    raw_tickers = list(df.columns[11:81])
    # Filter to valid ticker symbols (remove NaN, unnamed columns)
    tickers = [t for t in raw_tickers if isinstance(t, str) and not t.startswith('Unnamed')]

    rows = []
    for idx, row in df.iterrows():
        try:
            # Parse date from column 4
            date_val = row.iloc[4]
            if pd.isna(date_val):
                continue
            if isinstance(date_val, str):
                date = datetime.strptime(date_val, '%Y-%m-%d')
            else:
                date = pd.to_datetime(date_val).to_pydatetime()

            # Parse confirming markets
            sum_confirming = int(row.iloc[0]) if not pd.isna(row.iloc[0]) else 0
            goldilocks = int(row.iloc[5]) if not pd.isna(row.iloc[5]) else 0
            reflation = int(row.iloc[6]) if not pd.isna(row.iloc[6]) else 0
            inflation = int(row.iloc[7]) if not pd.isna(row.iloc[7]) else 0
            deflation = int(row.iloc[8]) if not pd.isna(row.iloc[8]) else 0

            # Parse regimes
            market_regime = str(row.iloc[9]) if not pd.isna(row.iloc[9]) else "UNKNOWN"
            risk_regime = str(row.iloc[10]) if not pd.isna(row.iloc[10]) else "UNKNOWN"

            # Parse VAMS for each ticker
            vams = {}
            for i, ticker in enumerate(tickers):
                col_idx = 11 + raw_tickers.index(ticker)
                val = row.iloc[col_idx]
                if pd.notna(val):
                    try:
                        vams[ticker] = int(val)
                    except (ValueError, TypeError):
                        vams[ticker] = 0
                else:
                    vams[ticker] = 0

            grid_row = GridDataRow(
                date=date,
                sum_confirming_markets=sum_confirming,
                goldilocks_confirming=goldilocks,
                reflation_confirming=reflation,
                inflation_confirming=inflation,
                deflation_confirming=deflation,
                market_regime=market_regime.upper(),
                risk_regime=risk_regime.upper(),
                vams=vams
            )
            rows.append(grid_row)

        except Exception as e:
            continue  # Skip problematic rows

    # Sort by date
    rows.sort(key=lambda x: x.date)

    # Generate summary
    if rows:
        date_range_days = (rows[-1].date - rows[0].date).days
        years = date_range_days // 365
        weeks = (date_range_days % 365) // 7
        days = (date_range_days % 365) % 7

        regime_counts = {}
        for r in rows:
            regime_counts[r.market_regime] = regime_counts.get(r.market_regime, 0) + 1

        summary = {
            "start_date": rows[0].date.strftime("%m/%d/%Y"),
            "end_date": rows[-1].date.strftime("%m/%d/%Y"),
            "trading_days": len(rows),
            "date_range_formatted": f"{years} years, {weeks} weeks, {days} days",
            "tickers": tickers,
            "ticker_count": len(tickers),
            "regime_breakdown": regime_counts
        }
    else:
        summary = {"error": "No valid data rows found"}

    return rows, tickers, summary


def get_data_preview(rows: List[GridDataRow], n: int = 20) -> List[dict]:
    """Get most recent n rows for preview (most recent first)"""
    recent = rows[-n:][::-1]  # Last n rows, reversed
    preview = []
    for row in recent:
        preview.append({
            "date": row.date.strftime("%m/%d/%Y"),
            "regime": row.market_regime,
            "risk_regime": row.risk_regime,
            "sum_confirming": row.sum_confirming_markets,
            "goldilocks": row.goldilocks_confirming,
            "reflation": row.reflation_confirming,
            "inflation": row.inflation_confirming,
            "deflation": row.deflation_confirming,
            "spy_vams": row.vams.get("SPY", 0),
            "qqq_vams": row.vams.get("QQQ", 0),
            "tlt_vams": row.vams.get("TLT", 0),
            "gld_vams": row.vams.get("GLD", 0),
        })
    return preview


if __name__ == "__main__":
    # Test the parser
    rows, tickers, summary = parse_42macro_excel("/sessions/exciting-clever-lamport/mnt/uploads/Macro Regime Outlook (1).xlsx")
    print("Summary:", summary)
    print(f"\nParsed {len(rows)} rows")
    print(f"Date range: {rows[0].date} to {rows[-1].date}")
    print(f"\nFirst row VAMS sample: SPY={rows[0].vams.get('SPY')}, QQQ={rows[0].vams.get('QQQ')}")
    print(f"\nPreview (recent 5):")
    for p in get_data_preview(rows, 5):
        print(p)
