"""
Oak Bridge Financial Ranking Algorithm Implementation.

Calculates individual sub-scores, aggregate scores, and rankings for funds.
All formulas are implemented EXACTLY as specified in the requirements.
"""
from typing import Dict, List, Optional


# Parent categories that count as "equity" for price score calculation
EQUITY_PARENT_CATEGORIES = {"US Equity", "International Equity"}


class RankingCalculator:
    """
    Implements the Oak Bridge Financial Ranking Algorithm.
    Calculates individual sub-scores, aggregate scores, and rankings.
    """

    def _safe_float(self, value, default=0.0) -> float:
        """Safely convert a value to float, handling None and Decimal types."""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _is_equity_category(self, parent_category: Optional[str]) -> bool:
        """Determine if a fund's category is equity-based."""
        if parent_category is None:
            return False
        return parent_category in EQUITY_PARENT_CATEGORIES

    # =====================================================================
    # RISK SUB-SCORE CALCULATIONS
    # =====================================================================

    def _calc_beta(self, fund: dict, has_5yr: bool) -> float:
        beta_3yr = self._safe_float(fund.get('beta_3yr'))
        if has_5yr:
            beta_5yr = self._safe_float(fund.get('beta_5yr'))
            return ((1 - beta_3yr * 0.67) + (1 - beta_5yr * 0.33)) * 70
        else:
            return (1 - beta_3yr) * 70

    def _calc_r_squared(self, fund: dict, has_5yr: bool) -> float:
        r_squared_3yr = self._safe_float(fund.get('r_squared_3yr'))
        if has_5yr:
            r_squared_5yr = self._safe_float(fund.get('r_squared_5yr'))
            return ((r_squared_3yr * 0.067) + (r_squared_5yr * 0.033)) * 5
        else:
            return r_squared_3yr * 0.5

    def _calc_up_capture(self, fund: dict, has_5yr: bool) -> float:
        up_capture_3yr = self._safe_float(fund.get('up_capture_3yr'))
        if has_5yr:
            up_capture_5yr = self._safe_float(fund.get('up_capture_5yr'))
            return (((1 - up_capture_3yr) * 67) + (((1 - up_capture_5yr) * 33) * 100)) * -1
        else:
            return ((1 - up_capture_3yr) * 100) * -1

    def _calc_down_capture(self, fund: dict, has_5yr: bool) -> float:
        down_capture_3yr = self._safe_float(fund.get('down_capture_3yr'))
        if has_5yr:
            down_capture_5yr = self._safe_float(fund.get('down_capture_5yr'))
            return ((1 - down_capture_3yr) * 67) + ((1 - down_capture_5yr) * 33)
        else:
            return (1 - down_capture_3yr) * 100

    def _calc_sharpe(self, fund: dict, has_5yr: bool) -> float:
        sharpe_3yr = self._safe_float(fund.get('sharpe_ratio_3yr'))
        if has_5yr:
            sharpe_5yr = self._safe_float(fund.get('sharpe_ratio_5yr'))
            return ((sharpe_3yr * 50) + (sharpe_5yr * 25)) / 2
        else:
            return (sharpe_3yr * 75) / 2

    def _calc_tracking_error(self, fund: dict, has_5yr: bool) -> float:
        te_3yr = self._safe_float(fund.get('tracking_error_3yr'))
        if has_5yr:
            te_5yr = self._safe_float(fund.get('tracking_error_5yr'))
            return ((100 - (te_3yr / 2)) * 0.67) + ((100 - (te_5yr / 2)) * 0.33)
        else:
            return (100 - te_3yr) / 2

    def _calc_sortino(self, fund: dict, has_5yr: bool) -> float:
        sortino_3yr = self._safe_float(fund.get('sortino_ratio_3yr'))
        if has_5yr:
            sortino_5yr = self._safe_float(fund.get('sortino_ratio_5yr'))
            return ((sortino_3yr * 6.7) + (sortino_5yr * 3.3)) * 100
        else:
            return sortino_3yr * 100

    def _calc_treynor(self, fund: dict, has_5yr: bool) -> float:
        treynor_3yr = self._safe_float(fund.get('treynor_ratio_3yr'))
        if has_5yr:
            treynor_5yr = self._safe_float(fund.get('treynor_ratio_5yr'))
            return abs(((treynor_3yr * 0.67) + (treynor_5yr * 0.33)) * 400)
        else:
            return abs(treynor_3yr * 400)

    def _calc_info_ratio(self, fund: dict, has_5yr: bool) -> float:
        ir_3yr = self._safe_float(fund.get('information_ratio_3yr'))
        if has_5yr:
            ir_5yr = self._safe_float(fund.get('information_ratio_5yr'))
            return ((ir_3yr * 6.7) + (ir_5yr * 3.3)) * 30
        else:
            return ir_3yr * 300

    def _calc_kurtosis(self, fund: dict, has_5yr: bool) -> float:
        kurt_3yr = self._safe_float(fund.get('kurtosis_3yr'))
        if has_5yr:
            kurt_5yr = self._safe_float(fund.get('kurtosis_5yr'))
            return (kurt_3yr * 0.66) + (kurt_5yr * 0.34)
        else:
            return kurt_3yr

    def _calc_drawdown(self, fund: dict, has_5yr: bool) -> float:
        dd_3yr = self._safe_float(fund.get('max_drawdown_3yr'))
        if has_5yr:
            dd_5yr = self._safe_float(fund.get('max_drawdown_5yr'))
            return (100 - ((dd_3yr * 15 * 0.66) + (dd_5yr * 15 * 0.34))) / 2
        else:
            return ((100 - (dd_3yr * 15)) * 0.66) / 2

    def _calc_skewness(self, fund: dict, has_5yr: bool) -> float:
        skew_3yr = self._safe_float(fund.get('skewness_3yr'))
        if has_5yr:
            skew_5yr = self._safe_float(fund.get('skewness_5yr'))
            return ((skew_3yr * 66) + (skew_5yr * 34)) / 2
        else:
            return skew_3yr / 2

    # =====================================================================
    # RETURN SUB-SCORE CALCULATIONS
    # =====================================================================

    def _calc_alpha(self, fund: dict, has_5yr: bool) -> float:
        alpha_3yr = self._safe_float(fund.get('alpha_3yr'))
        if has_5yr:
            alpha_5yr = self._safe_float(fund.get('alpha_5yr'))
            return (alpha_3yr * 3) + (alpha_5yr * 1.5)
        else:
            return alpha_3yr * 4.5

    def _calc_yield(self, fund: dict) -> float:
        """
        yield_pct is stored as decimal (0.023 for 2.3%).
        Formula uses the percentage number: yield_pct_display = yield_pct * 100.
        yield_score = yield_pct_display * 6
        """
        yield_pct = self._safe_float(fund.get('yield_pct'))
        yield_pct_display = yield_pct * 100  # Convert from decimal to percentage
        return yield_pct_display * 6

    def _calc_relative_return(self, fund: dict, has_3yr: bool, has_5yr: bool, has_10yr: bool) -> float:
        # Get all return values (stored as decimals)
        return_qtd = self._safe_float(fund.get('return_qtd'))
        return_ytd = self._safe_float(fund.get('return_ytd'))
        return_1yr = self._safe_float(fund.get('return_1yr'))
        return_3yr = self._safe_float(fund.get('return_3yr'))
        return_5yr = self._safe_float(fund.get('return_5yr'))
        return_10yr = self._safe_float(fund.get('return_10yr'))

        bm_return_1yr = self._safe_float(fund.get('bm_return_1yr'))
        bm_return_3yr = self._safe_float(fund.get('bm_return_3yr'))
        bm_return_5yr = self._safe_float(fund.get('bm_return_5yr'))
        bm_return_10yr = self._safe_float(fund.get('bm_return_10yr'))

        batting_avg_3yr = self._safe_float(fund.get('batting_avg_3yr'))
        batting_avg_5yr = self._safe_float(fund.get('batting_avg_5yr'))

        batting_component = (batting_avg_3yr + batting_avg_5yr) / 3

        if has_10yr:
            return (
                (return_10yr - bm_return_10yr) +
                ((return_5yr - bm_return_5yr) * 2) +
                ((return_3yr - bm_return_3yr) * 2.5) +
                ((return_1yr - bm_return_1yr) * 1.5) +
                (return_qtd * 1.5) +
                (return_ytd * 2) +
                batting_component
            )
        elif has_5yr:
            return (
                ((return_5yr - bm_return_5yr) * 3) +
                ((return_3yr - bm_return_3yr) * 2.5) +
                ((return_1yr - bm_return_1yr) * 1.5) +
                (return_qtd * 1.5) +
                (return_ytd * 2) +
                batting_component
            )
        elif has_3yr:
            return (
                (((return_3yr - bm_return_3yr) + (return_1yr - bm_return_1yr)) / 2) * 4.5 +
                ((return_3yr - bm_return_3yr) * 1.5) +
                ((return_1yr - bm_return_1yr) * 1.5) +
                (return_qtd * 1.5) +
                (return_ytd * 2) +
                batting_component
            )
        else:
            # Less than 3 years
            return (
                ((return_1yr - bm_return_1yr) * 3.5) +
                (return_qtd * 1.5) +
                (return_ytd * 2) +
                batting_component
            )

    def _calc_price(self, fund: dict) -> float:
        """
        Price Score — EQUITIES ONLY.
        pe_score = ((20 - pe_ratio) / 20) * 100
        pb_score = ((3 - pb_ratio) * 10 / 3) * 10
        price_score = pe_score + pb_score
        """
        pe_ratio = self._safe_float(fund.get('pe_ratio'))
        pb_ratio = self._safe_float(fund.get('pb_ratio'))

        pe_score = ((20 - pe_ratio) / 20) * 100
        pb_score = ((3 - pb_ratio) * 10 / 3) * 10
        return pe_score + pb_score

    def _calc_fee(self, fund: dict) -> float:
        """
        fee_score = (1 - (net_expense_ratio * 100)) * 10 / 2
        net_expense_ratio stored as decimal e.g. 0.0045 for 0.45%
        """
        ner = self._safe_float(fund.get('net_expense_ratio'))
        return (1 - (ner * 100)) * 10 / 2

    # =====================================================================
    # ADJUSTMENT CALCULATIONS
    # =====================================================================

    def _calc_market_cap(self, fund: dict) -> float:
        """market_cap_score = market_cap / 1200 (market_cap in millions)"""
        market_cap = self._safe_float(fund.get('market_cap'))
        return market_cap / 1200

    def _calc_turnover(self, fund: dict) -> float:
        """
        if turnover <= 0.50: turnover_score = 0
        else: turnover_score = turnover / (-4)
        """
        turnover = self._safe_float(fund.get('turnover'))
        if turnover <= 0.50:
            return 0.0
        else:
            return turnover / (-4)

    # =====================================================================
    # MAIN CALCULATION METHOD
    # =====================================================================

    def calculate_all_scores(self, fund: dict) -> dict:
        """
        Calculate all scores for a single fund.
        fund: dict with all fund data fields.
        Returns dict of all score fields.
        """
        fund_age = self._safe_float(fund.get('fund_age_years'))
        has_5yr = fund_age >= 5
        has_10yr = fund_age >= 10
        has_3yr = fund_age >= 3

        parent_category = fund.get('parent_category', '')
        is_equity = self._is_equity_category(parent_category)

        scores = {}

        # Calculate each risk sub-score
        scores['beta_score'] = self._calc_beta(fund, has_5yr)
        scores['r_squared_score'] = self._calc_r_squared(fund, has_5yr)
        scores['up_capture_score'] = self._calc_up_capture(fund, has_5yr)
        scores['down_capture_score'] = self._calc_down_capture(fund, has_5yr)
        scores['sharpe_score'] = self._calc_sharpe(fund, has_5yr)
        scores['tracking_error_score'] = self._calc_tracking_error(fund, has_5yr)
        scores['sortino_score'] = self._calc_sortino(fund, has_5yr)
        scores['treynor_score'] = self._calc_treynor(fund, has_5yr)
        scores['info_ratio_score'] = self._calc_info_ratio(fund, has_5yr)
        scores['kurtosis_score'] = self._calc_kurtosis(fund, has_5yr)
        scores['drawdown_score'] = self._calc_drawdown(fund, has_5yr)
        scores['skewness_score'] = self._calc_skewness(fund, has_5yr)

        # Aggregate risk score
        scores['risk_score'] = (
            scores['beta_score'] + scores['r_squared_score'] +
            scores['up_capture_score'] + scores['down_capture_score'] +
            scores['sharpe_score'] + scores['tracking_error_score'] +
            scores['sortino_score'] + scores['treynor_score'] +
            scores['info_ratio_score'] + scores['kurtosis_score'] +
            scores['drawdown_score'] + scores['skewness_score']
        ) / 100

        # Calculate each return sub-score
        scores['alpha_score'] = self._calc_alpha(fund, has_5yr)
        scores['yield_score'] = self._calc_yield(fund)
        scores['relative_return_score'] = self._calc_relative_return(fund, has_3yr, has_5yr, has_10yr)
        scores['price_score'] = self._calc_price(fund) if is_equity else 0.0
        scores['fee_score'] = self._calc_fee(fund)

        # Aggregate return score
        scores['return_score'] = (
            scores['alpha_score'] + scores['yield_score'] +
            scores['relative_return_score'] + scores['price_score'] +
            scores['fee_score']
        ) / 30

        # Total RR
        scores['total_rr_score'] = scores['risk_score'] + scores['return_score']

        # Adjustment scores
        scores['market_cap_score'] = self._calc_market_cap(fund)
        scores['turnover_score'] = self._calc_turnover(fund)

        # Final GPA score
        scores['total_gpa_score'] = (
            (scores['risk_score'] + scores['return_score']) / 2 +
            scores['market_cap_score'] + scores['turnover_score']
        )

        return scores

    def rank_funds(self, funds_with_scores: List[dict]) -> List[dict]:
        """
        Assign global_rank and category_rank.
        Sort by total_gpa_score descending.
        """
        # Global ranking
        sorted_global = sorted(
            funds_with_scores,
            key=lambda f: f.get('total_gpa_score', 0) or 0,
            reverse=True
        )
        for i, fund in enumerate(sorted_global):
            fund['global_rank'] = i + 1

        # Category ranking
        by_category = {}
        for fund in funds_with_scores:
            cat = fund.get('category_id')
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(fund)

        for cat_funds in by_category.values():
            sorted_cat = sorted(
                cat_funds,
                key=lambda f: f.get('total_gpa_score', 0) or 0,
                reverse=True
            )
            for i, fund in enumerate(sorted_cat):
                fund['category_rank'] = i + 1

        return funds_with_scores
