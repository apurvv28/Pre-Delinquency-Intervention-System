from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_DIR = Path("backend/xg-datasets")
OUTPUT_DIR = Path("backend/outputs/eda") / "xgboost"


def _ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    transactions = pd.read_csv(DATA_DIR / "transactions.csv")
    prior_risk = pd.read_csv(DATA_DIR / "prior_risk_scores.csv")
    return customers, transactions, prior_risk


def _build_feature_table(
    customers: pd.DataFrame,
    transactions: pd.DataFrame,
    prior_risk: pd.DataFrame,
) -> pd.DataFrame:
    tx = transactions.copy()
    tx["transaction_time"] = pd.to_datetime(tx["transaction_time"], errors="coerce")

    tx_agg = (
        tx.groupby("customer_id", as_index=False)
        .agg(
            tx_count=("amount", "size"),
            amount_mean=("amount", "mean"),
            amount_median=("amount", "median"),
            amount_std=("amount", "std"),
            amount_p95=("amount", lambda s: float(np.percentile(s, 95))),
            days_since_last_payment_mean=("days_since_last_payment", "mean"),
            previous_declines_24h_mean=("previous_declines_24h", "mean"),
            international_ratio=("is_international", "mean"),
            unique_merchant_categories=("merchant_category", "nunique"),
        )
        .fillna(0)
    )

    risk_agg = (
        prior_risk.groupby("customer_id", as_index=False)
        .agg(
            prior_risk_mean=("risk_score", "mean"),
            prior_risk_max=("risk_score", "max"),
            prior_risk_count=("risk_score", "size"),
        )
        .fillna(0)
    )

    full = customers.merge(tx_agg, on="customer_id", how="left").merge(
        risk_agg, on="customer_id", how="left"
    )

    numeric_cols = full.select_dtypes(include=[np.number]).columns
    full[numeric_cols] = full[numeric_cols].fillna(0)
    return full


def _save_class_balance(feature_table: pd.DataFrame) -> None:
    counts = feature_table["contextual_target"].value_counts().sort_index()
    labels = [f"Class {idx}" for idx in counts.index]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(labels, counts.values, color=["#4E79A7", "#E15759"])
    ax.set_title("XGBoost Target Class Balance")
    ax.set_ylabel("Number of Customers")

    for bar, value in zip(bars, counts.values):
        ax.text(bar.get_x() + bar.get_width() / 2, value, f"{int(value)}", ha="center", va="bottom")

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "xgb_class_balance.png", dpi=180)
    plt.close(fig)


def _save_customer_feature_distributions(feature_table: pd.DataFrame) -> None:
    cols = ["monthly_income", "loan_amount", "account_age_months", "tx_count", "amount_mean", "prior_risk_mean"]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    axes = axes.ravel()

    for ax, col in zip(axes, cols):
        ax.hist(feature_table[col], bins=40, color="#76B7B2", edgecolor="white")
        ax.set_title(col)
        ax.grid(alpha=0.2)

    fig.suptitle("Customer and Aggregated Transaction Feature Distributions", fontsize=14)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "xgb_feature_distributions.png", dpi=180)
    plt.close(fig)


def _save_transaction_amount_patterns(transactions: pd.DataFrame) -> None:
    tx = transactions.copy()
    tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce").fillna(0)
    tx["merchant_category"] = tx["merchant_category"].fillna("unknown")

    top_merchants = tx["merchant_category"].value_counts().head(10)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].hist(np.log1p(tx["amount"]), bins=60, color="#59A14F", edgecolor="white")
    axes[0].set_title("Log-Scaled Transaction Amount Distribution")
    axes[0].set_xlabel("log(1 + amount)")
    axes[0].grid(alpha=0.2)

    axes[1].barh(top_merchants.index[::-1], top_merchants.values[::-1], color="#F28E2B")
    axes[1].set_title("Top Merchant Categories by Event Count")
    axes[1].set_xlabel("Transactions")

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "xgb_transaction_patterns.png", dpi=180)
    plt.close(fig)


def _save_target_vs_key_features(feature_table: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    feature_pairs = [
        ("loan_amount", "contextual_target"),
        ("tx_count", "contextual_target"),
        ("prior_risk_mean", "contextual_target"),
        ("previous_declines_24h_mean", "contextual_target"),
    ]

    for ax, (feature_col, target_col) in zip(axes.ravel(), feature_pairs):
        data0 = feature_table.loc[feature_table[target_col] == 0, feature_col]
        data1 = feature_table.loc[feature_table[target_col] == 1, feature_col]
        ax.boxplot([data0.values, data1.values], tick_labels=["target=0", "target=1"], patch_artist=True)
        ax.set_title(f"{feature_col} by {target_col}")
        ax.grid(alpha=0.2)

    fig.suptitle("Feature Separation vs Contextual Target", fontsize=14)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "xgb_target_feature_separation.png", dpi=180)
    plt.close(fig)


def _save_correlation_heatmap(feature_table: pd.DataFrame) -> None:
    cols = [
        "contextual_target",
        "monthly_income",
        "loan_amount",
        "account_age_months",
        "tx_count",
        "amount_mean",
        "days_since_last_payment_mean",
        "previous_declines_24h_mean",
        "international_ratio",
        "unique_merchant_categories",
        "prior_risk_mean",
        "prior_risk_max",
    ]

    corr = feature_table[cols].corr(numeric_only=True)

    fig, ax = plt.subplots(figsize=(11, 8))
    matrix = ax.imshow(corr.values, cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels(cols, rotation=45, ha="right")
    ax.set_yticks(range(len(cols)))
    ax.set_yticklabels(cols)
    ax.set_title("Correlation Heatmap for XGBoost Feature Candidates")

    cbar = fig.colorbar(matrix, ax=ax)
    cbar.set_label("Correlation")

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "xgb_feature_correlation_heatmap.png", dpi=180)
    plt.close(fig)


def _save_executive_infographic(feature_table: pd.DataFrame, transactions: pd.DataFrame) -> None:
    target_rate = feature_table["contextual_target"].mean() * 100
    mean_income = feature_table["monthly_income"].mean()
    mean_loan = feature_table["loan_amount"].mean()
    median_tx = transactions["amount"].median()
    total_events = len(transactions)

    fig = plt.figure(figsize=(13, 8))
    fig.patch.set_facecolor("#F8FAFC")

    # Header
    fig.text(0.05, 0.93, "PIE XGBoost EDA Snapshot", fontsize=22, fontweight="bold", color="#1F2937")
    fig.text(0.05, 0.89, "Customer behavior, risk context, and transaction dynamics", fontsize=12, color="#4B5563")

    # KPI cards
    kpis = [
        ("Customers", f"{len(feature_table):,}"),
        ("Transactions", f"{total_events:,}"),
        ("Positive Target Rate", f"{target_rate:.1f}%"),
        ("Median Transaction", f"{median_tx:,.2f}"),
    ]

    x0 = 0.05
    for title, value in kpis:
        fig.text(x0, 0.79, title, fontsize=10, color="#6B7280")
        fig.text(x0, 0.745, value, fontsize=17, fontweight="bold", color="#111827")
        x0 += 0.22

    # Narrative bullets
    bullets = [
        f"Average monthly income is {mean_income:,.0f} and average loan amount is {mean_loan:,.0f}.",
        "Transaction frequency and historical risk have visible separation across target classes.",
        "Prior risk signals and decline behavior should remain high-priority candidate features.",
        "Use this EDA to seed feature selection, class balancing, and threshold strategy for XGBoost training.",
    ]

    y = 0.62
    for text in bullets:
        fig.text(0.06, y, f"- {text}", fontsize=11, color="#374151")
        y -= 0.06

    # Sparkline-style miniature trend
    ax = fig.add_axes([0.58, 0.42, 0.35, 0.32])
    daily = transactions.copy()
    daily["transaction_time"] = pd.to_datetime(daily["transaction_time"], errors="coerce")
    by_day = (
        daily.dropna(subset=["transaction_time"]) 
        .groupby(daily["transaction_time"].dt.date)
        .size()
        .tail(60)
    )
    ax.plot(by_day.values, color="#2563EB", linewidth=2)
    ax.fill_between(range(len(by_day.values)), by_day.values, color="#93C5FD", alpha=0.3)
    ax.set_title("Recent daily transaction counts", fontsize=11)
    ax.set_xticks([])
    ax.grid(alpha=0.2)

    fig.savefig(OUTPUT_DIR / "xgb_eda_infographic.png", dpi=180)
    plt.close(fig)


def _write_summary(feature_table: pd.DataFrame, transactions: pd.DataFrame) -> None:
    positive_rate = feature_table["contextual_target"].mean() * 100
    top_occ = feature_table["occupation"].value_counts().head(3)
    top_merchants = transactions["merchant_category"].value_counts().head(5)

    lines = [
        "# XGBoost EDA Summary",
        "",
        f"- Customers analyzed: {len(feature_table):,}",
        f"- Transactions analyzed: {len(transactions):,}",
        f"- Positive class rate (contextual_target=1): {positive_rate:.2f}%",
        "",
        "## Top Occupations",
    ]
    lines.extend([f"- {idx}: {int(val):,}" for idx, val in top_occ.items()])
    lines.append("")
    lines.append("## Top Merchant Categories")
    lines.extend([f"- {idx}: {int(val):,}" for idx, val in top_merchants.items()])
    lines.append("")
    lines.append("Generated by backend/eda_xgboost.py")

    (OUTPUT_DIR / "xgb_eda_summary.md").write_text("\n".join(lines), encoding="utf-8")


def run() -> None:
    _ensure_output_dir()
    customers, transactions, prior_risk = _load_data()
    feature_table = _build_feature_table(customers, transactions, prior_risk)

    _save_class_balance(feature_table)
    _save_customer_feature_distributions(feature_table)
    _save_transaction_amount_patterns(transactions)
    _save_target_vs_key_features(feature_table)
    _save_correlation_heatmap(feature_table)
    _save_executive_infographic(feature_table, transactions)
    _write_summary(feature_table, transactions)

    print(f"EDA completed. Artifacts stored in: {OUTPUT_DIR}")


if __name__ == "__main__":
    run()
