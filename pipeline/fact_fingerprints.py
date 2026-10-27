"""Canonical fact fingerprint keys and columns shared by local/cloud checks."""

FACT_KEYS = {
    "fct_holdings_daily": "holding_id",
    "fct_transactions": "transaction_id",
}

FACT_COLUMNS = {
    "fct_holdings_daily": (
        "holding_id", "fund_id", "company_sk", "date_key", "units", "price",
        "market_value", "ingested_at",
    ),
    "fct_transactions": (
        "transaction_id", "fund_id", "company_sk", "date_key", "amount",
        "transaction_type", "currency", "ingested_at", "batch_id",
    ),
}
