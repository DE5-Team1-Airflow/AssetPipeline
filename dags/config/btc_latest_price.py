{
    "schema": "analysis",
    "table": "btc_latest_price",
    "sql": """
        SELECT platform, closing_price, record_date
        FROM raw_data.crypto_price
        ORDER BY record_date DESC
        LIMIT 3;
    """
}