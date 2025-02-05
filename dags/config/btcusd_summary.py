{
    "schema": "analysis",
    "table": "btcusd_summary",
    "sql": """
        SELECT record_date, platform, ticker, closing_price
        FROM raw_data.crypto_price
        WHERE ticker = 'BTC-USD'
        ORDER BY record_date, platform;
    """
        }