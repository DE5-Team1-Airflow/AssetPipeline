{
    "schema": "analysis",
    "table": "btc_platform_summary",
    "sql": """
        SELECT 
            platform,
            AVG(closing_price) AS avg_closing_price,
            MAX(highest_price) AS maximum_highest_price,
            MIN(lowest_price) AS minimum_lowest_price
        FROM raw_data.crypto_price
        GROUP BY platform;
    """
}