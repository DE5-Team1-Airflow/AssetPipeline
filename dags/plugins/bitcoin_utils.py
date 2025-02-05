import os
import requests
from datetime import datetime, timedelta
from typing import Final
import logging

import yfinance as yf
import pandas as pd
from glob import glob

COLUMN_ORDER: Final = [
        'ticker', 'record_date', 'platform', 'opening_price', 'highest_price', 
        'lowest_price', 'closing_price', 'trade_volume', 'created_at']
COLUMNS_TO_ROUND = ['opening_price', 'highest_price', 'lowest_price', 'closing_price', 'trade_volume']

def get_yfinance_data(start: datetime, end: datetime) -> pd.DataFrame:
    """_summary_

    Returns:
        _type_: _description_
    """
    btc = yf.Ticker("BTC-USD")
    df = btc.history(period='1mo', interval='1d', start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'))
    df.index = df.index.tz_convert(None)
    df.reset_index(inplace=True)
    df.rename(columns={
        'Open': 'opening_price',
        'High': 'highest_price',
        'Low': 'lowest_price',
        'Close': 'closing_price',
        'Volume': 'trade_volume',
    }, inplace=True)
    df['ticker'] = 'BTC-USD'
    df['platform'] = 'yfinance'
    df['record_date'] = pd.to_datetime(df['Date'])
    df[COLUMNS_TO_ROUND] = df[COLUMNS_TO_ROUND].apply(lambda x: x.round(2))
    df['created_at'] = datetime.now()
    return df[COLUMN_ORDER]


def get_upbit_data(end: datetime) -> pd.DataFrame:
    """_summary_

    Returns:
        _type_: _description_
    """
    base_url: Final = 'https://api.upbit.com/v1/candles/days'
    headers: Final = {"accept": "application/json"}
    params = {
        'market': "KRW-BTC",
        'to': end.strftime("%Y-%m-%dT%H:%M:%S"),
        'count': 7,
    }

    response = requests.get(base_url, params=params, headers=headers)
    data = response.json()
    if not data:
        raise ValueError("No data received from Upbit API.")

    # 날짜 순서 정렬 및 데이터프레임 생성
    df = pd.DataFrame(data)
    df = df[['market', 'candle_date_time_utc', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume', 'prev_closing_price', 'change_price', 'change_rate']]
    df.rename(columns={
        'market': 'ticker',
        'candle_date_time_utc': 'record_date',
        'high_price': 'highest_price',
        'low_price': 'lowest_price',
        'trade_price': 'closing_price',
        'candle_acc_trade_volume': 'trade_volume',
    }, inplace=True)
    df['platform'] = 'upbit'
    df['record_date'] = pd.to_datetime(df['record_date'])
    df[COLUMNS_TO_ROUND] = df[COLUMNS_TO_ROUND].apply(lambda x: x.round(2))
    df['created_at'] = datetime.now()
    return df[COLUMN_ORDER]


def get_binance_data(end:int = None) -> pd.DataFrame:
    """_summary_

    Args:

    Returns:
        pd.DataFrame: _description_
    """
    base_url: Final = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': 'BTCUSDT',
        'interval': '1d',
        'endTime': end,
        'limit': 7,
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    if not data:
        raise ValueError("No data received from binance API.")

    # 데이터프레임 생성
    columns = ['open_time', 'opening_price', 'highest_price', 'lowest_price', 'closing_price', 'trade_volume', 'close_time',
               'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
               'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(data, columns=columns)
    df['ticker'] = 'BTC-USD'
    df['platform'] = 'binance'
    df['record_date'] = pd.to_datetime(df['open_time'], unit='ms')
    df[COLUMNS_TO_ROUND] = df[COLUMNS_TO_ROUND].astype(float)
    df[COLUMNS_TO_ROUND] = df[COLUMNS_TO_ROUND].apply(lambda x: x.round(2))
    df['created_at'] = datetime.now()
    return df[COLUMN_ORDER]


def save_csv(df: pd.DataFrame, source_name: str, directory: str = './data'):
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        source_name (str): _description_
        directory (str, optional): _description_. Defaults to './data'.
    """
    # 디렉토리 생성 (존재하지 않으면 생성)
    os.makedirs(directory, exist_ok=True)
    # 파일 경로 생성
    filename = os.path.join(directory, f"{source_name}_data.csv")
    df.to_csv(filename, encoding='utf-8', index=False)


# test code
'''
if __name__ == "__main__":
    # 오늘 날짜를 2024년 1월 25일로 가정
    today = datetime(2024, 1, 25)

    # 기준 날짜 설정: 1월 24일을 기준으로 계산
    logical_date = today - timedelta(days=1)  # 1월 24일
    start_date = logical_date - timedelta(days=7)  # 1월 17일

    print(f"Fetching data for the period: {start_date} to {logical_date}")

    # 1. YFinance 데이터 테스트
    print("\nFetching YFinance data...")
    yfinance_data = get_yfinance_data(start=start_date, end=logical_date)

    # 2. Upbit 데이터 테스트
    print("\nFetching Upbit data...")
    upbit_data = get_upbit_data(end=logical_date)

    # 3. Binance 데이터 테스트
    print("\nFetching Binance data...")
    end_timestamp = int(logical_date.timestamp() * 1000)  # 밀리초 타임스탬프로 변환
    binance_data = get_binance_data(end=end_timestamp)

    # 4. 데이터 저장 (옵션)
    print("\nSaving data to CSV...")
    save_csv(upbit_data, 'upbit_test')
    save_csv(yfinance_data, 'yfinance_test')
    save_csv(binance_data, "binance_test")
    print("Data saved successfully!")
'''