import pandas as pd
import logging
import cloudscraper
from bs4 import BeautifulSoup
import numpy as np
import time
import random
import cot_reports as cot
from datetime import datetime, timedelta, timezone
import warnings
import tpqoa
from  PostGresConn import PostgresSQL

warnings.filterwarnings("ignore", category=DeprecationWarning)
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)

def get_last_1000_candles(pair_w_dash):
    api = tpqoa.tpqoa("oanda.cfg")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=3000)
    start_time_str = start_time.strftime("%Y-%m-%d")
    end_time_str = end_time.strftime("%Y-%m-%d")

    granularity = "D"
    df = api.get_history(instrument=pair_w_dash, start=start_time_str, end=end_time_str,
                         granularity=granularity, price="B")
    df = df.reset_index()
    return df

def cot_ema_df_returner(pair):
    mapping = {
        'USD_BRL': 'BRAZILIAN REAL - CHICAGO MERCANTILE EXCHANGE',
        'AUD_USD': 'AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE',
        'USD_CAD': 'CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE',
        'EUR_USD': 'EURO SHORT TERM RATE - CHICAGO MERCANTILE EXCHANGE',
        'EUR_GBP': 'EURO FX/BRITISH POUND XRATE - CHICAGO MERCANTILE EXCHANGE',
        'USD_JPY': 'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE',
        'USD_MXN': 'MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE',
        'USD_ZAR': 'SO AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE',
        'USD_CHF': 'SWISS FRANC - CHICAGO MERCANTILE EXCHANGE',
        'GBP_USD': 'BRITISH POUND - CHICAGO MERCANTILE EXCHANGE',
        'NZD_USD': 'NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE',
    }

    df = pd.concat([
        cot.cot_year(year=(datetime.utcnow() + timedelta(weeks=-156)).year),
        cot.cot_year(year=(datetime.utcnow() + timedelta(weeks=-104)).year),
        cot.cot_year(year=(datetime.utcnow() + timedelta(weeks=-52)).year),
        cot.cot_year(year=datetime.utcnow().year)
    ])

    df = df[df['Market and Exchange Names'] == mapping[pair]]
    df['time'] = pd.to_datetime(df['As of Date in Form YYYY-MM-DD'])
    df = df.reset_index().sort_values(by='time', ascending=True)

    df['Dealer_Net_Position'] = df['Traders-Commercial-Long (All)'] - df['Traders-Commercial-Short (All)']
    da = get_last_1000_candles(pair)
    da['time'] = pd.to_datetime(da.time)
    
    df_s = pd.merge_asof(da[['time', 'c']], df, on='time', direction='backward')
    df_s = df_s[['c', 'time', 'Dealer_Net_Position']]
    df_s['Dealer_EMA'] = df_s['Dealer_Net_Position'].ewm(span=5, adjust=False).mean()
    df_s['EMA_diff'] = df_s['Dealer_EMA'].diff()
    df_s['EMA_slope'] = df_s['EMA_diff'].ewm(span=5, min_periods=5).mean()
    df_s['EMA_slope'] = (df_s['EMA_slope'] - df_s['EMA_slope'].mean()) / df_s['EMA_slope'].std()
    df_s['EMA_encoded_cot'] = np.where(df_s.EMA_diff > 0, 1, 0)

    data = df_s.dropna()
    data['time'] = pd.to_datetime(data['time'])

    return data[['EMA_encoded_cot', 'time']]

def forex(pair):
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    pair_with_dash = pair
    pair_without_dash = pair.replace("_", "")
    url = f"https://www.myfxbook.com/community/outlook/{pair_without_dash}"
    scraper = cloudscraper.create_scraper()
    response = scraper.get(url, headers=headers)

    if response.status_code != 200:
        logging.error(f"Failed to fetch data for {pair}. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table_element = soup.select_one('#currentMetricsTable')
    if not table_element:
        return None

    try:
        rows = table_element.find_all("tr")
        short_data = long_data = {}

        for row in rows:
            cols = row.find_all("td")
            if len(cols) == 4:
                action = cols[0].text.strip().lower()
                percentage = float(cols[1].text.strip().replace('%', ''))
                lots = float(cols[2].text.strip().replace(' lots', '').replace(',', ''))
                positions = int(cols[3].text.strip().replace(',', ''))

                if action == "short":
                    short_data = {'short_percentage': percentage, 'Short Lots': lots, 'Short Positions': positions}
                elif action == "long":
                    long_data = {'long_percentage': percentage, 'Long Lots': lots, 'Long Positions': positions}

        now = datetime.utcnow()
        total = short_data.get('Short Positions', 0) + long_data.get('Long Positions', 0)
        market_direction = 1 if short_data.get('short_percentage', 0) > long_data.get('long_percentage', 0) else 0

        df = pd.DataFrame([{
            'time': now,
            'Currency': pair_with_dash,
            'Short_lots': short_data.get('Short Lots'),
            'Long_Lots': long_data.get('Long Lots'),
            'Short_Positions': short_data.get('Short Positions'),
            'Long_Positions': long_data.get('Long Positions'),
            'total': total,
            'short_percentage': short_data.get('short_percentage') / 100,
            'long_percentage': long_data.get('long_percentage') / 100,
            'market_direction': market_direction
        }])

        return df

    except Exception as e:
        logging.error(f"Error parsing sentiment data: {e}")
        return None

# MAIN LOOP
list_currency = [ "AUD_USD", "EUR_USD", "USD_JPY", "USD_MXN", "USD_ZAR", "USD_CHF", "USD_CAD", "GBP_USD", "NZD_USD"]

final_rows = []

try:
    db = PostgresSQL()
    for pair in list_currency:
        df1 = forex(pair)
        if df1 is None:
            continue

        df2 = cot_ema_df_returner(pair)
        df2['time'] = pd.to_datetime(df2['time'])

        df = pd.merge_asof(df1, df2, on='time')
        df['Currency'] = pair
        # Get last row as dictionary for PostgreSQL insert
        last_row_dict = df.iloc[-1].to_dict()
        db.InsertData("sentimentdata", last_row_dict)
        time.sleep(random.uniform(0.3, 1.5))
except Exception as e:
    logging.error(f"Error during processing: {e}")
