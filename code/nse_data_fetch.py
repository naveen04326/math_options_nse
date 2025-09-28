# code/nse_data_fetch.py
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException
import time
import os

# Helper function to construct headers
def get_adjusted_headers(mount_url):
    return {
        'authority': 'www.nseindia.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'referer': mount_url,
        'accept': 'application/json,text/html,*/*'
    }

def fetch_cookies(mount_url):
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=3, status_forcelist=[429,500,502,503,504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    try:
        res = session.get(mount_url, timeout=30, headers=get_adjusted_headers(mount_url))
        res.raise_for_status()
        return res.cookies.get_dict()
    except RequestException:
        time.sleep(1)
        return {}

def fetch_url_hist_nifty(mount_url, url, cookies3):
    session = requests.Session()
    try:
        res = session.get(url, timeout=60, headers=get_adjusted_headers(mount_url), cookies=cookies3)
        if res.status_code == 200:
            data = res.json()
            p1 = pd.DataFrame(data['data']['indexCloseOnlineRecords']).set_index('EOD_TIMESTAMP')
            p1.index = pd.to_datetime(p1.index, format="%d-%b-%Y", errors='coerce')
            return p1.reset_index()
    except Exception:
        return None
    return None

def get_nifty_hist_data():
    """
    Fetch historical NIFTY50 index data up to a cutoff date (default 01-Jan-2013 ~10+ years).
    Writes/returns a DataFrame standardized with columns Open/High/Low/Close/Volume/EOD_TIMESTAMP.
    """
    nifty_hist_data = pd.DataFrame()
    today = datetime.now().date()
    cutoff = datetime.strptime("01-Jan-2013", "%d-%b-%Y").date()
    mount_url = 'https://www.nseindia.com/reports-indices-historical-index-data'

    while today > cutoff:
        last_day = max(today - timedelta(days=365), cutoff)
        cookies3 = fetch_cookies(mount_url)
        url_hist = f"https://www.nseindia.com/api/historical/indicesHistory?indexType=NIFTY%2050&from={last_day.strftime('%d-%m-%Y')}&to={today.strftime('%d-%m-%Y')}"
        temp = fetch_url_hist_nifty(mount_url, url_hist, cookies3)
        if temp is not None and not temp.empty:
            nifty_hist_data = pd.concat([nifty_hist_data, temp], ignore_index=True)
        today = last_day

    if not nifty_hist_data.empty:
        nifty_hist_data = nifty_hist_data.rename(columns={
            'EOD_INDEX_NAME': 'index',
            'EOD_OPEN_INDEX_VAL': 'Open',
            'EOD_HIGH_INDEX_VAL': 'High',
            'EOD_LOW_INDEX_VAL': 'Low',
            'EOD_CLOSE_INDEX_VAL': 'Close',
            'HIT_TRADED_QTY': 'Volume'
        })
        if 'EOD_TIMESTAMP' in nifty_hist_data.columns:
            nifty_hist_data['EOD_TIMESTAMP'] = pd.to_datetime(nifty_hist_data['EOD_TIMESTAMP'], errors='coerce')
        if 'Volume' in nifty_hist_data.columns:
            nifty_hist_data['Volume'] = nifty_hist_data['Volume'].replace(0, np.nan)
            nifty_hist_data['Volume'] = nifty_hist_data['Volume'].fillna(nifty_hist_data['Volume'].rolling(3).mean().shift().ffill())
        nifty_hist_data = nifty_hist_data.sort_values(by='EOD_TIMESTAMP', ascending=True).reset_index(drop=True)
    return nifty_hist_data