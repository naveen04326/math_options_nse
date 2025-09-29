import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from hyper.contrib import HTTP20Adapter   # ✅ correct replacement for httpx


# ------------------------------
# Headers (mimic real browser)
# ------------------------------
BASE_URL = 'https://www.nseindia.com/option-chain'

def get_adjusted_headers(mount_url):
    return {
        'authority': 'www.nseindia.com',
        'method': 'GET',
        'path': '/',
        'scheme': 'https',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Referer': mount_url,
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }


# ------------------------------
# Fetch cookies (mimic browser session)
# ------------------------------
def fetch_cookies(mount_url):
    while True:
        try:
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=5,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount('https://', adapter)
            session.mount('http://', adapter)

            response = session.get(mount_url, timeout=90, headers=get_adjusted_headers(mount_url))

            if response.status_code != requests.codes.ok:
                raise ValueError(f"Request failed with status code {response.status_code}. Please try again in a minute.")

            return response.cookies.get_dict()

        except Exception as e:
            print(f"An error occurred in fetch_cookies: {e} " + datetime.now().time().strftime("%H:%M:%S"))
            time.sleep(60)
            continue


# ------------------------------
# Fetch historical Nifty data
# ------------------------------
def fetch_url_hist_nifty(mount_url, url, cookies3):
    try:
        session = requests.session()
        session.mount(mount_url, HTTP20Adapter())  # ✅ HTTP/2 adapter from hyper
        response = session.get(url, timeout=120, headers=get_adjusted_headers(mount_url), cookies=cookies3)

        if response.status_code == requests.codes.ok:
            data = response.json()

            p1 = pd.DataFrame(data['data']['indexCloseOnlineRecords']).set_index('EOD_TIMESTAMP')
            p1.index = pd.to_datetime(p1.index, format="%d-%b-%Y")

            p2 = pd.DataFrame(data['data']["indexTurnoverRecords"]).set_index('HIT_TIMESTAMP')
            p2.index = pd.to_datetime(p2.index, format="%d-%m-%Y")
            p2.index.name = 'EOD_TIMESTAMP'

            result = p1.join(p2[['HIT_TRADED_QTY', 'HIT_TURN_OVER']],
                             lsuffix='_left', rsuffix='_right', how='inner')

            return result.reset_index()
        else:
            print("Response not received in fetch_url_hist_nifty =>", response.status_code)
            return pd.DataFrame()

    except Exception as e:
        print(f"An error occurred in fetch_url_hist_nifty: {e} " + datetime.now().time().strftime("%H:%M:%S"))
        time.sleep(60)
        return pd.DataFrame()


# ------------------------------
# Orchestrator for fetching all years
# ------------------------------
def get_nifty_hist_data():
    nifty_hist_data = pd.DataFrame()
    first_day = datetime.date(datetime.now())
    cutoff_date = datetime.strptime("14-Jan-2021", "%d-%b-%Y").date()
    i = 0
    J = first_day.year - cutoff_date.year + 1
    mount_url = 'https://www.nseindia.com/reports-indices-historical-index-data'

    while i < J:
        if first_day < cutoff_date:
            break
        last_day = first_day - timedelta(365)

        if last_day < cutoff_date:
            last_day = cutoff_date

        cookies3 = fetch_cookies(mount_url)
        url_hist_nifty = (
            f"https://www.nseindia.com/api/historical/indicesHistory?"
            f"indexType=NIFTY%2050&from={last_day.strftime('%d-%m-%Y')}&to={first_day.strftime('%d-%m-%Y')}"
        )

        temp_data = fetch_url_hist_nifty(mount_url, url_hist_nifty, cookies3)
        nifty_hist_data = pd.concat([nifty_hist_data, temp_data], ignore_index=True)

        first_day = first_day - timedelta(365)
        i += 1

    if nifty_hist_data.empty:
        return pd.DataFrame(columns=['index', 'Open', 'High', 'Low', 'Close', 'Volume', 'EOD_TIMESTAMP'])

    nifty_hist_data['EOD_TIMESTAMP'] = pd.to_datetime(nifty_hist_data['EOD_TIMESTAMP'], format='%d-%m-%Y')
    nifty_hist_data = nifty_hist_data.sort_values(by='EOD_TIMESTAMP', ascending=True)

    nifty_hist_data = nifty_hist_data.rename(columns={
        'EOD_INDEX_NAME': 'index',
        'EOD_OPEN_INDEX_VAL': 'Open',
        'EOD_HIGH_INDEX_VAL': 'High',
        'EOD_LOW_INDEX_VAL': 'Low',
        'EOD_CLOSE_INDEX_VAL': 'Close',
        'HIT_TRADED_QTY': 'Volume'
    })

    # Replace 0 with rolling mean
    nifty_hist_data["Volume"] = nifty_hist_data["Volume"].replace(0, np.NaN)
    nifty_hist_data["Volume"] = nifty_hist_data["Volume"].fillna(
        nifty_hist_data["Volume"].rolling(3).mean().shift().ffill()
    )

    # Drop extra cols
    for col in ['TIMESTAMP', '_id']:
        if col in nifty_hist_data.columns:
            nifty_hist_data.drop(col, axis=1, inplace=True)

    return nifty_hist_data
