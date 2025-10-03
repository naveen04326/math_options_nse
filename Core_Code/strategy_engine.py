# code/strategy_engine.py
import os
import time
import threading
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
import plotly.io as pio
import plotly.graph_objects as go
import requests
import logging
from Core_Code.nse_data_fetch import get_nifty_hist_data, get_option_data_from_nse, get_nifty_live_nse   
from Core_Code.dhan_service import DhanService
from Core_Code.dhan_service import DhanService
from Core_Code.order_manager import OrderManager

# -----------------------------
# Assets and logging setup
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)

LOG_FILE = os.path.join(ASSETS_DIR, "strategy_engine.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("StrategyEngine")

# Paths inside assets
NIFTY_PICKLE = os.path.join(ASSETS_DIR, "nifty_data.pickle")
TEMP_OI_PICKLE = os.path.join(ASSETS_DIR, "temp_OI_data.pickle")
OI_RUNNING_PICKLE = os.path.join(ASSETS_DIR, "OI_RUNNING_data.pickle")
OI_DATA_PLOT = os.path.join(ASSETS_DIR, "OI_DATA_Plot.jpg")
VWAP_PLOT = os.path.join(ASSETS_DIR, "VWAP_Plot.jpg")
NIFTY_CHART_PLOT = os.path.join(ASSETS_DIR, "Nifty_chart_plot.jpg")
DONE_SIGNAL = os.path.join(ASSETS_DIR, "done_signal.txt")
PARAMS_FILE = os.path.join(ASSETS_DIR, "params_table.csv")
CREDENTIALS_FILE = os.path.join(ASSETS_DIR, "credentials.txt")

# -----------------------------
# Globals
# -----------------------------
_runner_thread = None
_runner_stop_event = None
_dhan_service = None
_order_manager = None
_last_tick_time = None
_live_mode = False
# Load params table if exists
if os.path.exists(PARAMS_FILE):
    try:
        params_table_nifty = pd.read_csv(PARAMS_FILE, index_col="Prams")
    except Exception:
        params_table_nifty = pd.DataFrame(columns=["Bear%ge", "Bull%ge"])
else:
    params_table_nifty = pd.DataFrame(columns=["Bear%ge", "Bull%ge"])


# -----------------------------
# StrategyEngine
# -----------------------------
class StrategyEngine:
    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy().reset_index(drop=True)
        if "Close" not in df.columns and "close" in df.columns:
            df.rename(columns={"close": "Close"}, inplace=True)
        if "Close" not in df.columns:
            raise ValueError("DataFrame must have 'Close' column")

        df["Vwap"] = df["Close"].rolling(10, min_periods=1).mean()
        df["Rolling_Vwap"] = df["Close"].rolling(20, min_periods=1).mean()
        std20 = df["Close"].rolling(20, min_periods=1).std().fillna(0)
        df["Upper_Bound"] = df["Vwap"] + 2 * std20
        df["Lower_Bound"] = df["Vwap"] - 2 * std20
        df["7MVA"] = df["Close"].rolling(7, min_periods=1).mean()
        df["Stoc_Signal"] = np.sign(df["Close"].diff().fillna(0)).astype(int)

        delta = df["Close"].diff().fillna(0)
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ru = up.rolling(14, min_periods=1).mean()
        rd = down.rolling(14, min_periods=1).mean()
        rs = ru / (rd.replace(0, 1e-6))
        df["RSI"] = 100 - (100 / (1 + rs))
        return df

    def day_today_params(self, hist_df: pd.DataFrame, live_snapshot: dict):
        if hist_df is None or hist_df.empty:
            return "Bullish 50.0"
        try:
            today_open = float(live_snapshot.get("OPEN", None))
        except Exception:
            return "Bullish 50.0"

        dp = hist_df.tail(1).copy().reset_index(drop=True)
        for col in ["Vwap", "Rolling_Vwap", "Upper_Bound", "Lower_Bound", "7MVA"]:
            if col not in dp.columns:
                dp = self.add_indicators(hist_df).tail(1).reset_index(drop=True)

        dp["Open2prevVwap"] = np.where(today_open > dp["Vwap"], "YES", "NO")
        dp["Open2prevRVwap"] = np.where(today_open > dp["Rolling_Vwap"], "YES", "NO")
        dp["open2uprbond"] = np.where(today_open > dp["Upper_Bound"], "YES", "NO")
        dp["open2lwrbond"] = np.where(today_open < dp["Lower_Bound"], "YES", "NO")
        dp["7-14MVA"] = np.where(today_open > dp["7MVA"], "YES", "NO")

        dp["Prams"] = dp.apply(
            lambda x: x["Open2prevVwap"]
            + x["Open2prevRVwap"]
            + x["open2uprbond"]
            + x["open2lwrbond"]
            + x["7-14MVA"],
            axis=1,
        )
        pr = dp["Prams"].iloc[0]

        if pr in params_table_nifty.index:
            row = params_table_nifty.loc[pr]
            try:
                bear = float(row["Bear%ge"])
                bull = float(row["Bull%ge"])
            except Exception:
                return "Bullish 50.0"
            return f"Bearish {bear:.2f}" if bear > 50 else f"Bullish {bull:.2f}"
        return "Bullish 50.0"

    def calculate_trend(self, arr):
        if len(arr) < 2:
            return "insufficient data"
        z = np.polyfit(range(len(arr)), arr, 1)
        return "up" if z[0] > 0 else "down"


# -----------------------------
# Helpers
# -----------------------------
def init_services(client_id=None, access_token=None, access_key=None):
    global _dhan_service, _order_manager
    if client_id and access_token:
        try:
            _dhan_service = DhanService(client_id, access_token)
        except Exception as e:
            logger.error(f"DhanService init failed: {e}")
            _dhan_service = None
    else:
        _dhan_service = None
    _order_manager = OrderManager(_dhan_service)


def get_nifty_live():
    """
    Get live Nifty data.
    Priority: Dhan API -> NSE API (get_nifty_live_nse).
    Returns a pd.Series with keys: OPEN, HIGH, LOW, LTP, Volume, Date, load_time
    """

    # ---- Try Dhan API first ----
    if _dhan_service is not None:
        try:
            quote = _dhan_service.get_quote("NSE:NIFTY50")
            if quote:
                row = {
                    "OPEN": float(quote.get("open", 0)),
                    "HIGH": float(quote.get("high", 0)),
                    "LOW": float(quote.get("low", 0)),
                    "LTP": float(quote.get("lastPrice", quote.get("ltp", 0))),
                    "Volume": float(quote.get("volume", 0)),
                    "Date": datetime.now().strftime("%d-%m-%Y"),
                    "load_time": datetime.now().strftime("%H:%M:%S"),
                }
                return pd.Series(row)
        except Exception as e:
            print(f"[get_nifty_live] Dhan API failed, trying NSE fallback: {e}")

    # ---- Fallback: NSE live API ----
    try:
        nse_row = get_nifty_live_nse()
        if not nse_row.empty:
            row = {
                "OPEN": float(nse_row.get("OPEN", 0)),
                "HIGH": float(nse_row.get("HIGH", 0)),
                "LOW": float(nse_row.get("LOW", 0)),
                "LTP": float(nse_row.get("LTP", 0)),
                "Volume": float(nse_row.get("Volume", 0)),
                "Date": nse_row.get("Date", datetime.now().strftime("%d-%m-%Y")),
                "load_time": nse_row.get("load_time", datetime.now().strftime("%H:%M:%S")),
            }
            return pd.Series(row)
    except Exception as e:
        print(f"[get_nifty_live] NSE fetch also failed: {e}")

    # ---- If both fail ----
    return pd.Series()


def adding_indicators(df):
    return StrategyEngine().add_indicators(df)


def get_option_data():
    global _dhan_service
    MAX_RETRIES = 2      # Total attempts = 1 (initial) + 2 (retries) = 3
    RETRY_DELAY = 120    # 2 minutes in seconds

    for attempt in range(MAX_RETRIES + 1):
        try:
            # 1. Try Dhan API (if initialized)
            if _dhan_service:
                df = _dhan_service.get_option_chain()
                if not df.empty:
                    logger.info(f"[Data Fetch] Success via Dhan API on attempt {attempt + 1}.")
                    return df

            # 2. Fallback to NSE scrape
            # NOTE: Assuming this is the scraping function defined/imported elsewhere
            df = get_option_data_from_nse() 
            if not df.empty:
                logger.info(f"[Data Fetch] Success via NSE Scrape on attempt {attempt + 1}.")
                return df

        except Exception as e:
            # Log the error but continue to the retry check
            logger.error(f"[Data Fetch ERROR] Attempt {attempt + 1} failed: {e}")
        
        # If both attempts failed or an exception occurred, check for retry
        if attempt < MAX_RETRIES:
            logger.warning(f"[Data Fetch] Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
        
    # If all attempts fail, return an empty DataFrame and log the final failure
    logger.error("[Data Fetch] All attempts failed after retries. Returning empty data.")
    return pd.DataFrame()


def nifty_Chart(df):
    fig = go.Figure()
    if "EOD_TIMESTAMP" in df.columns and "Close" in df.columns:
        fig.add_trace(go.Scatter(x=df["EOD_TIMESTAMP"], y=df["Close"], name="Close"))
    return fig


def get_OIDATA_Graph(oi_df, nifty_df):
    fig1 = go.Figure()
    if "Data_diff" in oi_df.columns:
        fig1.add_trace(go.Scatter(x=oi_df.index, y=oi_df["Data_diff"], name="Data_diff"))
    fig2 = go.Figure()
    if "Vwap" in nifty_df.columns:
        fig2.add_trace(go.Scatter(x=nifty_df.index, y=nifty_df["Vwap"], name="VWAP"))
    return fig1, fig2


def enter_order(identifier, price, strike_type, strike_price, live_mode=False):
    if not _order_manager:
        return None
    if live_mode: 
        return _order_manager.live_trade(identifier, 2, strike_type, strike_price, price) #Change LOT Size later
    else:
        return _order_manager.paper_trade(identifier, 2, strike_type, strike_price, price) #Change LOT Size later based on Risk Amount

# -----------------------------
# Runner Loop
# -----------------------------
def run_loop(client_id=None, access_token=None, access_key=None, stop_event=None, live_mode=False):
    global _last_tick_time, _live_mode
    _live_mode = live_mode
    init_services(client_id, access_token, access_key)
    engine = StrategyEngine()

    try:
        nifty_hist_data = get_nifty_hist_data()
    except Exception:
        nifty_hist_data = pd.DataFrame()

    columns = ["Open", "High", "Low", "Close", "Volume", "EOD_TIMESTAMP"]
    nifty_today = nifty_hist_data.loc[:, columns].tail(18).copy() if not nifty_hist_data.empty else pd.DataFrame(columns=columns)
    nifty_today["load_time"] = ""
    oi_running = pd.DataFrame()
    window_size = 5

    while not (stop_event and stop_event.is_set()):
        now = datetime.now().time()
        if now >= datetime.strptime("09:26", "%H:%M").time() and now <= datetime.strptime("15:25", "%H:%M").time():
            try:
                df_temp = get_nifty_live()
                _last_tick_time = datetime.now().strftime("%H:%M:%S")
                temp_df = pd.DataFrame(
                    [
                        {
                            "Open": df_temp["OPEN"],
                            "High": df_temp["HIGH"],
                            "Low": df_temp["LOW"],
                            "Close": df_temp["LTP"],
                            "Volume": df_temp["Volume"],
                            "EOD_TIMESTAMP": df_temp["Date"],
                            "load_time": df_temp["load_time"],
                        }
                    ]
                )
                nifty_today = pd.concat([nifty_today, temp_df], ignore_index=True)
                nifty_today = adding_indicators(nifty_today)

                temp_oi = get_option_data()
                put_sum = float(temp_oi["PUT_OI_DIFF"].sum())
                call_sum = float(temp_oi["Call_OI_Diff"].sum())
                new_row = {
                    "PUT_OI_DIFF_CUM": put_sum,
                    "Call_OI_DIFF_CUM": call_sum,
                    "Data_diff": put_sum - call_sum,
                    "PCR": put_sum / call_sum if call_sum else float("inf"),
                }

                try:
                    cs = int(temp_oi["Call_ODIN"].idxmax())
                    new_row["CALL_ODIN_MAX"] = f"{cs} {temp_oi.loc[cs, 'Call_ODIN']}"
                except Exception:
                    new_row["CALL_ODIN_MAX"] = "0 0"
                try:
                    ps = int(temp_oi["PUT_ODIN"].idxmax())
                    new_row["PUT_ODIN_MAX"] = f"{ps} {temp_oi.loc[ps, 'PUT_ODIN']}"
                except Exception:
                    new_row["PUT_ODIN_MAX"] = "0 0"

                new_row["Time_stamp"] = temp_oi["time_stamp"].iloc[0]
                new_row["underlying"] = temp_oi["underlyingValue"].iloc[0]
                new_row["Vwap"] = nifty_today.iloc[-1]["Vwap"]

                # Decision
                call_parts = new_row["CALL_ODIN_MAX"].split()
                put_parts = new_row["PUT_ODIN_MAX"].split()
                try:
                    if float(put_parts[1]) - float(call_parts[1]) > 0 and new_row["Data_diff"] > 0 and new_row["PCR"] > 1.25:
                        new_row["Decision"] = "CALL"
                    elif float(put_parts[1]) - float(call_parts[1]) < 0 and new_row["Data_diff"] < 0 and new_row["PCR"] < 0.75:
                        new_row["Decision"] = "PUT"
                    else:
                        new_row["Decision"] = "NEUTRAL"
                except Exception:
                    new_row["Decision"] = "NEUTRAL"

                # Day params + trend
                new_row["day_today"] = engine.day_today_params(nifty_hist_data, {"OPEN": float(temp_df["Open"].iloc[-1])})
                new_row["trend_data"] = ""
                oi_running = pd.concat([oi_running, pd.DataFrame([new_row])], ignore_index=True)
                temp1 = pd.to_numeric(oi_running["Data_diff"], errors="coerce")
                if len(temp1) >= window_size:
                    oi_running.at[oi_running.index[-1], "trend_data"] = engine.calculate_trend(temp1[-window_size:])

                # Save artifacts
                with open(NIFTY_PICKLE, "wb") as f:
                    pickle.dump(nifty_today, f)
                with open(TEMP_OI_PICKLE, "wb") as f:
                    pickle.dump(temp_oi, f)
                with open(OI_RUNNING_PICKLE, "wb") as f:
                    pickle.dump(oi_running, f)

                try:
                    fig_nifty = nifty_Chart(nifty_today)
                    fig_oi, fig_vwap = get_OIDATA_Graph(oi_running, nifty_today)
                    pio.write_image(fig_oi, OI_DATA_PLOT)
                    pio.write_image(fig_vwap, VWAP_PLOT)
                    pio.write_image(fig_nifty, NIFTY_CHART_PLOT)
                except Exception:
                    pass

                with open(DONE_SIGNAL, "w") as f:
                    f.write("done")

                # Order window
                if datetime.now().time() >= datetime.strptime("11:26", "%H:%M").time() and datetime.now().time() <= datetime.strptime("14:25", "%H:%M").time():
                    if _order_manager and not _order_manager.order_flag:
                        last_idx = -1
                        cond_call = (
                            oi_running["Decision"].iloc[last_idx] == "CALL"
                            and oi_running["trend_data"].iloc[last_idx] == "up"
                            and str(oi_running["day_today"].iloc[last_idx]).startswith("Bullish")
                            and int(nifty_today["Stoc_Signal"].tail(1).iloc[0]) == 1
                        )
                        cond_put = (
                            oi_running["Decision"].iloc[last_idx] == "PUT"
                            and oi_running["trend_data"].iloc[last_idx] == "down"
                            and str(oi_running["day_today"].iloc[last_idx]).startswith("Bearish")
                            and int(nifty_today["Stoc_Signal"].tail(1).iloc[0]) == -1
                        )
                        option_type = "CALL" if cond_call else "PUT" if cond_put else None
                        if option_type:
                            try:
                                if option_type == "CALL":
                                    strike = int(call_parts[0])
                                    identifier = temp_oi.loc[strike]["identifier_CE"]
                                    price = temp_oi.loc[strike]["CALL_value_Bid"]
                                else:
                                    strike = int(put_parts[0])
                                    identifier = temp_oi.loc[strike]["identifier_PE"]
                                    price = temp_oi.loc[strike]["put_value_Bid"]
                                enter_order(identifier, price, option_type, strike, live_mode=_live_mode)
                                _order_manager.order_flag = True
                                logger.info(f"Order placed: {option_type} {strike} @ {price}")
                            except Exception as e:
                                logger.error(f"Order placement failed: {e}")

                # Close at 15:00
                if datetime.now().time() >= datetime.strptime("15:00", "%H:%M").time():
                    if _order_manager and _order_manager.open_trades:
                        for ident, trade in list(_order_manager.open_trades.items()):
                            try:
                                parts = trade["Option"].split()
                                strike = int(parts[0])
                                typ = trade["Type"]
                                price = temp_oi.loc[strike]["CALL_value_Bid"] if typ == "CALL" else temp_oi.loc[strike]["put_value_Bid"]
                                _order_manager.close_paper_trade(ident, price)
                                logger.info(f"Order closed: {typ} {strike} @ {price}")
                            except Exception:
                                _order_manager.close_paper_trade(ident, None)
                        _order_manager.order_flag = False

            except Exception as e:
                logger.error(f"Loop error: {e}")
                time.sleep(5)

            time.sleep(300)
        else:
            time.sleep(60)


## -----------------------------
# Runner Wrappers
# -----------------------------

_runner_thread = None
_runner_stop_event = None

def _runner_target(client_id, access_token, access_key, stop_event, live_mode):
    run_loop(client_id, access_token, access_key, stop_event, live_mode)

def start_runner(client_id=None, access_token=None, access_key=None, live_mode=False):
    global _runner_thread, _runner_stop_event

    # If thread is already running, return
    if _runner_thread and _runner_thread.is_alive():
        return

    # --- Market time checks ---
    now = datetime.now()
    market_open = now.replace(hour=9, minute=26, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if now < market_open:
        sleep_secs = int((market_open - now).total_seconds())
        sleep_mins = sleep_secs // 60
        logger.info(f"Runner will sleep for {sleep_mins} minutes till 09:26 AM to RUN …")
        time.sleep(sleep_secs)
    elif now >= market_close:
        logger.info("Markets are closed. Runner will not start.")
        return

    # --- Start runner thread ---
    _runner_stop_event = threading.Event()
    _runner_thread = threading.Thread(
        target=_runner_target,
        args=(client_id, access_token, access_key, _runner_stop_event, live_mode),
        daemon=True,
    )
    _runner_thread.start()
    logger.info(f"Runner thread started (live_mode={live_mode})")


def stop_runner():
    global _runner_thread, _runner_stop_event
    if _runner_thread and _runner_thread.is_alive():
        _runner_stop_event.set()
        _runner_thread = None
        logger.info("Runner thread stopped")


def is_runner_running():
    return _runner_thread is not None and _runner_thread.is_alive()


def get_last_tick_time():
    return _last_tick_time