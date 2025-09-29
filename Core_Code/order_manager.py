import os
import pandas as pd
import threading
import time
from datetime import datetime

from Core_Code.nse_data_fetch import get_option_data   # ✅ fallback for paper trades

# Path to assets folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)

TRADE_LOG_PATH = os.path.join(ASSETS_DIR, "trade_log.csv")


class OrderManager:
    def __init__(self, dhan=None):
        """
        :param dhan: DhanService instance (must expose place_order, exit_order, get_ltp)
        """
        self.dhan = dhan
        self.open_trades = {}  # active trades {identifier: trade dict}
        self._lock = threading.Lock()

    # -------------------------------
    # PAPER TRADE
    # -------------------------------
    def paper_trade(self, identifier, qty, option_type, strike_price, entry_price):
        entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        trade = {
            "Mode": "PAPER",
            "Date": entry_time.split()[0],
            "Entry Time": entry_time,
            "Exit Time": None,
            "Option": f"{strike_price} {option_type}",
            "Type": option_type.upper(),
            "Qty": qty,
            "Entry Price": float(entry_price),
            "Exit Price": None,
            "P/L": None,
            "Identifier": str(identifier),
            "OrderID": None,
            "StrikePrice": strike_price,   # ✅ store for fallback lookup
        }

        with self._lock:
            self.open_trades[str(identifier)] = trade

        threading.Thread(target=self._monitor_trade, args=(str(identifier),), daemon=True).start()
        return trade

    # -------------------------------
    # LIVE TRADE
    # -------------------------------
    def live_trade(self, identifier, qty, option_type, strike_price, entry_price):
        """Place a live order via Dhan API and monitor TP/SL"""
        if not self.dhan:
            raise ValueError("Dhan API client not provided")

        order_id = self.dhan.place_order(identifier, qty, option_type, strike_price, entry_price)

        entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade = {
            "Mode": "LIVE",
            "Date": entry_time.split()[0],
            "Entry Time": entry_time,
            "Exit Time": None,
            "Option": f"{strike_price} {option_type}",
            "Type": option_type.upper(),
            "Qty": qty,
            "Entry Price": float(entry_price),
            "Exit Price": None,
            "P/L": None,
            "Identifier": str(identifier),
            "OrderID": order_id,
            "StrikePrice": strike_price,
        }

        with self._lock:
            self.open_trades[str(identifier)] = trade

        threading.Thread(target=self._monitor_trade, args=(str(identifier),), daemon=True).start()
        return trade

    # -------------------------------
    # MONITOR TP/SL
    # -------------------------------
    def _monitor_trade(self, identifier):
        """Check every minute for +13% profit / -6% loss"""
        while True:
            with self._lock:
                if identifier not in self.open_trades:
                    break
                trade = self.open_trades[identifier]

            ltp = None
            try:
                # Prefer Dhan LTP if available
                if self.dhan:
                    ltp = self.dhan.get_ltp(identifier)

                # Fallback to NSE option chain if no LTP from Dhan (esp. for paper trades)
                if not ltp:
                    oi_df = get_option_data()
                    strike = trade["StrikePrice"]
                    if trade["Type"] == "CALL" and strike in oi_df.index:
                        ltp = oi_df.loc[strike, "CALL_value_Bid"]
                    elif trade["Type"] == "PUT" and strike in oi_df.index:
                        ltp = oi_df.loc[strike, "put_value_Bid"]

            except Exception as e:
                print(f"[Monitor] Failed to fetch LTP for {identifier}: {e}")
                time.sleep(60)
                continue

            if not ltp or not trade["Entry Price"]:
                time.sleep(60)
                continue

            change_pct = ((ltp - trade["Entry Price"]) / trade["Entry Price"]) * 100

            if change_pct >= 13 or change_pct <= -6:
                self.close_trade(identifier, ltp)
                break

            time.sleep(60)

    # -------------------------------
    # CLOSE TRADE
    # -------------------------------
    def close_trade(self, identifier, exit_price):
        with self._lock:
            if identifier not in self.open_trades:
                return None

            trade = self.open_trades[identifier]
            trade["Exit Time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            trade["Exit Price"] = float(exit_price)

            if trade["Type"] == "CALL":
                trade["P/L"] = (trade["Exit Price"] - trade["Entry Price"]) * trade["Qty"]
            else:  # PUT
                trade["P/L"] = (trade["Entry Price"] - trade["Exit Price"]) * trade["Qty"]

            if trade["Mode"] == "LIVE" and self.dhan:
                try:
                    self.dhan.exit_order(trade["OrderID"])
                except Exception as e:
                    print(f"[Close Trade] Error closing live order: {e}")

            self._append_to_log(trade)
            del self.open_trades[identifier]

        return trade

    # -------------------------------
    # LOGGING
    # -------------------------------
    def _append_to_log(self, trade):
        df = pd.DataFrame([trade])
        if not os.path.exists(TRADE_LOG_PATH):
            df.to_csv(TRADE_LOG_PATH, index=False)
        else:
            df.to_csv(TRADE_LOG_PATH, mode="a", index=False, header=False)