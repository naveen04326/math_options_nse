import os
from dhanhq import dhanhq
import pandas as pd
from datetime import datetime, timedelta

# -------------------------------
# HELPER: Get Nearest Weekly Expiry
# -------------------------------
def get_nearest_expiry():
    """
    Returns nearest Thursday expiry (weekly) for Nifty.
    """
    today = datetime.now().date()
    # move forward to the next Thursday (3 = Thursday)
    days_ahead = (3 - today.weekday()) % 7 
    if days_ahead == 0:  # If today is Thursday, keep today
        expiry = today
    else:
        expiry = today + timedelta(days=days_ahead)
    
    return expiry.strftime("%Y-%m-%d")


class DhanService:
    def __init__(self, client_id=None, access_token=None, access_key=None):
        self.client_id = client_id
        self.access_token = access_token
        self.access_key = access_key
        self.client = None
        # Only attempt connection if all three required credentials are provided
        if client_id and access_token and access_key:
            self.connect()

    # -------------------------------
    # CONNECT TO DHAN
    # -------------------------------
    def connect(self):
        # NOTE: Your existing project uses three arguments for dhanhq initialization
        self.client = dhanhq(self.client_id, self.access_token, self.access_key)
        return self.client

    # -------------------------------
    # PLACE ORDER (LIVE)
    # -------------------------------
    def place_order(self, identifier, qty, option_type, strike_price, price):
        """Place live order on Dhan. Returns: order_id"""
        if not self.client: return None
        order = self.client.place_order(
            security_id=identifier,
            exchange_segment="NSE_FNO",
            transaction_type="BUY" if option_type.upper() == "CALL" else "SELL",
            quantity=qty,
            price=price,
            order_type="LIMIT"
        )
        return order.get("orderId")

    # -------------------------------
    # EXIT ORDER
    # -------------------------------
    def exit_order(self, order_id):
        """Square off an open order."""
        if not self.client: return None
        return self.client.cancel_order(order_id)

    # -------------------------------
    # GET LTP
    # -------------------------------
    def get_ltp(self, identifier):
        """Fetch latest LTP for a given instrument."""
        if not self.client: return None
        quote = self.client.get_quote(identifier)
        return float(quote.get("ltp")) if quote and quote.get("ltp") else None

    # -------------------------------
    # GET OPTION CHAIN (NEW LOGIC)
    # -------------------------------
    def get_option_chain(self):
        """
        Fetch option chain for Nifty 50 from Dhan API.
        Returns DataFrame indexed by strike or an empty DataFrame on failure.
        """
        if not self.client:
            print("[DhanService] Client not connected. Cannot fetch OC.")
            return pd.DataFrame()

        NIFTY_SECURITY_ID = 13
        UNDERLYING_SEGMENT = "IDX_I" 
        
        try:
            expiry_date = get_nearest_expiry()

            resp = self.client.option_chain(
                under_security_id=NIFTY_SECURITY_ID,
                under_exchange_segment=UNDERLYING_SEGMENT,
                expiry=expiry_date
            )

            rows = []
            if resp and resp.get("status") == "success":
                oc_data = resp.get("data", {}).get("oc", {})

                for strike_str, data in oc_data.items():
                    strike = float(strike_str)
                    ce = data.get("ce", {})
                    pe = data.get("pe", {})

                    rows.append({
                        "strike": strike,
                        # Mapping Dhan keys to your required DataFrame columns
                        "Call_ODIN": ce.get("oi", 0),
                        "PUT_ODIN": pe.get("oi", 0),
                        "Call_OI_Diff": ce.get("changeInOi", 0), 
                        "PUT_OI_DIFF": pe.get("changeInOi", 0), 
                        "CALL_value_Bid": ce.get("bidPrice", 0),
                        "put_value_Bid": pe.get("bidPrice", 0),
                        "identifier_CE": ce.get("securityId"), 
                        "identifier_PE": pe.get("securityId"), 
                    })

                if rows:
                    df = pd.DataFrame(rows).set_index("strike")
                    df["time_stamp"] = resp.get("data", {}).get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    df["underlyingValue"] = resp.get("data", {}).get("spotPrice", None)
                    return df

        except Exception as e:
            print(f"[DhanService] Option chain fetch failed: {e}")
        
        return pd.DataFrame()