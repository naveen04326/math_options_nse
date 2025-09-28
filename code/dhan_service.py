import os
from dhanhq import dhanhq


class DhanService:
    def __init__(self, client_id=None, access_token=None, access_key=None):
        self.client_id = client_id
        self.access_token = access_token
        self.access_key = access_key
        self.client = None
        if client_id and access_token and access_key:
            self.connect()

    # -------------------------------
    # CONNECT TO DHAN
    # -------------------------------
    def connect(self):
        self.client = dhanhq(self.client_id, self.access_token, self.access_key)
        return self.client

    # -------------------------------
    # PLACE ORDER (LIVE)
    # -------------------------------
    def place_order(self, identifier, qty, option_type, strike_price, price):
        """
        Place live order on Dhan.
        :returns: order_id
        """
        # Example (adjust according to dhanhq API structure)
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
        """
        Square off an open order.
        """
        return self.client.cancel_order(order_id)

    # -------------------------------
    # GET LTP
    # -------------------------------
    def get_ltp(self, identifier):
        """
        Fetch latest LTP for a given instrument.
        """
        quote = self.client.get_quote(identifier)
        return float(quote.get("ltp")) if quote else None
