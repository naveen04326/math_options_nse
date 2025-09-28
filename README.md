# Nifty Algo Trading with DhanHQ + NSE Fallback

This project implements a Nifty strategy engine with three data priorities:
1. **DhanHQ API** (primary, live + historical)
2. **NSE Scraper** (fallback for missing/blocked data)
3. **Yahoo Finance** (last fallback, >5 years history)

Includes:
- Paper trading & live trading modes
- Order management
- Technical indicators (RSI, Stochastic, VWAP, CPR, MAs, Sigma levels)
- Streamlit dashboard for visualization & manual order placement

## 🚀 Usage
```bash
# Install dependencies
pip install -r requirements.txt

# Run app
streamlit run streamlit_app.py
