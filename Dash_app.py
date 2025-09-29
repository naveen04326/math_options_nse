# Dash_app.py
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import socket
import os
import pickle
import json

# Import modules from code package
from Core_Code.dhan_service import DhanService
from Core_Code.nse_data_fetch import get_nifty_hist_data
from Core_Code.strategy_engine import start_runner, stop_runner, is_runner_running, init_services
from Core_Code.order_manager import OrderManager

# PROJECT PATHS
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(SCRIPT_DIR, "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)

CRED_FILE = os.path.join(ASSETS_DIR, "credentials.txt")

# Helper to read/write credentials
def read_credentials():
    creds = {"client_id": "", "access_token": "", "access_key": ""}
    if os.path.exists(CRED_FILE):
        with open(CRED_FILE, "r") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    creds[k.strip()] = v.strip()
    return creds

def write_credentials(client_id, access_token, access_key):
    os.makedirs(ASSETS_DIR, exist_ok=True)
    with open(CRED_FILE, "w") as f:
        f.write(f"client_id={client_id or ''}\n")
        f.write(f"access_token={access_token or ''}\n")
        f.write(f"access_key={access_key or ''}\n")

# Data files (in assets)
DATA_FILES = {
    "nifty_data": os.path.join(ASSETS_DIR, "nifty_data.pickle"),
    "temp_OI_data": os.path.join(ASSETS_DIR, "temp_OI_data.pickle"),
    "OI_RUNNING_data": os.path.join(ASSETS_DIR, "OI_RUNNING_data.pickle"),
}
IMAGE_FILES = [
    os.path.join(ASSETS_DIR, "Nifty_chart_plot.jpg"),
    os.path.join(ASSETS_DIR, "OI_DATA_Plot.jpg"),
    os.path.join(ASSETS_DIR, "VWAP_Plot.jpg"),
]
TRADE_LOG = os.path.join(ASSETS_DIR, "paper_trades.csv")

# Initialize app
app = dash.Dash(__name__)
app.title = "Nifty Options Strategy Dashboard"

# Preload creds to pre-fill inputs
creds = read_credentials()

app.layout = html.Div([
    html.H1("📈 Nifty Options Strategy Dashboard", style={'textAlign': 'center'}),

    # Credentials and runner controls
    html.Div([
        dcc.Input(id='input-client-id', placeholder='Dhan Client ID', value=creds.get('client_id', ''), style={'marginRight': '8px'}),
        dcc.Input(id='input-access-token', placeholder='Dhan Access Token', type='password', value=creds.get('access_token', ''), style={'marginRight': '8px'}),
        dcc.Input(id='input-access-key', placeholder='Dhan Access Key', type='password', value=creds.get('access_key', ''), style={'marginRight': '8px'}),
        html.Button('💾 Save Credentials', id='save-creds-btn', n_clicks=0, style={'marginRight': '8px'}),
        html.Button('▶️ Start Runner', id='start-runner-btn', n_clicks=0, style={'marginRight': '8px'}),
        html.Button('⏹ Stop Runner', id='stop-runner-btn', n_clicks=0),
    ], style={'padding': '10px', 'border': '1px solid #ddd', 'marginBottom': '12px'}),

    html.Div(id='save-creds-status', style={'marginTop': '6px', 'color': 'green'}),

    html.Div([
        html.H3(id='status-message', children="Status: Idle", style={'color': 'blue'}),
    ], style={'padding': '8px', 'border': '1px solid #eee', 'marginBottom': '16px'}),

    html.Div([
        html.Button('🔄 Refresh Data', id='refresh-button', n_clicks=0),
    ], style={'marginBottom': '12px'}),

    html.H2("Data Tables"),
    html.Div(id='data-tables-container'),

    html.H2("Plots"),
    html.Div(id='image-plots-container'),

    html.H2("Paper Trades Log"),
    html.Div(id='trade-log-container')
])


@app.callback(
    Output('save-creds-status', 'children'),
    Input('save-creds-btn', 'n_clicks'),
    State('input-client-id', 'value'),
    State('input-access-token', 'value'),
    State('input-access-key', 'value'),
    prevent_initial_call=True
)
def save_creds(n_clicks, client_id, access_token, access_key):
    try:
        write_credentials(client_id, access_token, access_key)
        return "✅ Credentials saved to assets/credentials.txt"
    except Exception as e:
        return f"❌ Save failed: {e}"


@app.callback(
    Output('status-message', 'children'),
    [
        Input('start-runner-btn', 'n_clicks'),
        Input('stop-runner-btn', 'n_clicks'),
        Input('refresh-button', 'n_clicks')
    ],
    [
        State('input-client-id', 'value'),
        State('input-access-token', 'value'),
        State('input-access-key', 'value'),
    ]
)
def control_runner(start_clicks, stop_clicks, refresh_clicks, client_id, access_token, access_key):
    ctx = dash.callback_context
    if not ctx.triggered:
        return "Status: Idle"
    btn = ctx.triggered[0]['prop_id'].split('.')[0]
    if btn == 'start-runner-btn':
        if not client_id or not access_token:
            return "Status: Provide client_id and access_token before starting."
        # initialize services in strategy module and start runner
        try:
            init_services(client_id, access_token, access_key or "")
            start_runner(client_id, access_token, access_key or "")
            return "Status: Runner started."
        except Exception as e:
            return f"Status: Failed to start runner: {e}"
    elif btn == 'stop-runner-btn':
        try:
            stop_runner()
            return "Status: Stop requested."
        except Exception as e:
            return f"Status: Stop failed: {e}"
    else:
        running = is_runner_running()
        return f"Status: Runner running = {running}"


def load_pickle(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'rb') as f:
            return pickle.load(f)
    except Exception:
        return None


@app.callback(
    [Output('data-tables-container', 'children'),
     Output('image-plots-container', 'children'),
     Output('trade-log-container', 'children')],
    Input('refresh-button', 'n_clicks')
)
def refresh(n_clicks):
    nifty = load_pickle(DATA_FILES['nifty_data'])
    temp_oi = load_pickle(DATA_FILES['temp_OI_data'])
    oi_run = load_pickle(DATA_FILES['OI_RUNNING_data'])

    tables = []
    def make_table(df, title):
        if df is None or (hasattr(df, 'empty') and df.empty):
            return html.Div(f"No data for {title}", style={'marginBottom': '12px'})
        # show last 20 rows
        return html.Div([
            html.H4(title),
            dash_table.DataTable(
                columns=[{"name": c, "id": c} for c in df.columns],
                data=df.tail(20).to_dict('records'),
                page_size=10,
                style_table={'overflowX': 'auto'}
            )
        ], style={'marginBottom': '20px'})

    tables.append(make_table(nifty, "Nifty Historical Data"))
    tables.append(make_table(temp_oi, "Temp OI Data"))
    tables.append(make_table(oi_run, "OI Running Data"))

    images = []
    for p in IMAGE_FILES:
        if os.path.exists(p):
            images.append(html.Img(src=p, style={'width': '85%', 'maxWidth': '1000px', 'marginBottom': '12px'}))
        else:
            images.append(html.Div(f"Image not found: {os.path.basename(p)}"))

    # trade log CSV preview
    trade_log_path = os.path.join(os.path.dirname(__file__), "assets", "paper_trades.csv")
    trade_table = html.Div("No trade log found.")
    if os.path.exists(trade_log_path):
        try:
            import pandas as pd
            df = pd.read_csv(trade_log_path)
            trade_table = html.Div([
                html.H4("Paper Trades (last 50)"),
                dash_table.DataTable(
                    columns=[{"name": c, "id": c} for c in df.columns],
                    data=df.tail(50).to_dict('records'),
                    page_size=10,
                    style_table={'overflowX': 'auto'}
                )
            ])
        except Exception:
            trade_table = html.Div("Failed to load trade log.")

    return tables, images, trade_table


if __name__ == '__main__':
    local_ip = socket.gethostbyname(socket.gethostname())
    print(f"Dash app running on: http://{local_ip}:8050")
    app.run_server(debug=False, host='0.0.0.0', port=8050, use_reloader=False)
