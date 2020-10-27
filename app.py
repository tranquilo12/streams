import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from connections import Connections
from flask_cache import Cache

from multi_thread_streams import get_equities_list
from multi_thread_streams import establish_ssh_tunnel

import plotly.express as px
import colorlover as cl
import pandas as pd
import datetime
import logger
import os

# establish all these clients
conns = Connections()
conns.establish_rest_client()
conns.establish_redis_connection()

equities_list = get_equities_list(
    db_conn_params=conns.db_conn_params,
    ssh_conn_params=conns.ssh_conn_params,
    logger=conns.logger,
)

external_stylesheets = [
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    dbc.themes.BOOTSTRAP,
]

app = dash.Dash(
    __name__,
    assets_url_path="https://cdn.plot.ly/plotly-finance-1.28.0.min.js",
    external_stylesheets=external_stylesheets,
)

# establish cache
CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://localhost:6379"),
}
cache = Cache()
cache.init_app(app=app.server, config=CACHE_CONFIG)


app.layout = html.Div(
    [
        html.Div(
            [
                html.H2(
                    "Finance Explorer",
                    style={
                        "display": "inline",
                        "float": "left",
                        "font-size": "2.65em",
                        "margin-left": "7px",
                        "font-weight": "bolder",
                        "font-family": "Product Sans",
                        "color": "rgba(117, 117, 117, 0.95)",
                        "margin-top": "20px",
                        "margin-bottom": "0",
                    },
                ),
                html.Img(
                    src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe.png",
                    style={"height": "100px", "float": "right"},
                ),
            ]
        ),
        dcc.Dropdown(
            id="stock-ticker-input",
            options=[{"label": s, "value": s} for s in equities_list],
            value=["YHOO", "GOOGL"],
            multi=True,
        ),
        html.Div(id="graphs"),
        html.Div(id="signal", hidden=True),
    ],
    className="container",
)


def b_bands(price, window_size=10, num_of_std=5):
    rolling_mean = price.rolling(window=window_size).mean()
    rolling_std = price.rolling(window=window_size).std()
    upper_band = rolling_mean + (rolling_std * num_of_std)
    lower_band = rolling_mean - (rolling_std * num_of_std)
    return rolling_mean, upper_band, lower_band


@cache.memoize()
def get_price(
    tickers: list,
    timespan: str,
    start_date: str,
    end_date: str,
    ssh_conn_params: dict,
    db_conn_params: dict,
    logger: logger,
) -> pd.DataFrame:
    """
    Just make through the tunnel
    :param tickers: a list of tickers to fetch
    :param timespan: either 'day' or 'min'
    :param start_date: str, convert to format %Y-%m-%d
    :param end_date:  str, convert to format %Y-%m-%d
    :param ssh_conn_params:
    :param db_conn_params:
    :param logger:
    :return: just the dataframe
    """
    tickers = ",".join(tickers)
    query = f"""SELECT t.timestamp, t.open, t.high, t.low, t.close, t.vwap
                FROM crypto.public.polygon_stocks_agg_candles t 
                WHERE t.timestamp BETWEEN {start_date} AND {end_date} 
                    AND t.ticker in ({tickers}) AND t.timespan = '{timespan}'
                ORDER BY t.timestamp;"""

    t = establish_ssh_tunnel(ssh_conn_params=ssh_conn_params)
    t.daemon_transport = True
    t.daemon_forward_servers = True
    t.start()
    db_conn_params["port"] = int(t.local_bind_port)

    logger.info(msg="Getting prices...")
    try:
        with psycopg2.connect(**db_conn_params) as e_conn:
            with e_conn.cursor() as cur:
                logger.info(msg="Querying db...")
                cur.execute(query)
                logger.info(msg="Fetchall query...")
                res = cur.fetchall()

    except psycopg2.OperationalError as e:
        logger.error(msg=f"Psycopg2 Op Error: {e}")
        res = None

    if res is not None:
        df = pd.DataFrame.from_records(
            columns=["timestamp", "open", "high", "low", "close", "vwap"],
            data=res,
            index="timestamp",
        )
    else:
        df = None

    if t.is_alive | t.is_active:
        t.stop()

    return df


@app.callback(
    dash.dependencies.Output("signal", "children"),
    [dash.dependencies.Input("stock-ticker-input", "value")],
)
def compute_global_df(tickers):
    fmt = "%Y-%m-%d"
    start = (datetime.date.today() - datetime.timedelta(days=365)).strftime(fmt)
    end = datetime.date.today().strftime(fmt)
    print("Trying to get price...")
    get_price(
        tickers=tickers,
        timespan="day",
        start_date=start,
        end_date=end,
        db_conn_params=conns.db_conn_params,
        ssh_conn_params=conns.ssh_conn_params,
        logger=conns.logger
    )
    return tickers


color_scale = cl.scales["9"]["qual"]["Paired"]


@app.callback(
    dash.dependencies.Output("graphs", "children"),
    [
        dash.dependencies.Input("stock-ticker-input", "value"),
    ],
)
def update_graph(tickers):

    graphs = []

    if not tickers:
        graphs.append(
            html.H3(
                "Select a stock ticker.", style={"marginTop": 20, "marginBottom": 20}
            )
        )
    else:
        for i, ticker in enumerate(tickers):

            dff = df[df["ticker"] == ticker]

            candlestick = {
                "x": dff["timestamp"],
                "open": dff["open"],
                "high": dff["high"],
                "low": dff["low"],
                "close": dff["close"],
                "type": "candlestick",
                "name": ticker,
                "legendgroup": ticker,
                "increasing": {"line": {"color": colorscale[0]}},
                "decreasing": {"line": {"color": colorscale[1]}},
            }
            bb_bands = bbands(dff.Close)
            bollinger_traces = [
                {
                    "x": dff["timestamp"],
                    "y": y,
                    "type": "scatter",
                    "mode": "lines",
                    "line": {
                        "width": 1,
                        "color": colorscale[(i * 2) % len(colorscale)],
                    },
                    "hoverinfo": "none",
                    "legendgroup": ticker,
                    "showlegend": True if i == 0 else False,
                    "name": "{} - bollinger bands".format(ticker),
                }
                for i, y in enumerate(bb_bands)
            ]
            graphs.append(
                dcc.Graph(
                    id=ticker,
                    figure={
                        "data": [candlestick] + bollinger_traces,
                        "layout": {
                            "margin": {"b": 0, "r": 10, "l": 60, "t": 0},
                            "legend": {"x": 0},
                        },
                    },
                )
            )

    return graphs


if __name__ == "__main__":
    app.run_server(debug=True)
