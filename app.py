import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from connections import Connections
from flask_caching import Cache

from multi_thread_streams import get_equities_list
from multi_thread_streams import establish_ssh_tunnel

import plotly.express as px
import psycopg2
import colorlover as cl
import pandas as pd
import datetime
import logging
import os

# establish all these clients here, as everything is non forced to be
# non-object-oriented by dash.
conns = Connections()
conns.establish_rest_client()
conns.establish_redis_connection()

equities_list = get_equities_list(
    db_conn_params=conns.db_conn_params,
    ssh_conn_params=conns.ssh_conn_params,
    logger=conns.logger,
)

# Bootstrap has some nice additions
external_stylesheets = [
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    dbc.themes.BOOTSTRAP,
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets,)

# establish cache
CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_REDIS_URL": os.environ.get(
        "REDIS_URL", f"redis://:{conns.redis_conn_params['password']}@localhost:6379"
    ),
}
cache = Cache(app=app.server, config=CACHE_CONFIG)


app.layout = html.Div(
    [
        html.Div([html.H2("Explorer",),]),
        dcc.Dropdown(
            id="stock-ticker-input",
            options=[{"label": s, "value": s} for s in equities_list],
            value=["AAPL", "TSLA"],
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
    logger: logging.Logger,
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
    tickers = ",".join([f"'{tkr.strip()}'" for tkr in tickers])
    query = f"""SELECT t.ticker, t.timestamp, t.open, t.high, t.low, t.close, t.vwap
                FROM polygon_stocks_agg_candles t 
                WHERE t.timestamp BETWEEN '{start_date}' AND '{end_date}' 
                    AND t.ticker in ({tickers}) AND t.timespan = '{timespan}'
                ORDER BY t.timestamp;"""

    t = establish_ssh_tunnel(ssh_conn_params)
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
        logger.info(msg="Trying to parse the res...")
        df = pd.DataFrame.from_records(
            columns=["ticker", "timestamp", "open", "high", "low", "close", "vwap"],
            data=res,
            index="timestamp",
        )
    else:
        logger.info(msg="Cannot parse res...")
        df = None

    if t.is_alive | t.is_active:
        t.stop()

    logger.info(msg="Returning df...")
    return df


color_scale = cl.scales["9"]["qual"]["Paired"]


@app.callback(
    dash.dependencies.Output("graphs", "children"),
    [dash.dependencies.Input("stock-ticker-input", "value"),],
)
def update_graph(tickers):

    fmt = "%Y-%m-%d"
    start = (datetime.date.today() - datetime.timedelta(days=365)).strftime(fmt)
    end = datetime.date.today().strftime(fmt)
    conns.logger.info("Trying to get price...")
    df = get_price(
        tickers,
        "day",
        start,
        end,
        conns.ssh_conn_params,
        conns.db_conn_params,
        conns.logger,
    )

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
                "x": dff.index,
                "open": dff["open"],
                "high": dff["high"],
                "low": dff["low"],
                "close": dff["close"],
                "type": "candlestick",
                "name": ticker,
                "legendgroup": ticker,
                "increasing": {"line": {"color": color_scale[0]}},
                "decreasing": {"line": {"color": color_scale[1]}},
            }
            bb_bands = b_bands(dff.close)
            bollinger_traces = [
                {
                    "x": dff.index,
                    "y": y,
                    "type": "scatter",
                    "mode": "lines",
                    "line": {
                        "width": 1,
                        "color": color_scale[(i * 2) % len(color_scale)],
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
