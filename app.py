import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

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
    dbc.themes.BOOTSTRAP,
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# establish cache
CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_REDIS_URL": os.environ.get(
        "REDIS_URL", f"redis://:{conns.redis_conn_params['password']}@localhost:6379"
    ),
}
cache = Cache(app=app.server, config=CACHE_CONFIG)


app.layout = dbc.Container(
    [
        html.Div([html.H2("Explorer", style={"padding": "1em"})]),
        html.Div(
            [
                dcc.Dropdown(
                    id="stock-ticker-input",
                    options=[{"label": s, "value": s} for s in equities_list],
                    value=["AAPL", "TSLA"],
                    multi=True,
                ),
            ],
            style={"padding": "1em"},
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            html.H5(
                                "Graph Type",
                                className="card-title",
                                style={"padding": "1em"},
                            ),
                            dbc.CardBody(
                                [
                                    dcc.Dropdown(
                                        id="graph-type-options",
                                        options=[
                                            {
                                                "label": "CandleStick",
                                                "value": "CandleStick",
                                            },
                                            {
                                                "label": "Close Only",
                                                "value": "Close Only",
                                            },
                                        ],
                                        multi=False,
                                    ),
                                ]
                            ),
                        ],
                    ),
                    width=4,
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            html.H5(
                                "Technical Indicators",
                                className="card-title",
                                style={"padding": "1em"},
                            ),
                            dbc.CardBody(
                                [
                                    dcc.Dropdown(
                                        id="technical-indicators-options",
                                        options=[
                                            {"label": "BBands", "value": "BBands"}
                                        ],
                                        multi=True,
                                    ),
                                ]
                            ),
                        ]
                    ),
                    width=4,
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            html.H5(
                                "Transformations",
                                className="card-title",
                                style={"padding": "1em"},
                            ),
                            dbc.CardBody(
                                [
                                    dcc.Dropdown(
                                        id="transformations-options",
                                        options=[
                                            {"label": "day-on-day returns", "value": "day_on_day_returns"},
                                            {"label": "day-to-first returns", "value": "day_to_first_returns"}
                                        ],
                                        multi=True,
                                    ),
                                ]
                            ),
                        ]
                    ),
                    width=4,
                ),
            ]
        ),
        html.Div(id="graphs", style={"padding": "1em"}),
    ],
    className="container",
)


def b_bands(price: pd.Series, window_size: int = 10, num_of_std: int = 5):
    rolling_mean = price.rolling(window=window_size).mean()
    rolling_std = price.rolling(window=window_size).std()
    upper_band = rolling_mean + (rolling_std * num_of_std)
    lower_band = rolling_mean - (rolling_std * num_of_std)
    return rolling_mean, upper_band, lower_band


def cal_returns(price: pd.Series, day_on_day: bool = True, day_to_first: bool = False):
    if day_on_day:
        return np.log(price).diff()

    if day_to_first:
        return np.log(price) - np.log(price.iloc[0])


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
    [
        dash.dependencies.Input("stock-ticker-input", "value"),
        dash.dependencies.Input("graph-type-options", "value"),
        dash.dependencies.Input("technical-indicators-options", "value"),
        dash.dependencies.Input("transformations-options", "value"),
    ],
)
def update_graph(
    tickers, graph_type_options, tech_indi_options, transformations_options
):

    if graph_type_options is None:
        graph_type_options = ["Close Only"]

    if tech_indi_options is None:
        tech_indi_options = []

    if transformations_options is None:
        transformations_options = []

    fmt = "%Y-%m-%d"
    start = (datetime.date.today() - datetime.timedelta(days=800)).strftime(fmt)
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
        all_traces = []
        for i, ticker in enumerate(tickers):

            dff = df[df["ticker"] == ticker]

            # different types of output for the graph, that can be toggled with the
            # radio buttons
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
            candlestick_scatter = go.Candlestick(**candlestick)

            close_only = {
                "x": dff.index,
                "y": dff["close"],
                "mode": "lines",
                "line": {"width": 3, "color": color_scale[(i * 2) % len(color_scale)],},
                "name": ticker,
                "legendgroup": ticker,
            }
            close_only_scatter = go.Scatter(**close_only)

            # always compute bollinger bands, and make bollinger traces along with that
            bb_bands = b_bands(dff.close)
            bollinger_scatters = [
                go.Scatter(
                    **{
                        "x": dff.index,
                        "y": y,
                        "mode": "lines",
                        "line": {
                            "width": 1,
                            "color": color_scale[(i * 2) % len(color_scale)],
                        },
                        "hoverinfo": "none",
                        "legendgroup": ticker,
                        "showlegend": True if i == 0 else False,
                        "name": f"{ticker} - bollinger bands",
                    }
                )
                for i, y in enumerate(bb_bands)
            ]

            if "CandleStick" in graph_type_options:
                traces = [candlestick_scatter]
                if "BBands" in tech_indi_options:
                    traces = [candlestick_scatter] + bollinger_scatters

            elif "Close Only" in graph_type_options:
                traces = [close_only_scatter]
                if "BBands" in tech_indi_options:
                    traces = [close_only_scatter] + bollinger_scatters

            else:
                traces = [candlestick_scatter]
                if "BBands" in tech_indi_options:
                    traces = [candlestick_scatter] + bollinger_scatters

            all_traces += traces

        fig = {
            "data": all_traces,
            "layout": {
                "margin": {"b": 0, "r": 10, "l": 60, "t": 0},
                "legend": {"x": 1},
                "xaxis": {
                    "rangeselector": {
                        "buttons": [
                            {
                                "count": 1,
                                "label": "1m",
                                "step": "month",
                                "stepmode": "backward",
                            },
                            {
                                "count": 3,
                                "label": "3m",
                                "step": "month",
                                "stepmode": "backward",
                            },
                            {
                                "count": 6,
                                "label": "6m",
                                "step": "month",
                                "stepmode": "backward",
                            },
                            {
                                "count": 1,
                                "label": "YTD",
                                "step": "month",
                                "stepmode": "todate",
                            },
                            {"step": "all"},
                        ]
                    },
                    "rangeslider": {"visible": True},
                },
            },
        }

        graphs = [
            dcc.Graph(id="Plot", figure=fig),
        ]

    return graphs


if __name__ == "__main__":
    app.run_server(debug=True)
