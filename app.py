import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

from connections import Connections
from flask_caching import Cache

from multi_thread_streams import get_multiple_distinct_col_values_from_equities_info
from multi_thread_streams import establish_ssh_tunnel

from typing import List, Union
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
# conns.establish_redis_connection()

res = get_multiple_distinct_col_values_from_equities_info(
    ssh_conn_params=conns.ssh_conn_params,
    db_conn_params=conns.db_conn_params,
    logger=conns.logger,
    col_names=["symbol", "sector", "industry"],
)

# Bootstrap has some nice additions
external_stylesheets = [
    dbc.themes.BOOTSTRAP,
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# establish cache
CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_REDIS_URL": os.environ.get(
        "REDIS_URL", f"redis://:{conns.redis_conn_params['password']}@localhost:6379"
    ),
}
cache = Cache(app=server, config=CACHE_CONFIG)


def get_dropdown_card(
    card_title: str,
    dropdown_options: list,
    value: list = [],
    multi: bool = False,
    hide_title: bool = False,
) -> dbc.Card:
    """
    A low-level card generator, that can be inserted into any div
    :param card_title: Will be displayed at the top left of card
    :param dropdown_options: all options to be presented, as a list
    :param hide_title: sometimes, you dont want the title to be displayed, but id is tied to title
    :param multi: can be multi select ?
    :param value: instantiated value
    :return: dbc.Card object
    """
    card = dbc.Card(
        [
            html.H5(
                card_title,
                className="" if hide_title else "card-title",
                style={
                    "padding": "0.0em" if hide_title else "0.3em",
                    "visibility": "hidden" if hide_title else "visible",
                    "font-size": "orem" if hide_title else "1rem",
                },
            ),
            dbc.CardBody(
                [
                    dcc.Dropdown(
                        id="-".join(card_title.lower().split(" ") + ["options"]),
                        options=[
                            {
                                "label": val,
                                "value": val.replace("-", "_").replace(" ", "_"),
                            }
                            for val in dropdown_options
                        ],
                        value=value,
                        multi=multi,
                    ),
                ]
            ),
        ],
        style={"padding-bottom": "1em"},
    )
    return card


equities_list = [val[0] for val in res["symbol"] if "^" not in val[0]]
equities_list = [val for val in equities_list if "." not in val]
choose_tickers_card = get_dropdown_card(
    card_title="Tickers",
    hide_title=False,
    dropdown_options=equities_list,
    multi=True,
    value=["AAPL", "TSLA"],
)

choose_sectors_card = get_dropdown_card(
    card_title="Sectors", hide_title=False, dropdown_options=res["sector"], multi=False,
)

choose_industry_card = get_dropdown_card(
    card_title="Industry",
    hide_title=False,
    dropdown_options=res["industry"],
    multi=False,
)

graph_options_card = get_dropdown_card(
    card_title="Chart Type", dropdown_options=["Candlestick", "Close"], multi=False
)

technical_indi_card = get_dropdown_card(
    card_title="Technical Indicators",
    dropdown_options=["BBands", "SRSI", "RSI", "SUP-5", "RES-5"],
    multi=True,
)

transformations_card = get_dropdown_card(
    card_title="Transformations",
    dropdown_options=[
        "day-to-day returns",
        "day-to-first returns",
        "normalize",
        "standardize",
    ],
    multi=False,
)

app.layout = html.Div(
    [
        html.Div([html.H2("streams", style={"padding": "0.5em"})]),
        dbc.Row(
            [
                dbc.Col(choose_tickers_card, width=4),
                dbc.Col(choose_industry_card, width=4),
                dbc.Col(choose_sectors_card, width=4),
            ],
            style={"padding": "0.5em"},
        ),
        dbc.Row(
            [
                dbc.Col(graph_options_card, width=4),
                dbc.Col(technical_indi_card, width=4),
                dbc.Col(transformations_card, width=4),
            ],
            style={"padding": "0.5em"},
        ),
        html.Div(id="graphs", style={"padding": "1em"}),
    ],
    className="container-fluid",
)


def b_bands(price: pd.Series, window_size: int = 10, num_of_std: int = 5):
    rolling_mean = price.rolling(window=window_size).mean()
    rolling_std = price.rolling(window=window_size).std()
    upper_band = rolling_mean + (rolling_std * num_of_std)
    lower_band = rolling_mean - (rolling_std * num_of_std)
    return rolling_mean, upper_band, lower_band


def cal_returns(price: pd.Series, day_on_day: bool = True, day_to_first: bool = False):
    res = None

    if day_on_day:
        res = np.log(price).diff()

    if day_to_first:
        res = np.log(price) - np.log(price.iloc[0])

    return res


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
    db_conn_params["port"] = 5433  # int(t.local_bind_port)

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
        dash.dependencies.Input("tickers-options", "value"),
        dash.dependencies.Input("chart-type-options", "value"),
        dash.dependencies.Input("technical-indicators-options", "value"),
        dash.dependencies.Input("transformations-options", "value"),
    ],
)
def update_graph(
    tickers, graph_type_options, tech_indi_options, transformations_options
):

    if graph_type_options is None:
        graph_type_options = ["Close"]

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

            if "Candlestick" in graph_type_options:
                traces = [candlestick_scatter]
                if "BBands" in tech_indi_options:
                    traces = [candlestick_scatter] + bollinger_scatters

            elif "Close" in graph_type_options:
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
                                "step": "year",
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
