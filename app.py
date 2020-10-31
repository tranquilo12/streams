import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

from connections import Connections
from flask_caching import Cache

from multi_thread_streams import (
    get_multiple_distinct_col_values_from_equities_info,
    get_distinct_col_values_from_equities_info,
)
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
    value: list = None,
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
                    "font-size": "0rem" if hide_title else "1rem",
                    "font-weight": "0rem" if hide_title else "1rem",
                    "padding-left": "0.0" if hide_title else "1.0em",
                    "margin-top": "0.0" if hide_title else "1.0em",
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


equities_list = [val for val in res["symbol"] if "^" not in val]
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

chart_options_card = get_dropdown_card(
    card_title="Chart Type",
    dropdown_options=["Candlestick", "Close"],
    multi=False,
    value=["Close"],
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
        html.Div([html.H2("Streams", style={"padding": "0.1em"})]),
        dbc.Tabs(
            [
                dbc.Tab(
                    label="Overview",
                    tab_id="overview",
                    children=[
                        html.Div(
                            [
                                dbc.Row(
                                    [
                                        dbc.Col(choose_tickers_card, width=3),
                                        dbc.Col(choose_sectors_card, width=3),
                                        dbc.Col(choose_industry_card, width=3),
                                        dbc.Col(chart_options_card, width=3),
                                    ]
                                ),
                            ]
                        ),
                    ],
                    style={"padding": "2em"},
                ),
                dbc.Tab(
                    label="Technical Analysis",
                    tab_id="t-analysis",
                    children=[
                        html.Div([dbc.Row([dbc.Col(technical_indi_card, width=12)])])
                    ],
                    style={"padding": "2em"},
                ),
                dbc.Tab(
                    label="Projections",
                    tab_id="projections",
                    children=[
                        html.Div([dbc.Row([dbc.Col(transformations_card, width=12)])])
                    ],
                    style={"padding": "2em"},
                ),
            ],
            id="tabs",
            active_tab="overview",
            style={"border": "white", "primary": "gold", "background": "cornsilk"},
        ),
        html.Div(id="tab-content", className="p-4", style={"padding": "1em"}),
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


def get_candlestick_from_df(
    df: pd.DataFrame, ticker: str, color_scale: list
) -> go.Candlestick:
    """
    For each ticker, build a candlestick plot
    Args:
        ticker: ticker symbol
        df: the df that contains details for that ticker
        color_scale: determines the color of the chart

    Returns:
        A go.Candlestick object
    """

    candlestick = {
        "x": df.index,
        "open": df["open"],
        "high": df["high"],
        "low": df["low"],
        "close": df["close"],
        "type": "candlestick",
        "name": ticker,
        "legendgroup": ticker,
        "increasing": {"line": {"color": color_scale[0]}},
        "decreasing": {"line": {"color": color_scale[1]}},
    }
    return go.Candlestick(**candlestick)


def get_scatter_from_df(df: pd.DataFrame, ticker: str, color_scale: str) -> go.Scatter:
    """

    Args:
        ticker: ticker symbol
        df: the df that contains details for that ticker
        color_scale: determines the color of the chart

    Returns:
        A go.Scatter object
    """

    close = {
        "x": df.index,
        "y": df["close"],
        "mode": "lines",
        "line": {"width": 3, "color": color_scale},
        "name": ticker,
        "legendgroup": ticker,
    }
    return go.Scatter(**close)


def get_ta_from_df(
    df: pd.DataFrame, ticker: str, indicator: str, color_scale: str
) -> List[Union[go.Scatter, go.Candlestick]]:
    if indicator == "BBands":
        bb_bands = b_bands(df.close)
        res = [
            go.Scatter(
                **{
                    "x": df.index,
                    "y": y,
                    "mode": "lines",
                    "line": {"width": 1, "color": color_scale,},
                    "hoverinfo": "none",
                    "legendgroup": ticker,
                    "showlegend": True if i == 0 else False,
                    "name": f"{ticker} - bollinger bands",
                }
            )
            for i, y in enumerate(bb_bands)
        ]
    else:
        res = []
    return res


def get_overview_plots(
    df: pd.DataFrame,
    tickers: list,
    chart_type: list,
    technical_indicators: list,
    sectors: list = None,
    industries: list = None,
) -> List[Union[go.Scatter, go.Candlestick]]:

    all_traces = []
    color_scale = cl.scales["9"]["qual"]["Paired"]

    # # decide if tickers or sectors + industries is going to be used here
    if technical_indicators is None:
        technical_indicators = []

    for i, ticker in enumerate(tickers):

        dff = df[df["ticker"] == ticker]
        close_color_scale = color_scale[(i * 2) % len(color_scale)]

        candlestick_plots = get_candlestick_from_df(
            df=dff, ticker=ticker, color_scale=[color_scale[0], color_scale[1]]
        )
        close_plots = get_scatter_from_df(
            df=dff, ticker=ticker, color_scale=close_color_scale
        )
        bbands_plots = get_ta_from_df(
            df=dff, indicator="BBands", ticker=ticker, color_scale=close_color_scale
        )

        # always compute bollinger bands, and make bollinger traces along with that
        if "Candlestick" in chart_type:
            traces = [candlestick_plots]
            if "BBands" in technical_indicators:
                traces = [candlestick_plots] + bbands_plots

        elif "Close" in chart_type:
            traces = [close_plots]
            if "BBands" in technical_indicators:
                traces = [close_plots] + bbands_plots

        else:
            traces = [candlestick_plots]
            if "BBands" in technical_indicators:
                traces = [candlestick_plots] + bbands_plots

        all_traces += traces

    return all_traces


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


@app.callback(
    dash.dependencies.Output("tab-content", "children"),
    [
        dash.dependencies.Input("tabs", "active_tab"),
        dash.dependencies.Input("tickers-options", "value"),
        dash.dependencies.Input("industry-options", "value"),
        dash.dependencies.Input("sectors-options", "value"),
        dash.dependencies.Input("chart-type-options", "value"),
        dash.dependencies.Input("technical-indicators-options", "value"),
        dash.dependencies.Input("transformations-options", "value"),
    ],
)
def render_tab_content(
    active_tab: str = "overview",
    tickers: list = None,
    industries: list = None,
    sectors: list = None,
    chart_type: list = ["Close"],
    technical_indicators: list = ["BBands"],
    transformations: list = ["day-to-day returns"],
) -> list:
    """
    Renders interactively, tab content, based on the "active_tab" value.
    Args:
        active_tab: the value that determines which to be active
        tickers: all tickers that can be individually moved around
        industries: all industries (tab 1)
        sectors: all sectors (tab 1)
        chart_type: both Candlestick and Close (tab 1)
        technical_indicators: all TA-Lib indicators (tab 2)
        transformations: all the scale and linear transformations (tab 2)

    Returns:
        a tab-content div
    """
    if sectors is not None:
        sectors_tickers = get_multiple_distinct_col_values_from_equities_info(
            col_names=["symbol"],
            logger=conns.logger,
            ssh_conn_params=conns.ssh_conn_params,
            db_conn_params=conns.db_conn_params,
            filter_col="sector",
            filter_val=sectors,
        )
    else:
        sectors_tickers = {"symbol": []}

    if industries is not None:
        industries_tickers = get_multiple_distinct_col_values_from_equities_info(
            col_names=["symbol"],
            logger=conns.logger,
            ssh_conn_params=conns.ssh_conn_params,
            db_conn_params=conns.db_conn_params,
            filter_col="industry",
            filter_val=industries,
        )
        if len(sectors_tickers["symbol"]) > 0:
            industries_tickers = list(
                set(sectors_tickers).intersection(set(industries_tickers))
            )
        else:
            pass
    else:
        industries_tickers = {"symbol": []}

    if len(industries_tickers["symbol"]) > 0:
        tickers += industries_tickers["symbol"]
    else:
        tickers += sectors_tickers["symbol"]

    fmt = "%Y-%m-%d"
    conns.logger.info("Trying to get price...")
    df = get_price(
        tickers=tickers,
        timespan="day",
        start_date=(datetime.date.today() - datetime.timedelta(days=800)).strftime(fmt),
        end_date=datetime.date.today().strftime(fmt),
        ssh_conn_params=conns.ssh_conn_params,
        db_conn_params=conns.db_conn_params,
        logger=conns.logger,
    )

    graphs = []

    if len(tickers) < 1:
        graphs = [
            html.H3(
                "Select a stock ticker.", style={"marginTop": 20, "marginBottom": 20}
            )
        ]

    if active_tab == "overview":
        all_overview_traces = get_overview_plots(
            df=df,
            tickers=tickers,
            chart_type=chart_type,
            technical_indicators=technical_indicators,
            # industries=industries,
            # sectors=sectors,
        )
        fig = {
            "data": all_overview_traces,
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
                            {"step": "all", "label": "All"},
                        ]
                    },
                    "rangeslider": {"visible": True},
                },
            },
        }
        graphs = [
            dcc.Graph(id="Plot", figure=fig),
        ]

    if active_tab == "t-analysis":
        graphs = [
            html.H3("New Tab for Analysis", style={"marginTop": 20, "marginBottom": 20})
        ]

    if active_tab == "projections":
        graphs = [
            html.H3("New Tab for Analysis", style={"marginTop": 20, "marginBottom": 20})
        ]

    return graphs


if __name__ == "__main__":
    app.run_server(debug=True)
