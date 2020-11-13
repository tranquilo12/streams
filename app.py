import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from flask_caching import Cache
from typing import List, Union
from scipy.signal import detrend
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import plotly.express as px
import colorlover as cl
import pandas as pd
import itertools
import psycopg2
import datetime
import logging
import os

from multi_thread_streams import (
    get_multiple_distinct_col_values_from_equities_info,
    get_distinct_col_values_from_equities_info,
    establish_ssh_tunnel,
)

from html_elements import (
    conns,
    choose_tickers_card,
    choose_industry_card,
    chart_options_card,
    choose_sectors_card,
    technical_indi_card,
    transformations_card,
    range_slider_layout,
)

from ta_elements import b_bands, min_max_scaler


# Bootstrap has some nice additions.
external_stylesheets = [dbc.themes.BOOTSTRAP]

# establish the app itself.
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# establish cache.
cache = Cache(app=server, config=conns.redis_cache_config)

# establish the app layout.
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
                    label="Forecasts",
                    tab_id="forecasts",
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


def get_plot_from_df(
    plot_type: str,
    df: pd.DataFrame,
    ticker: str,
    color_scale: list,
    col_name: str = "",
    mode: str = "",
    min_max_scale: bool = True,
) -> Union[go.Scatter, go.Bar, go.Candlestick]:
    """

    :param plot_type:
    :param df:
    :param ticker:
    :param color_scale:
    :param col_name:
    :param min_max_scale:
    :return:
    """
    assert plot_type in [
        "candlestick",
        "scatter",
        "bar",
    ], "'plot_type' must be in ['candlestick', 'scatter', 'bar']"

    col_name = "close" if col_name is "" else col_name

    if min_max_scale:
        y = min_max_scaler(close=df[col_name])
    else:
        y = df[col_name]

    if plot_type == "candlestick":
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
        ret = go.Candlestick(**candlestick)

    elif plot_type == "scatter":
        close = {
            "x": df.index,
            "y": y,
            "mode": "lines" if mode is "" else mode,
            "line": {"width": 3, "color": color_scale},
            "name": ticker if col_name is "" else col_name,
            "legendgroup": ticker,
        }
        ret = go.Scatter(**close)

    elif plot_type == "bar":
        close = {
            "x": df.index,
            "y": y,
            "name": f"{ticker}_returns" if col_name is "" else col_name,
            "legendgroup": ticker,
            "marker": {"color": color_scale},
        }
        ret = go.Bar(**close)

    else:
        ret = None

    return ret


def get_ta_from_df(
    df: pd.DataFrame,
    ticker: str,
    indicator: str,
    color_scale: str,
    detrend_switch: bool = False,
) -> List[Union[go.Scatter, go.Candlestick]]:
    """

    :param df:
    :param ticker:
    :param indicator:
    :param color_scale:
    :param detrend_switch:
    :return:
    """
    res = []
    if indicator == "BBands":
        bb_bands = b_bands(df.close)
        res = [
            go.Scatter(
                **{
                    "x": df.index,
                    "y": y,
                    "mode": "lines",
                    "line": {"width": 1, "color": color_scale},
                    "hoverinfo": "none",
                    "legendgroup": ticker,
                    "showlegend": True if i == 0 else False,
                    "name": f"{ticker} - bollinger bands",
                }
            )
            for i, y in enumerate(bb_bands)
        ]
    elif indicator == "returns":
        plots_min_max = [(f"{ticker}_returns", False)]
        df[f"{ticker}_returns"] = df["close"].pct_change()

        if detrend_switch:
            df[f"{ticker}_trend"] = df[f"{ticker}_returns"] - detrend(df[f"{ticker}_returns"].fillna(0.0))
            df[f"{ticker}_returns"] = df[f"{ticker}_returns"] - df[f"{ticker}_trend"]
            plots_min_max.append([f"{ticker}_trend", True])
        else:
            pass

        for i, (col_name, scaler) in enumerate(plots_min_max):
            color = color_scale[(i * 2) % len(color_scale)]
            plot = get_plot_from_df(
                plot_type="scatter",
                df=df,
                ticker=ticker,
                color_scale=color,
                col_name="",
                min_max_scale=scaler,
            )
            res.append(plot)
    else:
        res = []
    return res


def get_overview_plots(
    df: pd.DataFrame, tickers: list, chart_type: list, technical_indicators: list,
) -> List[Union[list, List[List[Union[go.Scatter, go.Candlestick]]]]]:
    """
    Get all these plots and
    :param df:
    :param tickers:
    :param chart_type:
    :param technical_indicators:
    :return:
    """

    all_traces = []
    returns_traces = []
    color_scale = cl.scales["9"]["qual"]["Paired"]
    color_scale_pairwise = list(
        itertools.combinations(iterable=color_scale, r=len(tickers))
    )

    # decide if tickers or sectors + industries is going to be used here
    if technical_indicators is None:
        technical_indicators = []

    for i, ticker in enumerate(tickers):
        dff = df[df["ticker"] == ticker]
        close_color_scale = color_scale[(i * 2) % len(color_scale)]
        candle_color_scale = color_scale_pairwise[i]

        if "Candlestick" in chart_type:
            candlestick_plot: go.Candlestick = get_plot_from_df(
                plot_type="candlestick",
                df=dff,
                ticker=ticker,
                color_scale=candle_color_scale,
            )
            traces = [candlestick_plot]

        elif "Close" in chart_type:
            scatter_plot: go.Scatter = get_plot_from_df(
                col_name="",
                plot_type="scatter",
                df=dff,
                ticker=ticker,
                color_scale=close_color_scale,
            )
            traces = [scatter_plot]

        else:
            bar_plot: go.Bar = get_plot_from_df(
                plot_type="bar",
                df=dff,
                ticker=ticker,
                color_scale=close_color_scale,
                col_name="",
                min_max_scale=True,
            )
            traces = [bar_plot]

        if "BBands" in technical_indicators:
            bbands_plots: list = get_ta_from_df(
                df=dff, indicator="BBands", ticker=ticker, color_scale=close_color_scale
            )
            traces += bbands_plots

        returns_traces += get_ta_from_df(
            df=dff,
            indicator="returns",
            ticker=ticker,
            color_scale=color_scale,
            detrend_switch=True,
        )

        all_traces += traces

    return [all_traces, returns_traces]


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
        # dash.dependencies.Input("technical-indicators-options", "value"),
        dash.dependencies.Input("transformations-options", "value"),
    ],
)
def render_tab_content(
    active_tab: str = "overview",
    tickers: list = None,
    industries: list = None,
    sectors: list = None,
    chart_type: list = None,
    technical_indicators: list = None,
    transformations: list = ["day-to-day returns"],
) -> list:
    """
    Renders interactively, tab content, based on the "active_tab" value.
    :param active_tab: the value that determines which to be active
    :param tickers: all tickers that can be individually moved around
    :param industries: all industries (tab 1)
    :param sectors: all sectors (tab 1)
    :param chart_type: both Candlestick and Close (tab 1)
    :param technical_indicators: all TA-Lib indicators (tab 2)
    :param transformations: all the scale and linear transformations (tab 2)
    :return: a tab-content div
    """

    if technical_indicators is None:
        technical_indicators = []

    if chart_type is None:
        chart_type = ["Close"]

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
        all_overview_traces, close_ret = get_overview_plots(
            df=df,
            tickers=tickers,
            chart_type=chart_type,
            technical_indicators=technical_indicators,
            # industries=industries,
            # sectors=sectors,
        )

        fig_1 = {"data": all_overview_traces, "layout": range_slider_layout}
        fig_2 = {"data": close_ret, "layout": range_slider_layout}

        graphs = [
            dcc.Graph(id="Plot1", figure=fig_1),
            html.Hr(),
            dcc.Graph(id="Plot2", figure=fig_2),
        ]

    if active_tab == "forecasts":
        graphs = [
            html.H3("New Tab for Analysis", style={"marginTop": 20, "marginBottom": 20})
        ]

    return graphs


if __name__ == "__main__":
    app.run_server(debug=True)
