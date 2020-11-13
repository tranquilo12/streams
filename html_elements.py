from connections import Connections
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from multi_thread_streams import (
    get_multiple_distinct_col_values_from_equities_info,
    get_distinct_col_values_from_equities_info,
    establish_ssh_tunnel,
)

# establish all these clients here, as everything is forced to be non-object-oriented by dash.
conns = Connections()
conns.establish_rest_client()


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


res = get_multiple_distinct_col_values_from_equities_info(
    ssh_conn_params=conns.ssh_conn_params,
    db_conn_params=conns.db_conn_params,
    logger=conns.logger,
    col_names=["symbol", "sector", "industry"],
)

equities_list = [val for val in res["symbol"] if ("^" not in val) | ("." not in val)]
industries_list = res["industry"]
sectors_list = res["sector"]

choose_tickers_card = get_dropdown_card(
    card_title="Tickers",
    hide_title=False,
    dropdown_options=equities_list,
    multi=True,
    value=["AAPL", "TSLA"],
)

choose_sectors_card = get_dropdown_card(
    card_title="Sectors", hide_title=False, dropdown_options=sectors_list, multi=False,
)

choose_industry_card = get_dropdown_card(
    card_title="Industry",
    hide_title=False,
    dropdown_options=industries_list,
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

range_slider_layout = {
    "margin": {"b": 0, "r": 10, "l": 60, "t": 0},
    "legend": {"x": 1},
    "xaxis": {
        "rangeselector": {
            "buttons": [
                {"count": 1, "label": "1m", "step": "month", "stepmode": "backward",},
                {"count": 3, "label": "3m", "step": "month", "stepmode": "backward",},
                {"count": 6, "label": "6m", "step": "month", "stepmode": "backward",},
                {"count": 1, "label": "YTD", "step": "year", "stepmode": "todate",},
                {"step": "all", "label": "All"},
            ]
        },
        "rangeslider": {"visible": True},
    },
}
