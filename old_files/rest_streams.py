import functools
import pandas as pd
import numpy as np
import requests
import datetime
import psycopg2
import os
import ast
import asyncio
import aiopg
import aiohttp
from tqdm.auto import tqdm
from polygon import WebSocketClient, RESTClient, STOCKS_CLUSTER
from multiprocessing import Pool, Semaphore, Manager
from typing import Optional, Union
from connections import Connections

import paramiko
import sshtunnel
from sshtunnel import SSHTunnelForwarder

from all_sql import (
    insert_into_polygon_trades,
    insert_into_polygon_quotes,
    insert_into_polygon_agg,
    insert_into_polygon_stocks_bbo,
)


class RestStreams(Connections):
    def __init__(self):
        super().__init__()

        # self.all_equities_symbols_list = pd.read_sql(
        #     con=self.rds_engine, sql="SELECT symbol from equities_info;"
        # ).values

        # self.all_equities_symbols_list = list(self.all_equities_symbols_list.flatten())

        self.historic_nbbo_quotes_columns: dict = {
            "T": "ticker",
            "t": "sip_timestamp",
            "y": "exchange_timestamp",
            "f": "trf_timestamp",
            "q": "sequence_number",
            "c": "conditions",
            "i": "indicators",
            "p": "bid_price",
            "x": "bid_exchange_id",
            "s": "bid_size",
            "P": "ask_price",
            "X": "ask_exchange_id",
            "S": "ask_size",
            "z": "tape",
        }
        self.historic_agg_columns: dict = {
            "v": "volume",
            "vw": "vwap",
            "o": "open",
            "c": "close",
            "h": "high",
            "l": "low",
            "t": "timestamp",
            "n": "n_items",
        }

    async def rest_historic_nbbo_quotes(
        self, ticker: str, start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        """
        A super function, which iterates over each day
        :param ticker: eg AAPL
        :param start_date: will be converted to %Y-%m-%d
        :param end_date: will be converted to %Y-%m-%d
        :return: pd.DataFrame
        """
        all_dates = [
            date.strftime("%Y-%m-%d")
            for date in pd.bdate_range(start=start_date, end=end_date)
        ]

        all_res = []
        for date in tqdm(all_dates):
            try:
                res = await self.rest_client.historic_n___bbo_quotes_v2(
                    ticker, date
                ).results
                df_ = pd.DataFrame.from_records(res)
                all_res.append(df_)
            except requests.HTTPError as e:
                print(f"HTTP-Error: {e}")
                pass

        res_df = pd.concat(all_res)
        res_df = res_df.rename(columns=self.historic_nbbo_quotes_columns)

        for col in ["sip_timestamp", "exchange_timestamp"]:
            res_df.loc[:, col] = pd.to_datetime(res_df[col], unit="ns")

        return res_df


def request_stocks_equities_aggregates(
    sem,
    ticker: str,
    client: RESTClient,
    multiplier: int = 1,
    timespan: str = "day",
    from_: datetime.date = datetime.date.today() - datetime.timedelta(days=365),
    to: datetime.date = datetime.date.today(),
) -> [Optional[requests.Response], int]:
    """
    Only handles the stocks equities aggregates, and
    :param sem:
    :param ticker: goes to the func stocks_equities_aggregates
    :param client:
    :param multiplier: goes to the func stocks_equities_aggregates
    :param timespan: goes to the func stocks_equities_aggregates
    :param from_: goes to the func stocks_equities_aggregates, will be converted to string, asks for datetime
    :param to: goes to the func stocks_equities_aggregates, will be converted to string, asks for datetime
    :return: None
    """

    assert client is not None, "Why is rest_client None??"

    pid = os.getpid()

    with sem:
        print(f"Pid {pid}, is downloading for ticker: {ticker}")

        try:
            records = client.stocks_equities_aggregates(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_.strftime("%Y-%m-%d"),
                to=to.strftime("%Y-%m-%d"),
                params={},
            )

        except Exception as e:
            records = None
            print(f"Ticker : {ticker} not pulling down data, due to error: {e}")

    return records, pid


def batch(iterable: list, n: int) -> list:
    """
    Take an iterable and give back batches
    :param iterable: a list
    :param n: batch size
    :return: a list
    """
    l = len(iterable)
    for idx in range(0, l, n):
        yield iterable[idx : min(idx + n, l)]


def process_stocks_equities_aggregates(
    records: Optional[requests.Response],
    historic_agg_columns: dict,
    ticker: str,
    timespan: str,
    multiplier: int,
) -> [list, str]:
    """
    Make sure it's easy to decipher the processing, by separating the logic
    :param records:
    :param historic_agg_columns:
    :param ticker:
    :param timespan:
    :param multiplier:
    :return:
    """
    # tried of dealing with instances where the dfs columns dont match. If they dont match, and get a KeyError,
    # then just return None, None
    try:
        try:
            df_ = pd.DataFrame.from_records(records.results)
        except TypeError as e:
            return None, None

        df_ = df_.rename(columns=historic_agg_columns)
        df_["ticker"] = ticker
        df_["timespan"] = timespan
        df_["multiplier"] = multiplier

        # df_ = df_[["ticker"] + list(historic_agg_columns.values())]
        df_.loc[:, "timestamp"] = pd.to_datetime(df_["timestamp"], unit="ms")

        if "n_items" in df_.columns:
            df_ = df_.drop(columns=["n_items"], inplace=False)

        df_ = df_.replace({np.NaN: None})
        columns = df_.columns.tolist()
        batch_size = len(df_) // 10
        values = [[val for val in d.values()] for d in df_.to_dict(orient="records")]

        # turn the entire dataframe into a batch
        try:
            batched = [b for b in batch(values, n=batch_size)]
        except ValueError as e:
            return None, None

        # make the query template for this query, that will be inserted into the table
        all_cols_str = ",".join(columns)
        query_template = f"INSERT INTO polygon_stocks_agg_candles ({all_cols_str}) VALUES %s ON CONFLICT (ticker, timespan, multiplier, volume, timestamp) DO NOTHING"

    except KeyError as e:
        return None, None

    return batched, query_template


def establish_ssh_tunnel(ssh_conn_params: dict) -> SSHTunnelForwarder:
    try:
        t = SSHTunnelForwarder(**ssh_conn_params)
    except (
        sshtunnel.BaseSSHTunnelForwarderError,
        sshtunnel.HandlerSSHTunnelForwarderError,
    ) as e:
        t = None
        print(f"Error: {e}")
    return t


def insert_into_db(
    ssh_conn_params: dict,
    db_conn_params: dict,
    batched: list,
    query_template: str,
    ticker: str,
):
    """
    Take the stuff from process_stocks_equities_aggregates and push it ino the database
    :param ssh_conn_params:
    :param db_conn_params:
    :param batched:
    :param query_template:
    :param ticker:
    :return:
    """
    with psycopg2.connect(**db_conn_params) as conn:
        with conn.cursor() as cur:
            for b in batched:
                try:
                    psycopg2.extras.execute_values(cur, query_template, b)
                    conn.commit()
                except psycopg2.errors.InFailedSqlTransaction as e:
                    print(f"Insert did not work, ticker: {ticker}")
                    conn.rollback()
                    pass


def wrapper_func(
    # for request_stocks_equities_agg params
    ticker: str,
    client: RESTClient,
    # insert_into_db params
    ssh_conn_params: dict,
    db_conn_params: dict,
    sem: Semaphore,
    multiplier: int = 1,
    timespan: str = "day",
    from_: datetime.date = datetime.date.today() - datetime.timedelta(days=365),
    to: datetime.date = datetime.date.today(),
    # process_stocks_equities_aggregates
    historic_agg_columns=None,
) -> None:

    if historic_agg_columns is None:
        historic_agg_columns = {
            "v": "volume",
            "vw": "vwap",
            "o": "open",
            "c": "close",
            "h": "high",
            "l": "low",
            "t": "timestamp",
            "n": "n_items",
        }

    records, pid = request_stocks_equities_aggregates(
        sem=sem,
        ticker=ticker,
        client=client,
        multiplier=multiplier,
        timespan=timespan,
        from_=from_,
        to=to,
    )

    batched, query_template = process_stocks_equities_aggregates(
        records=records,
        historic_agg_columns=historic_agg_columns,
        ticker=ticker,
        timespan=timespan,
        multiplier=multiplier,
    )

    if (batched is not None) & (query_template is not None):
        insert_into_db(
            ssh_conn_params=ssh_conn_params,
            db_conn_params=db_conn_params,
            query_template=query_template,
            batched=batched,
            pid=pid,
            ticker=ticker,
        )
    else:
        print(f"ticker : {ticker} has returned None.")


CONCURRENT_DOWNLOADS = 100
CPU_NUM = 8

if __name__ == "__main__":

    manager = Manager()
    semaphore = manager.Semaphore(value=CONCURRENT_DOWNLOADS)

    stream = RestStreams()
    stream.establish_rest_client()

    rest_client = stream.rest_client
    ssh_conn_params = stream.ssh_conn_params
    db_conn_params = stream.db_conn_params

    tunnel = establish_ssh_tunnel(ssh_conn_params)
    tunnel.start()
    db_conn_params["port"] = int(tunnel.local_bind_port)

    # query the db for equities list
    query = "SELECT DISTINCT t.symbol FROM public.equities_info t;"
    with psycopg2.connect(**db_conn_params) as e_conn:
        with e_conn.cursor() as cur:
            cur.execute(query)
            equities_list = cur.fetchall()

    tunnel.close()

    if "tunnel" in locals():
        del tunnel

    equities_list = [val[0] for val in equities_list]
    equities_list = equities_list[4046:]

    results = []
    with Pool(processes=CPU_NUM) as pool:
        target = functools.partial(
            wrapper_func,
            sem=semaphore,
            client=rest_client,
            db_conn_params=db_conn_params,
            ssh_conn_params=ssh_conn_params,
        )
        for tkr in equities_list:
            results.append(
                pool.apply_async(func=target, kwds={"ticker": tkr}).get(timeout=100)
            )
