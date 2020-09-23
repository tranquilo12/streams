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
from polygon import WebSocketClient, RESTClient, AsyncRESTClient, STOCKS_CLUSTER
from multiprocessing import Pool, Semaphore, Manager
from typing import Optional, Union
from connections import Connections
from all_sql import (
    insert_into_polygon_trades,
    insert_into_polygon_quotes,
    insert_into_polygon_agg,
    insert_into_polygon_stocks_bbo,
)


class PostgresConnector(object):
    def __init__(self, db_url):
        self.db_url = db_url
        self.pool = self.init_pool()

    def init_pool(self):
        cpus = multiprocessing.cpu_count()
        return multiprocessing.Pool(cpus, initializer=self.init_connection(self.db_url))

    @classmethod
    def init_connection(cls, db_url):
        def _init_connection():
            LOGGER.info("Creating Postgres engine")
            cls.engine = create_engine(db_url)

        return _init_connection

    def run_parallel_queries(self, queries):
        results = []
        try:
            for i in self.pool.imap_unordered(self.execute_parallel_query, queries):
                results.append(i)
        except Exception as exception:
            LOGGER.error(
                "Error whilst executing %s queries in parallel: %s",
                len(queries),
                exception,
            )
            raise
        finally:
            pass
            # self.pool.close()
            # self.pool.join()

        LOGGER.info(
            "Parallel query ran producing %s sets of results of type: %s",
            len(results),
            type(results),
        )

        return list(chain.from_iterable(results))

    def execute_parallel_query(self, query):
        with self.engine.connect() as conn:
            with conn.begin():
                result = conn.execute(query)
                return result.fetchall()

    def __getstate__(self):
        # this is a hack, if you want to remove this method, you should
        # remove self.pool and just pass pool explicitly
        self_dict = self.__dict__.copy()
        del self_dict["pool"]
        return self_dict


class RestStreams(Connections):
    def __init__(self):
        super().__init__()
        self.all_equities_symbols_list = pd.read_sql(
            con=self.rds_engine, sql="SELECT symbol from equities_info;"
        ).values
        self.all_equities_symbols_list = list(self.all_equities_symbols_list.flatten())

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
    records: Optional[requests.Response], historic_agg_columns: dict, ticker: str, timespan: str, multiplier: int,
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
    try:
        df_ = pd.DataFrame.from_records(records.results)
    except TypeError as e:
        return None, None

    df_ = df_.rename(columns=historic_agg_columns)
    df_["ticker"] = [ticker for i in range(len(df_))]

    df_ = df_[["ticker"] + list(historic_agg_columns.values())]
    df_.loc[:, "timestamp"] = pd.to_datetime(df_["timestamp"], unit="ms")
    df_["timespan"] = timespan
    df_["multiplier"] = multiplier

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

    return batched, query_template


def insert_into_db(
    conn_params: dict, batched: list, query_template: str, pid: int, ticker: str
):
    """
    Take the stuff from process_stocks_equities_aggregates and push it into the database
    :param conn_params:
    :param batched:
    :param query_template:
    :param pid:
    :param ticker:
    :return:
    """
    conn = psycopg2.connect(**conn_params)
    with conn.cursor() as cur:
        for b in batched:
            try:
                psycopg2.extras.execute_values(cur, query_template, b)
                conn.commit()
            except psycopg2.errors.InFailedSqlTransaction as e:
                print(f"Insert for pid: {pid} did not work, ticker: {ticker}")
                pass


def wrapper_func(
    # for request_stocks_equities_agg params
    ticker: str,
    client: RESTClient,
    # insert_into_db params
    conn_params: dict,
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
        records=records, historic_agg_columns=historic_agg_columns, ticker=ticker, timespan=timespan, multiplier=multiplier
    )

    if (batched is not None) & (query_template is not None):
        insert_into_db(
            conn_params=conn_params,
            query_template=query_template,
            batched=batched,
            pid=pid,
            ticker=ticker,
        )
    else:
        print(f"ticker : {ticker} has returned None.")


CONCURRENT_DOWNLOADS = 100
CPU_NUM = 4

if __name__ == "__main__":

    manager = Manager()
    semaphore = manager.Semaphore(value=CONCURRENT_DOWNLOADS)

    stream = RestStreams()
    equities_list = stream.all_equities_symbols_list[960:]
    rest_client = stream.rest_client
    rds_params = stream.conn_params

    results = []
    with Pool(processes=CPU_NUM) as pool:
        target = functools.partial(wrapper_func, sem=semaphore, client=rest_client, conn_params=rds_params)
        for tkr in equities_list:
            results.append(
                pool.apply_async(
                    func=target,
                    kwds={
                        "ticker": tkr,
                    },
                ).get(timeout=100)
            )

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(call_all_stocks_aggregates())
    # loop.run_forever(stream.call_all_stocks_aggregates())

    # assert stream.rds_connected, "Streams is not connected to the database"

    # s_date = datetime.date.today() - datetime.timedelta(days=1)
    # e_date = datetime.date.today()

    # for t in tqdm(tkr, desc="For each ticker..."):
    #     print(f"Ticker: {t}...")
    #     # bbo_df = stream.rest_historic_n_bbo_quotes(
    #     #     ticker=ticker, start_date=s_date, end_date=e_date
    #     # )
    #
    #     data_df = stream.rest_historic_aggregates(
    #         ticker=t, start_date=s_date, end_date=e_date, timespan="minute"
    #     )

    # stream.rest_insert_aggregates(
    #     df=data_df, insert_query_template=insert_into_polygon_stocks_bbo, nbbo=False
    # )

    # stream.establish_websocket_client()
    # stream.websocket_client.run_async()
    # stream.websocket_client.subscribe("A.MSFT", "AM.MSFT")

    #     with self.rds_conn.cursor() as cur:
    #         for batch in tqdm(batched, desc="Inserting each aggregate query..."):
    #             try:
    #                 psycopg2.extras.execute_values(cur, query_template, batch)
    #                 self.rds_conn.commit()
    #             except psycopg2.errors.InFailedSqlTransaction as e:
    #                 print(f"InFailedSQLTransaction: {e}")
    #                 self.rds_conn.rollback()
    #                 psycopg2.extras.execute_values(cur, query_template, batch)
    #                 self.rds_conn.commit()
    # except aiohttp.client_exceptions.ClientResponseError as e:
    #     print(f"Ticker: {ticker} not pulling")
