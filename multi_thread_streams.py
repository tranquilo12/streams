from polygon import RESTClient
from threading import Thread
from connections import Connections
from tqdm.auto import tqdm
from typing import Optional
from queue import Queue
import pandas as pd
import numpy as np
import requests
import psycopg2
import datetime


class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(f"Exception: {e}")

            self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_tasks(self, func, *args, **kwargs):
        self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        self.tasks.join()


def insert_into_db(
    db_conn_params: dict,
    batched: list,
    query_template: str,
    ticker: str,
):
    """
    Take the stuff from process_stocks_equities_aggregates and push it ino the database
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


def request_stocks_equities_aggregates(
    ticker: str,
    client: RESTClient,
    from_: datetime.date,
    to: datetime.date,
    multiplier: int = 1,
    timespan: str = "day",
) -> [Optional[requests.Response], int]:
    """
    Only handles the stocks equities aggregates, and
    :param ticker: goes to the func stocks_equities_aggregates
    :param client:
    :param multiplier: goes to the func stocks_equities_aggregates
    :param timespan: goes to the func stocks_equities_aggregates
    :param from_: goes to the func stocks_equities_aggregates, will be converted to string, asks for datetime
    :param to: goes to the func stocks_equities_aggregates, will be converted to string, asks for datetime
    :return: None
    """

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

    return records


def process_stocks_equities_aggregates(
    records: Optional[requests.Response], ticker: str, timespan: str, multiplier: int,
) -> [list, str]:
    """
    Make sure it's easy to decipher the processing, by separating the logic
    :param records:
    :param ticker:
    :param timespan:
    :param multiplier:
    :return:
    """

    historic_agg_columns: dict = {
        "v": "volume",
        "vw": "vwap",
        "o": "open",
        "c": "close",
        "h": "high",
        "l": "low",
        "t": "timestamp",
        "n": "n_items",
    }

    try:
        try:
            df_ = pd.DataFrame.from_records(records.results)
        except TypeError as e:
            # print(f"TypeError: {e}")
            return None, None

        df_ = df_.rename(columns=historic_agg_columns)
        df_["ticker"] = ticker
        df_["timespan"] = timespan
        df_["multiplier"] = multiplier

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
            # print(f"ValueError: {e}")
            return None, None

        # make te query template for this query, that will be inserted into the table
        all_cols_str = ",".join(columns)
        query_template = f"INSERT INTO polygon_stocks_agg_candles ({all_cols_str}) VALUES %s ON CONFLICT (ticker, timespan, multiplier, vwap, timestamp) DO NOTHING"

    except KeyError as e:
        print(f"KeyError: {e}")
        return None, None

    return batched, query_template


def download_and_push_into_db(
    logger,
    ticker: str,
    client: RESTClient,
    ssh_conn_params: dict,
    db_conn_params: dict,
    multiplier: int = 1,
    timespan: str = "day",
    from_: datetime.date = datetime.date.today() - datetime.timedelta(days=365),
    to: datetime.date = datetime.date.today(),
):
    """
    A wrapper function that lines all the others up
    :param ticker:
    :param ssh_conn_params:
    :param db_conn_params:
    :param multiplier:
    :param timespan:
    :param from_:
    :param to:
    :return:
    """

    records = request_stocks_equities_aggregates(
        ticker=ticker,
        client=client,
        multiplier=multiplier,
        timespan=timespan,
        from_=from_,
        to=to,
    )

    batched, query_template = process_stocks_equities_aggregates(
        records=records, ticker=ticker, timespan=timespan, multiplier=multiplier,
    )

    if (batched is not None) & (query_template is not None):
        insert_into_db(
            ssh_conn_params=ssh_conn_params,
            db_conn_params=db_conn_params,
            query_template=query_template,
            batched=batched,
            ticker=ticker,
        )
    else:
        logger.info(msg=f"ticker : {ticker} has returned None.")


def get_all_equities_list(logger, db_conn_params) -> list:
    # query the db for equities list

    logger.info(msg="Making ssh tunnel to get equities list...")
    try:
        query = "SELECT DISTINCT t.symbol FROM public.equities_info t;"
        with psycopg2.connect(**db_conn_params) as e_conn:
            with e_conn.cursor() as cur:
                logger.info(msg="Querying db...")
                cur.execute(query)
                logger.info(msg="Fetchall query...")
                equities_list = cur.fetchall()

        equities_list = [val[0].replace(" ", "") for val in equities_list]
        equities_list = [val for val in equities_list if ("^" not in val)]

    except psycopg2.OperationalError as e:
        logger.error(msg=f"Psycopg2 Op Error: {e}")
        equities_list = None

    return equities_list


if __name__ == "__main__":

    conns = Connections()
    conns.establish_rest_client()

    rest_client = conns.rest_client
    ssh_conn_params = conns.ssh_conn_params
    db_conn_params = conns.db_conn_params

    tunnel = establish_ssh_tunnel(ssh_conn_params=ssh_conn_params)
    tunnel.daemon_transport = True
    tunnel.daemon_forward_servers = True
    tunnel.start()
    db_conn_params["port"] = int(tunnel.local_bind_port)

    equities_list = get_all_equities_list(
        logger=conns.logger, db_conn_params=db_conn_params
    )

    conns.logger.info(msg="Starting thread pool...")
    pool = ThreadPool(num_threads=4)
    for eq in tqdm(equities_list):
        pool.add_tasks(
            func=download_and_push_into_db,
            logger=conns.logger,
            client=rest_client,
            ticker=eq,
            ssh_conn_params=ssh_conn_params,
            db_conn_params=db_conn_params,
            timespan="day",
            from_=datetime.date.today() - datetime.timedelta(days=10),
        )

    conns.logger.info(msg="Waiting for pool tasks to complete...")
    pool.wait_completion()

    if tunnel.is_alive | tunnel.is_active:
        tunnel.stop()
