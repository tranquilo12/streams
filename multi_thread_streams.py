from connections import Connections
from polygon import RESTClient
from threading import Thread
from tqdm.auto import tqdm
from typing import Optional
from queue import Queue
import pandas as pd
import numpy as np
import functools
import sshtunnel
import requests
import psycopg2
import datetime
import logging
import redis
import pickle
import json


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
    db_conn_params: dict, batched: list, query_template: str, ticker: str,
) -> None:
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


def establish_ssh_tunnel(ssh_conn_params: dict) -> sshtunnel.SSHTunnelForwarder:
    try:
        t = sshtunnel.SSHTunnelForwarder(**ssh_conn_params)
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


def process_stocks_equities_aggregates_for_redis(
    records: Optional[requests.Response], ticker: str, timespan: str, multiplier: int,
) -> [list, dict]:
    """
    Make sure it's easy to decipher the processing, by separating the logic
    :param records:
    :param ticker:
    :param timespan:
    :param multiplier:
    :return:
    """
    results = records.results
    if results is None:
        return None, None

    try:
        keys = [
            f"{ticker}_{timespan}_{multiplier}_{result['vw']}_{result['t']}"
            for result in results
        ]
    except Exception as e:
        keys = None
        print(f"Dont know exception location 1.1: {e}")

    try:
        new_results = {
            f"{ticker}_{timespan}_{multiplier}_{result['vw']}_{result['t']}": json.dumps(
                {
                    "volume": result["v"],
                    "open": result["o"],
                    "close": result["c"],
                    "high": result["h"],
                    "low": result["l"],
                }
            )
            for result in results
        }
    except Exception as e:
        new_results = None
        print(f"Dont know exception location 1.2: {e}")

    return keys, new_results


def process_stocks_equities_aggregates_for_postgres(
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

    try:
        try:
            df_ = pd.DataFrame.from_records(records.results)
        except TypeError as e:
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
            return None, None

        # make te query template for this query, that will be inserted into the table
        all_cols_str = ",".join(columns)
        query_template = f"INSERT INTO polygon_stocks_agg_candles ({all_cols_str}) VALUES %s ON CONFLICT (ticker, timespan, multiplier, vwap, timestamp) DO NOTHING"

    except KeyError as e:
        print(f"KeyError: {e}")
        return None, None

    return batched, query_template


def push_into_redis_cache(redis_client: redis.Redis, encoded_records: dict) -> None:
    """
    Take the response from request_stocks_equities_agg and
    push into the redis cache
    :param encoded_records:
    :param redis_client:
    :return:
    """
    for key, val in encoded_records.items():
        try:
            redis_client.set(key, val)
        except Exception as e:
            print(f"Exception while inserting to redis: {e}")


def pull_from_redis_cache(redis_client: redis.Redis, keys_list: list) -> dict:
    results = {}
    for key in keys_list:
        try:
            results[key] = redis_client.get(name=key)
        except Exception as e:
            print(f"Exception while fetching data from redis: {e}")

    return results


def parse_redis_output_for_postgres(results: dict) -> [list, str]:
    """
    Take the output from 'pull_from_redis_cache', an
    :param results:
    :return:
    """
    all_results = []
    for key, val in results.items():
        keys = key.split("_")
        val = json.loads(s=val)
        expanded_results = [
            keys[0],
            keys[1],
            keys[2],
            float(val["volume"]),
            keys[3],
            float(val["open"]),
            float(val["close"]),
            float(val["high"]),
            float(val["low"]),
            pd.to_datetime(keys[4], unit="ms"),
        ]
        all_results.append(expanded_results)

    try:
        batched = [b for b in batch(all_results, n=100)]
    except ValueError as e:
        return None, None

    all_cols_str = (
        "ticker, timespan, multiplier, volume, vwap, open, close, high, low, timestamp"
    )
    table_name = "polygon_stocks_agg_candles"
    conflict_cols = "(ticker, timespan, multiplier, vwap, timestamp)"
    query_template = f"INSERT INTO {table_name} ({all_cols_str}) VALUES %s ON CONFLICT {conflict_cols} DO NOTHING"
    return batched, query_template


def download_and_push_into_db(
    logger: logging.Logger,
    disable_logging: bool,
    ticker: str,
    rest_client: RESTClient,
    redis_client: redis.Redis,
    db_conn_params: dict,
    multiplier: int = 1,
    timespan: str = "day",
    from_: datetime.date = datetime.date.today() - datetime.timedelta(days=365),
    to: datetime.date = datetime.date.today(),
):
    """
    A wrapper function that lines all the others up
    :param ticker:
    :param redis_client:
    :param db_conn_params:
    :param rest_client:
    :param disable_logging:
    :param logger:
    :param multiplier:
    :param timespan:
    :param from_:
    :param to:
    :return:
    """

    records = request_stocks_equities_aggregates(
        ticker=ticker,
        client=rest_client,
        multiplier=multiplier,
        timespan=timespan,
        from_=from_,
        to=to,
    )

    # batched, query_template = process_stocks_equities_aggregates_for_postgres(
    #     records=records, ticker=ticker, timespan=timespan, multiplier=multiplier,
    # )

    try:
        keys, new_results = process_stocks_equities_aggregates_for_redis(
            records=records, ticker=ticker, timespan=timespan, multiplier=multiplier
        )
    except Exception as e:
        keys, new_results = None, None
        print(f"Dont know exception location 1: {e}")

    try:
        if isinstance(new_results, dict):
            push_into_redis_cache(
                redis_client=redis_client, encoded_records=new_results
            )
    except Exception as e:
        print(f"Dont know exception location 2: {e}")

    try:
        if isinstance(keys, list):
            results = pull_from_redis_cache(redis_client=redis_client, keys_list=keys)
        else:
            results = None
    except Exception as e:
        results = None
        print(f"Dont know exception location 3: {e}")

    try:
        if isinstance(results, dict):
            batched, query_template = parse_redis_output_for_postgres(results=results)
        else:
            batched, query_template = None, None
    except Exception as e:
        batched, query_template = None, None
        print(f"Dont know exception location 4: {e}")

    if (batched is not None) & (query_template is not None):
        logger.info(msg=f"Ticker : {ticker} Inserting to db...")
        insert_into_db(
            db_conn_params=db_conn_params,
            query_template=query_template,
            batched=batched,
            ticker=ticker,
        )
    else:
        if disable_logging:
            prev_state = logger.disabled
            logger.disabled = True
            logger.info(msg=f"Ticker : {ticker} has returned None.")
            logger.disabled = prev_state
        else:
            logger.info(msg=f"Ticker : {ticker} has returned None.")


def get_distinct_col_values_from_equities_info(
    logger: logging.Logger,
    col_name: str,
    db_conn_params: dict,
    filter_col: str = None,
    filter_val: list = None,
) -> list:
    """
    A template function for querying the "equities_info" table, it's in a function to make things
    easier from a multi-threading point of view
    :param col_name: the distinct values of this col
    :param logger: a logger object
    :param db_conn_params: database connection parameters
    :param filter_col: the col which will be used to filter by
    :param filter_val: the filter col val
    :return:
    """
    logger.info(msg="Making ssh tunnel to get sectors list...")

    if filter_col is not None and filter_val is not None:
        filter_val = filter_val.replace("_", " ").replace("-", " ")
        query = f"SELECT DISTINCT t.{col_name} FROM public.equities_info t WHERE t.{filter_col} = '{filter_val.strip()}';"
    else:
        query = f"SELECT DISTINCT t.{col_name} FROM public.equities_info t;"

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

    res = [r[0] for r in res]

    return res


def get_multiple_distinct_col_values_from_equities_info(
    logger: logging.Logger,
    ssh_conn_params: dict,
    db_conn_params: dict,
    col_names: list,
    filter_col: str = None,
    filter_val: list = None,
) -> dict:
    """
    A wrapper function that asks multiple distinct column values from equities_info.
    This wrapper just opens a ssh tunnel for all these queries.
    :param ssh_conn_params: to establish the ssh_conn_params
    :param logger: a logger object
    :param db_conn_params: the db connection params
    :param col_names: a list of all column names
    :param filter_col: the col which will be used to filter by
    :param filter_val: the filter col val
    :return:
    """
    res = {}

    t = establish_ssh_tunnel(ssh_conn_params=ssh_conn_params)
    t.daemon_transport = True
    t.daemon_forward_servers = True
    # t.start()
    db_conn_params["port"] = 5433  # int(t.local_bind_port)

    for col in col_names:
        res[col] = get_distinct_col_values_from_equities_info(
            col_name=col,
            logger=logger,
            db_conn_params=db_conn_params,
            filter_val=filter_val,
            filter_col=filter_col,
        )

    # if t.is_alive | t.is_active:
    #     t.stop()

    return res


if __name__ == "__main__":

    conns = Connections()
    conns.establish_rest_client()
    conns.establish_redis_connection()

    r_client = conns.rest_client
    ssh_params = conns.ssh_conn_params
    db_params = conns.db_conn_params

    tunnel = establish_ssh_tunnel(ssh_conn_params=ssh_params)
    tunnel.daemon_transport = True
    tunnel.daemon_forward_servers = True
    tunnel.start()
    db_params["port"] = int(tunnel.local_bind_port)

    res = get_distinct_col_values_from_equities_info(
        col_name="symbol", logger=conns.logger, db_conn_params=db_params
    )
    equities_list = [val for val in res if "^" not in val or "." not in val]

    conns.logger.info(msg="Starting thread pool...")
    pool = ThreadPool(num_threads=50)
    for eq in tqdm(equities_list):
        pool.add_tasks(
            func=download_and_push_into_db,
            logger=conns.logger,
            disable_logging=False,
            rest_client=r_client,
            redis_client=conns.redis_client,
            ticker=eq,
            db_conn_params=db_params,
            timespan="minute",
            from_=datetime.date.today() - datetime.timedelta(days=2),
        )

    conns.logger.info(msg="Waiting for pool tasks to complete...")
    pool.wait_completion()

    if tunnel.is_alive | tunnel.is_active:
        tunnel.stop()
