import pandas as pd
import numpy as np
import requests
import datetime
import psycopg2
import ast
import asyncio
import aiopg
import aiohttp
from tqdm.auto import tqdm
from polygon import WebSocketClient, AsyncRESTClient, STOCKS_CLUSTER
from connections import Connections
from all_sql import (
    insert_into_polygon_trades,
    insert_into_polygon_quotes,
    insert_into_polygon_agg,
    insert_into_polygon_stocks_bbo,
)


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

    async def request_and_insert_stocks_equities_aggregates(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_: datetime.date,
        to: datetime.date,
    ) -> None:
        """
        Only handles the stocks equities aggregates, and
        :param ticker: goes to the func stocks_equities_aggregates
        :param multiplier: goes to the func stocks_equities_aggregates
        :param timespan: goes to the func stocks_equities_aggregates
        :param from_: goes to the func stocks_equities_aggregates, will be converted to string, asks for datetime
        :param to: goes to the func stocks_equities_aggregates, will be converted to string, asks for datetime
        :return: None
        """

        assert self.rest_client is not None, "Why is rest_client None??"

        try:
            records = await self.rest_client.stocks_equities_aggregates(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_.strftime("%Y-%m-%d"),
                to=to.strftime("%Y-%m-%d"),
                params={},
            )
            df_ = pd.DataFrame.from_records(records.results)
            df_ = df_.rename(columns=self.historic_agg_columns)
            df_["ticker"] = [ticker for i in range(len(df_))]

            df_ = df_[["ticker"] + list(self.historic_agg_columns.values())]
            df_.loc[:, "timestamp"] = pd.to_datetime(df_["timestamp"], unit="ms")

            df_ = df_.replace({np.NaN: None})
            columns = df_.columns.tolist()
            batch_size = len(df_) // 10

            query_template = "INSERT INTO polygon_stocks_agg ({}) VALUES %s ON CONFLICT (event_type, symbol_ticker, start_timestamp, end_timestamp) DO NOTHING".format(
                ",".join(columns)
            )

            values = [[val for val in d.values()] for d in df_.to_dict(orient="records")]
            batched = [batch for batch in self.batch(values, n=batch_size)]
            conn = await self.establish_async_rds_connection()

            async with conn.cursor() as cur:
                for batch in batched:
                    try:
                        await psycopg2.extras.execute_values(cur, query_template, batch)
                    except psycopg2.errors.InFailedSqlTransaction as e:
                        pass
        except aiohttp.client_exceptions.ClientResponseError as e:
            print(f"Ticker : {ticker} not pulling down data")

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


async def call_all_stocks_aggregates():

    stream = RestStreams()
    # semaphore = asyncio.Semaphore(n)
    start_date = datetime.date.today() - datetime.timedelta(days=365)
    end_date = datetime.date.today()

    # async def sem_task(task):
    #     async with semaphore:
    #         await task

    all_calls = [
        stream.request_and_insert_stocks_equities_aggregates(
            ticker=tkr, multiplier=1, timespan="day", from_=start_date, to=end_date
        )
        for tkr in stream.all_equities_symbols_list
    ]

    await asyncio.gather(*all_calls)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(call_all_stocks_aggregates())
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
