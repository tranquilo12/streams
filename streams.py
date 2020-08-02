import pandas as pd
import numpy as np
import requests
import datetime
import psycopg2
import ast
from tqdm.auto import tqdm
from polygon import WebSocketClient, AsyncRESTClient, STOCKS_CLUSTER
from connections import Connections
from all_sql import (
    insert_into_polygon_trades,
    insert_into_polygon_quotes,
    insert_into_polygon_agg,
    insert_into_polygon_stocks_bbo,
)


class Streams(Connections):
    def __init__(self):
        super().__init__()

    def on_message(self, message: str) -> None:
        """

        :param message:
        :return:
        """
        message = ast.literal_eval(message)

        with self.conn.cursor() as cur:
            for msg in message:

                # for all trades
                if msg["ev"] == "T":
                    msg["t"] = datetime.datetime.fromtimestamp(msg["t"] / 1e3)
                    if "c" not in msg.keys():
                        msg["c"] = [-99]
                    insert_query = cur.mogrify(insert_into_polygon_trades, msg)

                # for all quotes
                if msg["ev"] == "Q":
                    msg["t"] = datetime.datetime.fromtimestamp(msg["t"] / 1e3)
                    insert_query = cur.mogrify(insert_into_polygon_quotes, msg)

                # for all aggregates
                if msg["ev"] in ["AM", "A"]:
                    msg["s"] = datetime.datetime.fromtimestamp(msg["s"] / 1e3)
                    msg["e"] = datetime.datetime.fromtimestamp(msg["e"] / 1e3)
                    insert_query = cur.mogrify(insert_into_polygon_agg, msg)

                elif msg["ev"] == "status":
                    print(msg)

                try:
                    cur.execute(insert_query)
                except psycopg2.errors.InFailedSqlTransaction as e:
                    print(f"Error: {e}")
                    self.conn.rollback()
                    cur.execute(insert_query)

                self.conn.commit()

    def rest_historic_n_bbo_quotes(
        self, ticker: str, start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        """
        A super function, which iterates over each day
        :param ticker: eg AAPL
        :param start_date: will be converted to %Y-%m-%d
        :param end_date: will be converted to %Y-%m-%d
        :return: pd.DataFrame
        """
        assert (
            self.rest_client is not None
        ), "Please make sure rest_client is established..."

        all_dates = [
            date.strftime("%Y-%m-%d")
            for date in pd.bdate_range(start=start_date, end=end_date)
        ]

        all_res = []
        for date in tqdm(all_dates):
            try:
                res = self.rest_client.historic_n___bbo_quotes_v2(ticker, date).results
                df_ = pd.DataFrame.from_records(res)
                all_res.append(df_)
            except requests.HTTPError as e:
                print(f"HTTP-Error: {e}")
                pass

        res_df = pd.concat(all_res)
        res_df = res_df.rename(
            columns={
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
        )
        # res_df.loc[:, ["sip_timestamp", "exchange_timestamp"]] = res_df[
        #     ["sip_timestamp", "exchange_timestamp"]
        # ].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e3))
        for col in ["sip_timestamp", "exchange_timestamp"]:
            res_df.loc[:, col] = pd.to_datetime(res_df[col], unit="ns")
            # res_df.loc[:, col] = res_df[col].apply(self.datetime_converter)

        return res_df

    def rest_historic_aggregates(
        self,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
        timespan: str,
    ) -> pd.DataFrame:
        """
        Get the aggregates and push into the
        :param ticker: all options
        :param start_date: of format "%Y-%m-%d:
        :param end_date: of format "%Y-%m-%d:
        :param timespan: either "minute", "hour", "day", "week", "month", "quarter", "year"
        :return:
        """
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        df_ = pd.DataFrame.from_records(
            self.rest_client.stocks_equities_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan=timespan,
                from_=start_date,
                to=end_date,
            ).results,
            columns=[
                "ticker",
                "volume",
                "open",
                "close",
                "high",
                "low",
                "timestamp",
                "n_items",
            ],
        )

        df_.loc[~pd.isna(df_["timestamp"]), "timestamp"] = df_.loc[
            ~pd.isna(df_["timestamp"]), "timestamp"
        ].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e3))

        return df_

    def rest_insert_aggregates(
        self, df: pd.DataFrame, insert_query_template: str, nbbo: bool = True
    ) -> None:
        """
        Insert into the polygon_stocks_bbo_quotes or candles
        :param nbbo: National Best Bid and Offer for those historic quotes
        :type nbbo: bool, if True then this function will be used to insert nbbo quotes, else normal quotes for that exchange
        :param df: either nbbo quotes or candles
        :param insert_query_template: either for nbbo or candles
        :return: None
        """
        df = df.replace({np.NaN: None})
        columns = df.columns.tolist()
        batch_size = len(df) // 10

        if nbbo:
            df.loc[df["indicators"].isnull(), "indicators"] = df.loc[
                df["indicators"].isnull(), "indicators"
            ].apply(lambda x: [])
            query = "INSERT INTO polygon_stocks_bbo_quotes ({}) VALUES %s ON CONFLICT (sip_timestamp, exchange_timestamp, sequence_number) DO NOTHING".format(
                ",".join(columns)
            )
        else:
            query = "INSERT INTO polygon_stocks_agg ({}) VALUES %s ON CONFLICT (event_type, symbol_ticker, start_timestamp, end_timestamp) DO NOTHING".format(
                ",".join(columns)
            )

        values = [[val for val in d.values()] for d in df.to_dict(orient="records")]
        batched = [batch for batch in self.batch(values, n=batch_size)]

        with self.conn.cursor() as cur:
            for batch in tqdm(batched, desc="Inserting each aggregate query..."):
                try:
                    psycopg2.extras.execute_values(cur, query, batch)
                    self.conn.commit()
                except psycopg2.errors.InFailedSqlTransaction as e:
                    print(f"InFailedSQLTransaction: {e}")
                    self.conn.rollback()
                    psycopg2.extras.execute_values(cur, query, batch)
                    self.conn.commit()


if __name__ == "__main__":
    stream = Streams()
    stream.establish_rest_client()

    tkr = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "GOOG", "NFLX", "FB", "AMZN"]
    assert stream.rds_connected, "Streams is not connected to the database"

    s_date = datetime.date.today() - datetime.timedelta(days=1)
    e_date = datetime.date.today()

    for t in tqdm(tkr, desc="For each ticker..."):
        print(f"Ticker: {t}...")
        # bbo_df = stream.rest_historic_n_bbo_quotes(
        #     ticker=ticker, start_date=s_date, end_date=e_date
        # )

        data_df = stream.rest_historic_aggregates(
            ticker=t, start_date=s_date, end_date=e_date, timespan="minute"
        )

        # stream.rest_insert_aggregates(
        #     df=data_df, insert_query_template=insert_into_polygon_stocks_bbo, nbbo=False
        # )

    # stream.establish_websocket_client()
    # stream.websocket_client.run_async()
    # stream.websocket_client.subscribe("A.MSFT", "AM.MSFT")
