import time
import datetime
from polygon import WebSocketClient, STOCKS_CLUSTER
from configparser import ConfigParser
import psycopg2
from psycopg2.errors import SqlclientUnableToEstablishSqlconnection, ConnectionFailure
import ast
from all_sql import (
    insert_into_polygon_trades,
    insert_into_polygon_quotes,
    insert_into_polygon_agg,
)


class Streams:
    def __init__(self):
        config = ConfigParser()
        config.read("config.ini")
        self.conn_params = {
            "host": config["TEST"]["host"],
            "password": config["TEST"]["password"],
            "port": config["TEST"]["port"],
            "user": config["TEST"]["user"],
            "database": config["TEST"]["db"],
        }
        self.conn = None
        self.api_key = config["POLYGON"]["key"]
        self.websocket_client = None
        self.rds_connected = self.establish_rds_connection()

    def establish_rds_connection(self):
        connected = 0
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            connected = 1
        except (
            ValueError,
            PermissionError,
            SqlclientUnableToEstablishSqlconnection,
            ConnectionFailure,
            psycopg2.OperationalError,
        ):
            print("No Connection Established.")
        return connected

    def establish_websocket_client(self):
        try:
            self.websocket_client = WebSocketClient(
                STOCKS_CLUSTER, auth_key=self.api_key, process_message=self.on_message
            )
        except (ValueError, ConnectionFailure, ConnectionRefusedError, ConnectionError):
            print("Websocket Client not established")

    # @staticmethod
    # def on_message(message):
    #     print(message)

    def on_message(self, message: str):

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
                    self.conn.rollback()
                    cur.execute(insert_query)

                self.conn.commit()


if __name__ == "__main__":
    stream = Streams()
    if stream.rds_connected:
        stream.establish_websocket_client()
        stream.websocket_client.run_async()
        stream.websocket_client.subscribe("A.MSFT", "AM.MSFT")
        # time.sleep(10)
        # stream.websocket_client.close_connection()
