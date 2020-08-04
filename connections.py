import configparser
import psycopg2
from polygon import WebSocketClient, AsyncRESTClient, STOCKS_CLUSTER
from logger import StreamsLogger
import select
import asyncpg


class Connections(StreamsLogger):
    def __init__(self):

        # read the damn file
        super().__init__()
        config = configparser.ConfigParser()
        config.read("config.ini")

        # for the other parameters
        self.conn_params = {
            "host": config["TEST"]["host"],
            "password": config["TEST"]["password"],
            "port": config["TEST"]["port"],
            "user": config["TEST"]["user"],
            "database": config["TEST"]["db"],
        }
        self.api_key = config["POLYGON"]["key"]
        self.rds_connected = None
        self.websocket_client = None
        self.rest_client = self.establish_rest_client()
        assert self.rest_client is not None, "Rest Client is not established"

    @staticmethod
    def datetime_converter(x: int):
        try:
            res = datetime.datetime.fromtimestamp(x / 1e3)
        except OSError as e:
            print(f"OS-Error: {e}")
            res = datetime.datetime.fromtimestamp(x / 1e9)
        except ValueError as e:
            print(f"Value-Error: {e}")
            res = np.NaN

        return res

    @staticmethod
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

    @staticmethod
    def psycopg2_async_wait(conn):
        """
        Source: (https://www.psycopg.org/docs/advanced.html#asynchronous-support)
        Helps in ensuring an async conn to the database
        :return:
        :rtype:
        """
        while True:
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                select.select([], [conn.fileno()], [])
            elif state == psycopg2.extensions.POLL_READ:
                select.select([conn.fileno()], [], [])
            else:
                raise psycopg2.OperationalError("poll() returned %s" % state)

    async def establish_rds_connection(self) -> psycopg2.connect:
        """
        Make sure the rds connection is made to the test database
        :return: None
        """
        try:
            conn = await asyncpg.connect(**self.conn_params)
            self.rds_connected = True
            self.logger.info(msg="RDS Connection established")
        except (
            ValueError,
            PermissionError,
            psycopg2.OperationalError,
        ):
            conn = None
            self.rds_connected = False
            self.logger.error(msg=f"RDS Connection not established, with error: {e}")

        return conn

    def establish_websocket_client(self) -> WebSocketClient:
        """
        For the websocket client polygon, with the API key
        :return: None
        """
        try:
            websocket_client = WebSocketClient(
                STOCKS_CLUSTER, auth_key=self.api_key, process_message=self.on_message
            )
            self.logger.info(msg="Polygon Websocket connection established")
        except (ValueError, ConnectionRefusedError, ConnectionError):
            websocket_client = None
            self.logger.error(
                msg=f"Polygon Websocket connection not established, with error: {e}"
            )
        return websocket_client

    def establish_rest_client(self) -> AsyncRESTClient:
        """
        For the REST client polygon, with the API key
        :return: None
        """
        try:
            rest_client = AsyncRESTClient(auth_key=self.api_key)
            self.logger.info(msg="Polygon REST connection established")
        except (ValueError, Exception) as e:
            rest_client = None
            self.logger.error(
                msg=f"Polygon REST connection not established, with error: {e}"
            )
        return rest_client

    def establish_all_connections(self) -> None:
        """
        Just call all the functions that make up conns
        :return:
        """
        # self.establish_rds_connection()
        # self.establish_websocket_client()
        # self.establish_rest_client()
