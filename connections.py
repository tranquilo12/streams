import configparser
import psycopg2
from sqlalchemy import create_engine
from polygon import WebSocketClient, AsyncRESTClient, STOCKS_CLUSTER
from logger import StreamsLogger


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
        self.rds_url = config["TEST"]["url"]
        self.rds_connected = None
        self.rds_conn = None
        self.rds_engine = None
        self.websocket_client = None
        self.rest_client = None

        # make sure all the conns are made
        self.establish_all_connections()

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

    def establish_rds_connection(self) -> None:
        """
        Make sure the rds connection is made to the test database
        :return: None
        """
        connected = False
        try:
            self.rds_conn = psycopg2.connect(**self.conn_params)
            self.rds_engine = create_engine(self.rds_url)
            self.rds_connected = True
            self.logger.info(msg="RDS Connection established")
        except (
            ValueError,
            PermissionError,
            psycopg2.OperationalError,
        ):
            self.logger.error(msg=f"RDS Connection not established, with error: {e}")
            self.rds_connected = False

    def establish_websocket_client(self) -> None:
        """
        For the websocket client polygon, with the API key
        :return: None
        """
        try:
            self.websocket_client = WebSocketClient(
                STOCKS_CLUSTER, auth_key=self.api_key, process_message=self.on_message
            )
            self.logger.info(msg="Polygon Websocket connection established")
        except (ValueError, ConnectionRefusedError, ConnectionError):
            self.logger.error(
                msg=f"Polygon Websocket connection not established, with error: {e}"
            )

    def establish_rest_client(self) -> None:
        """
        For the REST client polygon, with the API key
        :return: None
        """
        try:
            self.rest_client = AsyncRESTClient(auth_key=self.api_key)
            self.logger.info(msg="Polygon REST connection established")
        except (ValueError, Exception) as e:
            self.logger.error(
                msg=f"Polygon REST connection not established, with error: {e}"
            )

    def establish_all_connections(self) -> None:
        """
        Just call all the functions that make up conns
        :return:
        """
        self.establish_rds_connection()
        # self.establish_websocket_client()
        self.establish_rest_client()
