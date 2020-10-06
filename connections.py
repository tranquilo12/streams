import configparser
import psycopg2

# import asyncpg
import aiopg
from sqlalchemy import create_engine
from polygon import WebSocketClient, STOCKS_CLUSTER, RESTClient
from logger import StreamsLogger

import paramiko
import sshtunnel
from sshtunnel import SSHTunnelForwarder

sshtunnel.SSH_TIMEOUT = 10.0
sshtunnel.TUNNEL_TIMEOUT = 10.0


class Connections(StreamsLogger):
    def __init__(self):

        # read the damn file
        super().__init__()
        config = configparser.ConfigParser()
        config.read("config.ini")

        self.my_private_key_path = "C:\\Users\\SHIRAM\\Downloads\\new_ts_pair.pem"
        self.my_private_key = paramiko.RSAKey.from_private_key_file(
            self.my_private_key_path
        )
        self.tunnel = None

        # for the other parameters
        self.db_conn_params = {
            "host": config["DB"]["host"],
            "password": config["DB"]["password"],
            "port": int(config["DB"]["port"]),
            "user": config["DB"]["user"],
            "database": config["DB"]["name"],
            #"options": "-c statement_timeout=0",
            "keepalives": 1,
            "keepalives_idle": 5,
            "keepalives_interval": 2,
            "keepalives_count": 100
        }
        self.ssh_conn_params = {
            "ssh_address_or_host": (config["SSH"]["host"]),
            "ssh_username": config["SSH"]["user"],
            "ssh_pkey": self.my_private_key,
            "remote_bind_address": (
                config["SSH"]["host"],
                int(config["DB"]["port"]),
            ),
            "local_bind_address": (
                config["SSH"]["local_bind_address"],
                int(config["SSH"]["local_bind_port"]),
            ),
        }
        self.dsn = f"dbname={self.db_conn_params['database']} user={self.db_conn_params['user']} password={self.db_conn_params['password']} host={self.db_conn_params['host']}"
        self.api_key = config["POLYGON"]["key"]
        # self.rds_url = config["TEST"]["url"]
        self.rds_connected = None
        self.async_rds_connected = None
        self.async_rds_conn = None
        self.rds_conn = None
        self.rds_engine = None
        self.websocket_client = None
        self.rest_client = None

        # make sure all the conns are made
        # self.establish_ssh_tunnel()
        # self.start_ssh_tunnel()
        # self.establish_all_connections()

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

    def establish_ssh_tunnel(self) -> None:
        try:
            self.tunnel = SSHTunnelForwarder(**self.ssh_conn_params)
        except (
            sshtunnel.BaseSSHTunnelForwarderError,
            sshtunnel.HandlerSSHTunnelForwarderError,
        ) as e:
            self.tunnel = None
            print("Error: {e}")

    def start_ssh_tunnel(self) -> None:
        if self.tunnel:
            self.tunnel.start()

    def close_ssh_tunnel(self) -> None:
        if self.tunnel:
            self.tunnel.stop()

    def establish_rds_connection(self) -> None:
        """
        With ssh now,
        make sure tunnel is created, and open and then attempt to make connection object
        :return: None
        """
        try:
            if self.tunnel is not None:
                self.db_conn_params["port"] = self.tunnel.local_bind_port

            self.rds_conn = psycopg2.connect(**self.db_conn_params)
            self.rds_connected = True
            self.logger.info(msg="RDS Connection established")
        except (
            ValueError,
            PermissionError,
            psycopg2.OperationalError,
        ) as e:
            self.logger.error(msg=f"RDS Connection not established, with error: {e}")
            self.rds_connected = False

    async def establish_async_rds_connection(self) -> aiopg.connect:
        """
        Make sure the rds connection is made to the test database
        :return: None
        """
        try:
            async_rds_conn = await aiopg.connect(dsn=self.dsn)
            self.logger.info(msg="ASYNC RDS Connection established")
        except (
            ValueError,
            PermissionError,
            psycopg2.OperationalError,
        ):
            async_rds_conn = None
            self.logger.error(
                msg=f"ASYNC RDS Connection not established, with error: {e}"
            )

        return async_rds_conn

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
            # self.rest_client = AsyncRESTClient(auth_key=self.api_key)
            self.rest_client = RESTClient(auth_key=self.api_key)
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