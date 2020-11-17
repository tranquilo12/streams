from polygon import WebSocketClient, STOCKS_CLUSTER, RESTClient
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from logger import StreamsLogger
from typing import Optional
import numpy as np
import datetime
import configparser
import paramiko
import sshtunnel
import psycopg2
import aiopg
import redis
import sys
import os

sshtunnel.SSH_TIMEOUT = 10.0
sshtunnel.TUNNEL_TIMEOUT = 10.0


class Connections(StreamsLogger):
    def __init__(self):

        # read the damn file
        super().__init__()

        try:
            self.my_private_key_path = os.path.join(
                os.curdir, "ssl_certs", "new_ts_pair.pem"
            )
            self.my_private_key = paramiko.RSAKey.from_private_key_file(
                self.my_private_key_path
            )
            config_path = os.path.join(os.curdir, "config.ini")

        except FileNotFoundError as e:
            # most likely trying to execute from a notebook in curdir\\notebooks, so go back one step
            go_back_one = os.path.normpath(os.getcwd() + os.sep + os.pardir)
            self.my_private_key_path = os.path.join(
                go_back_one, "ssl_certs", "new_ts_pair.pem"
            )
            self.my_private_key = paramiko.RSAKey.from_private_key_file(
                self.my_private_key_path
            )
            config_path = os.path.join(go_back_one, "config.ini")

        config = configparser.ConfigParser()
        config.read(config_path)

        self.tunnel = None

        # for the other parameters
        self.db_conn_params = {
            "host": config["DB"]["host"],
            "password": config["DB"]["password"],
            "port": int(config["DB"]["port"]),
            "user": config["DB"]["user"],
            "database": config["DB"]["name"],
            "keepalives": 1,
            "keepalives_idle": 5,
            "keepalives_interval": 2,
            "keepalives_count": 100,
        }
        self.ssh_conn_params = {
            "ssh_address_or_host": (config["SSH"]["ssh_host"]),
            "ssh_username": config["SSH"]["ssh_user"],
            "ssh_pkey": self.my_private_key,
            "remote_bind_address": (
                config["SSH"]["ssh_host"],
                int(config["DB"]["ssh_port"]),
            ),
            "local_bind_address": (
                config["SSH"]["ssh_local_bind_address"],
                int(config["SSH"]["ssh_local_bind_port"]),
            ),
        }
        self.redis_conn_params = {
            "host": config["REDIS"]["host"],
            "port": int(config["REDIS"]["port"]),
            "password": config["REDIS"]["password"],
            "db": int(config["REDIS"]["db"]),
            "socket_timeout": int(config["REDIS"]["socket_timeout"]),
        }
        self.redis_cache_config = {
            "CACHE_TYPE": "redis",
            "CACHE_REDIS_URL": os.environ.get(
                "REDIS_URL",
                f"redis://:{self.redis_conn_params['password']}@localhost:6379",
            ),
        }
        self.dsn = f"dbname={self.db_conn_params['database']} user={self.db_conn_params['user']} password={self.db_conn_params['password']} host={self.db_conn_params['host']}"
        self.api_key = config["POLYGON"]["reverent_visvesvaraya_key"]
        self.rds_connected = None
        self.async_rds_connected = None
        self.async_rds_conn = None
        self.rds_conn = None
        self.rds_engine = None
        self.websocket_client = None
        self.rest_client = None
        self.redis_pool = None
        self.redis_client = None

        # make sure all the conns are made
        # self.establish_ssh_tunnel()
        # self.start_ssh_tunnel()
        # self.establish_all_connections()

    @staticmethod
    def datetime_converter(x: int) -> Optional[datetime.datetime]:
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

    def establish_redis_connection(self):
        """
        Establish the connection to the local/remote Redis server
        :return:
        """
        try:
            self.redis_pool = redis.ConnectionPool(**self.redis_conn_params)
            self.redis_client = redis.StrictRedis(
                connection_pool=self.redis_pool, charset="utf-8", decode_responses=True
            )
            ping = self.redis_client.ping()
            if ping:
                pass

        except (redis.AuthenticationError, ConnectionRefusedError) as e:
            print(f"Error: {e}")
            sys.exit(1)

        else:
            self.redis_pool = redis.ConnectionPool()
            self.redis_pool = self.redis_pool.from_url(
                url=os.environ.get(
                    "REDIS_URL",
                    f"redis://:{self.redis_conn_params['password']}@localhost:6379",
                ),
            )
            self.redis_client = redis.StrictRedis(
                connection_pool=self.redis_pool, charset="utf-8", decode_responses=True
            )
            ping = self.redis_client.ping()
            if ping:
                pass

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
        self.establish_redis_connection()
