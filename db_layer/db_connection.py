import psycopg2
from psycopg2 import pool
import logging
from dotenv import dotenv_values

from constants.error_message import ErrorMessage


class ConnectDB:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = dotenv_values(".env")
        self.db_connection = None
        self.pool = None
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(minconn=self.config["DB_MIN_CONNECTION_POOL"],
                                                           maxconn=self.config["DB_MAX_CONNECTION_POOL"],
                                                           user=self.config["DB_USER"],
                                                           password=self.config["DB_PASSWORD"],
                                                           host=self.config["DB_HOST"],
                                                           port=self.config["DB_PORT"],
                                                           database=self.config["DB_DATABASE"])
            
        except Exception as error:
            self.logger.error(ErrorMessage.DB_CONNECTION)
            self.logger.error(error)
            raise Exception

        try:
            ps_connection = self.pool.getconn()

        except Exception as error:
            self.logger.error(ErrorMessage.DB_GET_CONNECTION_POOL)
            self.logger.error(error)
            raise Exception

        self.db_connection = ps_connection

    def close_cursor_connection(self, cursor):
        try:
            cursor.close()
        except Exception as error:
            self.logger.error(ErrorMessage.DB_CLOSE_CURSOR_CONNECTION)
            self.logger.error(error)
            raise Exception

    def return_connection_to_pool(self, connection):
        try:
            self.pool.putconn(connection)
        except Exception as error:
            self.logger.error(ErrorMessage.DB_PUT_CONNECTION_TO_POOL)
            self.logger.error(error)
            raise Exception
