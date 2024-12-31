import logging
import psycopg2

from constants.error_message import ErrorMessage
from constants.info_message import InfoMessage
from constants.sql_operator import SqlOperator
from db.db_condition import DBCondition
from db.db_connection import ConnectDB


class QueryBuilder:
    def __init__(self, table_name):
        self.db = ConnectDB()
        self.table_name = table_name
        self.logger = logging.getLogger(__name__)

    # TODO check sql injection
    def insert(self, obj: object, returning_column:str=None):
        cursor = self.db.db_connection.cursor()
        d = vars(obj)
        print(d)
        query = "INSERT INTO {table} ({columns}) VALUES ({values}) {return_column};".\
            format(table=self.table_name,
                   columns=",".join(list(d.keys())),
                   values=",".join(["'" + str(x) + "'" for x in list(d.values())][0:]),
                   return_column=("RETURNING " + returning_column) if returning_column else "")

        print(query)
        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            self.db.db_connection.commit()
            if returning_column:
                result=cursor.fetchall()
                return result[0][0]
            
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_INSERT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)
        # self.db.return_connection_to_pool(self.db.db_connection)

    def exec_insert_query(self, query):
        cursor = self.db.db_connection.cursor()
        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            self.db.db_connection.commit()            
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_INSERT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)
        self.db.return_connection_to_pool(self.db.db_connection)

    def multiinsert(self, obj:object, value_lst:list):

        '''
            takes the value_lst as list of tuples and insert in the table
            with column name of object names.

        '''

        cursor = self.db.db_connection.cursor()
        d = vars(obj)
        key_list = list(d.keys())
        key_list.reverse()
        query = "INSERT INTO {table} ({columns}) {values};".\
            format(table=self.table_name,
                   columns=",".join(key_list),
                   values="VALUES " + ", ".join(map(str, value_lst)))
        print(query)
        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            self.db.db_connection.commit()

        except psycopg2.Error as error:
            if error.pgcode == "23505": #unique_violation Error Code in order not to raise error for duplicate row
                pass
            else:
                self.logger.error(ErrorMessage.DB_INSERT)
                self.logger.error(error)
                raise error
        self.db.close_cursor_connection(cursor)
        # self.db.return_connection_to_pool(self.db.db_connection)


    # TODO generalize select
    def select(self, column: list = "*", condition: str = None, group: list = None, order: str = None,
               asc_or_desc: str = None, limit: int = None, offset: int = None):
        cursor = self.db.db_connection.cursor()
        query = "SELECT {columns} FROM {table} {condition} {group} {order} " \
                "{asc_or_desc} {limit} {offset};".format(
                    columns=",".join(list(column)) if type(column) == list else "*",
                    table=self.table_name,
                    condition="WHERE " + condition if condition else "",
                    group="GROUP BY " + ",".join(group) if group else "",
                    order="ORDER BY " + order if order else "",
                    asc_or_desc=asc_or_desc if asc_or_desc else "",
                    limit="LIMIT " + str(limit) if limit else "",
                    offset="OFFSET " + str(offset) if offset else "")
        print(query)

        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            result = cursor.fetchall()
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_SELECT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)

        return result

    def exec_query(self,query):
        cursor = self.db.db_connection.cursor()
        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            result = cursor.fetchall()
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_SELECT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)

        return result


    # TODO check sql injection
    def delete(self, condition : DBCondition):
        cursor = self.db.db_connection.cursor()
        query = "DELETE FROM {table} WHERE {condition} ".\
            format(table=self.table_name,
            condition= condition)  
        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            self.db.db_connection.commit()
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_INSERT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)
        # self.db.return_connection_to_pool(self.db.db_connection)

    def mulidelete(self, condition : DBCondition,multivalue_column_name:str,values:list):

        preprocessed_values=", ".join(map(str, values))
        preprocessed_values = preprocessed_values.replace(" ", "")
        #preprocessed_values = preprocessed_values.replace(",", "','")
        print(preprocessed_values)
        
        cursor = self.db.db_connection.cursor()
        query = "DELETE FROM {table} WHERE {condition} AND {multivalue_column_name} IN ({multiple_delete});".\
            format(table=self.table_name,
            condition= condition,
            multivalue_column_name=multivalue_column_name,
            multiple_delete= preprocessed_values)
        print("new query",query)
        try:
            
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            self.db.db_connection.commit()
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_DELETE)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)
        #self.db.return_connection_to_pool(self.db.db_connection)

    def update(self, update: list, condition: DBCondition):
        cursor = self.db.db_connection.cursor()
        query = "UPDATE {table} SET {update} WHERE {condition};". \
            format(table=self.table_name,
                   update=",".join(update),
                   condition=condition)
        print(query)
        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            self.db.db_connection.commit()
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_INSERT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)
        # self.db.return_connection_to_pool(self.db.db_connection)

    def select_count(self, column: list = "*", condition: str = None, group: list = None, order: str = None,
               asc_or_desc: str = None, limit: int = None, offset: int = None):
        cursor = self.db.db_connection.cursor()
        query = "SELECT COUNT ({columns}) FROM {table} {condition};".format(
                    columns=",".join(list(column)) if type(column) == list else "*",
                    table=self.table_name,
                    condition="WHERE " + condition if condition else "",
                    )
        print(query)

        try:
            cursor.execute(query)
            self.logger.info(InfoMessage.DB_QUERY)
            self.logger.info(cursor.query)
            result = cursor.fetchall()
        except psycopg2.Error as error:
            self.logger.error(ErrorMessage.DB_SELECT)
            self.logger.error(error)
            raise error
        self.db.close_cursor_connection(cursor)

        return result

