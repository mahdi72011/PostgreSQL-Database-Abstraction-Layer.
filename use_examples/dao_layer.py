import logging
from sql_operator import SqlOperator
from db.db_condition import DBCondition
from db.query_builder import QueryBuilder
from error_message import ErrorMessage


class BaseDao:
    def __init__(self, table_name):
        self.db = QueryBuilder(table_name)
        self.logger = logging.getLogger(__name__)

    def create_entry(self, model_class: object, **kwargs):
        """
        General method to create an entry in any table.
        """
        table_model_class = model_class()
        
        # Set attributes dynamically based on provided keyword arguments
        for key, value in kwargs.items():
            setattr(table_model_class, key, value)
        
        self.insert(obj=table_model_class)
        return True

    def insert(self, obj):
        try:
            self.db.insert(obj)
        except Exception as error:
            self.logger.error(ErrorMessage.DB_INSERT)
            self.logger.error(error)
            raise error  

    def select_by_id(self, column, value):
        condition = DBCondition(term=column, operator=SqlOperator.EQL, const=value)
        condition.build_condition()
        try:
            return self.db.select(condition=condition.condition)
        except Exception as error:
            self.logger.error(ErrorMessage.DB_SELECT)
            self.logger.error(error)
            raise error 

    def select_by_condition(self, conditions: list):
        try:
            combined_condition = conditions[0]
            for cond in conditions[1:]:
                combined_condition = combined_condition.__and__(cond)
            return self.db.select(condition=combined_condition.condition)
        except Exception as error:
            self.logger.error(ErrorMessage.DB_SELECT)
            self.logger.error(error)
            raise error 

    def update(self, updates: list, condition):
        try:
            self.db.update(update=updates, condition=condition.condition)
        except Exception as error:
            self.logger.error(ErrorMessage.DB_UPDATE)
            self.logger.error(error)
            raise error

    def delete(self, condition):
        try:
            self.db.delete(condition=condition.condition)
        except Exception as error:
            self.logger.error(ErrorMessage.DB_DELETE)
            self.logger.error(error)
            raise error
