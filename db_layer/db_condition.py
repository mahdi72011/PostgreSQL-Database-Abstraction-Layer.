from constants import sql_operator



class DBCondition:

    def __init__(self, term: str = None, operator: sql_operator.SqlOperator = None, const: str = None):
        self.term = term
        self.operator = operator
        self.const = const
        self.condition = ""

    def build_condition(self):
        self.condition = self.term + str(self.operator) + "'%s'" % self.const

    def __or__(self, other):
        or_condition = DBCondition()
        or_condition.condition = "(" + self.condition + " OR " + other.condition + ")"
        return or_condition

    def __and__(self, other):
        and_condition = DBCondition()
        and_condition.condition = "(" + self.condition + " AND " + other.condition + ")"
        return and_condition
    
    # def ilike(self,phrase):
    #     ilike_condition = DBCondition()
    #     ilike_condition.condition = "%" + self.condition + " ILIKE " + phrase.condition + "%"
    #     return ilike_condition


