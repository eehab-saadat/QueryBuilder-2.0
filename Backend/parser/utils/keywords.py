from enum import Enum

class Keyworkd(Enum):
    SELECT = "SELECT"


    def __str__(self):
        return self.value
    
    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)



class OPERATORS(Enum):

    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT BETWEEN"
    
    GREATER = ">"
    LESSER = "<"
    GREATER_EQ = ">="
    LESSER_EQ = "<="
    EQUAL = "="
    NOT_EQUAL = "!="
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    ILIKE = "ILIKE"
    NOT_ILIKE = "NOT ILIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
