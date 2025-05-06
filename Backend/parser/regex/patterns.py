#For Between: r'(\w+\.?\w+)\s+(?:BETWEEN|NOT\sBETWEEN)\s+((?:\'[^\']*\'|\"[^\"]*\"|\d+\.\d+|\d+|\S+))\s+AND\s+((?:\'[^\']*\'|\"[^\"]*\"|\d+\.\d+|\d+|\S+))',
#For Others: r'((?:\w+\([^\)]*\)|\w+\.\w+|\w+))\s*(=|!=|>|<|>=|<=|IN|NOT\sIN|LIKE|ILIKE|BETWEEN|NOT\sBETWEEN)\s+(\(.*?\)|\'[^\']*\'|\S+)'
from ..utils.constants import DB
from enum import Enum
class PATTERNS(Enum):
    DB_TABLE = fr'({DB}\.?\w+)'
    DOT_SEPARATED_WORDS = r'(\w+\.\w+)'
    OPERATOR_CONDITION = r'(=|!=|>|<|>=|<=|IN|NOT\sIN|LIKE|ILIKE)'
    STRING_OPERATORS = r''
    NUMERIC_OPERATORS = r''
    DATETIME_OPERATORS = r''
    NULL_OPERATORS = r''

    def __str__(self):
        return self.value
    
    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)
    
