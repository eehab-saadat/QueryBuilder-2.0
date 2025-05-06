import re
from .utils.keywords import OPERATORS
from .utils.constants import DEBUG
from .regex.patterns import PATTERNS
import re
from .filter_util import *
from .Valuehandler import ConditionMatcher


# obj1 = ConditionMatcher()
column_name_mapping = {
    "AllergyDim.SeverityCode": "2.16.840.1.113883.3.88.12.3221.6.8",
    "AllergyDim.TypeCode": "2.16.840.1.113883.3.88.12.3221.6.2",
    "PatientDim.Ethnic": "2.16.840.1.114222.4.11.837",
    "PatientDim.Gender": "2.16.840.1.113883.1.11.1",
    "PatientDim.Marital Status": "2.16.840.1.113883.1.11.12212",
    "PatientDim.Race": "2.16.840.1.113883.1.11.14914",
    "PatientDim.Religion": "2.16.840.1.113883.1.11.19185",
    "PatientMedicationDim.RouteCode": "2.16.840.1.113883.3.88.12.3221.8.7",
    "ProblemsDim.SeverityTypeCode": "2.16.840.1.113883.3.88.12.3221.6.8",
    "ProblemsDim.StatusTypeCode": "2.16.840.1.113883.11.20.9.19",
    "VitalSignDim.TypeCode": "2.16.840.1.113883.3.88.12.3221.6.2",

}
# data_dict = {
#     "VitalSignDim.DisplayName": "VitalSignDim.VitalSign",
#     "AllergyDim.DisplayName": "AllergyDim.Allergies",
#     "ProblemsDim.DisplayName": "ProblemsDim.Problem",
#     "ProceduresDim.DisplayName": "ProceduresDim.Procedures",
#     "ImmunizationDim.DisplayName": "ImmunizationDim.Immunization",
#     "ResultsDim.DisplayName": "ResultsDim.Test",
#     "PatientMedicationDim.DisplayName": "PatientMedicationDim.Medication"
# }
condition_patterns = [
    fr'(\w+\([\w\s\(\)\-\+\.]*\)|\w+\.?\w*)\s+(?:BETWEEN|NOT BETWEEN)\s+((?:\w+\(.*\)|[\'"].+?[\'"]|\S+))\s+AND\s+((?:\w+\(.*\)|[\'"].+?[\'"]|\S+))',
    fr'(\w+\([\w\s\(\)\-\+\.]*\)|\w+\.?\w*|\w*\.?\`[\w\s]+\`)\s+(=|!=|>|<|>=|<=|IN|NOT\sIN|LIKE|NOT\sLIKE|ILIKE|NOT\sILIKE|is\snot)\s+((?:\w+\([\w\s\(\)\-\+]*\)|[\'"].+?[\'"]|[\(].*[\)]|\S+))'
]
pattern = fr'(\w+\([\w\s\(\)\-\+\.]*\)|\w+\.?\w*)\s+(is\s?null|is\snot\snull)'


class Filter:

    raw_text: str
    filter: str = None
    column: str = None
    operator: str = None
    value1: str = None
    value2: str = None

    def __init__(self, raw_text, table_name="",EP_ID="471782812.json"):
        self.raw_text = raw_text
        self.table_name = table_name
        self.EP_ID = EP_ID
        self.is_dictionary = False
        self.is_valid = True
        self.display_condition = False
        self.valueNotMatch = []
        self.returnNone = False
        self.unit = None
        try:
            self.parse()
        except Exception as e:
            if DEBUG:
                raise e
            self.returnNone = True

    def parse(self):
        """
        Parses the raw text to extract filter criteria including column, operator, and values.

        This method first attempts to find a match using a primary pattern. If a match is found,
        it extracts the filter, column, and operator, and initializes `value1` and `value2`. If no match
        is found with the primary pattern, the method iterates over a list of condition patterns, attempting
        to find a match for each. For matches found in this phase, it determines the appropriate operator 
        (e.g., BETWEEN or NOT BETWEEN) and extracts corresponding values if necessary. The operator is 
        then converted to lowercase, and the `process` method is called to handle further processing of 
        the extracted criteria.
        """
        
        match = re.search(pattern, self.raw_text, re.IGNORECASE)
        if match:
            self.filter = match.group(0)
            self.column = match.group(1)
            self.operator = match.group(2)
            self.value1 = None
            self.value2 = None
        else:
            for pattern_regex in condition_patterns:
                for match in re.finditer(pattern_regex, self.raw_text, re.IGNORECASE):
                    self.filter = match.group(0)
                    self.column = match.group(1)
                    if re.search(OPERATORS.BETWEEN.value, match.group(0), re.IGNORECASE):
                        if re.search(OPERATORS.NOT_BETWEEN.value, match.group(0), re.IGNORECASE):
                            self.operator = OPERATORS.NOT_BETWEEN.value
                        else:
                            self.operator = OPERATORS.BETWEEN.value
                        self.value1 = match.group(2)
                        self.value2 = match.group(3)
                    else:
                        self.operator = match.group(2)
                        self.value1 = match.group(3)
                    break
        self.column = re.sub(r'[^a-zA-Z\s\.\(\)]', '', self.column)
        self.operator = self.operator.lower()
        self.process()

    def process(self):
        """_summary_
        """

        if re.search(r'\bis\snot\snull\b|\bis\snull\b', self.operator, re.IGNORECASE):
            if f"{self.table_name}." not in self.column:
                self.column = f"{self.table_name}.{self.column}"
            if self.column == "ResultsDim.LowDate":
                self.column = "ResultsDim.ObsLowDate"
            self.input_type = get_input_type(self.column)
            self.operator = "notNull" if 'is not null' in self.operator else "null"

        else:
            if re.search(r'GetAge', self.column, re.IGNORECASE):
                self.column = "PatientDim.age"

                self.value1, unit1 = get_age_value(self.value1)

                if self.value2:
                    self.value2, unit2 = get_age_value(self.value2)
                self.unit = unit1 if unit1 is not None else unit2
                self.unit = self.unit.lower()
                if self.unit == 'year':
                    self.unit = 'yr'
                elif self.unit == 'month':
                    self.unit = 'mon'
                else:
                    self.unit = 'day'

            if f"{self.table_name}." not in self.column:
                self.column = f"{self.table_name}.{self.column}"

            # if self.column in column_name_mapping:
            #     self.value1, self.operator = obj1.get_code_for_condition(
                    # self.column, self.value1, self.operator)
            if self.column == "ResultsDim.LowDate":
                self.column = "ResultsDim.ObsLowDate"
            if self.column =='PatientProviderDim.TIN':
                self.column="PatientProviderDim.OrgIdRoot"
            if self.column == "PatientProviderDim.Specialty":
                self.column = " PatientProviderDim.SpecialtyCode"

            self.value1 = remove_single_quotes(self.value1)
            if self.value2:
                self.value2 = remove_single_quotes(self.value2)

            self.input_type = get_input_type(self.column,self.EP_ID)
            combined_value = f"{self.value1}|{self.value2}"

            if self.input_type == 'datetime-local':
                self.value1, self.value2, self.operator = process_date(
                    self.value1, self.value2, self.operator)

            if self.input_type == 'String':
                self.operator, self.value1 = sql_to_json(
                    self.column, self.operator, self.value1)
                if re.match(r"\(.*\)", self.value1) and self.value2 is None:
                    self.value1 = re.sub(r'[()]', '', self.value1)
                    print( re.split(r", (?=(?:'[^']*?(?: [^']*)*))|, (?=[^',]+(?:,|$))", self.value1))
                    self.value1 = [remove_single_quotes(
                        value.strip()) for value in re.split(r", (?=(?:'[^']*?(?: [^']*)*))|, (?=[^',]+(?:,|$))", self.value1)]

                self.operator, self.value1 = convert_gender_terms(
                    self.column, self.operator, self.value1)
                if isinstance(self.value1, str):
                    self.value1 = [self.value1]
            if re.search(r'\b(?:DisplayName)\b', self.column, re.IGNORECASE):
                self.display_condition = True

                # if re.match(r"in|not\s+in", self.operator, re.IGNORECASE):
                self.value1, self.gv_column, self.valueNotMatch = gv_value(
                    self.value1, self.column)
            if self.display_condition == False:
                self.value1, self.operator = find_most_similar_value(self.column, self.value1, self.operator,self.EP_ID)
            if re.search(r'(?:Int32|Int64|Float32|Float64)', self.input_type, re.IGNORECASE):
                
                if re.search(r'to\w+\(.*?\)', self.value1):
                    self.value1 = retrieve_date_from_clickhouse(self.value1)
                if self.value2 is not None and re.search(r'to\w+\(.*?\)', self.value2):
                    self.value2 = retrieve_date_from_clickhouse(self.value2)
                
                self.value1 = float(self.value1)
                if self.value2:
                    self.value2 = float(self.value2)
        if 'not between' in self.operator:
            self.operator = "notBetween"

    def __str__(self):
        return self.filter

    def matched_str(self):
        return self.filter

    def json(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        if self.returnNone:
            return None
        if (self.input_type == 'datetime-local'):
            return [{
                'condition_section': [{
                    'and_or_value': "",
                    'operator': self.operator,
                    'value': self.value1,
                    'value1': self.value2
                }],
                'field_name': self.column,
                'is_dictionary': self.is_dictionary,
                'interval_by': '',
                'interval_value': '',
                'trend': False,
                'raw_text': self.raw_text

            }]
        elif check_age_suffix(self.column):
            return [{
                'condition_section': [{
                    'and_or_value': "",
                    'operator': self.operator,
                    'value': self.value1,
                    'value1': self.value2

                }],
                'field_name': self.column,
                'is_dictionary': self.is_dictionary,
                'unit': self.unit,
                'raw_text': self.raw_text
            }]
        elif self.display_condition == True:
            list_2_return = [
                {
                    'condition_section': [{
                        'and_or_value': "",
                        'operator': process_operator(self.operator, self.value1),
                        'value': self.value1,
                        'value1': self.value2

                    }],
                    'field_name': self.gv_column,
                    'fxFunction': '',
                    'is_dictionary': True,
                    'raw_text': self.raw_text

                },
                {
                    'condition_section': [{
                        'and_or_value': "",
                        'operator': process_operator(self.operator, self.valueNotMatch),
                        'value': self.valueNotMatch,
                        'value1': self.value2

                    }],
                    'field_name': self.column,
                    'fxFunction': '',
                    'is_dictionary': False,
                    'raw_text': self.raw_text

                }
            ]
            self.json_objects = []
            for object in list_2_return:
                if len(object['condition_section'][0]['value']) != 0:
                    self.json_objects.append(object)
            return self.json_objects

        else:
            return [{
                'condition_section': [{
                    'and_or_value': "",
                    'operator': self.operator,
                    'value': self.value1,
                    'value1': self.value2

                }],
                'field_name': self.column,
                'fxFunction': '',
                'is_dictionary': self.is_dictionary,
                'raw_text': self.raw_text
            }]
