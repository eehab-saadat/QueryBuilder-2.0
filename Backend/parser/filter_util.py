import os
from .Gvhandler import Variations_Handler
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from difflib import SequenceMatcher
import json
from .utils.constants import BASE_PATH
from langchain_community.utilities.sql_database import SQLDatabase


obj = Variations_Handler()
# Print the content of the JSON file

data_dict = {
    "VitalSignDim.DisplayName": "VitalSignDim.VitalSign",
    "AllergyDim.DisplayName": "AllergyDim.Allergies",
    "ProblemsDim.DisplayName": "ProblemsDim.Problem",
    "ProceduresDim.DisplayName": "ProceduresDim.Procedures",
    "ImmunizationDim.DisplayName": "ImmunizationDim.Immunization",
    "ResultsDim.DisplayName": "ResultsDim.Test",
    "PatientMedicationDim.DisplayName": "PatientMedicationDim.Medication",
    "EncountersDim.DisplayName": "EncountersDim.Type"
}


def get_value(key):
    value = data_dict.get(key, "Key not found")
    return value


def remove_single_quotes(value):
    return re.sub(r"^'(.*)'$", r'\1', value)


def is_valid_date(date_string):
    """
    Checks if a given date string matches the expected format (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) 
    and returns it in a standardized ISO 8601 format.

    Args:
        date_string (str): The date string to validate and format.

    Returns:
        str or None: The formatted date string in YYYY-MM-DDTH:M:S:000Zformat if valid, otherwise None.
    """
    date_pattern = r'^(\d{4})-(\d{1,2})-(\d{1,2})(?: (\d{1,2}):(\d{1,2}):(\d{1,2}))?$'

    match = re.match(date_pattern, date_string)

    if match:
        year, month, day, hour, minute, second = match.groups()
        month = month.zfill(2)
        day = day.zfill(2)
        hour = (hour or '00').zfill(2)
        minute = (minute or '00').zfill(2)
        second = (second or '00').zfill(2)

        formatted_date = f"{year}-{month}-{day}T{hour}:{minute}:{second}.000Z"

        return formatted_date
    else:
        return None


def get_input_type(column_name,EP_ID):
    
    """
    Retrieve the input type for a given column name.

    Args:
    - column_name (str): The name of the column to find.

    Returns:
    - str: The input type of the column or a message if the column is not found.
    """
    with open(f"{BASE_PATH}enterprises/field_info_map_{EP_ID}", 'r') as file:
        field_info_map = json.load(file)

    if column_name in field_info_map:

        column_info = field_info_map[column_name][0]
        return column_info.get("inputType", "Input type not found")
    else:
        return "Column not found"


def get_age_value(text):
    pattern = r"today\(\)\s*([\-])\s*INTERVAL\s+(\d+)\s+(YEAR|MONTH|DAY)?"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        value = match.group(2)
        unit = match.group(3)
        return value, unit
    pattern = r"today\(\)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return 0, None

    pattern = pattern = r"today\(\)\s*([\-])\s*INTERVAL\s+(\d+)\s+(YEAR|MONTH|DAY)?"


def retrieve_date_from_clickhouse(value):

    print("RUN ON DB:", value)
    host = os.getenv("CLICKHOUSE_HOST", "")
    port = os.getenv("CLICKHOUSE_PORT", "")
    user = os.getenv("CLICKHOUSE_USER", "")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    instance = os.getenv("CLICKHOUSE_INSTANCE", "")
    uri = f'clickhouse://{user}:{password}@{host}:{port}/{instance}'

    # Creating SQLAlchemy Instance of the database
    db = SQLDatabase.from_uri(uri)
    try:
        response = db.run_no_throw(f"SELECT {value}")
        print("DB Response:", response)
        pattern = r'\(([^,]+),'
        is_match = re.search(pattern, response)
        if is_match:
            if "'" == is_match.group(1)[0]:
                return is_match.group(1)[1:-1]
            return is_match.group(1)
    except Exception as e:
        print("ERR:", e)
        pass
    return None


def calculate_date_from_string(value, operator):
    """
    Calculates a date based on a string value that may contain a relative 
    time expression (e.g., "today() + INTERVAL 1 YEAR").

    Args:
        value (str): A string containing a relative time expression.
        operator (str): The comparison operator (e.g., '=', '!=').

    Returns:
        tuple: The modified operator and resulting date range or date string.
    """
    today = datetime.today()
    operator_conversion = {
        "=": "between",
        "!=": "not between"
    }
    # pattern = r"today\(\)\s*([\+\-])\s*INTERVAL\s+(\d+)\s+(YEAR|MONTH|DAY)?"
    datetime_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
    date_pattern = r"\d{4}-\d{2}-\d{2}"
    result_date_str = None
    value2 = None

    if re.fullmatch(datetime_pattern, value) or re.fullmatch(date_pattern, value):
        if re.fullmatch(datetime_pattern, value):
            result_date = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        elif re.fullmatch(date_pattern, value):
            result_date = datetime.strptime(value, '%Y-%m-%d')
            result_date = result_date.replace(hour=0, minute=0, second=0)

        value1 = result_date.strftime('%Y-%m-%d %H:%M:%S')
        value2 = result_date.replace(
            hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')
        if operator in operator_conversion.keys():
            operator = operator_conversion.get(operator, operator)
            return value1, value2, operator
        else:
            return value1, None, operator
    else:

        print("\n\n************************")
        print("VALUE:", value)
        print("OPERATOR:", operator)
        print("************************\n\n")

        return retrieve_date_from_clickhouse(value), None, operator
        return value, None, operator

    # if operator == "=":
    #     operator = "between"

    # if operator not in ["=", "!="]:
    #     match = re.search(pattern, value, re.IGNORECASE)
    #     if match:
    #         op, number, interval_type = match.groups()
    #         number = int(number)
    #         delta_args = {f"{interval_type.lower()}s": number}
    #         result_date = today + \
    #             relativedelta(**delta_args) if op == '+' else today - \
    #             relativedelta(**delta_args)
    #     elif re.search(r"today", value, re.IGNORECASE):
    #         result_date = today
    #     else:
    #         return value, None, operator

    #     result_date_str = result_date.strftime('%Y-%m-%d %H:%M:%S')
    #     return result_date_str, None, operator

    # elif operator in ["=", "!="]:
    #     try:
    #         result_date = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    #     except ValueError:
    #         return None, None, f"Invalid date format: {value}"

    #     value1 = result_date.replace(
    #         hour=00, minute=00, second=00).strftime('%Y-%m-%d %H:%M:%S')
    #     value2 = result_date.replace(
    #         hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')
    #     if operator == "=":
    #         operator = "between"
    #     else:
    #         operator = "not between"
    #     return value1, value2, operator

    # return result_date_str, value2, operator


def process_date(value1, value2, operator):
    value1, temp_value2, operator = calculate_date_from_string(
        value1, operator)

    if temp_value2 is not None:
        value2 = temp_value2

    if value2 is not None:
        value2, _, operator = calculate_date_from_string(
            value2, operator)
        value2 = is_valid_date(value2)

    value1 = is_valid_date(value1)

    return value1, value2, operator


def get_age(column, operator, condition1, condition2):
    """
    Processes an SQL-like condition to extract details about age-related queries, 
    particularly when using a custom function like GetAge. Converts 'GetAge' 
    references to 'PatientDim.Age' and determines the appropriate comparison values.

    Args:
        column (str): The column name, potentially including a function like 'GetAge'.
        operator (str): The comparison operator, such as '=', '>', 'BETWEEN', etc.
        condition1 (Any): The first condition value, typically a number.
        condition2 (Any): The second condition value, used when the operator is 'BETWEEN'.

    Returns:
        dict: A dictionary containing the processed column, operator, and condition values.
    """
    value1 = condition1
    value2 = False

    regex_get = re.search(r'GetAge', column, re.IGNORECASE)

    if regex_get:
        column = "PatientDim.Age"

        between_pattern = r'not\s?between|between'
        regex_get = re.search(between_pattern, operator, re.IGNORECASE)

        if regex_get:
            value1 = condition1
            value2 = condition2
        else:
            value2 = False

        is_dictionary = False
    else:
        value1 = condition1
        value2 = False
        is_dictionary = False

    result = {
        "column": column,
        "operator": operator,
        "value1": value1,
        "value2": value2,
        "is_dictionary": is_dictionary
    }

    return result


def sql_to_json(column_name, operator, value):
    """
    Converts an SQL-like condition into a JSON-compatible format, handling specific 
    operators and patterns.

    Args:
        column_name (str): The name of the column involved in the condition.
        operator (str): The comparison operator, such as '=', '!=', 'ilike', etc.
        value (str): The value or pattern to compare against.

    Raises:
        ValueError: If the operator is not one of the accepted types.

    Returns:
        tuple: A tuple containing the converted operator and value.
        return operators like contain, beginwith ,end with
    """
    accepted_operators = re.compile(
        r"^\s*(=|!=|<|>|ilike|not\s+ilike|like|not\s+like|null|not\s+null|in|not\sin)\s*$", re.IGNORECASE)
    operator = re.sub(r'\s+', ' ', operator.strip())  # Normalize whitespace

    if not accepted_operators.match(operator):
        raise ValueError(
            "This function only supports 'ilike', 'not ilike', 'like', and 'not like' operators.")

    output_json = {
        "field_name": column_name,
        "is_dictionary": False,
        "operator": "",
        "value": "",
        "value1": None
    }

    if re.match(r"ilike|like", operator, re.IGNORECASE):
        if re.match(r"^%.*%$", value):
            output_json["operator"] = "contains"
            output_json["value"] = value[1:-1]
        elif re.match(r"^%.*$", value):
            output_json["operator"] = "endsWith"
            output_json["value"] = value[1:]
        elif re.match(r"^.*%$", value):
            output_json["operator"] = "beginsWith"
            output_json["value"] = value[:-1]
        else:
            output_json["operator"] = "="
            output_json["value"] = value
            output_json["fxFunction"] = ""

    elif re.match(r"not\silike|not\slike", operator, re.IGNORECASE):
        if re.match(r"^%.*%$", value):
            output_json["operator"] = "doesNotContain"
            output_json["value"] = value[1:-1]
        elif re.match(r"^%.*$", value):
            output_json["operator"] = "doesNotEndWith"
            output_json["value"] = value[1:]
        elif re.match(r"^.*%$", value):
            output_json["operator"] = "doesNotBeginWith"
            output_json["value"] = value[:-1]
        else:
            output_json["operator"] = "!="
            output_json["value"] = value
            output_json["fxFunction"] = ""
    elif operator in ["not null", "null"]:
        output_json["operator"] = "null"
        output_json["value"] = "null"
    else:
        output_json["operator"] = operator
        output_json["value"] = value
    return output_json["operator"], output_json["value"]


def convert_gender_terms(column_name, operator, value):
    """
    Converts gender-related terms to standardized codes ('M' for male, 'F' for female, 'UN' for unknown)
    in a specified column, based on recognized gender-related words.

    Args:
        column_name (str): The name of the column, potentially containing the word 'gender'.
        operator (str): The comparison operator, such as '=', '!=', etc.
        value (str): The value containing gender-related terms to be standardized.

    Returns:
        tuple: A tuple containing the operator and the converted value.
    """

    male_terms = [
        "man", "male", "boy", "gentleman", "guy", "dude",
        "fellow", "gent", "bloke", "chap"
    ]
    female_terms = [
        "woman", "female", "girl", "lady", "dame",
        "miss", "mademoiselle", "queen", "princess"
    ]

    male_pattern = re.compile(
        r'(?:' + '|'.join(male_terms) + r')', re.IGNORECASE)
    female_pattern = re.compile(
        r'(?:' + '|'.join(female_terms) + r')', re.IGNORECASE)

    def replace_terms(text, pattern, replacement):
        """Helper function to replace terms using a pattern."""
        return pattern.sub(replacement, text)

    if re.search(r'gender', column_name, re.IGNORECASE):

        value = replace_terms(value, female_pattern, 'F')
        value = replace_terms(value, male_pattern, 'M')
        value = ",".join(
            [val if val in ["F", "M"] else "UN" for val in value.split(",")])

    return operator, value


def check_age_suffix(column):
    pattern = r'\.age$'
    return bool(re.search(pattern, column))


def gv_value(value, column):
    """
    Process the comma-separated values in self.value1 and update it based on the results
    from the get_GV_for_word method. Also check if self.column is in data_dict.

    Parameters:
    - data_dict (dict): A dictionary to check if self.column is a key.

    Returns:
    - None
    """
    gvs_output = []
    for stripped_value in value:
        gv = obj.get_GV_for_word2_0(stripped_value, column)
        gvs_output.append(gv)
    is_dictionary = False
    unique_values = []
    for gv in gvs_output:
        if gv[1]:
            unique_values.extend(gv[0])
    unique_values = list(set(unique_values))
    # value = [gv[0] for gv in gvs_output if gv[1]]
    valueNotMatch = [gv[0] for gv in gvs_output if gv[1] == False]

    if column in data_dict:
        gv_column = get_value(column)
        is_dictionary = True
    else:
        gv_column = column
    return unique_values, gv_column, valueNotMatch


def process_operator(operator, values):
    operator = re.sub(r'\s+', '', operator)
    if len(values) <= 1:
        if re.search(r'(=|!=)', operator, re.IGNORECASE):
            return operator
        else:
            return "!=" if re.search(r'not', operator, re.IGNORECASE) else "="
    else:
        if re.search(r'not', operator, re.IGNORECASE):
            return "notIn"
        else:
            return "in"

def find_most_similar_value(column_name : str, input_values : list, operator:str, EP_ID:str,similarity_threshold : float =0.0) -> list:

    with open(f"{BASE_PATH}enterprises/value_info_map_{EP_ID}", 'r') as file:
        schema_data = json.load(file)

    value_names = []
    

    if column_name in schema_data:
        display_names_both = []
        for i in schema_data[column_name]:
            current = i.get('info',{}).get('values',[])

            if len(schema_data[column_name]) > 1:

    
                value_names.append(i['tableAlias'])

                for j in current:
                    j['code'] = i['tableAlias'] + "-" + j['code']
     
            display_names_both.append(current)
        
        similar_values = []


        for i,j in enumerate(display_names_both):

            display_names = [(value['displayName'], value['code'])
                            for value in j]

            

            for input_value in input_values:
                normalized_input = input_value.lower()
                similar_words = {}

                for display_name, code in display_names:
                    
                    normalized_display_name = display_name.lower()

                    similarity = SequenceMatcher(
                        None, normalized_input, normalized_display_name).ratio()
                    

                    if similarity >= similarity_threshold:
                        similar_words[display_name] = (code, similarity)
                        

                if similar_words:
                    best_match = max(similar_words.items(), key=lambda x: x[1][1])
                    best_code = best_match[1][0]
                    similar_values.append(best_code)
                else:
                    if len(display_names_both) > 1 :
                        similar_values.append(value_names[i] + '-' + input_value)
                    else:
                        similar_values.append(input_value)

        if "not" in operator.lower() or "!=" in operator.lower():
            operator = "!="
        else:
            operator = "="
        print("similar values----------------",similar_values)

        return similar_values, operator
    else:
        print(f"Column '{column_name}' is not multi-select, returning the input values as-is.")
        return input_values, operator
    