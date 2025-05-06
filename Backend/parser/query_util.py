import re
import json
from .utils.constants import BASE_PATH


def split_columns(columns_str):
    """_summary_

    Args:
        columns_str (_type_): _description_

    Returns:
        _type_: _description_
    """

    stack = []
    columns = []
    current_col = []
    for char in columns_str:
        if char == ',' and not stack:
            column = ''.join(current_col).strip()
            if column:
                columns.append(column)
            current_col = []
        else:
            if char == '(':
                stack.append('(')
            elif char == ')' and stack:
                stack.pop()
            current_col.append(char)
    column = ''.join(current_col).strip()
    if column:
        columns.append(column)
    return columns


def clean_column_name(column):
    """_summary_

    Args:
        column (_type_): _description_

    Returns:
        _type_: _description_
    """

    if '(' in column and ')' in column:
        return column.strip()
    else:
        parts = column.split()
        return parts[-1]


def extract_table_name(query):
    """
        Extracts table name from a SQL query.

        This function searches for table names in a SQL  a lquery by looking for keywords typically followed by table names such as FROM, JOIN, INTO, UPDATE, and TABLE. It returnsist of tables found in the query.

        Args:
            query (str): The SQL query string from which to extract table names.

        Returns:
            table_name: A table name is returned for the provided query.
        """

    table_pattern = re.compile(
        r'\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([\w\.]+)', re.IGNORECASE)
    tables = table_pattern.findall(query)
    if tables:
        return tables[0]
    else:
        return ""


def extract_column_names(query):
    """
        Extracts the column names from a SQL query's SELECT statement.

        This function identifies and extracts column names from a given SQL query, specifically from the SELECT statement. It can optionally preprocess the column names for further analysis or usage.

        Args:
            query (str): The SQL query string from which to extract column names.
            preprocessed (bool, optional): If True, the column names will be cleaned and preprocessed. Defaults to True.

        Returns:
            list: A list of column names extracted from the SQL query. If preprocessing is enabled, returns a list of cleaned column names; otherwise, returns the raw column names.

        """

    select_from_pattern = re.compile(
        r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
    match = select_from_pattern.search(query)
    if match:
        columns_str = match.group(1)
        columns = split_columns(columns_str)
        processed_columns = [clean_column_name(col) for col in columns]
        return processed_columns
    return []


def extract_where_conditions(sql_query):
    """
        Extracts WHERE conditions from the given SQL query and returns only the sub-conditions.

        Args:
            sql_query (str): The SQL query string from which to extract WHERE conditions.

        Returns:
            list[str]: A list of strings, each representing a sub-condition found in the WHERE clause.
    """
    string_terms = re.findall(r"'(.*?)'", sql_query)
    replacement_map = {}
    for idx, term in enumerate(string_terms):
        replacement_map[f"condition{idx+1}"] = term
        sql_query = sql_query.replace(f"'{term}'", f"'condition{idx+1}'")

    pattern = r'WHERE\s+(.+)$'
    match = re.search(pattern, sql_query, re.IGNORECASE | re.MULTILINE)
    sub_conditions_list = []

    if match:
        condition_clause = match.group(1).strip()
        between_pattern = r'\(?(\w+\([\w\s\(\)\-\+\.]*\)|\w+\.?\w*)\s+(?:BETWEEN|NOT BETWEEN)\s+((?:to\w\(.*?\)|\w+\(.*?\)|[\'"].+?[\'"]|[-+]?\d*\.?\d+|\S+))\s+AND\s+((?:to\w\(.*?\)|\w+\(.*?\)|[\'"].+?[\'"]|[-+]?\d*\.?\d+|\S+))\s*(?=AND|OR|\s*$)'
        for between_match in re.finditer(between_pattern, condition_clause, re.IGNORECASE):
            between_condition = between_match.group(0).strip()
            sub_conditions_list.append(between_condition)
            condition_clause = condition_clause.replace(between_condition, '')

        condition_clause = re.sub(r'\s+', ' ', condition_clause.strip())

        if condition_clause != '':
            conditions = re.split(
                r'\b(?:AND|OR)\b(?![\(A-Z])', condition_clause, flags=re.IGNORECASE)
            for condition in conditions:
                condition = condition.strip()

                if condition.startswith('(') and (condition.endswith(')') or condition.endswith('"')):
                    condition = condition[1:-1].strip()

                sub_conditions = re.split(
                    r'\s+(AND|OR)\s+', condition, flags=re.IGNORECASE)
                for sub_condition in sub_conditions:
                    sub_condition = sub_condition.strip()
                    if sub_condition:  # Check if the sub_condition is not an empty string
                        sub_conditions_list.append(sub_condition)
    for idx, sub_condition in enumerate(sub_conditions_list):

        found_words = [word for word in replacement_map.keys() if word in sub_condition]
        if found_words:

            for word in found_words:

                sub_condition = sub_condition.replace(word, replacement_map[word])

        sub_conditions_list[idx] = sub_condition

    
    sub_conditions_list = [match_brackets_count(
        sub_condition) for sub_condition in sub_conditions_list]
    return sub_conditions_list


def get_table_label_from_tab(table_name,EP_ID):
    """Retrieves the table label(s) associated with a given table name.

    Args:
        table_name (str): The name of the table for which to retrieve labels.

    Returns:
        list[str]: A list of table labels associated with the table name.
    """
    all_names = set()
    with open(f"{BASE_PATH}enterprises/field_info_map_{EP_ID}", 'r') as file:
        field_info_map = json.load(file)
    for key, value_list in field_info_map.items():
        for value in value_list:
            if value.get('tableName') == table_name:
                all_names.add(value.get('tableLabel'))
    
    return list(all_names)

def get_table_label_from_col(column_name,EP_ID):
    """Retrieves the table label associated with a given column name.

    Args:
        column_name (str): The name of the column for which to retrieve the label.

    Returns:
        str: The table label associated with the column name.
    """
    
    with open(f"{BASE_PATH}enterprises/field_info_map_{EP_ID}", 'r') as file:
        field_info_map = json.load(file)

    if column_name in field_info_map:
        column_info = field_info_map[column_name][0]
        return column_info.get('tableLabel')
    


def get_only_table_name(table_name):
    """Extracts and returns the simple table name without any schema or database prefixes.

    Args:
        table_name (str): The full table name potentially including schema or database prefixes.



    Returns:
        str: The simple table name without prefixes.
    """
    split_name = re.split(r'\.', table_name)
    only_table_name = split_name[-1]
    return only_table_name


def check_ppid_in(text):
    """Checks if the text contains a condition involving PPID or RESOURCEID.

    Args:
        text (str): The text to search for PPID or RESOURCEID conditions.

    Returns:
        bool: True if the text contains PPID or RESOURCEID conditions, False otherwise.
    """
    pattern = r'(PPID|RESOURCEID)\s(NOT\sIN|IN)'
    match = re.search(pattern, text, re.IGNORECASE)
    return match is not None


def get_unique_col_label(conditions):
    """Gets a unique table label from a list of conditions.

    Args:
        conditions (list[dict]): A list of condition dictionaries where each dictionary contains field names.

    Returns:
        str: The first unique table label found in the conditions, or None if no labels are found.
    """
    table_labels = set()
    for condition in conditions:
        table_labels.add(get_table_label_from_col(condition['field_name']))
    return next(iter(table_labels), None)


def match_brackets_count(query):
    opening_count = 0
    closing_count = 0
    for charachter in query:
        if charachter == "(":
            opening_count += 1
        elif charachter == ")":
            closing_count += 1

    if closing_count > opening_count:
        return query[:-1].strip()
    elif closing_count < opening_count:
        return query[1:].strip()
    else:
        return query
