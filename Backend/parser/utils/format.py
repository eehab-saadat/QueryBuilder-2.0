import re


def convert_to_singleline_sql(clickhouse_query):
    """
    Converts a multi-line SQL query to a single-line formatted SQL query.

    Args:
        s (str): The input multi-line SQL query.

    Returns:
        str: The single-line formatted SQL query.

    This function takes a multi-line SQL query as input and converts it to a single-line formatted SQL query. It removes leading and trailing whitespace, replaces newline characters with spaces, and removes extra whitespace between symbols. The modified query is then returned.
    """
    clickhouse_query = clickhouse_query.strip()
    clickhouse_query = re.sub("\n", " ", clickhouse_query)
    clickhouse_query = re.sub("\s+", " ", clickhouse_query)
    clickhouse_query = re.sub("\s\)", ")", clickhouse_query)
    return clickhouse_query
