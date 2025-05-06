import re
import json

def break_into_subqueries(full_query, exclude_brackets=True) -> list[str]:
    """
    Extract all subqueries by recursively checking for SELECT blocks.

    Args:
        full_query (str): The complete SQL query.
        exclude_brackets (bool): Whether to exclude brackets from the extracted subqueries.

    Returns:
        list[str]: A list of subqueries.
    """
    stack = []
    substrings = []
    current_substring_start = None

    for i, char in enumerate(full_query):
        if char == '(':
            stack.append(i)
            if len(stack) == 1:
                current_substring_start = i  # Start of a new substring
        elif char == ')':
            if stack:
                stack.pop()
                if len(stack) == 0 and current_substring_start is not None:
                    if exclude_brackets:
                        queries = full_query[current_substring_start + 1:i].split("INTERSECT")
                        # substrings.append(
                        #     )
                        substrings.extend(queries)
                    else:
                        queries = full_query[current_substring_start:i + 1].split("INTERSECT")
                        substrings.extend(queries)
                        # substrings.append(
                        #     full_query[current_substring_start:i + 1])
                    current_substring_start = None  # Reset start
                    if exclude_brackets:
                        substrings.extend(
                            break_into_subqueries(substrings[-1]))
                    else:
                        substrings.extend(
                            break_into_subqueries(substrings[-1][1:-1]))

    return substrings


def remove_non_select_queries(all_queries: list[str]) -> list[str]:
    """
    Remove queries that do not contain the SELECT keyword.

    Args:
        all_queries (list[str]): List of SQL queries.

    Returns:
        list[str]: Filtered list of queries containing the SELECT keyword.
    """
    pattern_for_subquery = r"\bSELECT\b"
    all_queries = [query for query in all_queries if re.search(
        pattern_for_subquery, query, re.IGNORECASE)]
    return all_queries


def extract_hierarchy(all_sub_queries: list[str]) -> dict:
    """
    Extract a hierarchy of sub-queries from the given list of queries by sorting them based on their length 
    and replacing occurrences of longer queries with shorter ones in subsequent queries.

    Args:
        all_sub_queries (list[str]): A list of SQL sub-queries.

    Returns:
        dict: A dictionary where keys are sub-query identifiers and values are the corresponding sub-queries
    """

    sorted_queries = sorted(all_sub_queries, key=len)
    subquery_counter = 1
    sub_query_map = {}

    for i, query in enumerate(sorted_queries):
        subquery_name = f"SubQuery_{subquery_counter}" if (
            i+1) != len(sorted_queries) else "SubQuery_0"

        sub_query_map[subquery_name] = query
        subquery_counter += 1

        # Replace occurrences of longer queries in subsequent queries
        for j in range(i + 1, len(sorted_queries)):
            sorted_queries[j] = re.sub(
                re.escape(query), subquery_name, sorted_queries[j])

    return sub_query_map
def print_subqueries_with_resource_id_in(queries):
    """
    Print subqueries that are preceded by 'ResourceId IN'.
    
    Args:
        queries (dict): Dictionary of query objects where keys are query identifiers
                        and values are query objects with a 'raw_text' attribute.
    """
    pattern = re.compile(r'\bResourceId\s+IN\s*\(\s*(SubQuery_\d+)\s*\)', re.IGNORECASE)
    for key, query in queries.items():
        match = pattern.search(query.raw_text)
        if match:
            subquery_key = match.group(1)
            if subquery_key in queries:
                print(f"SubQuery with 'ResourceId IN':\n{queries[subquery_key].raw_text}\n")

def print_parent_queries_for_resource_id_in(queries):
    """
    Print the parent queries that contain subqueries with 'ResourceId IN'.
    
    Args:
        queries (dict): Dictionary of query objects where keys are query identifiers
                        and values are query objects with a 'raw_text' attribute.
    """
    pattern = re.compile(r'\bResourceId\s+IN\s*\(\s*(SubQuery_\d+)\s*\)', re.IGNORECASE)
    
    # Create a dictionary to map subquery keys to their parent query keys
    parent_map = {}
    
    # First, find the parent query for each subquery with 'ResourceId IN'
    for key, query in queries.items():
        matches = pattern.findall(query.raw_text)
        if matches:
            for subquery_key in matches:
                parent_map[subquery_key] = key
    
    # Now print the parent queries
    for subquery_key, parent_key in parent_map.items():
        if parent_key in queries:
            print(f"Parent Query for {subquery_key}:\n{queries[parent_key].raw_text}\n")

def merge_queries(parent_json, subquery_json):
    """
    Merge the subquery JSON into the parent JSON with appropriate modifications.

    Args:
        parent_json (dict): JSON of the parent query.
        subquery_json (dict): JSON of the subquery.

    Returns:
        dict: Merged JSON of the parent query with subquery conditions.
    """
    if not isinstance(parent_json, list) or not isinstance(subquery_json, list):
        raise ValueError("Both parent_json and subquery_json should be lists of dictionaries.")
    
    # Assuming there is only one item in the list for simplicity
    parent_query = parent_json[0]
    subquery_conditions = subquery_json[0]["conditions"]

    # Determine the prefix based on the section_name in the parent JSON using regex
    section_name = parent_query.get("section_name", "")
    section_name_normalized = section_name.strip().lower()

    # Define regex patterns for matching section names
    procedures_pattern = re.compile(r'^procedures\s*dim$', re.IGNORECASE)
    encounters_pattern = re.compile(r'^encounters\s*dim$', re.IGNORECASE)
    
    if procedures_pattern.match(section_name_normalized):
        prefix = "ProcedureProvider-"
    elif encounters_pattern.match(section_name_normalized):
        prefix = "EncounterProvider-"
    else:
        prefix = ""
    
    # Modify field_names in subquery conditions and filter values based on prefix
    for condition in subquery_conditions:
        if "field_name" in condition:
            condition["field_name"] = f"{prefix}{condition['field_name']}"
        
        # Check for condition_section and update the values based on the prefix
        if "condition_section" in condition:
            for sub_condition in condition["condition_section"]:
                if "value" in sub_condition and isinstance(sub_condition["value"], list):
                    prefixed_values = [val for val in sub_condition["value"] if val.startswith(prefix)]
                    
                    if prefixed_values:
                        sub_condition["value"] = [prefixed_values[0][len(prefix):]]
                    else:
                        sub_condition["value"] = sub_condition["value"]

    # Merge subquery conditions into the parent JSON
    parent_query["conditions"].extend(subquery_conditions)

    return [parent_query]

def invert_operator(operator):
    invert_dict = {
        "=": "!=",
        "!=": "=",
        "in": "notIn",
        "notIn": "in",
    }
    return invert_dict.get(operator, operator)

def process_final_json(final_json):
    pattern = re.compile(r"DisplayName", re.IGNORECASE)

    for item in final_json:
        item["NOT"] = True
        if isinstance(item, dict) and "conditions" in item:
            for condition in item["conditions"]:
                if condition.get("is_dictionary") == True or pattern.search(condition.get("field_name", "")):
                    condition["operator"] = invert_operator(condition["operator"])
    
    return final_json
def update_final_json(final_json):
    final_json["data"]["json"]["selection"]["demographics"].extend([
            {
                "field_name": "PatientDim.PPID",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            },
            {
                "field_name": "PatientDim.MRN",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            },
            {
                "field_name": "PatientDim.FirstName",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            },
            {
                "field_name": "PatientDim.LastName",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            },
            {
                "field_name": "PatientDim.Gender",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            },
            {
                "field_name": "PatientDim.DOB",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            },
            {
                "field_name": "PatientDim.age",
                "fxFunction": "",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            }
        ])

    for section in final_json["data"]["json"]["section_containers"][0]["sections"]:
        section_name = section.get("section_name")
        section_text = section.get("section_text")

        if section.get("NOT", None) == True:
            del section["NOT"]
            continue

        if section_name == "Demographics":
            for condition in section["conditions"]:
                condition_data = {
                    "field_name": condition.get("field_name"),
                    "fxFunction": "",
                    "section_name": section_name,
                    "section_text": section_text
                }
                final_json["data"]["json"]["selection"]["demographics"].append(condition_data)

        else:
            for condition in section["conditions"]:
                condition_data = {
                    "field_name": condition.get("field_name"),
                    "fxFunction": "",
                    "section_name": section_name,
                    "section_text": section_text
                }
                final_json["data"]["json"]["selection"]["others"].append(condition_data)

                if condition.get("is_dictionary"):
                    condition_data_copy = condition_data.copy()
                    condition_data_copy["field_name"] = condition_data["field_name"].split(".")[0] + ".DisplayName"
                    final_json["data"]["json"]["selection"]["others"].append(condition_data_copy)

    final_json["data"]["json"]["selection"]["demographics"] = remove_duplicates(
        final_json["data"]["json"]["selection"]["demographics"]
    )
    final_json["data"]["json"]["selection"]["others"] = remove_duplicates(
        final_json["data"]["json"]["selection"]["others"]
    )

    return final_json

def remove_duplicates(query):
    return [x for idx, x in enumerate(query) if x not in query[:idx]]

def merged_sections(data):
   
    section_containers = data['data']['json']['section_containers']
    
    in_pattern = re.compile(r"^(=|in)$")
    notin_pattern = re.compile(r"^(!=|notIn)$")
    
    for container in section_containers:
        sections = container.get("sections", [])
        merged = {}

        for section in sections:
            section_name = section["section_name"]
            
            if section_name not in merged:
                merged[section_name] = {
                    "section_name": section_name,
                    "section_text": section["section_text"],
                    "conditions": section["conditions"].copy()
                }
            else:
                for condition in section["conditions"]:
                    field_name = condition["field_name"]
                    is_dictionary = condition["is_dictionary"]
                    operator = condition['condition_section'][0]['operator'].strip()
                    found = False

                    for existing_condition in merged[section_name]["conditions"]:
                        if (existing_condition["field_name"] == field_name and
                            existing_condition["is_dictionary"] and is_dictionary):
                            
                            existing_operator = existing_condition['condition_section'][0]['operator'].strip()

                            if operator == existing_operator:
                                if re.match(in_pattern, operator):
                                    existing_condition['condition_section'][0]['value'].extend(condition['condition_section'][0]['value'])  # Add the new value to the list
                                    existing_condition['condition_section'][0]['operator']= "in"
                                elif re.match(notin_pattern, operator):
                                    existing_condition['condition_section'][0]['value'].extend(condition['condition_section'][0]['value'])  # Add the new value to the list
                                    existing_condition['condition_section'][0]['operator'] = "notIn"
                                found = True

                                break
                            else:
                                if (re.match(in_pattern, operator) and re.match(in_pattern, existing_operator)) or \
                                (re.match(notin_pattern, operator) and re.match(notin_pattern, existing_operator)):
                                    existing_condition['condition_section'][0]['value'].extend(condition['condition_section'][0]['value'])  # Add the new value to the list
                                    existing_condition['condition_section'][0]['operator']= "in" if re.match(in_pattern, operator) else "notIn"
                                    found = True
                                    break
                                elif (re.match(in_pattern, operator) and re.match(notin_pattern, existing_operator)) or \
                                    (re.match(notin_pattern, operator) and re.match(in_pattern, existing_operator)):
                                    existing_condition['condition_section'][0]['value'].extend(condition['condition_section'][0]['value'])  # Add the new value to the list
                                    existing_condition['condition_section'][0]['operator'] = "in"
                                    found = True

                                    break

                    if not found:
                        merged[section_name]["conditions"].append(condition)

       
        for section_name in merged:
            unique_conditions = {}
            filtered_conditions = []

            for condition in merged[section_name]["conditions"]:
                field_name = condition["field_name"]
                if field_name not in unique_conditions:
                    unique_conditions[field_name] = condition
                    filtered_conditions.append(condition)

            merged[section_name]["conditions"] = filtered_conditions

        container["sections"] = list(merged.values())

    return data



def merge_not_json(final_json, skipped_json):
    # Check if skipped_json is not empty
    if not skipped_json:
        return final_json

    # Create the new section with the modified combinator label and value
    new_section_container = {
        "IsShowAddSection": False,
        "combinator": {
            "label": "All of the conditions are not met:",
            "value": "not and"
        },
        "sections": []  # This will hold the skipped_json data
    }

    # Add skipped_json to the sections part of the new section container
    new_section_container["sections"].extend(skipped_json)

    # Add the new section container to final_json's section_containers
    final_json["data"]["json"]["section_containers"].append(new_section_container)

    return final_json
