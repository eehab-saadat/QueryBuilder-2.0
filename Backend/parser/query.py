import json
from .query_util import * 
from .filter import Filter
from .utils.constants import DEBUG
import numpy as np


class Query:
    def __init__(self, raw_text, EP_ID="471782812.json"):
        """Initialize the Query object with raw SQL text and parse it.

        Args:
            raw_text (str): The raw SQL query text.
        """
        self.raw_text = raw_text
        self.EP_ID = EP_ID

        self.returnNone = False
        try:
            self.parse()
        except Exception as e:
            if DEBUG:
                raise e
            self.returnNone = True

    def process(self):
        """Placeholder method for additional processing.

        This method is currently not implemented.

        Returns:
            None
        """
        pass

    def parse(self):
        """Parse the raw SQL text to extract the table name, selected columns, and WHERE conditions.

        This method also filters out conditions that involve PPID and ResourceID.

        Args:
            None

        Returns:
            None
        """
        self.tableName = extract_table_name(self.raw_text)
        self.selection = extract_column_names(self.raw_text)
        self.filters = extract_where_conditions(self.raw_text)
        self.filters = [
            filter for filter in self.filters if not check_ppid_in(filter)]

        self.filter_objects = []
        for filter in self.filters:
            self.filter_objects.append(
                Filter(filter, get_only_table_name(self.tableName), EP_ID=self.EP_ID))
    def json(self):
        """Generates a JSON representation of the query's filter conditions.

        This method processes the filter objects to create a structured JSON representation of
        the conditions applied in the query. It handles merging of conditions with similar fields
        and operators, and groups them by unique table labels.

        Args:
            None

        Returns:
            list[dict] or None: A list of dictionaries where each dictionary represents a group
            of conditions tied to a specific table section. If there are no conditions, returns None.
        """
        try:
            self.conditions = []
            for filter_object in self.filter_objects:
                temp_json = filter_object.json()
                if temp_json is not None:
                    self.conditions.extend(temp_json)

            self.section_name = get_only_table_name(self.tableName)

            if self.conditions:
                
                tableLabels = []
                for condition in self.conditions:
                    tableLabels.append(
                        get_table_label_from_col(condition['field_name'],self.EP_ID))
                tableLabels = np.array(tableLabels)
                valid_conditions = [condition for condition, label in zip(self.conditions,tableLabels ) if label is not None]
                tableLabels = np.array([label for label in tableLabels if label is not None])
                self.conditions = np.array(valid_conditions)

                unique_names = np.unique(tableLabels)
                self.conditions = np.array(self.conditions)
                conditions_separators = {}
                new_json = []

                for unique_name in unique_names:
                    relevant_conditions = self.conditions[tableLabels == unique_name].tolist(
                    )

                    merged_conditions = []
                    processed = set()
                    for condition in relevant_conditions:
                        field_name = condition['field_name']
                        operator = condition['condition_section'][0]['operator'].strip()
                        positive_operators = {"in", "="}
                        negative_operators = {"notin", "!="}
                        if (field_name, operator) not in processed:

                            same_field_conditions = [
                                cond for cond in relevant_conditions
                                if cond['field_name'] == field_name
                                and (
                                        (operator in positive_operators)
                                        or (operator in negative_operators)
                                    )

                            ]
                            if same_field_conditions:

                                if len(same_field_conditions) > 1 and operator in positive_operators and isinstance(same_field_conditions[0]['condition_section'][0]['value'], list):
                                    merged_value = list(set([item for cond in same_field_conditions for item in cond['condition_section'][0]['value']]))
                                    new_condition = same_field_conditions[0].copy()
                                    new_condition['condition_section'][0]['value'] = merged_value  
                                    new_condition['condition_section'][0]['operator'] = 'in'  


                                    merged_conditions.append(new_condition)
                                    processed.add((field_name, operator))
                                elif len(same_field_conditions) > 1 and operator in negative_operators and isinstance(same_field_conditions[0]['condition_section'][0]['value'], list):
                                    merged_value =  list(set([item for cond in same_field_conditions for item in cond['condition_section'][0]['value']]))
                                    new_condition = same_field_conditions[0].copy()
                                    new_condition['condition_section'][0]['value'] = merged_value
                                    new_condition['condition_section'][0]['operator'] = 'notIn'


                                    merged_conditions.append(new_condition)
                                    processed.add((field_name, operator))
                                else:
                                    for cond in same_field_conditions:
                                        merged_conditions.append(cond)
                                    processed.add((field_name, operator))
                            else:
                                merged_conditions.append(condition)

                    conditions_separators[unique_name] = merged_conditions
                    new_json.append(
                        {
                            "conditions": merged_conditions,
                            "section_name": self.section_name,
                            "section_text": unique_name
                        }
                    )
                return new_json
            else:
                tableLabels = get_table_label_from_tab(self.section_name,self.EP_ID)
                if len(tableLabels) == 1:
                    return [{
                        "conditions": [],
                        "section_name": self.section_name,
                        "section_text": tableLabels[0]
                    }]
                else:
                    return None
        except Exception as e:
            if DEBUG:
                raise e
            return None
