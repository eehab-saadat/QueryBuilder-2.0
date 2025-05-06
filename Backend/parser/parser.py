from .parser_util import *
from .utils.constants import DEBUG
from .utils.format import convert_to_singleline_sql
import re
import json
from .query import Query
from dotenv import load_dotenv
# load_dotenv()

class Parser:
    def __init__(self, raw_text="", returnNone=False, responseMessage="Text to JSON parsed",EP_ID="471782812.json"):
        """
        Initialize the Parser with raw SQL text.

        Args:
            raw_text (str): Raw SQL query text.
        """
        self.raw_text = raw_text
        self.processed_text = None
        self.hierarchy = None
        self.returnNone = returnNone
        self.response_messsage = responseMessage
        self.EP_ID = EP_ID

        if self.raw_text.startswith("Issue:"):
            self.response_messsage = self.raw_text
            self.returnNone = True
        if self.returnNone==False:
            try:
                self.process()
                self.parse()
            except Exception as e:
                if DEBUG:
                    raise e
                self.returnNone = True

    def process(self):
        """
        Process the raw SQL text by converting it to a single-line format.
        """
        self.processed_text = convert_to_singleline_sql(self.raw_text)
        self.processed_text = self.processed_text.replace(";", "")
        self.processed_text = re.sub(
            r"`Marital Status`", "Marital Status", self.processed_text)
        self.processed_text = re.sub(
            r"`MaritalStatus`", "MaritalStatus", self.processed_text)
        self.processed_text = re.sub(
            r"Marital Status", "`Marital Status`", self.processed_text)
        self.processed_text = re.sub(
            r"MaritalStatus", "`Marital Status`", self.processed_text)

    def parse(self):
        """
        Parse the processed SQL text into subqueries, filter non-select queries,
        and extract the hierarchy of queries.
        """
        all_queries = [self.processed_text] + \
            break_into_subqueries(self.processed_text)
        all_queries = remove_non_select_queries(all_queries)
        self.hierarchy = extract_hierarchy(all_queries)
        self.queries = {}
        for key, value in self.hierarchy.items():
            self.queries[key] = Query(value,EP_ID=self.EP_ID)

    def __str__(self):
        """
        Return a string representation of the Parser object.

        Returns:
            str: String representation of the SQL query and its hierarchy.
        """

        return f"\nSQL:{self.raw_text}\nHierarchy:\n{self.hierarchy}\n"

    def json(self):
        """
        Return the hierarchy as a JSON object and print JSON for parent and subquery.
        """
        final_json = {
            "data": {
                "json": {
                    "isDetailView": True,
                    "section_containers": [
                        {
                            "IsShowAddSection": False,
                            "combinator": {
                                "label": "All of the conditions are met:",
                                "value": "and"
                            },
                            "sections": []
                        }
                    ],
                    "selection": {
                        "demographics": [],
                        "is_default": False,
                        "others": []
                    }
                }
            },
            "message": "OK",
            "moreInfo": self.response_messsage,
            "param status": self.raw_text,
            "status": "SUCCESS",
            "success": False if self.returnNone else True
        }

        if self.returnNone:
            # final_json["moreInfo"] = "Try again with clear Instructions!"
            # final_json["success"] = False
            return final_json

        count = None
        
        subquery_pattern = re.compile(
            r"NOT\s+IN\s*\(\s*(SubQuery_\d+)\s*\)", re.IGNORECASE)
        not_in_subqueries = []
        skipped_jsons=[]
        # Collect subqueries and their parents
        pattern = re.compile(
            r'ResourceId\s*(?:IN|NOT IN)\s*\(\s*(SubQuery_\d+)\s*\)', re.IGNORECASE)
        pattern_count_ppid = re.compile(
            r'count\(\s*(DISTINCT)?\s*PPID\s*\)', re.IGNORECASE)
        pattern_count_all = re.compile(r'count\(.*\)', re.IGNORECASE)
        subquery_keys = []
        parent_jsons = {}
        sorted_queries = {key: self.queries[key]
                          for key in sorted(self.queries.keys())}

        for key, query in sorted_queries.items():
            query_json = query.json()
            query_text = query.raw_text

            matches = subquery_pattern.findall(query_text)
            if matches:
                not_in_subqueries.extend(matches)
            if key in not_in_subqueries and query_json is not None:
                # process_final_json(query_json)
                skipped_jsons.extend(query_json)
            if query_json is not None and key not in not_in_subqueries :
                final_json["data"]["json"]["section_containers"][0]["sections"].extend(
                    query_json)

            # Sania Resource ID IN
            matches = pattern.findall(query.raw_text)
            if matches:
                for subquery_key in matches:
                    subquery_keys.append(subquery_key[1])
                    parent_jsons[subquery_key] = query_json

            # Ali Count
            if hasattr(query, 'selection') and isinstance(query.selection, list):
                if any(pattern_count_ppid.match(sel) for sel in query.selection):
                    count = "DistinctPPID"
                elif any(pattern_count_all.match(sel) for sel in query.selection):
                    count = "DistinctGVS"

        # Sania Outside Loop Logic for Resource ID
        # Print JSON for parent queries
        for subquery_key, parent_json in parent_jsons.items():

            # Print JSON for the subquery directly from the queries
            subquery_json = self.queries.get(subquery_key, {}).json()

            # Merge parent and subquery JSONs
            if parent_json and subquery_json:
                merge_queries(parent_json, subquery_json)

        final_json["data"]["json"]["section_containers"][0]["sections"] = [section for section in final_json["data"]
                                                                           ["json"]["section_containers"][0]["sections"] if section["section_name"] != "PatientProviderDim"]

        # Ali Outside Loop Logic for Count
        for section in final_json["data"]["json"]["section_containers"][0]["sections"]:
            for condition in section["conditions"]:
                if condition["is_dictionary"] and count is not None:
                    final_json["data"]["json"]["selection"]["others"].append(
                        {
                            "field_name": condition["field_name"],
                            "fxFunction": "",
                            "section_name": section["section_name"],
                            "section_text": section["section_text"]
                        }
                    )

                    if count == "DistinctGVS":
                        final_json["data"]["json"]["selection"]["others"].append({
                            "field_name": condition["field_name"],
                            "fxFunction": "fxCount",
                            "section_name": section["section_name"],
                            "section_text": section["section_text"]
                        })

        if count == "DistinctPPID":
            final_json["data"]["json"]["selection"]["demographics"].append({
                "field_name": "PatientDim.PPID",
                "fxFunction": "fxDistinctCount",
                "section_name": "PatientDim",
                "section_text": "Demographics"
            })
        

        if count is None:
            update_final_json(final_json)
        merged_sections(final_json)
        merge_not_json(final_json, skipped_jsons)
        print(json.dumps(final_json, sort_keys=True))
        return json.loads(json.dumps(final_json, sort_keys=True))


# def get_messages():
#     return {
#         "SUCCESS":[],
#         "INFO":[],
#         "ERROR":[],
#         "WARNING":[]
#     }

# def format_message(sql2json):

#     return sql2json
#     pass
