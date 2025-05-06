import pandas as pd
import difflib
import re
from .utils.constants import BASE_PATH

column_name_mapping = {
    "AllergyDim.SeverityCode": "2.16.840.1.113883.3.88.12.3221.6.8",
    "AllergyDim.TypeCode": "2.16.840.1.113883.3.88.12.3221.6.2",
    "PatientDim.Ethnic": "2.16.840.1.114222.4.11.837",
    "PatientDim.Gender":"2.16.840.1.113883.1.11.1",
    "PatientDim.Marital Status":"2.16.840.1.113883.1.11.12212",
    "PatientDim.Race":"2.16.840.1.113883.1.11.14914",
    "PatientDim.Religion" : "2.16.840.1.113883.1.11.19185",
    "PatientMedicationDim.RouteCode": "2.16.840.1.113883.3.88.12.3221.8.7",
    "ProblemsDim.SeverityTypeCode":"2.16.840.1.113883.3.88.12.3221.6.8",
    "ProblemsDim.StatusTypeCode":"2.16.840.1.113883.11.20.9.19",
    "VitalSignDim.TypeCode":"2.16.840.1.113883.3.88.12.3221.6.2",
    
}

class ConditionMatcher:
    def __init__(self) -> None:
        self.csv_path = f"{BASE_PATH}parser/terminology_set (1).csv"
        # self.csv_path = csv_path  # Initialize CSV path

    def find_similar_words(self, substring, word_list, similarity_threshold=0.50):
        normalized_substring = substring.lower()
        similar_words = {}

        for word in word_list:
            normalized_word = word.lower()
            similarity = difflib.SequenceMatcher(None, normalized_substring, normalized_word).ratio()

            #if similarity >= similarity_threshold:
            similar_words[word] = similarity

        similar_words = dict(sorted(similar_words.items(), key=lambda item: item[1], reverse=True))
        return similar_words

    def get_code_for_condition(self, column_name, condition,operator):
        if re.search(r'\bNOT\b|!=', operator, re.IGNORECASE):
            operator="!="
        else:
            operator='='
        normalized_column_name = column_name.lower()

        if column_name in column_name_mapping:
            column_name = column_name_mapping[column_name]
        
        else:
            return condition, "", False  

        df = pd.read_csv(self.csv_path)
        filtered_df = df[df["value_set_id"] == column_name]

        if filtered_df.empty:
            return condition, "", False  

        similar_words = self.find_similar_words(condition, filtered_df["display_name"].to_list())
        similar_words = list(similar_words.keys())

        if len(similar_words) > 0:
            matched_display_name = similar_words[0]
            matched_code = filtered_df[filtered_df["display_name"] == matched_display_name]["code"].values[0]
            value = matched_code
        # operator_match=re.match(r'\bNOT|NULL\b',operator,re.IGNORECASE)
        # if operator_match:
            
        else:
            matched_display_name = condition
            value = ""
   
        return value,operator
