import re
import difflib
import pandas as pd
import numpy as np
import json
import os
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from .utils.constants import BASE_PATH
from dotenv import load_dotenv
# load_dotenv()
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
openai_embedder = OpenAIEmbeddings()
gv_mapper = {
    "VitalSignDim.DisplayName": "VitalSignDim",
    "AllergyDim.DisplayName": "AllergyDim",
    "ProblemsDim.DisplayName": "ProblemsDim",
    "ProceduresDim.DisplayName": "ProceduresDim",
    "ImmunizationDim.DisplayName": "ImmunizationDim",
    "ResultsDim.DisplayName": "ResultsDim",
    "PatientMedicationDim.DisplayName": "PatientMedicationDim",
    "EncountersDim.DisplayName":"EncountersDim"
}
def serialize_numpy(obj):
    if isinstance(obj, np.ndarray):
        return {
            "__ndarray__": obj.tolist(),
            "dtype": str(obj.dtype)
        }
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def deserialize_numpy(dct):
    if "__ndarray__" in dct:
        return np.array(dct["__ndarray__"], dtype=dct["dtype"])
    return dct

from sentence_transformers import SentenceTransformer

class Variations_Handler:
    # This class read gvs variations csv file and return the selected gv for the substring
    # This class would be used to handle variations for displaynames as well

    def __init__(self) -> None:
        self.updated_gvs_path = f"{BASE_PATH}parser/Disease_Vocabulary_202405141755.csv"
        self.gv_variations_path = f"{BASE_PATH}parser/GVS_Variations.csv"
        self.displayname_variations_path = None  # To Do
        self.initialize_gv_openai()
        self.initialize_sentence_transformer()
        pass
    def initialize_sentence_transformer(self):
        self.gv_df = pd.read_csv(self.updated_gvs_path)
        self.tables = self.gv_df["section_name"].unique()
        model_name = "pritamdeka/S-PubMedBert-MS-MARCO" #all-mpnet-base-v2
        self.model = SentenceTransformer(model_name, device="cpu")
        model_name = model_name.split('/')[-1]
        if os.path.exists(f"{BASE_PATH}{model_name}.json"):
            print("Loading embeddings from File")
            self.transformer_tables = json.load(open(f"{BASE_PATH}{model_name}.json","r"), object_hook=deserialize_numpy)
        else:
            self.transformer_tables = {}
            samples = 10
            for table in self.tables:
                print("Encoding GVs:", table)
                self.transformer_tables[table] = {
                    "terms": self.gv_df[self.gv_df["section_name"]==table].values[:],
                    "embeddings": self.model.encode(self.gv_df[self.gv_df["section_name"]==table]["sub_disease_name"].values[:])
                }
            json.dump(self.transformer_tables, open(f"{BASE_PATH}{model_name}.json", "w"), default=serialize_numpy)
            # print("Saving Embedding to File")

    def initialize_gv_openai(self):
        self.gv_df = pd.read_csv(self.updated_gvs_path)
        self.tables = self.gv_df["section_name"].unique()
        self.openai_tables = {}
        for table in self.tables:
            self.openai_tables[table] = FAISS.load_local(f"{BASE_PATH}rag/{table}", openai_embedder)
            
        # print(self.tables)
        # model_name = "all-mpnet-base-v2"
        # self.model = SentenceTransformer("all-mpnet-base-v2", device="cpu")
        # if os.path.exists(f"./{model_name}.json"):
        #     # print("Loading embeddings from File")
        #     self.embeddings_tables = json.load(open(f"./{model_name}.json","r"), object_hook=deserialize_numpy)
        # else:
        #     self.embeddings_tables = {}
        #     samples = 10
        #     for table in self.tables:
        #         # print("Encoding:", table)
        #         self.embeddings_tables[table] = {
        #             "terms": self.gv_df[self.gv_df["section_name"]==table]["sub_disease_name"].values[:],
        #             "embeddings": self.model.encode(self.gv_df[self.gv_df["section_name"]==table]["sub_disease_name"].values[:])
        #         }
        #     json.dump(self.embeddings_tables, open(f"./{model_name}.json", "w"), default=serialize_numpy)
            # print("Saving Embedding to File")
        

    def find_similar_words(self, substring, word_list, similarity_threshold=0.90):
        """
        Finds words in a given word list that are similar to a given substring.

        Args:
            substring (str): The substring to compare against.
            word_list (List[str]): The list of words to search.
            similarity_threshold (float, optional): The minimum similarity threshold. Defaults to 0.90.

        Returns:
            Dict[str, float]: A dictionary containing the similar words as keys and their similarity scores as values.
        """

    # Normalize substring to lowercase for case-insensitive comparison
        normalized_substring = substring.lower()

        # Prepare to store similar words
        similar_words = {}

        # Iterate over each word in the list
        for word in word_list:
            # Normalize word to lowercase for case-insensitive comparison
            normalized_word = str(word).lower()

            # Calculate similarity between normalized word and substring
            similarity = difflib.SequenceMatcher(
                None, normalized_substring, normalized_word).ratio()

            # Check if similarity meets the threshold
            if similarity >= similarity_threshold:
                similar_words[word] = similarity

        similar_words = dict(sorted(similar_words.items(),
                             key=lambda item: item[1], reverse=True))
        return similar_words

    def get_GV_from_openai(self, substring, columnName):
        try:
            IS_FOUND = True
            table = columnName.split(".")[0]
            top_10_results = [(doc.page_content, score) for doc,score in self.openai_tables[table].similarity_search_with_score(substring, k=10) if score<0.3]
            # print(top_10_results)
            if len(top_10_results)>0:
                return [result for result, _ in top_10_results], IS_FOUND
            else:
                return substring, False
        except Exception as e:
            return substring, False
            pass
    
    def get_GV_from_sentenceTransformer(self, substring, columnName):
        # print("FINDING SIMILARITY FOR :", substring, "Column:", columnName)
        try:
            IS_FOUND = True
            table = columnName.split(".")[0]
            embeddings = self.model.encode([substring])
            similarities = self.model.similarity(embeddings, self.transformer_tables[table]["embeddings"])[0].cpu().numpy()
            sorted_ = np.argsort(similarities)[::-1]
            # print("SORTED:",sorted_)
            # if "PatientMedicationDim" in columnName:
            #     similarity_threshold = 0.98
            # else:
            similarity_threshold = 0.9

            top_k_similarities = similarities[similarities[sorted_]>similarity_threshold]
            top_k_terms = self.transformer_tables[table]["terms"][sorted_][similarities[sorted_]>similarity_threshold]
            print("GV HANDLER")
            print(similarities[sorted_][:5])
            print(self.transformer_tables[table]["terms"][sorted_][:5])
            if len(top_k_similarities)>0:
                return [top_k_terms[:,0][0]], IS_FOUND
            else:
                return substring, False
        except Exception as e:
            # print(e)
            raise e

        return substring, False
        
        pass
    def get_GV_for_word2_0(self, substring, columnName):
        IS_FOUND = False
        try:
            GV,IS_FOUND =  self.get_GV_for_word(substring, columnName)
        except Exception as e:
            print(e)
        if not IS_FOUND:
            GV,IS_FOUND =  self.get_GV_from_sentenceTransformer(substring, columnName)
        # if not IS_FOUND:
        #     GV,IS_FOUND =  self.get_GV_from_openai(substring, columnName)
        # print(GV, IS_FOUND)
        return GV, IS_FOUND
        # print("FINDING SIMILARITY FOR :", substring, "Column:", columnName)
        # try:
        #     IS_FOUND = True
        #     table = columnName.split(".")[0]
        #     embeddings = self.model.encode([substring])
        #     similarities = self.model.similarity(embeddings, self.embeddings_tables[table]["embeddings"])[0].cpu().numpy()
        #     sorted_ = np.argsort(similarities)[::-1]
        #     # print("SORTED:",sorted_)
        #     top_k_similarities = similarities[similarities[sorted_]>0.7]
        #     top_k_terms = self.embeddings_tables[table]["terms"][sorted_][similarities[sorted_]>0.7]
        #     # print("GV HANDLER")
        #     print(similarities[sorted_][:5])
        #     print(self.embeddings_tables[table]["terms"][sorted_][:5])
        #     if len(top_k_similarities)>0:
        #         return top_k_terms, IS_FOUND
        #     else:
        #         return substring, False
        # except Exception as e:
        #     # print(e)
        #     raise e

        # return substring, False


    def get_GV_for_word(self, substring, columnName):
        """
        Given a substring, this function finds the most similar word in the GVS_Variations dataframe and returns it as the selected_GV. 
        If no similar word is found, the original substring is returned. The function also returns a boolean value indicating whether a similar word was found or not.

        Parameters:
            substring (str): The substring to find a similar word for.

        Returns:
            Tuple[str, bool]: A tuple containing the selected_GV (str) and a boolean value indicating whether a similar word was found or not.
        """
        print("FINDING GV FOR :",substring, "Column:", columnName)

        IS_FOUND = True
        GVS = pd.read_csv(self.updated_gvs_path)

        if columnName in gv_mapper:
            name = gv_mapper.get(columnName,"None")
            GVS_Variations = pd.read_csv(self.gv_variations_path)
            similar_words = self.find_similar_words(
                substring, GVS_Variations[GVS_Variations["Entity"] == name]["Adhoc_Param_Syn_Lower"].to_list())
            if len(similar_words) > 0:

                # for word in similar_words.keys():
                GVS_ALL = GVS_Variations[GVS_Variations["Entity"] == name]["Ad-hoc_Param"][GVS_Variations[GVS_Variations["Entity"] == name]["Adhoc_Param_Syn_Lower"].isin(list(similar_words.keys()))].tolist()

                if len(GVS_ALL)==1:
                    selected_GV = GVS_ALL[0]
                elif len(GVS_ALL)>1:
                    similar_gvs = self.find_similar_words(
                    substring, GVS_ALL, 0.0)
                    similar_gvs = list(similar_gvs.keys())
                    selected_GV = similar_gvs[0]
                else:
                    IS_FOUND = False
                    selected_GV = substring
            else:
                IS_FOUND = False
                selected_GV = substring
        else:
            IS_FOUND = False
            selected_GV = substring

        return [selected_GV], IS_FOUND

    def get_DisplayName_for_word(self, substring):
        """
        Get the display name for a given word.

        Args:
            substring (str): The word for which to get the display name.

        Returns:
            str: The same substring is returned.

        This function can be used to handle variations for displaynames
        """
        return substring  # currently just sending the same word for displayname



# variation_handler = Variations_Handler()
