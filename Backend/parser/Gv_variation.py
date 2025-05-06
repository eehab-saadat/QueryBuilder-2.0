from langchain import OpenAI
import csv

client = OpenAI(base_url="http://10.20.30.153:1235/v1", api_key="none")

def generate_prompt(standard_term):
    prompt =f"""
    I am an agent designed to generate relevant abbreviations, full forms specifically for the term '{standard_term}' in the domain of medical and patient healthcare.

    Include abbreviations if they are commonly use , full forms all in one continuous list. Focus only on relevant terms directly associated with '{standard_term}'.
    Return the final answer in the form of a python list with [] and comma separated terms as provided in examples below.

    Here are a few examples:

    1. Term: 'Analgesic Agent Allergy'
    Response:  [ "Pain Reliever Allergy", "Analgesic Drug Allergy", "Painkiller Allergy", "NSAID Allergy"]

    2. Term: 'Ambulatory or ED Encounter'
    Response: ["ED Visit", "Emergency Room Visit", "Ambulatory Visit", "ER Encounter", "Emergency Depart"] 

    Question:"{standard_term}"
    Answer:"""
    return prompt


def generate_and_save_variations(input_filename="parser/Disease_Vocabulary_202405141755.csv", output_filename="output.csv"):
    with open(input_filename, mode='r') as infile, open(output_filename, mode='w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Adhoc_Param_Syn_Lower", "Ad-hoc_Param", "Entity"])

        for row in reader:
            # print(row)
            if row["level"]=='0':
                standard_term = row['sub_disease_name']
                entity = row['section_name']
                if entity is not None:
                    print(row)
                    print("\n\n\n")
                    prompt = generate_prompt(standard_term)
                    try:
                        response = client(
                            prompt=prompt,
                            model="MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF",
                            max_tokens=200,
                            temperature=0
                        )

                        response_text = response.strip()

                        print(f"Cleaned response text for term {standard_term}: {response_text}")
                        variations = eval(response_text)
                        if standard_term not in variations:
                            variations.append(standard_term)
                        # variations = [var.strip() for var in response_text.split(',')]

                        for variation in variations:
                            writer.writerow([variation, standard_term, entity])
                        
                    except Exception as e:
                        print(f"An error occurred while processing term '{standard_term}': {e}")

generate_and_save_variations(input_filename="/mnt/d/soliton/querybuilder/solitonquerybuilder/parser/Disease_Vocabulary_202405141755.csv", output_filename="variation_output.csv")