import json
import csv


with open('aefis_original.txt', 'r', encoding='utf-8') as f:
    aefis =  json.load(f)

with open('manually_labeled_data_ratios.csv', 'r', encoding='utf-8') as f:
    ratios = csv.DictReader(f)
    ratios = list(ratios)


def main():
    aefis_clean = make_dict_lower(aefis)
    aefis_clean = replace_u2019(aefis_clean)
    
    with open('symptoms_to_remove.txt', 'r') as f:
        symptoms_to_remove = f.read().split('\n')
    with open('null_symptoms.txt', 'r') as f:
        symptoms_to_remove += f.read().split('\n')
        
#    print("crest syndrome" in symptoms_to_remove)
    aefis_clean = remove_symptoms_from_aefis(aefis_clean, symptoms_to_remove)

    # remove duplicates within each synonym list
    aefis_clean = remove_synonyms_within_aefi(aefis_clean)

    aefis_clean = remove_conflicting_synoyms_from_aefi(aefis_clean, "autoimmune disorders")
    aefis_clean = remove_conflicting_synoyms_from_aefi(aefis_clean, "glomerulonephritis")

    with open('aefis_clean.txt', 'w', encoding='utf-8') as f:
        json.dump(aefis_clean, f, indent=4)


def replace_u2019(aefis):
    aefis_new = {}
    for key in aefis:
        new_key = key.replace("’", "'")
        new_list = []
        for synonym in aefis[key]:
            new_list.append(synonym.replace("’", "'"))
        aefis_new[new_key] = new_list
    return aefis_new
    

def remove_symptoms_from_aefis(aefis, symptoms_to_remove):
    for key in aefis:
        symptoms = aefis[key]
        for sr in symptoms_to_remove:
            if sr in symptoms:
                symptoms.remove(sr)
        aefis[key] = symptoms
    return aefis


def create_symptoms_to_remove_file():
    symptoms_below_threshold = find_symptoms_below_mention_threshold(ratios)
    plural_symptoms = find_plural_symptoms(aefis)
    
    symptoms_to_remove = symptoms_below_threshold + plural_symptoms
    out = "\n".join(symptoms_to_remove)
    with open('symptoms_to_remove.txt', 'w') as f:
        f.write(out)
        
        
def find_symptoms_below_mention_threshold(ratio_rows):
    threshold = 0.2
    symptoms_below_threshold = []
    for row in ratio_rows:
        if float(row['mentioned_ratio']) <= threshold:
            symptoms_below_threshold.append(row['symptom'])
    return symptoms_below_threshold
    
    
def remove_synonyms_within_aefi(aefis_dict):
    for key in aefis_dict:
        synonyms_for_symptom = aefis_dict[key]
        synonyms_for_symptom = [s.lower().strip() for s in synonyms_for_symptom]
        aefis_dict[key] = list(set(synonyms_for_symptom))
    return aefis_dict
    
    
def find_duplicate_synonyms(aefis_dict):
    synonyms_so_far = []
    duplicate_symptoms = []
    for key in aefis_dict:
        synonyms_for_symptom = aefis_dict[key]
        for synonym in synonyms_for_symptom:
            for existing_symptom, existing_synonym in synonyms_so_far:
                if existing_synonym == synonym:
                    duplicate_symptoms.append([key, synonym])
                    if not [existing_symptom, existing_synonym] in duplicate_symptoms:
                        duplicate_symptoms.append([existing_symptom, existing_synonym])
            synonyms_so_far.append([key, synonym])
    duplicate_symptoms.sort(key=lambda x: x[1])
    return duplicate_symptoms

def find_plural_symptoms(symptom_dict):
    plural_symptoms = []

    all_synonyms = []
    for key in symptom_dict:
        all_synonyms+=symptom_dict[key]
        
    for synonym in all_synonyms:
        if synonym[-1] == "s":
            if synonym[:-1] in all_synonyms:
                plural_symptoms.append(synonym)
    return plural_symptoms
    
    
def remove_conflicting_synoyms_from_aefi(aefis_dict, aefi):
    aefi_synonyms = aefis_dict[aefi]
    all_other_synonyms = []
    for key in aefis_dict:
        if key != aefi:
            all_other_synonyms += aefis_dict[key]
    aefi_synonyms_without_duplicates = []
    for synonym in aefi_synonyms:
        if synonym not in all_other_synonyms:
            aefi_synonyms_without_duplicates.append(synonym)
    aefis_dict[aefi] = aefi_synonyms_without_duplicates
    return aefis_dict


def dict_lists_len(inp_dict):
    total_len = 0
    for key in inp_dict:
        total_len += len(inp_dict[key])
    return total_len
    
    
def make_dict_lower(upper_dict):
    lower_dict = {}
    for key in upper_dict:
        new_key = key.strip().lower()
        new_list = []
        for i in upper_dict[key]:
            new_list.append(i.strip().lower())
        lower_dict[new_key] = new_list
    return lower_dict
    
    
if __name__ == "__main__":
    main()
