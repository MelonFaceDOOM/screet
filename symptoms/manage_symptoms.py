import json
import csv

with open('symptoms/symptom_dict.txt', 'r') as f:
    symptom_dict = json.loads(f.read())

with open("symptoms/symptom_patterns.txt", 'r') as f:
    patterns = json.loads(f.read())

with open("symptoms/symptom_wordbank.txt", 'r') as f:
    wordbank = json.loads(f.read())


def create_symptom_queries_file(client):
    symptoms = create_full_symptom_list()
    queries = symptoms_to_queries(client, symptoms)
    symptoms_and_queries = list(zip(symptoms, queries))
    unique_symptoms_and_queries = []
    unique_queries = set()
    for symptom, query in symptoms_and_queries:
        if query not in unique_queries:
            unique_queries.add(query)
            unique_symptoms_and_queries.append([symptom, query])

    with open('symptoms/symptom_queries.txt', 'w', newline="") as f:
        writer = csv.writer(f)
        writer.writerows(unique_symptoms_and_queries)


def create_full_symptom_list():
    symptoms_from_combinations = create_combinations()
    symptoms_from_dict = [symptom_dict[key] for key in symptom_dict]
    symptoms_from_dict = [symptom for sublist in symptoms_from_dict for symptom in sublist]
    symptoms = symptoms_from_combinations + symptoms_from_dict
    return symptoms


def add_words_to_wordbank(key, new_words):
    with open('symptoms/symptom_wordbank.txt', 'r') as f:
        wordbank = json.loads(f.read())

    #create backup file
    with open('symptoms/symptom_wordbank.bkp', 'w') as f:
        json.dump(wordbank, f, indent=4)

    wordbank[key] += new_words
    wordbank[key] = list(set(wordbank[key]))

    with open('symptoms/symptom_wordbank.txt', 'w') as f:
        json.dump(wordbank, f, indent=4)


def symptoms_to_queries(client, symptoms):
    return [client.create_query(s) for s in symptoms]


def create_combinations():
    combinations = create_negative_effect_combinations() + create_function_combinations() +\
                   create_sensory_combinations()
    return combinations


def create_function_combinations():
    combinations = []
    for function in wordbank['functions']:
        for deficiency in wordbank['deficiencies']:
            for pattern in patterns['function_deficiency']:
                combinations.append(pattern.format(deficiency=deficiency, function=function))
        for inability in wordbank['inabilities']:
            for pattern in patterns['function_inability']:
                combinations.append(pattern.format(inability=inability, function=function))
    return combinations


def create_sensory_combinations():
    combinations = []
    for sense in wordbank['senses']:
        for pattern in patterns['sensory_loss']:
            combinations.append(pattern.format(sense=sense))
        for deficiency in wordbank['deficiencies']:
            for pattern in patterns['sensory_deficiency']:
                combinations.append(pattern.format(deficiency=deficiency, sense=sense))
        for inability in wordbank['inabilities']:
            for pattern in patterns['sensory_inability']:
                combinations.append(pattern.format(inability=inability, sense=sense))
    return combinations


def create_negative_effect_combinations():
    combinations = []
    for bodypart in wordbank['bodyparts']:
        for effect in wordbank['effects']:
            for pattern in patterns['negative_effect']:
                combinations.append(pattern.format(effect=effect, bodypart=bodypart))
    return combinations


def create_combinations_for_effect(effect):
    combinations = []
    for pattern in patterns['negative_effect']:
        for bodypart in wordbank['bodyparts']:
            combinations.append(pattern.format(bodypart=bodypart, effect=effect))
    return combinations


def create_combinations_for_bodypart(bodypart):
    combinations = []
    for effect in wordbank['effects']:
        for pattern in patterns['negative_effect']:
            combinations.append(pattern.format(effect=effect, bodypart=bodypart))
    return combinations


def create_combinations_for_deficiency(deficiency):
    combinations = []
    for function in wordbank['functions']:
        for pattern in patterns['function_deficiency']:
            combinations.append(pattern.format(deficiency=deficiency, function=function))
    for sense in wordbank['senses']:
        for pattern in patterns['sensory_deficiency']:
            combinations.append(pattern.format(deficiency=deficiency, sense=sense))
    return combinations


def create_combinations_for_inability(inability):
    combinations = []
    for function in wordbank['functions']:
        for pattern in patterns['function_inability']:
            combinations.append(pattern.format(inability=inability, function=function))
    for sense in wordbank['senses']:
        for pattern in patterns['sensory_inability']:
            combinations.append(pattern.format(sense=sense, inability=inability))
    return combinations


def create_combinations_for_function(function):
    combinations = []
    for deficiency in wordbank['deficiencies']:
        for pattern in patterns['function_deficiency']:
            combinations.append(pattern.format(deficiency=deficiency, function=function))
    for inability in wordbank['inabilities']:
        for pattern in patterns['function_inability']:
            combinations.append(pattern.format(inability=inability, function=function))
    return combinations


def create_combinations_for_sense(sense):
    combinations = []
    for pattern in patterns['sensory_loss']:
        combinations.append(pattern.format(sense=sense))
    return combinations


def create_combinations_for_negative_effect_pattern(pattern):
    combinations = []
    for bodypart in wordbank['bodyparts']:
        for effect in wordbank['effects']:
            combinations.append(pattern.format(effect=effect, bodypart=bodypart))
    return combinations


def create_combinations_for_function_inability_pattern(pattern):
    combinations = []
    for function in wordbank['functions']:
        for inability in wordbank['inabilities']:
            combinations.append(pattern.format(function=function, inability=inability))
    return combinations


def create_combinations_for_sensory_inability_pattern(pattern):
    combinations = []
    for sense in wordbank['senses']:
        for inability in wordbank['inabilities']:
            combinations.append(pattern.format(sense=sense, inability=inability))
    return combinations


def create_combinations_for_sensory_deficiency_pattern(pattern):
    combinations = []
    for sense in wordbank['senses']:
        for deficiency in wordbank['deficiencies']:
            combinations.append(pattern.format(sense=sense, deficiency=deficiency))
    return combinations


def create_combinations_for_sensory_loss_pattern(pattern):
    combinations = []
    for sense in wordbank['senses']:
        combinations.append(pattern.format(sense=sense))
    return combinations