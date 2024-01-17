import json

"""
find keys no longer present
find synonyms no longer present
remove any synonyms that are the same as the key
"""

with open('../symptom_dict_old.txt', 'r', encoding='utf-8') as f:
    old_symptoms = json.loads(f.read())

with open('aefis_new.txt', 'r', encoding='utf-8') as f:
    new_symptoms = json.loads(f.read())

removed_keys = [k for k in old_symptoms if k not in new_symptoms]
new_keys = [k for k in new_symptoms if k not in old_symptoms]

# print(removed_keys)
# print()
# print(new_keys)

all_old_symptoms = []
for k in old_symptoms:
    synonyms = old_symptoms[k]
    all_old_symptoms += synonyms

all_new_symptoms = []
for k in new_symptoms:
    synonyms = new_symptoms[k]
    all_new_symptoms += synonyms

removed_symptoms = [s for s in all_old_symptoms if s not in all_new_symptoms]
newly_added_symptoms = [s for s in all_new_symptoms if s not in all_old_symptoms]

# print(removed_symptoms)
# print()
# print(newly_added_symptoms)

for k in new_symptoms:
    synonyms = new_symptoms[k]
    if k in synonyms:
        print(k)
