import json

def get_inter_entity_co_occurrences(sorted_files, entity1, entity2):
    """
    Find co-occurrences between two different types of entities across a list of JSON files.

    Parameters:
        sorted_files (list of str): List of file paths to the JSON files.
        entity1 (str): The first entity type (e.g., "disease") to find co-occurrences for.
        entity2 (str): The second entity type (e.g., "phenomenon") to find co-occurrences for.

    Returns:
        dict: A dictionary with co-occurrences and their respective PMIDs and sentences.
    """
    co_occurrences = {}

    for file_path in sorted_files:
        with open(file_path, 'r') as file:
            data = json.load(file)
            for pmid, content in data.items():
                for sentence in content['sentences']:
                    entities = sentence['entities']
                    if entity1 in entities and entity2 in entities:
                        e1_entities = entities[entity1]
                        e2_entities = entities[entity2]
                        for e1 in e1_entities:
                            for e2 in e2_entities:
                                key = (e1, e2)
                                if key not in co_occurrences:
                                    co_occurrences[key] = {}
                                if pmid not in co_occurrences[key]:
                                    co_occurrences[key][pmid] = []
                                if sentence['text'] not in co_occurrences[key][pmid]:
                                    co_occurrences[key][pmid].append(sentence['text'])

    # Convert the nested dictionary to the desired structure and sort the keys
    sorted_keys = sorted(co_occurrences.keys(), key=lambda x: (x[0], x[1]))
    result = {str(key): [{'pmid': pmid, 'sentences': sentences} for pmid, sentences in co_occurrences[key].items()]
              for key in sorted_keys}

    return result
