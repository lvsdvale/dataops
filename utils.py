def dataframe_to_collection(data):
    collection = list()
    for row in range(data.shape[0]):
        collection_dict = dict()
        for column in data.columns:
            collection_dict[column] = data[column][row]
        collection.append(collection_dict)
    return collection
