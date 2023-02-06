import pandas as pd
import pymongo
from utils import dataframe_to_collection
from bson.json_util import dumps

cars_dataframe = pd.read_csv('carros.csv', sep=',', names=['Carro', 'Cor', 'Montadora'] )
print(cars_dataframe.head())
assembler_dataframe = pd.read_csv('Montadoras.csv', sep=',', names=['Montadora', 'Pais'] )
print(assembler_dataframe.head())

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['mongodb']

cars_collection = db['carros']
assembler_collection = db['montadoras']

cars_collection.insert_many(dataframe_to_collection(cars_dataframe))
assembler_collection.insert_many(dataframe_to_collection(assembler_dataframe))

for assembler in assembler_collection.find():
    print(assembler)

for car in cars_collection.find():
    print(car)


cars_dump = dumps(list(cars_collection.find()))
assembler_dump = dumps(list(assembler_collection.find()))

with open('cars_collection.json', 'w') as file:
    file.write(cars_dump)

with open('assembler_collection.json', 'w') as file:
    file.write(assembler_dump)
'''

assembler_pipeline = [
  {
      "$merge": {
         "into": "carros",
         "on":"Montadora",
         "whenMatched": "replace", 
         "whenNotMatched": "insert"
      }
   }, 
]


cars_collection.aggregate(assembler_pipeline)

'''