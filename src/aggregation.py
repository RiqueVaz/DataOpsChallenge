import json
from pymongo import MongoClient


# Conexão com o banco
client = MongoClient("mongodb://localhost:27017/")
db = client["dataops_test"]


# Pipeline assim como está no aggregation.js
# (nele está melhor documentado o que cada etapa faz, esse arquivo usei para facilitar a execução do pipeline)
pipeline = [
    { "$lookup": {
        "from": "Montadoras",
        "localField": "Montadora",
        "foreignField": "Montadora",
        "as": "PaisCarros"
    }},
    { "$unwind": "$PaisCarros" },
    { "$project": {
        "Carro":     1,
        "Cor":       1,
        "Montadora": 1,
        "País": "$PaisCarros.País"
    }},
    { "$group": {
        "_id":   "$País",
        "Carros": {
            "$addToSet": {
                "Carro":     "$Carro",
                "Cor":       "$Cor",
                "Montadora": "$Montadora"
            }
        }
    }},
    { "$sort": { "_id": 1 } }
]

# Printa o resultado da pipeline
for doc in db.Carros.aggregate(pipeline):
    print(json.dumps(doc, ensure_ascii=False, indent=2))
