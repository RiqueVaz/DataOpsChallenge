import pandas as pd
from pymongo import MongoClient


# Dados dos carros 
dados_carros = [
    {"Carro": "Onix", "Cor": "Prata", "Montadora": "Chevrolet"},
    {"Carro": "Polo", "Cor": "Branco", "Montadora": "Volkswagen"},
    {"Carro": "Sandero", "Cor": "Prata", "Montadora": "Renault"},
    {"Carro": "Fiesta", "Cor": "Vermelho", "Montadora": "Ford"},
    {"Carro": "City", "Cor": "Preto", "Montadora": "Honda"}
]

# Dados das montadoras
dados_montadoras = [
    {"Montadora": "Chevrolet",  "País": "EUA"},
    {"Montadora": "Volkswagen", "País": "Alemanha"},
    {"Montadora": "Renault",   "País": "França"},
    {"Montadora": "Ford",      "País": "EUA"},
    {"Montadora": "Honda",     "País": "Japão"}
]

# Criação dos dataframes
df_carros = pd.DataFrame(dados_carros)
df_montadoras = pd.DataFrame(dados_montadoras)

# Conexão com o banco
client = MongoClient("mongodb://localhost:27017/")
db = client["dataops_test"]


# Inserção dos dados
db.Carros.insert_many(df_carros.to_dict(orient="records"))
db.Montadoras.insert_many(df_montadoras.to_dict(orient="records"))

print("Dados inseridos com sucesso!")
