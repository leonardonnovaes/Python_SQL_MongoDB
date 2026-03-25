from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import os
from dotenv import load_dotenv
load_dotenv()

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    return db[col_name]

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col,data):
    result = col.insert_many(data)
    n_docs_inseridos = len(result.inserted_ids)
    return n_docs_inseridos


if __name__ == "__main__":
    
    client = connect_mongo(os.getenv("uri"))
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    data = extract_api_data(os.getenv("api"))
    print(f"\nQuantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")

    client.close()

