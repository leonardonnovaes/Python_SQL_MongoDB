from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

def visualize_collection(collection):
    for doc in collection.find():
        print(doc)

def rename_column(collection, coluna_name, new_name):
    collection.update_many({}, {"$rename":{f"{coluna_name}":f"{new_name}"}})
    print(f"Coluna '{coluna_name}' renomeada para '{new_name}' com sucesso.")

def select_category(collection, category):
    query = { "Categoria do Produto": f"{category}"}
    
    lista_categoria = []
    for doc in collection.find(query):
        lista_categoria.append(doc)

    return lista_categoria

def make_regex(collection , coluna , regex):
    query = {coluna: {"$regex": f"{regex}"}}
    lista_regex = []
    for doc in collection.find(query):
        lista_regex.append(doc)
    return lista_regex

def create_dataframe(lista):
    df = pd.DataFrame(lista)
    return df

def format_date(df, coluna_data):
    df[f"{coluna_data}"] = pd.to_datetime(df[f"{coluna_data}"], format="%d/%m/%Y")

def save_csv(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"DataFrame salvo como '{file_path}' com sucesso.")


if __name__ == "__main__":

    # estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo(os.getenv("uri"))
    db = create_connect_db(client, "db_produtos")
    col = create_connect_collection(db, "produtos")

    # renomeando as colunas de latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros, "Data da Compra")
    save_csv(df_livros, "../data/tb_livros.csv")

    # salvando os dados dos produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "Data da Compra", "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos, "Data da Compra")
    save_csv(df_produtos, "../data/tb_produtos.csv")

