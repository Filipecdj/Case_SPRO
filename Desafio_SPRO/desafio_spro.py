from pymongo import MongoClient
import pandas as pd

def conectar_mongodb():
    """
    Conecta ao banco de dados MongoDB e retorna as coleções relevantes.

    Returns:
        tuple: Uma tupla contendo as coleções 'collection_carros', 'collection_montadoras' e 'collection_resultado'.
    """
    client = MongoClient('localhost', 27017)
    db = client['local']
    collection_carros = db['carros']
    collection_montadoras = db['montadoras']
    collection_resultado = db['resultado']
    return collection_carros, collection_montadoras, collection_resultado

def criar_dataframe_carros():
    """
    Cria um DataFrame 'carros' com dados predefinidos.

    Returns:
        pd.DataFrame: DataFrame contendo informações sobre carros.
    """
    dados_carros = {
        'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
        'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
        'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']
    }
    carros = pd.DataFrame(dados_carros)
    return carros

def criar_dataframe_montadoras():
    """
    Cria um DataFrame 'montadoras' com dados predefinidos.

    Returns:
        pd.DataFrame: DataFrame contendo informações sobre montadoras.
    """
    dados_montadoras = {
        'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
        'País': ['EUA', 'Alemanha', 'França', 'EUA', 'Japão']
    }
    montadoras = pd.DataFrame(dados_montadoras)
    return montadoras

def inserir_dataframes_no_mongodb(collection_carros, collection_montadoras, carros, montadoras):
    """
    Insere DataFrames no banco de dados MongoDB.

    Args:
        collection_carros: Coleção de carros no MongoDB.
        collection_montadoras: Coleção de montadoras no MongoDB.
        carros (pd.DataFrame): DataFrame contendo informações sobre carros.
        montadoras (pd.DataFrame): DataFrame contendo informações sobre montadoras.
    """
    collection_carros.insert_many(carros.to_dict('records'))
    collection_montadoras.insert_many(montadoras.to_dict('records'))

def realizar_agregacao(collection_carros, collection_resultado):
    """
    Realiza uma operação de agregação entre as coleções 'collection_carros' e 'collection_montadoras',
    e insere o resultado na coleção 'collection_resultado'.

    Args:
        collection_carros: Coleção de carros no MongoDB.
        collection_resultado: Coleção de resultado no MongoDB.

    Returns:
        list: Lista contendo o resultado da operação de agregação.
    """
    pipeline = [
        {
            '$lookup': {
                'from': 'montadoras',
                'localField': 'Montadora',
                'foreignField': 'Montadora',
                'as': 'montadoras'
            }
        },
        {
            '$unwind': '$montadoras'
        },
        {
            '$group': {
                '_id': '$montadoras.País',
                'Carros': {
                    '$push': {
                        'Carro': '$Carro',
                        'Cor': '$Cor'
                    }
                }
            }
        }
    ]

    result = list(collection_carros.aggregate(pipeline))
    collection_resultado.insert_many(result)
    return result

def main():
    """
    Função principal que orquestra as operações do script.
    """
    collection_carros, collection_montadoras, collection_resultado = conectar_mongodb()

    carros = criar_dataframe_carros()
    montadoras = criar_dataframe_montadoras()

    inserir_dataframes_no_mongodb(collection_carros, collection_montadoras, carros, montadoras)

    resultado = realizar_agregacao(collection_carros, collection_resultado)

    print(resultado)

if __name__ == "__main__":
    main()
