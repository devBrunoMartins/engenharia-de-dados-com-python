from sqlalchemy import create_engine
from conect_postgre_sql import DBConnect
import pandas as pd
import unidecode
import re
import sys


dbname      = "python"
user        = "postgres"
password    = "postgres"
host        = "192.168.0.112"
port        = "5432"

conection = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
engine      = create_engine(conection)

nome_tabela = 'anac_sqlachemy'

def exit():
    sys.exit("\n\n EXIT.")

pd.set_option('display.max_columns', None)
path = "/data/Engenharia de Dados/Datasets/Anac/V_OCORRENCIA_AMPLA.json"
df = pd.read_json(path, encoding='utf-8-sig')

df = df[['Numero_da_Ocorrencia',
        'Classificacao_da_Ocorrência',
        'Data_da_Ocorrencia',
        'Municipio',
        'UF',
        'Regiao',
        'Nome_do_Fabricante',
        'Modelo'
        ]]


def padronize_titulos(title:str) -> str:
    """
    Padroniza os nomes dos títulos das colunas em um formato 
    formado snake_case
    
    :param title: Nome de coluna a ser padronizado
    :type title: str
    :return: Nome de coluna padronizado
    :rtype: str
    """
    title = title.lower().strip().replace(' ', '_')
    title = unidecode.unidecode(title)
    title = re.sub(r'[^a-zA-Z0-9_]', '', title)
    return title
    

def exec_query(cursor, buffer):
    """
    
    """

    cursor.executemany("""
            INSERT INTO mapeamento_anac              
                (id,
                classificacao_da_ocorrencia,
                dt_ocorrencia,
                municipio,
                uf,
                regiao,
                fabricante,
                Modelo) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", buffer)


df.columns = df.columns.map(padronize_titulos)


with DBConnect(connector=connector, dbname=dbname, user=user, password=password, host=host, port=port) as cursor:
    #Limpar dados antes da carga
    cursor.execute("DELETE FROM mapeamento_anac")
    
    buffer_size = 1000
    buffer = []
    
    for _, row in df.iterrows():
        buffer.append((
        row['numero_da_ocorrencia'],
        row['classificacao_da_ocorrencia'],
        row['data_da_ocorrencia'],
        row['municipio'],
        row['uf'],
        row['regiao'],
        row['nome_do_fabricante'],
        row['modelo']

        ))

        if len(buffer) == buffer_size:
            exec_query(cursor, buffer)

            buffer.clear()
    if buffer:
        exec_query(cursor, buffer)