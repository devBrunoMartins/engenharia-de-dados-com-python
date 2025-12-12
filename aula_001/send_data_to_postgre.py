import psycopg2
from conect_postgre import DBConnect
import pandas as pd
import unidecode
import re


pd.set_option('display.max_columns', None)
path = "/data/Engenharia de Dados/Datasets/Anac/V_OCORRENCIA_AMPLA.json"
df = pd.read_json(path, encoding='utf-8-sig')

df = df[['Numero_da_Ocorrencia',
        'Classificacao_da_Ocorrência',
        'Data_da_Ocorrencia',
        'Municipio',
        'UF',
        'Regiao',
        'Nome_do_Fabricante'
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
    cursor.executemany("""
            INSERT INTO anac              
                (numero_da_ocorrencia,
                classificacao_da_ocorrencia,
                data_da_ocorrencia,
                municipio,
                uf,
                regiao,
                nome_do_fabricante) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)""", buffer)


df.columns = df.columns.map(padronize_titulos)


with DBConnect(connector = psycopg2, dbname="python", user="postgres", password="postgres", host="192.168.0.112", port="5432") as cursor:
    #Limpar dados antes da carga
    cursor.execute("DELETE FROM anac")

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
        row['nome_do_fabricante']
        ))

        if len(buffer) == buffer_size:
            exec_query(cursor, buffer)

            buffer.clear()
    
    if buffer:
        exec_query(cursor, buffer)