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


def corrigir(title:str):
    title = title.lower().strip().replace(' ', '_')
    title = unidecode.unidecode(title)
    title = re.sub(r'[^a-zA-Z0-9_]', '', title)
    return title

df.columns = df.columns.map(corrigir)
# print(df.columns)
df.head(10)
buffer_size = 1000
buffer = []

with DBConnect(connector = psycopg2, dbname="python", user="postgres", password="postgres", host="192.168.0.112", port="5432") as cursor:
    registros =  df.iterrows()
    for _, row in registros:
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

            buffer.clear()
    
    if buffer:
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
    