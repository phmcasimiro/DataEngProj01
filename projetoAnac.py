import pandas as pd
import psycopg2
import codecs
from chardet.universaldetector import UniversalDetector

# ARQUIVO DE ENTRADA
caminho_do_arquivo = r"C:\Users\phmcasimiro\Documents\Cursos&Projetos\EngenhariaDadosPy\dataset_Postgre\Origem_dados\V_OCORRENCIA_AMPLA.json"

# INÍCIO DA DETECÇÃO AUTOMÁTICA DE ENCODING
detector = UniversalDetector()

with codecs.open(caminho_do_arquivo, 'rb') as arquivo: #rb esta lendo como binario em forma de bits no lugar de texto ou outra coisa 
    for linha in arquivo:
        detector.feed(linha)
        if detector.done:
            break

detector.close()

encoding_detectado = detector.result['encoding']
# FIM DA DETECÇÃO AUTOMÁTICA DE ENCODING

# DATAFRAME
df_json = pd.read_json(caminho_do_arquivo, encoding=encoding_detectado)

# LISTAR AS COLUNAS QUE SERÃO UTILIZADAS EM UM NOVO DATAFRAME "LIMPO"
colunas = ["Numero_da_Ocorrencia",
           "Classificacao_da_Ocorrência",
           "Data_da_Ocorrencia",
           "Municipio",
           "UF",
           "Regiao",
           "Nome_do_Fabricante",
           "Latitude",
           "Longitude"
           ]

# CRIAR UM NOVO DATAFRAME QUE RECEBE AS COLUNAS SELECIONADAS DO DATAFRAME ORIGINAL
df = df_json[colunas]

# RENOMEAR AS COLUNAS APAGANDO ACENTOS E RETIRANDO ESPAÇOS
df.rename(
    columns={
        'Numero_da_Ocorrencia':'numOcorrencia',
        'Classificacao_da_Ocorrência':'classifOcorrencia',
        'Data_da_Ocorrencia':'dt_Ocorrencia',
        'Municipio':'nm_mun',
        'UF':'uf',
        'Regiao':'regiao',
        'Nome_do_Fabricante':'nm_Fabricante',
        'Latitude':"lat",
        'Longitude':'long'
        },
    inplace=True
    )
# SUBSTITUIR A VÍRGULA POR PONTO
df['lat'] = df['lat'].astype(str).str.replace(',', '.')
df['long'] = df['long'].astype(str).str.replace(',', '.')

# CRIAR TABELA DIRETAMENTE NO BANCO DE DADOS
'''
   CREATE TABLE IF NOT EXISTS gisdb.desenvolvimento.proj_anac_acidentes (
        numOcorrencia int,
        classifOcorrencia VARCHAR(50),
        dt_Ocorrencia DATE,
        nm_mun VARCHAR(50),
        uf VARCHAR(30),
        regiao VARCHAR(30),
        nm_Fabricante VARCHAR(100),
        lat varchar(11),
        long varchar(11)
    )
'''

# PARÂMETROS DE CONEXÃO
dbname   = 'gisdb'
user     = 'postgres'
password = 'MudarePreciso123'
host     = 'localhost'
port     = '5432' 

# CRIAR CONEXÃO
conexao = psycopg2.connect(dbname=dbname,
                        user=user,
                        password=password,
                        host=host,
                        port=port)

# CRIAR UM CURSOR PARA EXECUTAR TAREFAS NO BANCO
cursor = conexao.cursor()

# DELETAR CONTEÚDO DA TABELA ANTES DA NOVA CARGA
cursor.execute('delete from gisdb.desenvolvimento.proj_anac_acidentes')

#CARGA DE DADOS
for indice,coluna_df in df.iterrows():
    cursor.execute ("""
                INSERT INTO gisdb.desenvolvimento.proj_anac_acidentes (numOcorrencia,classifOcorrencia,dt_Ocorrencia,nm_mun,uf,regiao,nm_Fabricante,lat,long) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (coluna_df["numOcorrencia"],coluna_df["classifOcorrencia"],coluna_df["dt_Ocorrencia"],coluna_df["nm_mun"],coluna_df["uf"],coluna_df["regiao"],coluna_df["nm_Fabricante"],coluna_df["lat"],coluna_df["long"])                   
                )
conexao.commit()
cursor.close()
conexao.close()

