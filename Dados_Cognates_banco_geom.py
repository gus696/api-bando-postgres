import requests
import base64
import pandas as pd
import gzip
import io
from shapely import wkt
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2

# Credenciais da API
user = "data@leroym.com.br"
password = "Cog@2023"
env_id = 39

# Credenciais do PostgreSQL
db_config = {
    "dbname": "Leroy",
    "user": "admin",
    "password": "123456",
    "host": "127.0.0.1",
    "port": "5433"
}

print("Iniciando processo de autenticação...")

# Encode base64
user_b64 = base64.b64encode(user.encode()).decode()
password_b64 = base64.b64encode(password.encode()).decode()

# URL de autenticação
url_auth = 'http://tier1.cognatis.com.br/dev/passport/api/token/auth'
h = {"Content-Type": "application/json"}

# Autenticação
response_auth = requests.post(url_auth, headers=h, json={"username": user_b64, "password": password_b64, "environmentId": env_id})
if response_auth.status_code == 200:
    token = response_auth.json().get('token')
    print(f"Token obtido com sucesso: {token}")
else:
    print("Erro ao autenticar:", response_auth.text)
    exit()

print("Processo de autenticação concluído.")
print("Iniciando solicitação de dados de geometria...")

# Headers com token
h2 = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# URL para geometria
url_geometry = 'http://35.231.247.4/hml/datalead/api/v1/module/geometry'
print(f"URL de solicitação: {url_geometry}")

# Solicitação de dados de geometria
response_geometry = requests.post(url_geometry, headers=h2, json={
    "cache": True,
    "compact": True,
    "delimiter": "|",
    "geoLevel": 3,
    "formatType": "csv"
})

if response_geometry.status_code == 200:
    print("Dados de geometria obtidos com sucesso.")
else:
    print("Erro ao obter geometria:", response_geometry.text)
    exit()

print("Processando os dados compactados...")

# Processar resposta compactada
try:
    with gzip.GzipFile(fileobj=io.BytesIO(response_geometry.content)) as f:
        dfGeometry = pd.read_csv(f, delimiter='|')
        print("Dados lidos com sucesso.")
except Exception as e:
    print("Erro ao ler o arquivo compactado:", str(e))
    exit()

print(f"Quantidade de registros lidos: {len(dfGeometry)}")

# Limpar e processar dados
print("Removendo registros com geometria nula...")
dfGeometry.dropna(subset=['geom'], inplace=True)
print(f"Registros restantes após limpeza: {len(dfGeometry)}")

print("Convertendo geometria WKT para objetos Shapely...")
dfGeometry['geom'] = dfGeometry['geom'].apply(wkt.loads)

# Conexão com o PostgreSQL
print("Estabelecendo conexão com o banco de dados PostgreSQL...")
    # Conectar ao banco de dados
try:
        conn = psycopg2.connect(
            dbname="arcgisprd",
            user="arcgisprd",
            password="AVNS_so500TuV8AZ8n_HKWFG",
            host="pg-arcgis-brlm-p-brl-dextech.f.aivencloud.com",
            port="12833"
        )
        engine = create_engine('postgresql+psycopg2://arcgisprd:AVNS_so500TuV8AZ8n_HKWFG@pg-arcgis-brlm-p-brl-dextech.f.aivencloud.com:12833/arcgisprd')
        print("Conexão com o banco de dados estabelecida com sucesso.")
except psycopg2.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        exit(1)
    
    # Criar tabela no banco de dados
table_name = "z_bairros_cognates_geom"
print(f"Iniciando criação da tabela '{table_name}' no banco de dados...")
    
    # Converte geometria para texto WKT antes de salvar no banco
dfGeometry['geom'] = dfGeometry['geom'].apply(lambda x: x.wkt)
    
try:
        dfGeometry.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Tabela '{table_name}' criada com sucesso e dados inseridos.")
except Exception as e:
        print(f"Erro ao criar a tabela ou inserir os dados: {e}")
        exit(1)
    
    # Atualizar o tipo de coluna geom para Geometry
print("Alterando a coluna 'geom' para o tipo Geometry no banco de dados...")
try:
        with engine.begin() as connection:  # Usando 'engine.begin' para garantir execução apropriada
            connection.execute(text(f"""
                ALTER TABLE {table_name}
                ALTER COLUMN geom TYPE geometry USING ST_GeomFromText(geom);
            """))
        print("Coluna 'geom' alterada para o tipo Geometry com sucesso.")
except Exception as e:
        print(f"Erro ao alterar o tipo da coluna geom: {e}")
    
print("Processo concluído com sucesso.")

