import requests
import base64
import pandas as pd
import gzip
import io
from sqlalchemy import create_engine
import psycopg2

def data_cognatis_api(password, env_id, user_b64, password_b64, module_ids, submodule=None):

    url_auth = 'http://tier1.cognatis.com.br/dev/passport/api/token/auth'
    h = {"Content-Type":"application/json"}

    response_auth = requests.post(url_auth, headers=h, json={"username":user_b64, "password":password_b64, "environmentId": env_id})
    if response_auth.status_code != 200:
        print(f"Erro na autenticação: {response_auth.status_code} - {response_auth.text}")
        return pd.DataFrame()

    token = response_auth.json().get('token')
    if not token:
        print("Token não encontrado na resposta de autenticação.")
        return pd.DataFrame()

    print(f"Token: {token}")

    h2 = {
        "Authorization": f"Bearer {token}",
        "Content-Type":"application/json"
    }

    df = pd.DataFrame()  # Inicializa a variável df como um DataFrame vazio

    for moduleId in module_ids:

        url_extract = f'http://35.231.247.4/hml/datalead/api/v1/module/{moduleId}/extract'
        
        print(f"URL de extração: {url_extract}")
            
        body = {
            "cache": True, #Indica se deverá ser utilizado um arquivo em cache quando disponível.
            "compact": True, # Indica se deverá ser utilizada compactação de arquivo (gzip).
            "delimiter": ";", # Caractere delimitador para separar as colunas no arquivo. 
            "geoLevel": 2, # Id do Geolevel,indicando do nível geográfico para agregar o dado.
            "where": [],
            "groupBy": [], # Coleção de ids para agrupar o dado.
            "having": [], # Coleção de elementos para filtrar o dado após o agrupamento. 
            "orderBy": [], # Coleção de ids para ordenar o dado.
            "formatType": "csv", # Formato de saída do dado (csv).
        }

        if submodule is not None:
            body["submodule"] = submodule

        response = requests.post(url_extract, json=body, headers=h2)
        print(f"Resposta da extração: {response.status_code} - {response.text}")

        if response.status_code == 200:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                csv_content = f.read()
                df = pd.read_csv(io.StringIO(csv_content), delimiter='|')
                print("Dados lidos com sucesso:")
                print(df.head())
        else:
            print(f"Erro na extração dos dados: {response.status_code} - {response.text}")

    if not df.empty:
        df_geo = df
    else:
        df_geo = pd.DataFrame()  # Retorna um DataFrame vazio se df não foi atribuído

    return df_geo

if __name__ == "__main__":
    # Dados de login
    user = "data@leroym.com.br"
    password = "Cog@2023"
    env_id = 39

    # Codificar o nome de usuário e a senha em Base64
    user_b64 = base64.b64encode(user.encode()).decode()
    password_b64 = base64.b64encode(password.encode()).decode()

    module_ids = [21]
    submodule = []

    # Buscar dados na API
    df_geo = data_cognatis_api(password, env_id, user_b64, password_b64, module_ids, submodule=None)
    #print(df_geo)

    # Conectar ao banco de dados
    try:
        conn = psycopg2.connect(
            dbname="arcgisdev",
            user="arcgisdev",
            password="AVNS_PaPlVCInSYqz_KDQ7PA",
            host="pg-arcgis-brlm-p-brl-dextech.f.aivencloud.com",
            port="12833"
        )
        engine = create_engine('postgresql+psycopg2://arcgisprd:AVNS_so500TuV8AZ8n_HKWFG@pg-arcgis-brlm-p-brl-dextech.f.aivencloud.com:12833/arcgisprd')
        print("Conexão com o banco de dados estabelecida com sucesso.")
    except psycopg2.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        exit(1)
    
    # Definir o nome da tabela com base no module_id e geolevel
    module_id = '21' # Exemplo de module_id
    geolevel = 'geolevel2'  # Exemplo de geolevel
    table_name = f'dados_cognatis_{module_id}_{geolevel}'
    
    # Verificar se a tabela existe
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            # Verificar se a tabela está vinculada a alguma view
            cursor.execute(f"""
                SELECT table_name 
                FROM information_schema.view_table_usage 
                WHERE table_name = '{table_name}';
            """)
            views = cursor.fetchall()
            
            # Dropar as views que dependem da tabela
            for view in views:
                cursor.execute(f'DROP VIEW IF EXISTS "{view[0]}" CASCADE;')
                conn.commit()
                print(f"View {view[0]} dropada com sucesso.")
            
            # Dropar a tabela
            cursor.execute(f'DROP TABLE "{table_name}";')
            conn.commit()
            print(f"Tabela {table_name} dropada com sucesso.")
        
        # Criar a tabela com base no DataFrame
        columns = []
        for col in df_geo.columns:
            if pd.api.types.is_numeric_dtype(df_geo[col]):
                columns.append(f'"{col}" NUMERIC')
            else:
                columns.append(f'"{col}" TEXT')
        columns_str = ", ".join(columns)
        create_table_query = f'CREATE TABLE "{table_name}" ({columns_str});'
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Tabela {table_name} criada com sucesso.")
    
    # Inserir dados em blocos
    chunk_size = 10000
    for start in range(0, len(df_geo), chunk_size):
        df_chunk = df_geo.iloc[start:start + chunk_size]
        df_chunk.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Bloco de {start} a {start + chunk_size} inserido com sucesso na tabela {table_name}.")