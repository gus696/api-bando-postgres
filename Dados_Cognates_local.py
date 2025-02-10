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

    response_auth = requests.post(url_auth, headers=h, json={"username": user_b64, "password": password_b64, "environmentId": env_id})
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
        "Content-Type": "application/json"
    }

    df = pd.DataFrame()

    for moduleId in module_ids:
        url_extract = f'http://35.231.247.4/hml/datalead/api/v1/module/{moduleId}/extract'
        print(f"URL de extração: {url_extract}")

        body = {
            "cache": True,
            "compact": True,
            "delimiter": ";",
            "geoLevel": 5,
            "where": [],
            "groupBy": [],
            "having": [],
            "orderBy": [],
            "formatType": "csv",
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

    return df if not df.empty else pd.DataFrame()

def process_database(conn, engine, table_name, df_geo):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT view_definition 
            FROM information_schema.views 
            WHERE table_name = '{table_name}';
        """)
        views = cursor.fetchall()
        views_sql = [view[0] for view in views]

        if table_exists:
            cursor.execute(f'TRUNCATE TABLE "{table_name}";')
            conn.commit()
            print(f"Dados da tabela {table_name} apagados.")

        else:
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

    chunk_size = 10000
    for start in range(0, len(df_geo), chunk_size):
        df_chunk = df_geo.iloc[start:start + chunk_size]
        df_chunk.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Bloco de {start} a {start + chunk_size} inserido na tabela {table_name}.")

    with conn.cursor() as cursor:
        for view_sql in views_sql:
            cursor.execute(view_sql)
            conn.commit()
            print("View recriada com sucesso.")

    conn.close()

if __name__ == "__main__":
    user = "data@leroym.com.br"
    password = "Cog@2023"
    env_id = 39

    user_b64 = base64.b64encode(user.encode()).decode()
    password_b64 = base64.b64encode(password.encode()).decode()

    module_ids = [23]

    df_geo = data_cognatis_api(password, env_id, user_b64, password_b64, module_ids, submodule=None)

    dbs = {
        "DEV": {
            "dbname": "arcgisdev",
            "user": "arcgisdev",
            "password": "AVNS_PaPlVCInSYqz_KDQ7PA",
            "host": "pg-arcgis-brlm-p-brl-dextech.f.aivencloud.com",
            "port": "12833"
        },
        "PRD": {
            "dbname": "arcgisprd",
            "user": "arcgisprd",
            "password": "AVNS_so500TuV8AZ8n_HKWFG",
            "host": "pg-arcgis-brlm-p-brl-dextech.f.aivencloud.com",
            "port": "12833"
        }
    }

    table_name = f'dados_cognatis_23_geolevel5'

    for env, config in dbs.items():
        try:
            conn = psycopg2.connect(
                dbname=config["dbname"],
                user=config["user"],
                password=config["password"],
                host=config["host"],
                port=config["port"]
            )
            engine = create_engine(f'postgresql+psycopg2://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["dbname"]}')
            print(f"\nConexão com o banco {env} estabelecida.")
            process_database(conn, engine, table_name, df_geo)
        except psycopg2.OperationalError as e:
            print(f"Erro ao conectar ao banco {env}: {e}")
