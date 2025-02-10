import requests
import base64
import dask.dataframe as dd
import gzip
import io
import os
import tempfile
from sqlalchemy import create_engine
import psycopg2


def data_cognatis_api(env_id, user_b64, password_b64, module_ids, submodule=None):
    url_auth = 'http://tier1.cognatis.com.br/dev/passport/api/token/auth'
    headers_auth = {"Content-Type": "application/json"}

    # Autenticação na API
    response_auth = requests.post(
        url_auth,
        headers=headers_auth,
        json={"username": user_b64, "password": password_b64, "environmentId": env_id}
    )
    
    if response_auth.status_code != 200:
        print(f"Erro na autenticação: {response_auth.status_code} - {response_auth.text}")
        return None

    token = response_auth.json().get('token')
    if not token:
        print("Token não encontrado na resposta de autenticação.")
        return None

    print(f"Token obtido com sucesso!")

    headers_data = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    all_data = []  # Lista para armazenar os dados de todos os módulos

    for moduleId in module_ids:
        url_extract = f'http://35.231.247.4/hml/datalead/api/v1/module/{moduleId}/extract'
        print(f"Extraindo dados de: {url_extract}")

        body = {
            "cache": True,
            "compact": True,
            "delimiter": ";",
            "geoLevel": 2,
            "where": [],
            "groupBy": [],
            "having": [],
            "orderBy": [],
            "formatType": "csv",
            "exportModule": moduleId
        }

        if submodule is not None:
            body["submodule"] = submodule

        response = requests.post(url_extract, json=body, headers=headers_data)

        if response.status_code == 200:
            print(f"Dados extraídos com sucesso para o módulo {moduleId}.")
            try:
                with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                    for line in f:  # Processa linha por linha
                        all_data.append(line)
            except Exception as e:
                print(f"Erro ao processar os dados do módulo {moduleId}: {e}")
                return None
        else:
            print(f"Erro na extração dos dados: {response.status_code} - {response.text}")
    
    return "\n".join(all_data) if all_data else None


if __name__ == "__main__":
    # Dados de login
    user = 
    password = 
    env_id = 

    # Codificar credenciais em Base64
    user_b64 = base64.b64encode(user.encode()).decode()
    password_b64 = base64.b64encode(password.encode()).decode()

    module_ids = [25]
    submodule = 14

    # Buscar dados na API
    combined_csv_data = data_cognatis_api(env_id, user_b64, password_b64, module_ids, submodule=submodule)

    if combined_csv_data:
        # Criar um arquivo temporário para salvar os dados
        temp_file_path = os.path.join("D:\\zukk\\Leroy", "dados_cognatis.csv")

        with open(temp_file_path, "w", encoding="utf-8") as temp_file:
            temp_file.write(combined_csv_data)

        print(f"Dados salvos no arquivo: {temp_file_path}")

        try:
            # Ler o arquivo usando Dask em chunks
            ddf = dd.read_csv(temp_file_path, delimiter=';', blocksize="16MB")
            print("Carregando o arquivo usando Dask...")
            print(ddf.head())

            # Configurar conexão com o banco de dados PostgreSQL
            db_uri = 
            engine = create_engine(db_uri)
            print("Conexão com o banco de dados estabelecida com sucesso.")

            for module_id in module_ids:
                table_name = f'dados_cognatis_{module_id}_geolevel2'
                print(f"Escrevendo os dados na tabela {table_name}...")

                # Escrever os dados no banco de dados
                ddf.to_sql(
                    table_name,
                    con=db_uri,  # Passando a string de conexão diretamente
                    if_exists='replace',
                    index=False,
                    chunksize=10000
                )

                print(f"Dados inseridos com sucesso na tabela {table_name}.")

        except Exception as e:
            print(f"Erro ao processar os dados: {e}")
