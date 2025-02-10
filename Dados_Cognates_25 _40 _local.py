import requests
import base64
import dask.dataframe as dd
import gzip
import io
import os
import tempfile
from sqlalchemy import create_engine
import psycopg2


def data_cognatis_api(password, env_id, user_b64, password_b64, module_ids, submodule=None):
    url_auth = 'http://tier1.cognatis.com.br/dev/passport/api/token/auth'
    h = {"Content-Type": "application/json"}

    # Autenticação na API
    response_auth = requests.post(
        url_auth, headers=h, json={"username": user_b64, "password": password_b64, "environmentId": env_id}
    )
    if response_auth.status_code != 200:
        print(f"Erro na autenticação: {response_auth.status_code} - {response_auth.text}")
        return None

    token = response_auth.json().get('token')
    if not token:
        print("Token não encontrado na resposta de autenticação.")
        return None

    print(f"Token: {token}")

    h2 = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    all_data = []  # Lista para armazenar os dados de todos os módulos

    for moduleId in module_ids:
        url_extract = f'http://35.231.247.4/hml/datalead/api/v1/module/{moduleId}/extract'
        print(f"URL de extração: {url_extract}")

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
        response = requests.post(url_extract, json=body, headers=h2)
        print(f"Resposta da extração: {response.status_code} - {response.text}")
        if response.status_code == 200:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                all_data.append(f.read())  # Armazena os dados compactados
        else:
            print(f"Erro na extração dos dados: {response.status_code} - {response.text}")
    # Combina os dados de todos os módulos e retorna como string
    if all_data:
        combined_data = "\n".join(all_data)
        return combined_data
    else:
        return None
if __name__ == "__main__":
    # Dados de login
    user = "data@leroym.com.br"
    password = "Cog@2023"
    env_id = 39
    # Codificar o nome de usuário e senha em Base64
    user_b64 = base64.b64encode(user.encode()).decode()
    password_b64 = base64.b64encode(password.encode()).decode()
    module_ids = [40]
    submodule = 1

    # Buscar dados na API
    combined_csv_data = data_cognatis_api(password, env_id, user_b64, password_b64, module_ids, submodule=submodule)

    if combined_csv_data:
        # Criar um arquivo temporário para salvar os dados
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir="D:\zukk\Leroy") as temp_file:

            temp_file.write(combined_csv_data.encode('utf-8'))
            temp_file_path = temp_file.name

        print(f"Dados salvos no arquivo temporário: {temp_file_path}")

        try:
            # Ler o arquivo usando Dask em chunks
            ddf = dd.read_csv(temp_file_path, delimiter='|', blocksize="16MB")
            print("Carregando o arquivo usando Dask...")
            print(ddf.head())

            # Conectar ao banco de dados
            uri = 'postgresql+psycopg2://admin:123456@127.0.0.1:5433/Leroy'
            engine = create_engine(uri)
            print("Conexão com o banco de dados estabelecida com sucesso.")

            for module_id in module_ids:
                # Definir o nome da tabela com base no module_id e geolevel
                geolevel = 'geolevel2'  # Exemplo de geolevel
                table_name = f'dados_cognatis_{module_id}_{geolevel}'

                # Escrever os dados no banco de dados
                print(f"Escrevendo os dados no banco de dados: {table_name}...")
                ddf.to_sql(table_name, uri, if_exists='replace', index=False, chunksize=10000)
                print(f"Dados inseridos com sucesso na tabela {table_name}.")
        except Exception as e:
            print(f"Erro ao processar os dados: {e}")


