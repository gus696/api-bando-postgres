# API

Este repositório contém scripts para extrair dados da API Cognats e armazená-los em bancos de dados PostgreSQL. Os scripts utilizam a biblioteca `requests` para fazer chamadas à API, `pandas` para manipulação de dados, `shapely` para manipulação de geometria e `sqlalchemy` para interagir com o banco de dados.

## Estrutura do Repositório

- `Dados_Cognates_Banco.py`: Script para extrair dados da API Cognats e armazená-los em bancos de dados PostgreSQL.
- `Dados_Cognates_banco_geom.py`: Script para extrair dados de geometria da API Cognats e armazená-los em bancos de dados PostgreSQL.

## Requisitos

- Python 3.7+
- Bibliotecas Python:
  - `requests`
  - `pandas`
  - `shapely`
  - `sqlalchemy`
  - `psycopg2`

Você pode instalar as bibliotecas necessárias usando o comando:

```bash
pip install requests pandas shapely sqlalchemy psycopg2
