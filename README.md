## OpenDataSUS - Campanha nacional de vacinação contra Covid-19

Desafio carregar dados via API pública, sobre vacinação contra Covid-19, com o objetivo em fazer ingestão e carga de dados:

* Extrair 10 colunas
* Persistir dados em uma base não relacional (NoSql).
* Persistir dados em uma base relacional (SQL).
* Persistir dados no format parquet em ambiente local.

Dados público disponivel no site do [OpenDataSUS](https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao).

Desenvolvido aplicação em Python para fazer o processo de ingestão de dados:

- Verifica status conexão API.
- Carrega dados json via API usando biblioteca requests.
  - Salva dados em listas.
  - Gerado dataframe usando a biblioteca pandas, a partir das listas.
- Criado banco de dados Mysql Server na Cloud da AWS.
  - A partir do dataframe criado, realizado a carga de dados no banco de dados.
 - Usando o mesmo dataframe, usada para gerar o arquivo parquet.
 - Usando DynamoDB (NoSql), criado tabela para importar dados json.

[APP em Python](https://github.com/villani31/Python_API_OpenDataSUS/tree/main/app)

[Arquivo parquet](https://github.com/villani31/Python_API_OpenDataSUS/tree/main/parquet)

[Imagens](https://github.com/villani31/Python_API_OpenDataSUS/tree/main/imagens)
