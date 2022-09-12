# imortando pacotes
import conexao_stack
import requests
import json
from sqlalchemy import create_engine

# Conexao API
class ConexaoApi:
    def __init__(self,url,auth):
        self.url = url
        self.auth = auth

        global retorno
        parametro = json.dumps({
            "size": 10000
        })
        headers = {
            'Content-Type': 'application/json'
        }
        retorno = requests.request(
            "GET",
            url = self.url,
            auth = self.auth,
            data = parametro,
            headers = headers
        )
    # Verifica status
    def StatusApi(self):
        status = retorno
        return (status.status_code)

    # Carrega json
    def Load_Json(self):
        data = json.loads(retorno.text)
        return data

# Conexao banco de dados relacional SQL
class ConexaoDbSql:
    def __init__(self,host,port,database,user,password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        global conn
        conn = create_engine("mysql+pymysql://"+conexao_stack.db_user+":"+conexao_stack.db_pass+"@"+conexao_stack.db_url+":"+conexao_stack.db_port+"/"+conexao_stack.db_database)

#####################
# Dados conexao API #
#####################
authapi = ConexaoApi(
    conexao_stack.api_url,
    conexao_stack.api_auth
)

##########################
# Dados conexao DB_Mysql #
##########################
dbsql = ConexaoDbSql(
    conexao_stack.db_user,
    conexao_stack.db_pass,
    conexao_stack.db_url,
    conexao_stack.db_port,
    conexao_stack.db_database
)
