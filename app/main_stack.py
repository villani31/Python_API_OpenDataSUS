# importando pacotes
import class_stack
import pandas as pd
from datetime import date
import json
from decimal import Decimal
import boto3

# Status conexao
def Status_Api():
    try:
        status = class_stack.authapi.StatusApi()
        if (status == 200):
            print("Status conexao API - SUCESSO -")
    except:
        print("Conexao com API falhou.")
        exit(1)

# Carrega da API
def Carrega_Dados(inicio,fim):
    # carrega dados json
    data = class_stack.authapi.Load_Json()
    #print(json.dumps(data, indent=2))

    # listas
    estalecimento_noFantasia = []
    vacina_lote = []
    vacina_nome = []
    paciente_idade = []
    vacina_descricao_dose = []
    estabelecimento_uf = []
    estabelecimento_razaoSocial = []
    paciente_dataNascimento = []
    paciente_endereco_uf = []
    vacina_fabricante_nome = []
    paciente_endereco_nmMunicipio = []
    estabelecimento_municipio_nome = []
    paciente_enumSexoBiologico = []
    vacina_grupoAtendimento_nome = []
    vacina_dataAplicacao = []

    # carrega lista com os dados a partir do json
    try:
        for x in range(inicio,fim):
            # filtra dados dentro do json
            dados = data["hits"]["hits"][x]["_source"]
            for chave,valor in dados.items():
                if (chave == "estalecimento_noFantasia"):
                    estalecimento_noFantasia.append(valor)
                elif (chave == "vacina_lote"):
                    vacina_lote.append(valor)
                elif (chave == "vacina_nome"):
                    vacina_nome.append(valor)
                elif (chave == "paciente_idade"):
                    paciente_idade.append(valor)
                elif (chave == "vacina_descricao_dose"):
                    vacina_descricao_dose.append(valor)
                elif (chave == "estabelecimento_uf"):
                    estabelecimento_uf.append(valor)
                elif (chave == "estabelecimento_razaoSocial"):
                    estabelecimento_razaoSocial.append(valor)
                elif (chave == "paciente_dataNascimento"):
                    paciente_dataNascimento.append(valor)
                elif (chave == "paciente_endereco_uf"):
                    paciente_endereco_uf.append(valor)
                elif (chave == "vacina_fabricante_nome"):
                    vacina_fabricante_nome.append(valor)
                elif (chave == "paciente_endereco_nmMunicipio"):
                    paciente_endereco_nmMunicipio.append(valor)
                elif (chave == "estabelecimento_municipio_nome"):
                    estabelecimento_municipio_nome.append(valor)
                elif (chave == "paciente_enumSexoBiologico"):
                    paciente_enumSexoBiologico.append(valor)
                elif (chave == "vacina_grupoAtendimento_nome"):
                    vacina_grupoAtendimento_nome.append(valor)
                elif (chave == "vacina_dataAplicacao"):
                    vacina_dataAplicacao.append(valor)
    except:
                print("Erro, chave não encontada")    
                exit(1)

    # criando dicionario 
    data_dic = {
        "estalecimento_noFantasia" : estalecimento_noFantasia,
        "vacina_lote" : vacina_lote,
        "vacina_nome" : vacina_nome,
        "paciente_idade" : paciente_idade,
        "vacina_descricao_dose" : vacina_descricao_dose,
        "estabelecimento_uf" : estabelecimento_uf,
        "estabelecimento_razaoSocial" : estabelecimento_razaoSocial,
        "paciente_dataNascimento" : paciente_dataNascimento,
        "paciente_endereco_uf" : paciente_endereco_uf,
        "vacina_fabricante_nome" : vacina_fabricante_nome,
        "paciente_endereco_nmMunicipio" : paciente_endereco_nmMunicipio,
        "estabelecimento_municipio_nome" : estabelecimento_municipio_nome,
        "paciente_enumSexoBiologico" : paciente_enumSexoBiologico,
        "vacina_grupoAtendimento_nome" : vacina_grupoAtendimento_nome,
        "vacina_dataAplicacao" : vacina_dataAplicacao
    }

    # Gera dataframe para importar no banco
    global df_covid19
    df_covid19 = pd.DataFrame(data_dic, columns=["estalecimento_noFantasia", \
        "vacina_lote", "vacina_nome", "paciente_idade", "vacina_descricao_dose", \
        "estabelecimento_uf", "estabelecimento_razaoSocial", "paciente_dataNascimento", \
        "paciente_endereco_uf", "vacina_fabricante_nome", "paciente_endereco_nmMunicipio", \
        "estabelecimento_municipio_nome", "paciente_enumSexoBiologico", "vacina_grupoAtendimento_nome", \
        "vacina_dataAplicacao" ])

# Grava dados da API no banco de dados mysql
def Envia_DB_Sql(table, dataframe):
    try:
        dataframe.to_sql(table, class_stack.conn, index=False, if_exists="append")
        print("Dados inserido com sucesso...")
    except:
        print("Erro ao conectar no banco de dados.")

# Gera arquivo parquet a partir do dataframe
def Gera_Parquet():
    try:
        dataatual = date.today()
        df_covid19.to_parquet(f"{dataatual}-covid19.parquet", index = False)
        print("Arquivo .parquet gerado com sucesso...")
    except:
        print("Erro ao gerar arquivo .parquet.")

# Grava dados tabela nosql
# Criar usuario AWS IAM
# aws configure
# AWS Access Key ID 
# AWS Secret Access Key 
def Envia_DB_NoSql(jsondata):
    datadict = json.loads(json.dumps(jsondata), parse_float=Decimal)
    #print(datadict)
    database = boto3.resource('dynamodb')
    table = database.Table("covid19")
    table.put_item(Item = datadict)

######################
## Executar funcoes ##
######################
##
Status_Api()

##
Carrega_Dados(0,25)

##
Envia_DB_Sql("covid19",df_covid19)

##
Gera_Parquet()

##
# carrega dados json
#data = class_stack.authapi.Load_Json()
#Envia_DB_NoSql(data)


