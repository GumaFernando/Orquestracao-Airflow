from airflow import DAG 
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import  PythonOperator
from airflow.operators.bash_operator import  BashOperator
from airflow.operators.hive_operator import  HiveOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import zipfile
import requests
from io import BytesIO
import os


#VARIÁVEIS

url = 'http://www.portaltransparencia.gov.br/download-de-dados/viagens/2020'
folder = '/opt/airflow/dags/files/viagens2020'

# - criar diretorio para armazenar os arquivos.
# - faz um download dos dados
# -sensor para verificar se os dados foram baixados
# - cria uma pasta input
# - cria tabelas no hive
# - inseri cada arquivo em suas devidas tabela
# - envia email informando que deu ok


default_args = {
    'owner':'Guma',
    'start_date':datetime(2021,4,22, 2,30),
    'depends_on_past':False,
    'email_failure':False,
    'email_on_retry':False,
    'email': 'guma_fernando@hotmail.com',
    'retries':1,
    'retry_delay': timedelta(minutes=2)

}

with DAG (
    'viagens_2020',
    schedule_interval='@daily',
    default_args=default_args,
) as dag :



    #criando diretório para armazenar os dados de viagens

    def _create_folder_viagens ():

        os.makedirs(folder, exist_ok=True) ## exist_ok = se ja existir a pasta, ela nao é criada


    # Download dos dados
    def _download_file ():
        res = requests.get(url)
        zip = zipfile.ZipFile(BytesIO(res.content))
        zip.extractall(folder)  



    hql_query_pagamentos= """   
            CREATE DATABASE IF NOT  EXISTS viagens;
            USE viagens;
            CREATE EXTERNAL TABLE IF NOT EXISTS pagamentos (
                Id_processo_viagem STRING,
                Numero_Proposta STRING,
                Codigo_orgao_superior STRING,
                Nome_do_orgao_superior STRING,
                Codigo_orgao_pagador STRING,
                Nome_orgao_pagador STRING,
                Codigo_unid_pagadora STRING,
                Nome_unid_pagadora STRING,
                Tipo_de_pagamento STRING,
                Valor  DOUBLE
                ) 
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE 
            TBLPROPERTIES('skip.header.line.count'='1');
            
            LOAD DATA INPATH '/input/viagens/2020_Pagamento.csv' INTO TABLE  viagens.pagamentos
            """

    hql_query_passagem = """
            USE viagens;
            CREATE TABLE IF NOT EXISTS passagem (
                Id_viagem STRING
                ,Numero_proposta STRING
                ,Meio_de_transporte STRING
                ,Pais_Origem_ida STRING
                ,UF_Origem_ida STRING
                ,Cidade_Origem_ida STRING
                ,Pais_Destino_ida STRING
                ,UF_Destino_ida STRING
                ,Cidade_Destino_ida STRING
                ,Pais_Origem_volta STRING
                ,UF_Origem_volta STRING
                ,Cidade_Origem_volta STRING
                ,Pais_Destino_volta STRING
                ,UF_Destino_volta STRING
                ,Cidade_Destino_volta STRING
                ,Valor_da_passagem DOUBLE 
                ,Taxa_de_servico DOUBLE 
                ,Data_da_emissao_compra STRING
                ,Hora_da_emissao_compra STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            TBLPROPERTIES('skip.header.line.count'='1');

            LOAD DATA INPATH '/input/viagens/2020_Passagem.csv' INTO TABLE  viagens.passagem
            """

    hql_query_trecho = """
        USE viagens;
        CREATE TABLE IF NOT EXISTS trecho(
            Id_viagem STRING
            ,Numero_da_Proposta STRING
            ,Sequencia_Trecho STRING
            ,Origem_Data STRING
            ,Origem_Pais STRING
            ,Origem_UF STRING
            ,Origem_Cidade STRING
            ,Destino_Data STRING
            ,Destino_Pais STRING
            ,Destino_UF STRING
            ,Destino_Cidade STRING
            ,Meio_de_transporte STRING
            ,Numero_Diarias DOUBLE
            ,Missao STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';'
        STORED AS TEXTFILE
        TBLPROPERTIES('skip.header.line.count'='1');

        LOAD DATA INPATH '/input/viagens/2020_Trecho.csv' INTO TABLE  viagens.trecho

        """
    hql_query_viagem= """   
        USE viagens;
        CREATE TABLE IF NOT EXISTS viagem (
        id_viagem STRING
        ,Numero_da_Proposta STRING
        ,Situacao STRING
        ,Viagem_Urgente STRING
        ,Justificativa_Urgencia_Viagem STRING
        ,Codigo_do_orgao_superior STRING
        ,Nome_do_orgao_superior STRING
        ,Codigo_orgao_solicitante STRING
        ,Nome_orgao_solicitante  STRING
        ,CPF_viajante STRING
        ,Nome STRING
        ,Cargo STRING
        ,Funcao STRING
        ,Descricao_Funcao STRING
        ,Periodo_Data_de_inicio STRING
        ,Periodo_Data_de_fim STRING
        ,Destinos STRING
        ,Motivo STRING
        ,Valor_diarias DOUBLE
        ,Valor_passagens DOUBLE
        ,Valor_outros_gastos DOUBLE
    )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';'
        STORED AS TEXTFILE
        TBLPROPERTIES('skip.header.line.count'='1');

        LOAD DATA INPATH '/input/viagens/2020_Viagem.csv' INTO TABLE  viagens.viagem

        """



        # Starting no projeto
    starting = BashOperator (
        task_id= 'starting',
        bash_command= 'echo Start no projeto viagens 2020'
    )

      #criando diretório para armazenar os dados
    create_folder_viagens = PythonOperator (
        task_id='create_folder_viagens',
        python_callable= _create_folder_viagens
    )
     # Download dos dados
    download_file = PythonOperator (
        task_id = 'download_file',
        python_callable= _download_file
    )
      #Sensor para monitorar os arquivos .csv
    sensor_file_csv = FileSensor (
        task_id = 'sensor_file_csv',
        fs_conn_id= 'viagens_path',
        filepath = "*.csv",
        poke_interval=5,
        timeout=20
    )

    # Salvando arquivos no HDFS
    saving_hdfs = BashOperator (
        task_id='saving_hdfs',
        bash_command= """ hdfs dfs -mkdir -p /input/viagens && \
            hdfs dfs -put -f /opt/airflow/dags/files/viagens2020/*.csv /input/viagens 
        """
    )

    # Criando  as tabelas no Hive para armazenar os arquivos.
    create_table_hive_pagamentos = HiveOperator (
        task_id= 'create_table_hive_pagamentos',
        hive_cli_conn_id= 'conn_hive',
        hql = hql_query_pagamentos
    )
    
    create_table_hive_passagem = HiveOperator (
        task_id='create_table_hive_passagem',
        hive_cli_conn_id='conn_hive',
        hql = hql_query_passagem
    )

    create_table_hive_trecho = HiveOperator (
        task_id='create_table_hive_trecho',
        hive_cli_conn_id='conn_hive',
        hql = hql_query_trecho
    )
    
    create_table_hive_viagem = HiveOperator (
        task_id='create_table_hive_viagem',
        hive_cli_conn_id='conn_hive',
        hql = hql_query_viagem
    )

    send_email_sucess = EmailOperator (
        task_id='send_email_sucess',
        to= 'guma_fernando@hotmail.com',
        subject= 'Pipeline Enviado !',
        html_content="<h3>Pipeline Finalizado com Sucesso"
    )

    starting >> create_folder_viagens >> download_file
    download_file >> sensor_file_csv >> saving_hdfs
    saving_hdfs >> [create_table_hive_pagamentos, create_table_hive_passagem, create_table_hive_trecho, create_table_hive_viagem] >> send_email_sucess
    