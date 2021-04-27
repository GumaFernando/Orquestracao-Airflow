# Projeto - Criação de um pipeline utilizando o Apache Airflow.
Criação de um pipeline utilizando a ferramenta de orquestração Apache Airflow.

Projeto inicial na qual utilizei o portal da transparência para extração dos dados:

URL Utilizada para extração:

http://www.portaltransparencia.gov.br/download-de-dados/viagens/2020

# Objetivo:

Realização de extração dos dados para o sistema de arquivo local utilizando uma técnica conhecida como Crawler,
e na sequência a inserção dos arquivos coletados para o HDFS, e logo após foram criadas as
tabelas externas no HIVE utilizando esses arquivos. Utilizando uma ferramenta de
orquestração Apache Airflow.

### Etapas realizadas:
- Criação de um diretório local para armazenar os dados
- Download dos dados no formato  ZIP
- Descompactação dos arquivos e inserção no diretório criado
- Utilizado um sensor para verificação do arquivos
- Criação de um uma pasta no HDFS
- Inserção dos dados no HDFS
- Criação das tabelas externas no Hive
- Carregamento dos dados nas tabelas

### Visão  Graph View:

![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/projeto_airflow1.PNG)

### Visão Tree View
![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/airflow_tree_view.PNG)

### Email recebido confirmando Pipeline finalizado com sucesso !

![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/projeto_airflow2.PNG)

### Visalizando dados no HDFS

![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/airflow_hdfs.PNG)

### Visualizando tabelas no Hive

![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/airflow_hive.PNG)

