# Projeto - Criação de um pipeline utilizando o Apache Airflow.
Criação de um pipeline utilizando a ferramenta de orquestração Apache Airflow.

Projeto inicial na qual utilizei o portal da transparência para extração dos dados:

URL Utilizada para extração:

http://www.portaltransparencia.gov.br/download-de-dados/viagens/2020

# Objetivo:

Realização de extração dos dados para o sistema de arquivo local utilizando uma técnica conhecida como Crawler,
e na sequência inserir os arquivos baixados no HDFS, e logo após criar
tabelas externas no HIVE utilizando esses arquivos no HDFS. Utilizando uma ferramenta de
orquestração Apache Airflow.

### Etapas realizadas:
- Criação de um diretório para armazenar os dados
- Download dos dados no formato  ZIP
- Descompactação dos arquivos e inserção no diretório criado
- Utilizado um sensor para verificação do arquivos
- criação de um uma pasta no HDFS
- inserção dos dados no HDFS
- Criação das tabelas externas no Hive
- Carregamento dos dados nas tabelas

### Visão  Graph View:

![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/projeto_airflow1.PNG)

### Email recebido confirmando Pipeline finalizado com sucesso !

![alt text](https://github.com/GumaFernando/Projeto_Airflow/blob/main/projeto_airflow2.PNG)

