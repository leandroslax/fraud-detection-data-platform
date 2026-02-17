📘 01 — Requisitos e Especificação do Projeto
Plataforma de Dados Multicanal para Detecção de Fraudes Financeiras
1. Contexto

A área de risco identificou um aumento no volume de transações e na complexidade dos padrões de fraude.
Para melhorar a identificação de comportamentos suspeitos, a gestão solicitou a construção de uma plataforma moderna de engenharia de dados que integre múltiplas fontes e permita análises avançadas e modelos de machine learning.

2. Objetivos do Projeto
2.1 Objetivo Geral

Construir uma arquitetura de dados escalável e robusta para ingestão, armazenamento, processamento e análise de dados relacionados a transações financeiras e eventos suspeitos.

2.2 Objetivos Específicos

Ingerir dados via API, CSV, banco relacional (PostgreSQL) e não relacional (MongoDB Atlas).

Implementar pipelines Batch e Streaming.

Criar Data Lake em AWS S3 com camadas Bronze, Silver e Gold.

Processar dados com Spark / PySpark e Databricks.

Orquestrar tudo via Airflow.

Transformar dados e modelar tabelas no Snowflake via DBT.

Criar dashboards com métricas de fraude.

Versionar todo o código via GitHub.

3. Escopo
3.1 Escopo de Ingestão

O projeto deve ingerir dados provenientes de:

API de transações financeiras.

Arquivos CSV carregados diariamente.

PostgreSQL (AWS RDS).

MongoDB Atlas conectado à AWS.

Streaming em tempo real (simulado via socket/kafka).

3.2 Escopo de Processamento

Tratamento e limpeza dos dados (Bronze → Silver).

Criação de features e agregações analíticas (Silver → Gold).

Pipelines batch diários.

Pipelines streaming contínuos.

Notebooks de validação no Databricks.

3.3 Escopo de Armazenamento

AWS S3 com três camadas:

Bronze: dados crus.

Silver: dados limpos.

Gold: dados analíticos.

3.4 Escopo de Modelagem de Dados

Snowflake como Data Warehouse final.

Modelagem via DBT (staging + marts).

Criação de:

dim_customer

dim_card

fact_transactions

fact_fraud_risk

4. Requisitos Funcionais

RF01 – A plataforma deve receber dados de múltiplas fontes.

RF02 – Dados devem ser armazenados no S3 em camadas múltiplas.

RF03 – Pipelines devem ser orquestrados no Airflow.

RF04 – Deve existir um pipeline batch.

RF05 – Deve existir um pipeline streaming.

RF06 – Dados limpos devem ir para o Snowflake.

RF07 – Transformações finais devem ser feitas via DBT.

RF08 – Deve existir logging das execuções.

RF09 – Dashboards devem ser gerados com base no Snowflake.

5. Requisitos Não Funcionais

RNF01 – A solução deve ser escalável.

RNF02 – O sistema deve ser resiliente e recuperável.

RNF03 – O Data Lake deve seguir boas práticas de particionamento.

RNF04 – Dados sensíveis devem ser mascarados.

RNF05 – Todo código deve ser versionado no GitHub.

6. Arquitetura Proposta (Visão Geral)
                    +------------- API ---------------+
                    |                                 |
                    v                                 |
    +----------+   +---------+    +-------------+     |
    |  CSVs    |-->| Airflow |--->|   S3 Bronze |<----+
    +----------+   +---------+    +-------------+
                      ^   ^               |
                      |   |               v
       +--------------+   +---------+  Spark/Databricks
       |                           |       Silver
+-------------+         +-------------+
| PostgreSQL  |         | MongoDB     |
+-------------+         +-------------+
                             |
                             v
                         S3 Gold
                             |
                             v
                        Snowflake + DBT
                             |
                             v
                          Dashboards

7. Entregáveis
Técnicos

Código das DAGs Airflow

Scripts PySpark

Notebooks Databricks

Modelos DBT

Scripts SQL (PostgreSQL)

Configuração do Data Lake

Documentação

Documento de requisitos

Documento de arquitetura

README.md profissional

Mapa do fluxo de dados

Guia de execução

8. Critérios de Aceite

Pipelines Airflow funcionando.

Bronze/Silver/Gold populados.

Streaming ativo.

Snowflake com dados modelados.

Dashboard funcional.

Repositório GitHub completo.

9. Riscos

Mudança de schema na API.

Instabilidade do Databricks Community.

Custos da AWS (usar free tier quando possível).

🔹 Fim do documento.
