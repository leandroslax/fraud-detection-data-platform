# 🚨 Fraud Detection Data Platform

Plataforma de dados moderna para detecção de fraudes em transações
financeiras, utilizando ingestão de múltiplas fontes, arquitetura
Lakehouse, processamento distribuído e modelagem analítica.

Este projeto demonstra habilidades práticas de **Data Engineering** em
um pipeline **end-to-end**, adequado para apresentação em entrevistas.

------------------------------------------------------------------------

# 🏗️ Arquitetura Geral

A plataforma segue o padrão **Data Lakehouse**, utilizando:

-   AWS S3 como Data Lake\
-   Databricks / Spark para processamento\
-   Snowflake para Data Warehouse\
-   Airflow para orquestração\
-   dbt Cloud para modelagem analítica

``` mermaid
flowchart LR
    A[Fontes de Dados
API / CSV / PostgreSQL / MongoDB] --> B[Airflow
Ingestão Batch + Streaming]
    B --> C[S3 Data Lake
Bronze Layer]
    C --> D[Databricks / Spark
Processamento]
    D --> E[S3 Data Lake
Silver Layer]
    E --> F[dbt
Transformações Analíticas]
    F --> G[Snowflake
Camada Gold]
    G --> H[Dashboards
Power BI / Tableau]
```

------------------------------------------------------------------------

# 🧩 Componentes do Projeto

## ✔️ **Ingestão de Dados (Airflow)**

Fontes integradas: - API (JSON)\
- Arquivos CSV\
- Banco Relacional (PostgreSQL)\
- Banco Não-Relacional (MongoDB)

Pipelines batch e streaming salvam dados brutos no Data Lake.

------------------------------------------------------------------------

## ✔️ **Armazenamento -- Data Lake (S3)**

Estruturado pela arquitetura **Medallion**:

### **Bronze**

Dados brutos, sem transformação.

### **Silver**

Limpeza, padronização, deduplicação.

### **Gold**

Modelagem analítica / features de fraude.

------------------------------------------------------------------------

## ✔️ **Processamento -- Databricks / PySpark**

Transformações Bronze → Silver:

-   Normalização de schema\
-   Conversão de tipos\
-   Tratamento de nulos\
-   Deduplicação\
-   Enriquecimento\
-   Particionamento

------------------------------------------------------------------------

## ✔️ **Modelagem Analítica -- dbt Cloud**

Silver → Gold:

-   Tabelas fato e dimensão\
-   Features de fraude\
-   Métricas agregadas\
-   Testes de qualidade\
-   Documentação automática

------------------------------------------------------------------------

## ✔️ **Data Warehouse -- Snowflake**

Armazena:

-   `dim_customer`\
-   `dim_device`\
-   `fact_transactions`\
-   `fact_fraud_features`\
-   Métricas para dashboards

------------------------------------------------------------------------

## ✔️ **Dashboards Analíticos**

KPIs de fraude:

-   \% transações suspeitas\
-   Score de risco\
-   Anomalias por geolocalização\
-   Volume por canal\
-   Tendência temporal de ocorrências

Criados em Power BI ou Tableau.

------------------------------------------------------------------------

# 🔄 Fluxo End-to-End

``` mermaid
sequenceDiagram
    participant S as Fontes
    participant A as Airflow
    participant B as S3 Bronze
    participant C as Databricks
    participant D as S3 Silver
    participant E as dbt
    participant F as Snowflake
    participant G as Dashboard

    S->>A: Ingestão API/CSV/Postgres/MongoDB
    A->>B: Salva dados brutos (Bronze)
    A->>C: Trigger job Spark
    C->>D: Salva dados tratados (Silver)
    A->>E: Executa dbt run + dbt test
    E->>F: Publicação camada Gold
    F->>G: Dashboard lê métricas
```

------------------------------------------------------------------------

# 📁 Estrutura do Repositório

``` bash
fraud-detection-data-platform/
│
├── README.md
│
├── architecture/
│   ├── diagrams/
│   └── documentation.md
│
├── airflow/
│   └── dags/
│       ├── ingest_api.py
│       ├── ingest_csv.py
│       ├── ingest_postgres.py
│       ├── ingest_mongodb.py
│       ├── process_databricks.py
│       └── run_dbt.py
│
├── databricks/
│   ├── notebooks/
│   ├── pyspark_jobs/
│   └── tests/
│
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   └── dbt_project.yml
│
├── sql/
│   ├── snowflake/
│   └── postgres/
│
├── dashboard/
│   └── documentation.md
│
└── docs/
    ├── setup_guide.md
    ├── architecture_explanation.md
    └── interview_talking_points.md
```

------------------------------------------------------------------------

# 🚀 Como Executar o Projeto

## 1. Preparar Infra AWS

-   Criar buckets:
    -   `fraud-detection-lake`
    -   `apache-airflow-slax-bucket`
-   Criar permissões IAM
-   Criar segredos no Secrets Manager

## 2. Airflow

-   Subir as DAGs para o bucket configurado\
-   Ativar ingestões

## 3. Databricks

-   Importar notebooks\
-   Configurar cluster\
-   Criar Jobs Bronze → Silver

## 4. dbt Cloud

-   Conectar ao Snowflake\
-   Executar:

``` bash
dbt run
dbt test
```

## 5. Dashboards

-   Conectar ao Snowflake\
-   Criar KPIs de fraude

------------------------------------------------------------------------

# 💡 Pontos importantes para entrevistas

-   Arquitetura Lakehouse\
-   Medallion Architecture\
-   Pipelines batch + streaming\
-   Databricks + PySpark\
-   dbt para governança\
-   Uso de Snowflake\
-   Airflow como orquestrador\
-   Segurança com Secrets Manager

------------------------------------------------------------------------

# 🧭 Melhorias Futuras

-   Implementar Great Expectations\
-   Criar Feature Store\
-   Implementar CDC (Debezium)\
-   Criar testes automatizados de Spark\
-   Adicionar modelo ML de detecção de fraude

------------------------------------------------------------------------

# 🙌 Contato

Este repositório foi criado como parte do meu portfólio profissional
para demonstrar habilidades de Engenharia de Dados em um projeto
end-to-end.
