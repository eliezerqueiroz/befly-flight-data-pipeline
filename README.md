# BeFly Data Engineering Challenge - Flight Data Pipeline

## Visão Geral
Este projeto implementa um pipeline de dados seguindo a arquitetura **Medalhão (Bronze, Silver, Gold)** utilizando **PySpark**. O objetivo é processar dados históricos de voos para gerar insights sobre pontualidade e cancelamentos.

O projeto foi desenvolvido com foco em:
- **Performance:** Uso de formato Parquet e avaliação preguiçosa (Lazy Evaluation).
- **Modularidade:** Código organizado em funções e utilitários reutilizáveis.
- **Robustez:** Tratamento de tipos, nulos e logs detalhados.
- **Otimização Local:** Configurações específicas para execução eficiente em ambiente local (ex: Apple M1).

---

## Arquitetura do Pipeline

O fluxo de dados segue a estrutura de Data Lake:

1.  **Ingestão (Bronze):**
    - Leitura dos CSVs brutos (`airlines`, `airports`, `flights`).
    - Aplicação de filtro de particionamento (Mês = 1) para otimizar processamento.
    - Gravação em formato **Parquet** (compressão Snappy).

2.  **Transformação (Silver):**
    - Limpeza de dados e *Type Casting* (garantia de schema).
    - Criação de colunas derivadas (`FLIGHT_DATE`, `FLIGHT_STATUS`).
    - Enriquecimento via **Joins** com tabelas de dimensão (Companhias e Aeroportos).
    - Tratamento de ambiguidade em colunas de Origem/Destino.

3.  **Agregação (Gold):**
    - Geração de métricas de negócio (KPIs).
    - Tabelas finais: `airline_performance` (Pontualidade por cia) e `daily_summary` (Visão temporal).

---

## Estrutura do Projeto

```text
befly-flight-data-pipeline/
├── data/
│   ├── raw/          # CSVs originais (Ignorados no Git)
│   ├── bronze/       # Dados brutos em Parquet
│   ├── silver/       # Dados limpos e enriquecidos
│   └── gold/         # Agregações finais
├── notebooks/
│   ├── exploracao_silver.ipynb   # Validação de schema e joins
│   └── exploracao_gold.ipynb     # Validação de regras de negócio
├── src/
│   ├── jobs/
│   │   ├── ingest_bronze.py      # Script de ingestão
│   │   ├── transform_silver.py   # Script de transformação
│   │   └── aggregate_gold.py     # Script de agregação
│   └── utils.py                  # Configurações compartilhadas (SparkSession)
├── requirements.txt
└── README.md