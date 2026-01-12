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

1. **Ingestão (Bronze):**
    - Leitura dos CSVs brutos (`airlines`, `airports`, `flights`).
    - Aplicação de filtro de particionamento (Mês = 1) para otimizar processamento.
    - Gravação em formato **Parquet** (compressão Snappy).

2. **Transformação (Silver):**
    - Limpeza de dados e *Type Casting* (garantia de schema).
    - Criação de colunas derivadas (`FLIGHT_DATE`, `FLIGHT_STATUS`).
    - Enriquecimento via **Joins** com tabelas de dimensão (Companhias e Aeroportos).
    - Tratamento de ambiguidade em colunas de Origem/Destino.

3. **Agregação (Gold):**
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
```

---

## Como Executar

### Pré-requisitos

- Python 3.8+
- Java 8 ou 11 (Necessário para o Spark)

### 1. Configuração do Ambiente

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd befly-flight-data-pipeline

# Crie e ative o ambiente virtual
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Instale as dependências
pip install -r requirements.txt
```

### 2. Obtenção dos Dados

Devido ao tamanho, os dados brutos não estão no repositório.

1. Baixe o dataset "2015 Flight Delays and Cancellations" no Kaggle.
2. Extraia os arquivos `flights.csv`, `airlines.csv` e `airports.csv`.
3. Mova-os para a pasta: `data/raw/`.

### 3. Execução do Pipeline

Existem duas formas de executar o projeto:

#### Opção A: Orquestrador Automatizado (Recomendado)

Para simular um ambiente produtivo, foi desenvolvido um script Shell que gerencia a dependência entre as camadas (Bronze → Silver → Gold) e interrompe o fluxo imediatamente em caso de erro (**Fail Fast**).

1. Dê permissão de execução ao script (apenas na primeira vez):

   ```bash
   chmod +x run_pipeline.sh
   ```

2. Execute o pipeline completo:

   ```bash
   ./run_pipeline.sh
   ```

#### Opção B: Execução Manual (Passo a Passo)

Caso queira rodar ou debugar uma etapa específica isoladamente:

```bash
# 1. Ingestão Bronze
python src/jobs/ingest_bronze.py

# 2. Transformação Silver
python src/jobs/transform_silver.py

# 3. Agregação Gold
python src/jobs/aggregate_gold.py
```

---

## Decisões Técnicas & Otimizações

- **Leitura Otimizada:** O filtro `MONTH=1` é aplicado imediatamente após a leitura do CSV, reduzindo o volume de dados em memória antes de qualquer transformação pesada.
- **Gerenciamento de Memória:** A `SparkSession` foi configurada via `src/utils.py` com limite de 2GB e 4 partições para evitar *Out of Memory* em ambientes de desenvolvimento (ex: Laptops com 8GB RAM).
- **Logs:** Utilização da biblioteca `logging` em vez de `print` para garantir observabilidade profissional.
- **Orquestração Local:** Implementação de um script Shell (`run_pipeline.sh`) que atua como um orquestrador linear simples. Ele garante a integridade do processo, impedindo que a camada Silver rode se a Bronze falhar, simulando o comportamento de dependência de tarefas de ferramentas como Apache Airflow.

---

## Arquitetura em Nuvem (AWS)

Para adaptar este pipeline local para um ambiente produtivo na AWS, a arquitetura seria modernizada da seguinte forma:

1. **Storage (Data Lake):**
    - Substituição do sistema de arquivos local pelo **Amazon S3**.
    - Buckets separados para camadas: `s3://befly-datalake-bronze`, `s3://...-silver`, `s3://...-gold`.

2. **Processamento (Compute):**
    - Utilização do **AWS Glue** (Serverless Spark) para executar os jobs Python. O Glue gerencia a escala automática dos workers, ideal para processar o dataset completo (5M+ linhas) sem preocupação com infraestrutura.
    - Alternativamente, **EMR** poderia ser usado para cargas de trabalho contínuas e muito pesadas.

3. **Catálogo e Consumo:**
    - **AWS Glue Crawler** para catalogar automaticamente os dados gerados na camada Gold.
    - **Amazon Athena** para permitir que analistas façam consultas SQL diretamente nos arquivos Parquet no S3.

4. **Orquestração:**
    - **Apache Airflow (MWAA)** para gerenciar a dependência entre as tarefas (só rodar a Silver se a Bronze tiver sucesso) e agendamento diário.

Essa arquitetura garante escalabilidade horizontal, baixo custo de armazenamento (S3) e separação entre computação e storage.

---

## 🚀 Roadmap e Melhorias Futuras

Este projeto foi desenhado como um MVP para demonstrar competências em PySpark e Arquitetura de Dados. Em um cenário produtivo real, as seguintes evoluções seriam priorizadas:

- [ ] **Qualidade de Dados (Data Quality):**
    - Integração com **Great Expectations** ou **Soda** para validação robusta de contratos de dados (ex: garantir que não existam códigos de aeroporto inválidos ou datas futuras) antes da promoção para a camada Silver.

- [ ] **Testes Automatizados:**
    - Implementação de testes unitários com **PyTest** para validar a lógica de funções isoladas (ex: cálculo de atrasos) e *mocks* de DataFrames para testes de integração sem dependência de dados externos.

- [ ] **Modernização do Formato de Armazenamento:**
    - Migração de Parquet para **Delta Lake**. Isso habilitaria:
        - Transações ACID (garantia de integridade em escritas concorrentes).
        - Suporte a operações de `MERGE` (Upserts) para processar apenas dados novos (Incremental Load) em vez de reprocessamento total.
        - *Time Travel* para auditoria e rollback de dados.

- [ ] **Containerização:**
    - Criação de um `Dockerfile` para encapsular o ambiente (Java + Spark + Python), garantindo reprodutibilidade exata em qualquer sistema operacional e facilitando o deploy em Kubernetes ou AWS ECS.

- [ ] **CI/CD:**
    - Configuração de pipeline no **GitHub Actions** para execução automática de linter (`ruff`/`black`), testes unitários e deploy de infraestrutura (Terraform) a cada Pull Request.