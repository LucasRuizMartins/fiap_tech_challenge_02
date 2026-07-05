teach_challenge_02
==============================

segundo projeto de datascience da pós tech fiap

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├──├── raw            <- The original, immutable data dump.
    │   ├──├── Bronze     <-  Arquivos processados
    │   ├──├── Prata      <-  Arquivos processados
    │   └──└── Ouro       <-  Arquivos processados
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   │
    │   ├── streaming      <- Kafka Producer/Consumer scripts for real-time ingestion
    │   │   ├── producer.py        <- CLI script para envio local de eventos ao Kafka
    │   │   ├── consumer.py        <- Consumer legado (alternativa sem Lambda)
    │   │   └── lambda_function.py <- Consumer Serverless (AWS Lambda)
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

## Fluxo da Arquitetura de Dados

O projeto implementa uma arquitetura **Medallion + Serverless** organizada em dois fluxos:

### Ingestão em Streaming: Bronze ➔ Silver (AWS Lambda + Kafka)

```text
[ Computador Local ]                        [ AWS Cloud - VPC: fiap-msk-vpc ]

  producer.py          Porta 9092             subnet-1 (Pública)
     │           (Internet Gateway)          ┌─────────────────────────────┐
     └──────────────────────────────────────►│  EC2 t3.small (Docker)      │
                                             │  Apache Kafka 3.7.0         │
                                             │  Porta 9092: Listener Pub.  │
                                             │  Porta 9094: Listener Priv. │
                                             └───────────┬─────────────────┘
                                                         │ Porta 9094
                                              subnet-2 (Privada)
                                             ┌─────────────────────────────┐
                                             │  AWS Lambda                 │
                                             │  fiap-kafka-s3-consumer     │◄── Gatilho Kafka
                                             │  Runtime: Python 3.12       │
                                             │  Layer: AWSSDKPandas        │
                                             └───────────┬────────┬────────┘
                                                         │        │
                                             NAT Gateway │        │ boto3
                                             (saída para │        ▼
                                              AWS APIs)  │  Amazon S3
                                                         │  prata/ano=YYYY/
                                                         │  (Silver - Parquet)
                                                         ▼
                                             AWS STS / Lambda API
```

### Ingestão em Batch: Silver ➔ Gold

`Amazon S3 (Bronze) ──► AWS Glue Job (Spark Batch) ──► Amazon S3 (Silver/Gold)`

---

## Padrão de Processamento: Stream-Static Join

O pipeline implementa o padrão arquitetural **Stream-Static Join**, onde cada tabela é processada de acordo com a sua natureza de atualização:

| Tabela | Modo | Motivo |
|:---|:---:|:---|
| `TS_ESTADO` | **Batch (Estático)** | Tabela de referência com médias estaduais por tipo de rede. Publicada 1x por ano pelo INEP. Carregada uma única vez na memória no início do Consumer. |
| `TS_MUNICIPIO` | **Batch (Estático)** | Tabela de referência com médias municipais por tipo de rede. Mesmo ciclo anual do INEP. Carregada uma única vez na memória no início do Consumer. |
| `TS_ALUNO` | **Streaming (Dinâmico)** | Cada linha representa o resultado individual de um aluno. Processada evento a evento via Kafka, simulando o recebimento em tempo real das notas conforme as provas são finalizadas. |

```text
                    ┌── TS_MUNICIPIO (Batch) ──┐
                    │                          ▼
TS_ALUNO ──► Kafka ──► Consumer ──► Stream-Static Join ──► Silver (Parquet)
                    │                          ▲
                    └──── TS_ESTADO (Batch) ───┘
```

> As tabelas estáticas (`TS_ESTADO` e `TS_MUNICIPIO`) são carregadas em **cache global de memória** fora da função `lambda_handler` durante o *cold start* da Lambda. Isso significa que o download do S3 ocorre apenas uma vez por instância da Lambda, e os lotes de eventos de alunos chegando via gatilho do Kafka são enriquecidos instantaneamente contra essas dimensões em cache.

---

## Documentação do Módulo de Utilitários (`src/data/utils.py`)

O arquivo utils.py  centraliza todas as funções utilitárias do pipeline de ingestão e leitura da base de dados do INEP.

### Funções Principais:

*   **`gerar_df_dic(ano, nome_tabela)`**: Carrega o DataFrame de dados bruto (`.csv`) e seu respectivo dicionário (`.xlsx`) com base no ano e na tabela requisitada.
    *   *Diferencial:* Possui um mapeamento inteligente de colunas (`'Variável'`, `'Nome da Variável'`) para lidar de forma transparente com as mudanças de nomenclatura do INEP entre 2023, 2024 e 2025.
    *   *Garantia:* Trata o tipo das colunas do dicionário convertendo-as para `str` para evitar falhas do motor `pyarrow` ao salvar Parquet devido a colunas de tipos mistos.
*   **`carregar_parquet_local(ano, nome_tabela, ler_dicionario=False)`**: Lê arquivos Parquet diretamente da camada `bronze` local baseando-se no ano e tabela.
*   **`converter_para_parquet_bytes(df, index=True)`**: Converte o DataFrame para bytes no formato Parquet em memória (utilizando `BytesIO`), o que permite enviar os dados diretamente para o Amazon S3 sem a necessidade de gravação intermediária no disco local.
*   **`salvar_parquet_local(df, caminho_destino, index=True)`**: Grava um DataFrame em formato Parquet localmente, garantindo que qualquer subdiretório inexistente na estrutura (ex: `data/bronze/ano=2023/`) seja criado de forma recursiva.
*   **`salvar_parquet_s3(s3_client, bucket, chave_s3, parquet_bytes)`**: Realiza o upload direto dos bytes de Parquet para o S3 no caminho lógico fornecido.

---

## Documentação dos Notebooks (`notebooks/`)

Os notebooks estão organizados seguindo as etapas do ciclo de engenharia e análise exploratória de dados:

### 1. pipeline_s3
Este notebook realiza a ingestão e estruturação inicial na camada **Bronze**:
*   Carrega as credenciais da AWS configuradas no arquivo `.env` de forma segura.
*   Lê os arquivos brutos baixados localmente na pasta `data/raw/` (anos 2023, 2024 e 2025).
*   Processa as tabelas em lotes (`TS_ALUNO`, `TS_ITEM`, `TS_ESTADO`, `TS_MUNICIPIO`) e cria cópias locais no formato Parquet.
*   Utiliza a função `converter_para_parquet_bytes` para carregar e enviar esses dados diretamente para a estrutura de partições virtuais do **Amazon S3** (`bronze/ano=YYYY/...`).

### 2. EDA.ipynb
Notebook dedicado à **Análise Exploratória de Dados (EDA)** das bases:
*   **Análise de Dados Faltantes:** Estuda a relação de dados nulos nas tabelas de alunos. Mapeia comportamentos como a proporção de alunos ausentes/provas anuladas contra o total de nulos na nota de proficiência.
*   **Mapeamento Metodológico:** Analisa a estrutura de testes baseada no Design de Blocos Incompletos (BIB), onde a presença de valores nulos nos blocos de resposta está associada à atribuição de cadernos de prova específicos recebidos por cada aluno.
*   **Estatística de Itens (TRI):** Analisa parâmetros psicométricos de itens dicotômicos e politômicos da Teoria de Resposta ao Item.

### 3. pipeline_bronze_silver_kafka
Este notebook realiza a modelagem inicial e o teste de streaming híbrido local/nuvem utilizando o Apache Kafka:
*   **Limpeza e Recriação:** Conecta via AdminClient para zerar e configurar tópicos com partições na nuvem.
*   **Prototipagem:** Roda os loops experimentais de envio e consumo para testar a comunicação de rede local com a AWS.

---

## Documentação dos Scripts de Ingestão Contínua (`src/streaming/`)

Estes scripts foram criados para automatizar a produção e o consumo contínuo:

*   **[`producer.py`](file:///c:/Users/deth_/Carmel%20Capital/TECNOLOGIA%20-%20Geral/LUCAS/Estudos/FIAP/POSTECH_AI_SCIENTIST/tech_challenge_02/src/streaming/producer.py)**: Utilitário CLI para simular o streaming de eventos **a partir do computador local** direto para o Kafka na EC2 (usando o IP Público via porta 9092). Aceita parâmetros dinâmicos:
    ```bash
    python -m src.streaming.producer [ano] [limite_registros]
    # Exemplo: python -m src.streaming.producer 2024 10000
    ```

*   **[`lambda_function.py`](file:///c:/Users/deth_/Carmel%20Capital/TECNOLOGIA%20-%20Geral/LUCAS/Estudos/FIAP/POSTECH_AI_SCIENTIST/tech_challenge_02/src/streaming/lambda_function.py)**: Consumer Serverless de alto desempenho rodando no **AWS Lambda**, ativado automaticamente pelo gatilho nativo do Apache Kafka Self-Managed da AWS.
    *   *Cache de Cold Start:* Armazena as dimensões estáticas em variáveis globais para que o download do S3 ocorra apenas na primeira inicialização da instância Lambda.
    *   *Particionamento Automático:* Grava arquivos Parquet enriquecidos na Silver em `prata/ano=YYYY/arquivo.parquet` por lote processado.
    *   *Decode automático:* Decodifica o payload Base64 nativo do formato de eventos do gatilho Kafka da AWS.
    *   *IAM Roles necessárias:* `AmazonS3FullAccess` + `AWSLambdaVPCAccessExecutionRole` + `AmazonEC2ReadOnlyAccess`.

*   **[`consumer.py`](file:///c:/Users/deth_/Carmel%20Capital/TECNOLOGIA%20-%20Geral/LUCAS/Estudos/FIAP/POSTECH_AI_SCIENTIST/tech_challenge_02/src/streaming/consumer.py)**: Consumer alternativo (legado) projetado para rodar 24/7 diretamente na EC2 quando não se deseja usar o Lambda.
    *   Útil como fallback se o NAT Gateway for desativado para economizar custos.
    *   *Diferencial Multi-Ano:* Pré-carrega em cache as dimensões estáticas de todos os anos e particiona automaticamente os lotes no S3 (`prata/ano=YYYY/`).

---

---

## Decisões de Arquitetura & FinOps

Como exigido pelas diretrizes do Tech Challenge da FIAP, justificamos as escolhas arquiteturais com base em restrições reais de orçamento, escalabilidade e performance:

### 1. Kafka no EC2 (Docker) vs Amazon MSK
Optamos por rodar o Apache Kafka em contêineres Docker em uma instância **EC2 (`t3.small`)** em vez do Amazon MSK:
*   **FinOps:** O MSK custa $100-$200/mês mesmo sem uso. A EC2 `t3.small` custa ~U$ 15/mês e pode ser pausada nos períodos ociosos.
*   **Portabilidade:** O container Docker roda identicamente no ambiente local e na nuvem.

### 2. Consumer Serverless (AWS Lambda) vs Consumer em EC2
O `lambda_function.py` substitui o `consumer.py` rodando na própria EC2:
*   **Escalabilidade:** A Lambda escala automaticamente com o volume de mensagens, sem necessidade de gerenciar processos, reinicializações ou daemons.
*   **Custo:** A Lambda cobra apenas pelo tempo de execução real (milissegundos). Com o volume do projeto, o custo mensal é praticamente zero.
*   **Custo Adicional (Contrapartida):** Requer um **NAT Gateway** (~U$ 30/mês) para que a Lambda, dentro da VPC privada, consiga se autenticar nas APIs da AWS (STS, S3). Para contornar esse custo, pode-se criar/destruir o NAT Gateway via IaC (`terraform apply` / `terraform destroy`) apenas nos períodos de uso ativo.

### 3. Dual-Listener no Kafka (Portas 9092 e 9094)
O Kafka na EC2 está configurado com dois listeners distintos:
*   **Porta 9092 (`PLAINTEXT`):** Aberta ao IP Público da EC2. Usada pelo `producer.py` local para enviar mensagens da internet.
*   **Porta 9094 (`INTERNAL`):** Acessível apenas dentro da VPC privada da AWS. Usada pela Lambda para consumir mensagens de forma segura sem exposição pública.

### 4. Isolamento de Sub-redes (subnet-1 Pública / subnet-2 Privada)
*   **`fiap-msk-subnet-1` (10.0.1.0/24):** Sub-rede pública onde a EC2 e o NAT Gateway residem. Rota para o Internet Gateway.
*   **`fiap-msk-subnet-2` (10.0.2.0/24):** Sub-rede privada exclusiva da Lambda. Rota para o NAT Gateway (que provê saída para as APIs da AWS sem expor a Lambda publicamente).

### 5. Particionamento Virtual por Ano (`ano=YYYY/`) no S3
Toda a estrutura do S3 (Bronze, Silver, Gold) usa particionamento Hive: `prata/ano=2023/`, `prata/ano=2024/`.
*   **FinOps:** Habilita *partition pruning* no Athena e no Glue, reduzindo o volume de dados lidos em queries em mais de 60%, cortando o custo de consultas pela metade.

---

[Link oficial para a Base de dados (INEP)](https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao/resultados)