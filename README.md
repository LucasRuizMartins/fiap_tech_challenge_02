# tech_challenge_02
==============================

Segundo projeto de Data Science da Pós Tech FIAP — Pipeline de Dados com Arquitetura Medallion + Streaming via Kafka e AWS Lambda.

## Organização do Projeto

```
    ├── LICENSE
    ├── Makefile               <- Comandos úteis: `make data`, `make train`, etc.
    ├── README.md              <- Documentação principal do projeto.
    ├── data
    │   ├── raw                <- Dados brutos originais (imutáveis) baixados do INEP.
    │   ├── bronze             <- Dados convertidos para Parquet (estrutura: ano=YYYY/dados/ e ano=YYYY/dicionario/)
    │   ├── prata              <- Dados enriquecidos da camada Silver (Stream-Static Join)
    │   └── ouro               <- Dados agregados da camada Gold (a construir)
    │
    ├── docs                   <- Documentação adicional do projeto.
    ├── models                 <- Modelos treinados e serializados.
    ├── notebooks              <- Jupyter Notebooks organizados por etapa do pipeline.
    ├── references             <- Dicionários de dados e materiais de referência do INEP.
    ├── reports                <- Análises exportadas (HTML, PDF, etc.)
    │   └── figures            <- Gráficos e figuras gerados para relatórios.
    ├── requirements.txt       <- Dependências do projeto (pip freeze > requirements.txt)
    ├── setup.py               <- Torna o pacote `src` instalável via `pip install -e .`
    ├── src                    <- Código-fonte do projeto.
    │   ├── __init__.py        <- Torna `src` um módulo Python.
    │   ├── data
    │   │   ├── make_dataset.py
    │   │   └── utils.py       <- Funções utilitárias centrais do pipeline (leitura, escrita, transformação, AWS)
    │   ├── features
    │   │   └── build_features.py
    │   ├── models
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   ├── streaming          <- Scripts de ingestão contínua via Kafka
    │   │   ├── producer.py        <- CLI para envio local de eventos ao Kafka
    │   │   └── lambda_function.py <- Consumer Serverless (AWS Lambda)
    │   └── visualization
    │       └── visualize.py
    └── tox.ini                <- Configuração do tox para testes.
```

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

---

## Fluxo da Arquitetura de Dados

O projeto implementa uma arquitetura **Medallion + Serverless** organizada em dois fluxos principais:

### Fluxo 1 — Ingestão em Streaming: Bronze ➔ Silver (Kafka + AWS Lambda)

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

### Fluxo 2 — Ingestão em Batch: Silver ➔ Gold

```
Amazon S3 (Bronze) ──► AWS Glue Job (Spark Batch) ──► Amazon S3 (Silver/Gold)
```

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

> As tabelas estáticas (`TS_ESTADO` e `TS_MUNICIPIO`) são carregadas em **cache global de memória** fora da função `lambda_handler` durante o *cold start* da Lambda. Isso significa que o download do S3 ocorre apenas uma vez por instância, e os lotes de alunos são enriquecidos instantaneamente.

---

## Documentação do Módulo de Utilitários (`src/data/utils.py`)

O arquivo [`utils.py`](src/data/utils.py) centraliza todas as funções utilitárias do pipeline — leitura, transformação, escrita e autenticação AWS.

### Funções de Leitura e Ingestão (Raw → Bronze)

*   **`gerar_df_dic(ano, nome_tabela)`**: Carrega o DataFrame de dados bruto (`.csv`) e seu respectivo dicionário (`.xlsx`) com base no ano e na tabela requisitada.
    *   *Diferencial:* Mapeamento inteligente de colunas (`'Variável'`, `'Variavel'`, `'Nome da Variável'`, `'Nome da Variavel'`) para lidar com mudanças de nomenclatura do INEP entre 2023, 2024 e 2025.
    *   *Garantia:* Converte todas as colunas do dicionário para `str` para evitar falhas do motor `pyarrow` por tipos mistos ao salvar Parquet.

*   **`carregar_parquet_local(ano, nome_tabela, ler_dicionario=False)`**: Lê arquivos Parquet da camada `bronze` local a partir do ano e tabela. Suporta leitura de dados (`dados/`) ou dicionários (`dicionario/`).

*   **`converter_para_parquet_bytes(df, index=True)`**: Converte um DataFrame para bytes no formato Parquet em memória (`BytesIO`), permitindo upload direto para o S3 sem gravação intermediária em disco.

*   **`salvar_parquet_local(df, caminho_destino, index=True)`**: Grava um DataFrame em Parquet localmente. Cria automaticamente qualquer subdiretório inexistente (ex: `data/bronze/ano=2023/dados/`).

*   **`salvar_parquet_s3(s3_client, bucket, chave_s3, parquet_bytes)`**: Realiza upload direto de bytes Parquet para o S3 no caminho lógico fornecido.

### Funções de Transformação (Bronze → Silver)

*   **`preparar_dimensoes_silver(ano, path_bronze)`**: Carrega `TS_MUNICIPIO` e `TS_ESTADO` da Bronze, realiza o pivot por `ID_TIPO_REDE` (FEDERAL, ESTADUAL, MUNICIPAL, PRIVADA, etc.) e retorna dois DataFrames de dimensões normalizados prontos para merge. Aplica fallback automático `TOTAL → PUBLICA_EST_MUN` para municípios sem redes federal ou privada.

*   **`enriquecer_alunos_silver(df_alunos, municipio_dim, uf_dim)`**: Aplica o merge duplo (alunos ← município ← UF), calcula colunas derivadas (`DESVIO_MEDIA_MUNICIPIO`, `DESVIO_MEDIA_UF`) e remove colunas inativas (redes zeradas e Bloco 4 das provas). Compatível tanto com a base completa (batch) quanto com micro-lotes (streaming).

### Funções de Infraestrutura AWS

*   **`iniciar_cessao_aws()`**: Cria e retorna uma `boto3.Session` autenticada usando as credenciais do `.env`: `AWS_ACESS_KEY_ID`, `AWS_SECRET_ACESS_KEY` e `AWS_REGION`.

---

## Documentação dos Notebooks (`notebooks/`)

Os notebooks estão organizados sequencialmente pelas etapas do pipeline:

### `00 - EDA.ipynb`
**Análise Exploratória de Dados (EDA)** das bases do INEP:
*   **Dados Faltantes:** Mapeia a proporção de alunos ausentes/provas anuladas vs. nulos na nota de proficiência.
*   **Design BIB:** Analisa o Design de Blocos Incompletos (Balanced Incomplete Block), onde nulos nos blocos de resposta indicam o caderno de prova atribuído a cada aluno.
*   **TRI:** Analisa parâmetros psicométricos (discriminação, dificuldade, chute) de itens dicotômicos e politômicos da Teoria de Resposta ao Item.

### `01 - pipeline_local.ipynb`
**Pipeline Raw → Bronze 100% local** (sem necessidade de conexão AWS):
*   Lê os arquivos CSV brutos de `data/raw/dados_YYYY/` usando `gerar_df_dic`.
*   Itera sobre os anos [2023, 2024, 2025] e as 4 tabelas (`TS_ALUNO`, `TS_ITEM`, `TS_ESTADO`, `TS_MUNICIPIO`).
*   Salva dados e dicionários em Parquet na estrutura `data/bronze/ano=YYYY/dados/` e `data/bronze/ano=YYYY/dicionario/` via `salvar_parquet_local`.
*   Ponto de entrada para execução local sem acesso à nuvem.

### `01 - pipeline_s3.ipynb`
**Pipeline Raw → Bronze com envio direto ao Amazon S3**:
*   Autentica na AWS via `iniciar_cessao_aws` (credenciais do `.env`).
*   Converte os dados brutos para Parquet em memória via `converter_para_parquet_bytes`.
*   Faz upload direto para `bronze/ano=YYYY/dados/` e `bronze/ano=YYYY/dicionario/` no S3 via `salvar_parquet_s3`, sem gravação intermediária em disco.

### `02 - configurar_aws.ipynb`
**Provisionamento e configuração da infraestrutura AWS** via boto3:
*   Documenta a criação de VPC, sub-redes, Security Groups, instância EC2 e configuração do Kafka via Docker na nuvem.
*   Registra as tentativas com AWS Academy (bloqueado por políticas) e a decisão de migrar para conta pessoal da AWS.
*   Serve como guia de referência para recriar a infraestrutura do zero.

### `02 - pipeline_bronze_silver_local.ipynb`
**Pipeline Bronze → Silver 100% local** com a lógica completa de enriquecimento:
*   Define `processar_camada_silver(ano)` que orquestra toda a transformação:
    *   Carrega `TS_ALUNO`, `TS_MUNICIPIO` e `TS_ESTADO` da Bronze local.
    *   Realiza pivot de `TS_MUNICIPIO` e `TS_ESTADO` por `ID_TIPO_REDE`.
    *   Executa merge duplo (alunos ← município e UF).
    *   Calcula `DESVIO_MEDIA_MUNICIPIO` e `DESVIO_MEDIA_UF`.
    *   Remove colunas inativas (redes zeradas e Bloco 4).
*   Itera sobre os anos [2023, 2024, 2025] e salva em `data/prata/ano=YYYY/alunos_prata.parquet`.
*   **Volume processado confirmado:** 1.747.439 (2023) · 2.120.560 (2024) · 2.222.792 (2025) registros.

### `03 - pipeline_bronze_silver_kafka.ipynb`
**Prototipagem e teste do fluxo de streaming** com Kafka (local via Docker + remoto via EC2):
*   Usa `docker-compose.yml` + WSL para levantar um broker Kafka local para testes de integração.
*   Demonstra que trocar `localhost` pelo IP público da EC2 é a única mudança necessária para apontar ao Kafka na nuvem.
*   Gerencia tópicos via `AdminClient`: deleta e recria `transactions` com 3 partições para garantir estado limpo antes dos testes.
*   Protótipo do loop Producer/Consumer usando `preparar_dimensoes_silver` e `enriquecer_alunos_silver` do `src/data/utils.py`.

### `04 - pipeline_silver_gold.ipynb`
**Leitura e consolidação da camada Silver** a partir do S3:
*   Define `ler_silver_s3(ano)` que autentica via `iniciar_cessao_aws`, lista todos os arquivos Parquet do prefixo `prata/ano=YYYY/` e os concatena em um único DataFrame.
*   Valida o volume final por ano, confirmando a integridade do pipeline de streaming.
*   Ponto de partida para a futura construção da camada Gold com agregações analíticas.

---

## Documentação dos Scripts de Streaming (`src/streaming/`)

### [`producer.py`](src/streaming/producer.py)
Utilitário CLI para simular o streaming de eventos **do computador local** direto ao Kafka na EC2 (porta 9092 pública).

```bash
python -m src.streaming.producer [ano] [limite_registros]

# Exemplos:
python -m src.streaming.producer 2024 10000   # 10k registros do ano 2024
python -m src.streaming.producer 2025          # todos os alunos de 2025
python -m src.streaming.producer               # ano padrão do .env, todos os registros
```

| Parâmetro | Descrição |
|:---|:---|
| `SERVER_KAFKA` | Endereço do broker Kafka (default: `localhost:9092`) |
| `TOPIC_NAME` | Nome do tópico Kafka (default: `transactions`) |
| `ANO_PROCESSAMENTO` | Ano dos microdados (default: `2023`) |

*   **Lê de:** `data/bronze/ano=YYYY/dados/TS_ALUNO.parquet`
*   **Tratamento de Buffer:** Controla `BufferError` (Queue Full) via `producer.poll(0.5)` para evitar perda de mensagens em alta velocidade.
*   **Log de progresso:** Status a cada 10.000 mensagens enviadas.
*   **Serialização:** Cada registro de aluno é convertido para JSON com tratamento de `NaN` e tipos NumPy antes do envio.

### [`lambda_function.py`](src/streaming/lambda_function.py)
Consumer Serverless rodando no **AWS Lambda**, ativado automaticamente pelo gatilho nativo de Kafka Self-Managed da AWS.

*   **Cache de Cold Start (`cache_dimensoes`):** `TS_MUNICIPIO` e `TS_ESTADO` são baixados do S3 e armazenados em variáveis globais. O download ocorre apenas uma vez por instância Lambda, eliminando latência em invocações subsequentes.
*   **Processamento Multi-Ano:** Agrupa o lote recebido por `NU_ANO_AVALIACAO` via `groupby`, carregando referências do ano correto automaticamente.
*   **Decode Base64:** Decodifica o payload nativo do evento Kafka da AWS e valida campos obrigatórios (`NU_ANO_AVALIACAO`, `CO_UF`, `CO_MUNICIPIO`, `TP_SERIE`) antes de processar.
*   **Enriquecimento:** Executa pivot, merge duplo (município + UF) e cálculo de desvios idênticos ao pipeline batch local.
*   **Saída:** Grava `prata/ano=YYYY/lote_alunos_<timestamp>.parquet` no S3 por lote processado.
*   **IAM Roles necessárias:** `AmazonS3FullAccess` + `AWSLambdaVPCAccessExecutionRole` + `AmazonEC2ReadOnlyAccess`
*   **Variável de ambiente Lambda:** `BUCKET_NAME` (default: `fiap-postech-challenge-datascience-002`)

---

## Decisões de Arquitetura & FinOps

### 1. Kafka no EC2 (Docker) vs Amazon MSK
*   **FinOps:** O MSK custa $100–$200/mês mesmo sem uso. A EC2 `t3.small` custa ~$15/mês e pode ser pausada nos períodos ociosos.
*   **Portabilidade:** O container Docker roda identicamente no ambiente local e na nuvem — troca apenas o IP de conexão.

### 2. Consumer Serverless (Lambda) vs Consumer na EC2
*   **Escalabilidade:** Lambda escala automaticamente com o volume de mensagens, sem necessidade de gerenciar daemons ou reinicializações.
*   **Custo:** Cobrança apenas pelo tempo de execução real (milissegundos) — custo mensal praticamente zero no volume do projeto.
*   **Contrapartida:** Requer **NAT Gateway** (~$30/mês) para a Lambda (VPC privada) acessar APIs da AWS. Estratégia de mitigação: criar/destruir o NAT Gateway via IaC (`terraform apply` / `terraform destroy`) apenas durante períodos de uso ativo. <font color='red'>(Estratégia não aplicada no código) </font>

### 3. Dual-Listener no Kafka (Portas 9092 e 9094)
*   **Porta 9092 (`PLAINTEXT`):** Aberta ao IP Público da EC2 — usada pelo `producer.py` local.
*   **Porta 9094 (`INTERNAL`):** Acessível apenas dentro da VPC privada — usada pela Lambda, sem exposição pública.

### 4. Isolamento de Sub-redes
*   **`fiap-msk-subnet-1` (10.0.1.0/24):** Sub-rede pública com EC2 e NAT Gateway. Rota para Internet Gateway.
*   **`fiap-msk-subnet-2` (10.0.2.0/24):** Sub-rede privada exclusiva da Lambda. Rota para o NAT Gateway.

### 5. Particionamento Hive por Ano (`ano=YYYY/`) no S3
Toda a estrutura do S3 usa particionamento Hive: `bronze/ano=2023/`, `prata/ano=2024/`, etc.
*   **FinOps:** Habilita *partition pruning* no Athena e Glue, reduzindo o volume de dados lidos em queries em mais de 60% — cortando o custo de consultas pela metade.

---

[Base de dados oficial — INEP (Avaliação da Alfabetização)](https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao/resultados)