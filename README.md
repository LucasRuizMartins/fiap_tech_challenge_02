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
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

## Fluxo da Arquitetura de Dados
```text
Script Python (Producer) ──> Amazon MSK (Kafka) ──> AWS Lambda (Consumer) ──> Amazon S3 (Bronze)
Amazon S3 (Bronze) ──> Amazon Glue (Spark/Transformador) ──> Amazon S3 (Silver/Gold)
```

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

### 3. pipeline_bronze_silver
Este notebook cuida da transição dos dados da camada **Bronze** para a **Prata**, realizando operações de:
*   Tratamentos e padronizações adicionais dos dados.
*   Limpezas e junções preliminares baseadas nas chaves relacionais identificadas.

---

[Link oficial para a Base de dados (INEP)](https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao/resultados)