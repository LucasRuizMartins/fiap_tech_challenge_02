from datetime import datetime
import pandas as pd
import numpy as np

# -----------------------------------------------
# Colunas que devem ser convertidas para inteiro
# -----------------------------------------------
COLUNAS_INT = [
    "NU_ANO_AVALIACAO",
    "CO_UF",
    "ID_ALUNO",
    "TP_SERIE",
    "ID_ESCOLA",
    "TP_DEPENDENCIA",
    "CO_MUNICIPIO",
    "IN_PRESENCA_LP",
    "IN_PREENCHIMENTO_LP",
    "CO_CADERNO_LP",
    "IN_ALFABETIZADO",
    "ID_TIPO_REDE",
    "ANO",
    "CO_BLOCO",
    "NU_POSICAO",
    "CO_ITEM",
    "TP_DISCIPLINA",
    "TP_RESPOSTA_ITEM",
    "TP_MODELO_TRI",
    "NU_PARAM_A",
    "NU_PARAM_B",
    "NU_PARAM_C",
    "NU_PARAM_B1",
    "NU_PARAM_B2",
    "NU_PARAM_B3",
    "NU_PARAM_B4",
    "IN_ITEM_COMUM",
    "CO_BLOCO_1",
    "CO_BLOCO_2",
    "CO_BLOCO_3",
    "CO_BLOCO_4"
]



# -----------------------------------------------
# Funções de Data Quality
# -----------------------------------------------

def remover_linhas_vazias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas totalmente vazias.
    """
    return df.dropna(how="all")


def remover_duplicados(
    df: pd.DataFrame,
    chaves: list[str] | None = None
) -> pd.DataFrame:
    """
    Remove registros duplicados.

    Se 'chaves' for informado, considera apenas essas colunas.
    Caso contrário, compara a linha inteira.
    """
    if chaves:
        return df.drop_duplicates(subset=chaves)

    return df.drop_duplicates()


def tratar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza valores nulos.
    """

    valores_nulos = [
        "",
        " ",
        "  ",
        "NULL",
        "null",
        "NaN",
        "nan",
        "N/A",
        "-",
    ]

    return df.replace(valores_nulos, pd.NA)


def padronizar_textos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços e padroniza colunas texto.
    """

    colunas_texto = df.select_dtypes(include="object").columns

    for coluna in colunas_texto:

        df[coluna] = (
            df[coluna]
            .astype("string")
            .str.strip()
        )

    return df


def converter_colunas_int(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    """
    Converte as colunas informadas para inteiro (Int64),
    ignorando colunas inexistentes.
    """

    for coluna in colunas:

        if coluna in df.columns:

            df[coluna] = (
                pd.to_numeric(
                    df[coluna],
                    errors="coerce"
                )
                .astype("Int64")
            )

    return df

# -----------------------------------------------
# Função principal de Data Quality
# -----------------------------------------------   

def aplicar_data_quality(
    df: pd.DataFrame,
    dicionario: pd.DataFrame,
    chaves: list[str] | None = None
) -> pd.DataFrame:
    """
    Executa todas as regras de qualidade.
    """
    print(f"Iniciando Data Quality para {len(df):,} linhas e {len(df.columns):,} colunas.")

    df = remover_linhas_vazias(df)

    print(f"Após remover vazias : {len(df):,}")

    df = remover_duplicados(df, chaves)

    print(f"Após remover duplicados : {len(df):,}")

    df = tratar_nulos(df)

    print(f"Nulos tratados : {df.isna().sum().sum()}")

    df = padronizar_textos(df)

    print(f"Textos padronizados : {df.select_dtypes(include='object').columns.size}")

    df = converter_colunas_int(df, COLUNAS_INT)

    print(f"Colunas convertidas : {df.select_dtypes(include='Int64').columns.size}")

    print(f"Data Quality concluída.")

    return df


#   -----------------------------------------------
#   Relatório de Data Quality
#   -----------------------------------------------

def gerar_relatorio_data_quality(
    bronze: pd.DataFrame,
    silver: pd.DataFrame,
    ano: int,
    tabela: str,
    camada: str = "silver"
) -> pd.DataFrame:
    """
    Gera um relatório de Data Quality da execução.
    """

    relatorio = {

        "DT_EXECUCAO": datetime.now(),

        "CAMADA": camada,

        "ANO": ano,

        "TABELA": tabela,

        "LINHAS_BRONZE": len(bronze),

        "LINHAS_SILVER": len(silver),

        "COLUNAS": len(silver.columns),

        "DUPLICADOS_BRONZE": int(bronze.duplicated().sum()),

        "DUPLICADOS_SILVER": int(silver.duplicated().sum()),

        "LINHAS_VAZIAS_BRONZE": int(
            bronze.isna().all(axis=1).sum()
        ),

        "LINHAS_VAZIAS_SILVER": int(
            silver.isna().all(axis=1).sum()
        ),

        "NULOS_BRONZE": int(
            bronze.isna().sum().sum()
        ),

        "NULOS_SILVER": int(
            silver.isna().sum().sum()
        ),

        "STATUS": "OK"
    }

    return pd.DataFrame([relatorio])
