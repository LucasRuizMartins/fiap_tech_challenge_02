from datetime import datetime
import pandas as pd

COLUNAS_INT = [
    "NU_ANO_AVALIACAO",
    "CO_UF",
    "ID_ALUNO",
    "TP_SERIE",
    "ID_ESCOLA",
    "TP_DEPENDENCIA",
    "CO_MUNICIPIO",
    "IN_PRESENCA_LP",
    "ID_TIPO_REDE",
    "ANO",
    "CO_BLOCO",
    "NU_POSICAO",
    "CO_ITEM",
    "TP_SERIE",
    "TP_DISCIPLINA"

]


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
import pandas as pd
import numpy as np


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


def converter_tipos(
    df: pd.DataFrame,
    dicionario: pd.DataFrame
) -> pd.DataFrame:
    """
    Converte tipos automaticamente.

    Atualmente converte apenas números.
    Pode ser expandido futuramente usando o dicionário.
    """

    for coluna in df.columns:

        # ignora colunas inexistentes no dicionário
        if coluna not in dicionario.index:
            continue

        serie = df[coluna]

        # tenta converter para inteiro
        convertido = pd.to_numeric(
            serie,
            errors="ignore"
        )

        df[coluna] = convertido

    return df


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

 #   df = converter_tipos(df, dicionario)

 #   print(f"Tipos convertidos : {df.select_dtypes(include=np.number).columns.size}")

    print(f"Data Quality concluída.")

    return df