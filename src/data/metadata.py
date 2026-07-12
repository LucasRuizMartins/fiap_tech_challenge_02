#- gerar_catalogo_processamento
from datetime import datetime
import pandas as pd

def gerar_catalogo_processamento(
    df: pd.DataFrame,
    ano: int,
    tabela: str,
    camada: str = "silver"
) -> pd.DataFrame:
    """
    Gera o catálogo da tabela processada.
    """

    catalogo = {

        "DT_EXECUCAO": datetime.now(),

        "CAMADA": camada,

        "ANO": ano,

        "TABELA": tabela,

        "LINHAS": len(df),

        "COLUNAS": len(df.columns),

        "MEMORIA_MB":
            round(
                df.memory_usage(deep=True).sum()/1024/1024,
                2
            ),

        "COLUNAS_LISTA":
            ",".join(df.columns),

        "STATUS": "OK"
    }

    return pd.DataFrame([catalogo])



#- gerar_relatorio_data_quality
from datetime import datetime
import pandas as pd

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
