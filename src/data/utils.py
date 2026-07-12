
from pathlib import Path
import pandas as pd



#-----------Leitura CSV
def gerar_df_dic(ano: int | str, nome_tabela: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carrega o dataframe de dados e o respectivo dicionário a partir de um ano e do nome da tabela.
    exemplo de uso:
                    dataframe, dicionario = gerar_df_dic(2023, 'TS_ALUNO')
    """
    # Usando caminhos relativos ao diretório raiz do projeto.
    # Nota: Caso execute o script fora da raiz, usar caminhos absolutos baseados no arquivo atual pode ser mais seguro.
    path_dados = Path(f'../data/raw/dados_{ano}')
    path_alunos = path_dados / 'DADOS' / f'{nome_tabela}.csv'
    
    try:
        raw = pd.read_csv(path_alunos, sep=";", encoding="latin-1")
    except PermissionError as e:
        raise PermissionError(
            f"Erro de permissão ao ler os dados em '{path_alunos}'. "
            f"Certifique-se de que o arquivo não está aberto no Excel ou em outro programa."
        ) from e
    

    #nome alterado para DICIONARIO a base possui acento 
    path_dicionario = path_dados / 'DICIONARIO' / f'Dicionario_Microdados_AEEB_{ano}.xlsx'
    
    print(f"Lendo dicionário em: {path_dicionario}")
    
    try:
        dicionario = pd.read_excel(path_dicionario, sheet_name=nome_tabela)   
    except PermissionError as e:
        raise PermissionError(
            f"Erro de permissão ao ler o dicionário em '{path_dicionario}'. "
            f"Certifique-se de que o arquivo não está aberto no Excel ou em outro programa."
        ) from e
    dicionario.columns = dicionario.iloc[0].to_list()
    dicionario = dicionario[1:]
    

    #Foi necessario criar esse tratamento porque o nome em 2024 era diferente gerando problema na hora de mapear o dicionario pelo index
    possiveis_nomes = ['Variável', 'Variavel', 'Nome da Variável', 'Nome da Variavel']
    coluna_encontrada = next((col for col in dicionario.columns if col in possiveis_nomes), None)
    
    if coluna_encontrada:
        if coluna_encontrada != 'Variável':
            dicionario.rename(columns={coluna_encontrada: 'Variável'}, inplace=True)
        dicionario.set_index('Variável', inplace=True)
    else:
        print(f"Aviso: Nenhuma coluna de variável identificada na tabela {nome_tabela}. Colunas: {list(dicionario.columns)}")
        
    dicionario = dicionario.astype(str)

    return raw, dicionario



#-----PARQUET
from io import BytesIO
"""
A razão para usar BytesIO aqui é que o método to_parquet espera um objeto de arquivo, e o BytesIO fornece uma maneira conveniente de criar um objeto de arquivo temporário em memória.
Em vez de gravar os dados em um arquivo físico no sistema de arquivos local e, em seguida, lê-los de volta para enviá-los para o S3, o BytesIO permite que você escreva os dados diretamente em um buffer de memória.
"""

def carregar_parquet_local(ano: int | str, nome_tabela: str, ler_dicionario: bool = False) -> pd.DataFrame:
    """
    Carrega um arquivo Parquet específico (dados ou dicionário) local a partir do ano e nome da tabela.
    """
    pasta_tipo = "dicionario" if ler_dicionario else "dados"
    nome_arquivo = f"dicionario_{nome_tabela}.parquet" if ler_dicionario else f"{nome_tabela}.parquet"
    
    caminho = Path(f"../data/bronze/ano={ano}/{pasta_tipo}/{nome_arquivo}")
    
    print(f"Lendo Parquet em: {caminho}")
    return pd.read_parquet(caminho)


def converter_para_parquet_bytes(df: pd.DataFrame,index: bool = True) -> bytes:
    """
    Converte um DataFrame para o formato Parquet na memória e retorna seus bytes.
    """
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=index)
    return parquet_buffer.getvalue()


def salvar_parquet_local(df: pd.DataFrame, caminho_destino: Path | str, index: bool = True) -> None:
    """
    Salva um DataFrame localmente no formato Parquet.
    Cria as pastas pai automaticamente caso elas não existam (ex: pasta 'bronze').
    """
    caminho_destino = Path(caminho_destino)
    # Garante que a pasta pai exista antes de salvar (ex: data/bronze/)
    caminho_destino.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(caminho_destino, index=index)  
    #print(f"Arquivo salvo localmente em: {caminho_destino}")


def salvar_parquet_s3(s3_client, bucket: str, chave_s3: str, parquet_bytes: bytes) -> None:
    """
    Envia os bytes de um arquivo Parquet diretamente para um bucket no S3.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=chave_s3,
        Body=parquet_bytes
    )
   # print(f"Arquivo enviado para o S3: s3://{bucket}/{chave_s3}")


#-----------QUALIDADE-------------
import pandera as pa

silver_schema = pa.DataFrameSchema(
    columns={
        "NU_ANO_AVALIACAO": pa.Column(
            "Int64", 
            checks=pa.Check.isin([2023, 2024, 2025]), 
            coerce=True
        ),
        "VL_PROFICIENCIA_LP": pa.Column(
            float, 
            nullable=True, 
            checks=pa.Check.between(0, 1000), 
            coerce=True
        ),
        "CO_UF": pa.Column(
            "Int64",  #Int64 para aceitar os nulos
            coerce=True
        ),
        "DESVIO_MEDIA_MUNICIPIO": pa.Column(
            float, 
            nullable=True, 
            coerce=True
        )
    },
    # Permite que o DataFrame tenha outras colunas 
    # coerce = True transforma os tipos de acordo com o schema inferido
    strict=False 
)





 
#-----------------BRONZE -> SILVER-----------------

def preparar_dimensoes_silver(ano: int | str, path_bronze: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carrega as bases de município e UF para o ano informado, realiza o pivot
    e retorna os DataFrames de dimensões prontos para o merge.
    """
    raw_municipio = pd.read_parquet(path_bronze / f"ano={ano}/dados/TS_MUNICIPIO.parquet")
    raw_uf = pd.read_parquet(path_bronze / f"ano={ano}/dados/TS_ESTADO.parquet")
    
    MAPA_REDES = {
        0: 'TOTAL', 1: 'FEDERAL', 2: 'ESTADUAL', 3: 'MUNICIPAL',
        4: 'PRIVADA', 5: 'PUBLICA_EST_MUN', 6: 'PUBLICA_TOTAL'
    }
    METRICAS = ['VL_MEDIA_LP', 'PC_ALUNO_ALFABETIZADO']
    
    # Preparação do Município
    CHAVES_MUN = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
    municipio_prep = raw_municipio.assign(REDE_NOME=lambda df: df['ID_TIPO_REDE'].map(MAPA_REDES)).query('REDE_NOME.notna()')
    municipio_pivot = municipio_prep.pivot(index=CHAVES_MUN, columns='REDE_NOME', values=METRICAS)
    municipio_pivot.columns = [f"{metrica}_{rede}_MUNICIPIO" for metrica, rede in municipio_pivot.columns]
    municipio_dim = municipio_pivot.reset_index()
    
    # Fallback TOTAL -> PUBLICA_EST_MUN
    REDES_MUN = list(MAPA_REDES.values())
    colunas_esperadas_mun = [f"{metrica}_{rede}_MUNICIPIO" for metrica in METRICAS for rede in REDES_MUN]
    municipio_dim = municipio_dim.reindex(columns=CHAVES_MUN + colunas_esperadas_mun)
    for metrica in METRICAS:
        col_total = f"{metrica}_TOTAL_MUNICIPIO"
        col_publica = f"{metrica}_PUBLICA_EST_MUN_MUNICIPIO"
        municipio_dim[col_total] = municipio_dim[col_total].fillna(municipio_dim[col_publica])

    # Preparação da UF
    CHAVES_UF = ['NU_ANO_AVALIACAO', 'CO_UF', 'TP_SERIE']
    uf_prep = raw_uf.assign(REDE_NOME=lambda df: df['ID_TIPO_REDE'].map(MAPA_REDES)).query('REDE_NOME.notna()')
    uf_pivot = uf_prep.pivot(index=CHAVES_UF, columns='REDE_NOME', values=METRICAS)
    uf_pivot.columns = [f"{metrica}_{rede}_UF" for metrica, rede in uf_pivot.columns]
    uf_dim = uf_pivot.reset_index()
    
    # Fallback TOTAL -> PUBLICA_EST_MUN
    REDES_UF = list(MAPA_REDES.values())
    colunas_esperadas_uf = [f"{metrica}_{rede}_UF" for metrica in METRICAS for rede in REDES_UF]
    uf_dim = uf_dim.reindex(columns=CHAVES_UF + colunas_esperadas_uf)
    for metrica in METRICAS:
        col_total = f"{metrica}_TOTAL_UF"
        col_publica = f"{metrica}_PUBLICA_EST_MUN_UF"
        uf_dim[col_total] = uf_dim[col_total].fillna(uf_dim[col_publica])
        
    return municipio_dim, uf_dim

def enriquecer_alunos_silver(df_alunos: pd.DataFrame, municipio_dim: pd.DataFrame, uf_dim: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as regras de cruzamento (merge), cálculo de desvios e limpeza
    sobre um DataFrame de alunos. Aceita tanto a base inteira (batch) quanto micro-lotes (streaming).
    """
    CHAVES_MUN = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
    CHAVES_UF = ['NU_ANO_AVALIACAO', 'CO_UF', 'TP_SERIE']

    # Junções
    df_silver = pd.merge(df_alunos, municipio_dim, on=CHAVES_MUN, how='left')
    df_silver = pd.merge(df_silver, uf_dim, on=CHAVES_UF, how='left')
    
    # Cálculos de Desvios
    df_silver['DESVIO_MEDIA_MUNICIPIO'] = df_silver['VL_PROFICIENCIA_LP'] - df_silver['VL_MEDIA_LP_TOTAL_MUNICIPIO']
    df_silver['DESVIO_MEDIA_UF'] = df_silver['VL_PROFICIENCIA_LP'] - df_silver['VL_MEDIA_LP_TOTAL_UF']
    
    # Limpeza de colunas vazias/inativas 
    colunas_para_remover = [
        'VL_MEDIA_LP_FEDERAL_MUNICIPIO', 'PC_ALUNO_ALFABETIZADO_FEDERAL_MUNICIPIO',
        'VL_MEDIA_LP_PRIVADA_MUNICIPIO', 'PC_ALUNO_ALFABETIZADO_PRIVADA_MUNICIPIO',
        'VL_MEDIA_LP_PUBLICA_TOTAL_MUNICIPIO', 'PC_ALUNO_ALFABETIZADO_PUBLICA_TOTAL_MUNICIPIO',
        'VL_MEDIA_LP_FEDERAL_UF', 'PC_ALUNO_ALFABETIZADO_FEDERAL_UF',
        'VL_MEDIA_LP_PRIVADA_UF', 'PC_ALUNO_ALFABETIZADO_PRIVADA_UF',
        'VL_MEDIA_LP_PUBLICA_TOTAL_UF', 'PC_ALUNO_ALFABETIZADO_PUBLICA_TOTAL_UF',
        'CO_BLOCO_4', 'TX_RESPOSTA_BLOCO_4', 'TX_GABARITO_BLOCO_4'
    ]
    df_silver = df_silver.drop(columns=colunas_para_remover, errors='ignore')
    try:
        df_silver = silver_schema.validate(df_silver)
    except pa.errors.SchemaErrors as err:
        print(f"[AVISO DE QUALIDADE DE DADOS] Falha na validação da Silver:\n{err}")

    return df_silver


#---------------AWS 
import boto3
import dotenv
import os 


dotenv.load_dotenv()

# Criando a cessão 

def iniciar_cessao_aws():
    ID_CONTA = os.getenv("ID_CONTA")
    AWS_REGION = os.getenv("AWS_REGION")
    AWS_ACCESS_KEY_ID=os.getenv("AWS_ACESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACESS_KEY")


    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    return session

#-- carregar parquet por camada do s3
def carregar_parquet_s3(
    s3_client,
    bucket: str,
    ano: int,
    nome_tabela: str,
    camada: str = "bronze",
    ler_dicionario: bool = False,
) -> pd.DataFrame:
    """
    Carrega um arquivo Parquet do S3.

    Estrutura esperada:

    bronze/
        ano=2025/
            dados/
                TS_ALUNO.parquet

    ou

    bronze/
        ano=2025/
            dicionario/
                dicionario_TS_ALUNO.parquet
    """

    pasta = "dicionario" if ler_dicionario else "dados"

    arquivo = (
        f"dicionario_{nome_tabela}.parquet"
        if ler_dicionario
        else f"{nome_tabela}.parquet"
    )

    chave = f"{camada}/ano={ano}/{pasta}/{arquivo}"

    print(f"Lendo: s3://{bucket}/{chave}")

    try:

        obj = s3_client.get_object(
            Bucket=bucket,
            Key=chave
        )

        return pd.read_parquet(
            BytesIO(obj["Body"].read())
        )

    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Arquivo não encontrado:\n"
            f"s3://{bucket}/{chave}"
        )

    except Exception as e:
        raise RuntimeError(
            f"Erro ao ler {chave} do bucket {bucket}.\n{e}"
        )

#- salvar parquet por camada no s3

from io import BytesIO
import pandas as pd


def salvar_parquet_s3(
    s3_client,
    bucket: str,
    df: pd.DataFrame,
    ano: int,
    tabela: str,
    camada: str = "silver",
    ler_dicionario: bool = False,
    index: bool = False
) -> str:
    """
    Salva um DataFrame em formato Parquet no S3.

    Estrutura:
        camada/
            ano=2025/
                dados/
                    TS_ALUNO.parquet

    ou

        camada/
            ano=2025/
                dicionario/
                    dicionario_TS_ALUNO.parquet
    """

    pasta = "dicionario" if ler_dicionario else "dados"

    arquivo = (
        f"dicionario_{tabela}.parquet"
        if ler_dicionario
        else f"{tabela}.parquet"
    )

    chave = f"{camada}/ano={ano}/{pasta}/{arquivo}"

    buffer = BytesIO()

    df.to_parquet(buffer, index=index)

    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=chave,
        Body=buffer.getvalue()
    )

    print(f"Arquivo salvo: s3://{bucket}/{chave}")

    return chave