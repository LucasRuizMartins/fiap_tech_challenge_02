from pathlib import Path
import pandas as pd

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
    
    raw = pd.read_csv(path_alunos, sep=";", encoding="latin-1")
    
    path_dicionario = path_dados / 'DICIONÁRIO' / f'Dicionario_Microdados_AEEB_{ano}.xlsx'
    
    print(f"Lendo dicionário em: {path_dicionario}")
    
    dicionario = pd.read_excel(path_dicionario, sheet_name=nome_tabela)   
    dicionario.columns = dicionario.iloc[0].to_list()
    dicionario = dicionario[1:]
    
    try:
        dicionario.set_index('Variável', inplace=True)
    except Exception:
        print('coluna variavel nao encontrada no dicionario')
    # Converte todas as colunas e o índice do dicionário para string para evitar tipos mistos
    dicionario = dicionario.astype(str)

    return raw, dicionario




#--------------------------

from io import BytesIO
"""
A razão para usar BytesIO aqui é que o método to_parquet espera um objeto de arquivo, e o BytesIO fornece uma maneira conveniente de criar um objeto de arquivo temporário em memória.
Em vez de gravar os dados em um arquivo físico no sistema de arquivos local e, em seguida, lê-los de volta para enviá-los para o S3, o BytesIO permite que você escreva os dados diretamente em um buffer de memória.
"""

def converter_para_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    Converte um DataFrame para o formato Parquet na memória e retorna seus bytes.
    """
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    return parquet_buffer.getvalue()


def salvar_parquet_local(df: pd.DataFrame, caminho_destino: Path | str) -> None:
    """
    Salva um DataFrame localmente no formato Parquet.
    Cria as pastas pai automaticamente caso elas não existam (ex: pasta 'bronze').
    """
    caminho_destino = Path(caminho_destino)
    # Garante que a pasta pai exista antes de salvar (ex: data/bronze/)
    caminho_destino.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(caminho_destino, index=False)
    print(f"Arquivo salvo localmente em: {caminho_destino}")


def salvar_parquet_s3(s3_client, bucket: str, chave_s3: str, parquet_bytes: bytes) -> None:
    """
    Envia os bytes de um arquivo Parquet diretamente para um bucket no S3.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=chave_s3,
        Body=parquet_bytes
    )
    print(f"Arquivo enviado para o S3: s3://{bucket}/{chave_s3}")