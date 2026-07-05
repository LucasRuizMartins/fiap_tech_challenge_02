import os
import io
import json
import base64
import boto3
import pandas as pd
"""
Politicas para conseguir executar a Lambda:
AmazonEC2ReadOnlyAccess
AmazonS3FullAccess
AWSLambdaVPCAccessExecutionRole
"""
# Inicializa o cliente do S3 (usará as credenciais de execução da role da Lambda)
s3_client = boto3.client('s3')

# Configurações do Bucket obtidas das variáveis de ambiente do Lambda
BUCKET_NAME = os.environ.get("BUCKET_NAME", "fiap-postech-challenge-datascience-002")

# Cache global das tabelas de referência para evitar downloads repetidos
cache_dimensoes = {}

# Mapeamento para as tabelas
MAPA_REDES = {
    0: 'TOTAL', 1: 'FEDERAL', 2: 'ESTADUAL', 3: 'MUNICIPAL',
    4: 'PRIVADA', 5: 'PUBLICA_EST_MUN', 6: 'PUBLICA_TOTAL'
}
METRICAS = ['VL_MEDIA_LP', 'PC_ALUNO_ALFABETIZADO']

def carregar_referencias_s3(ano):
    """
    Baixa e prepara as tabelas de referência (Município/UF) do S3 e as coloca no cache.
    """
    if ano in cache_dimensoes:
        return cache_dimensoes[ano]

    print(f"Baixando referências do ano {ano} do S3...")
    try:
        # Download e leitura de TS_MUNICIPIO
        chave_mun = f"bronze/ano={ano}/dados/TS_MUNICIPIO.parquet"
        obj_mun = s3_client.get_object(Bucket=BUCKET_NAME, Key=chave_mun)
        raw_municipio = pd.read_parquet(io.BytesIO(obj_mun['Body'].read()))

        # Download e leitura de TS_ESTADO
        chave_uf = f"bronze/ano={ano}/dados/TS_ESTADO.parquet"
        obj_uf = s3_client.get_object(Bucket=BUCKET_NAME, Key=chave_uf)
        raw_uf = pd.read_parquet(io.BytesIO(obj_uf['Body'].read()))

        # Processamento idêntico ao utils.py
        # 1. Município
        CHAVES_MUN = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
        municipio_prep = raw_municipio.assign(REDE_NOME=lambda df: df['ID_TIPO_REDE'].map(MAPA_REDES)).query('REDE_NOME.notna()')
        municipio_pivot = municipio_prep.pivot(index=CHAVES_MUN, columns='REDE_NOME', values=METRICAS)
        municipio_pivot.columns = [f"{metrica}_{rede}_MUNICIPIO" for metrica, rede in municipio_pivot.columns]
        municipio_dim = municipio_pivot.reset_index()

        REDES_MUN = list(MAPA_REDES.values())
        colunas_esperadas_mun = [f"{metrica}_{rede}_MUNICIPIO" for metrica in METRICAS for rede in REDES_MUN]
        municipio_dim = municipio_dim.reindex(columns=CHAVES_MUN + colunas_esperadas_mun)
        for metrica in METRICAS:
            municipio_dim[f"{metrica}_TOTAL_MUNICIPIO"] = municipio_dim[f"{metrica}_TOTAL_MUNICIPIO"].fillna(
                municipio_dim[f"{metrica}_PUBLICA_EST_MUN_MUNICIPIO"]
            )

        # 2. UF
        CHAVES_UF = ['NU_ANO_AVALIACAO', 'CO_UF', 'TP_SERIE']
        uf_prep = raw_uf.assign(REDE_NOME=lambda df: df['ID_TIPO_REDE'].map(MAPA_REDES)).query('REDE_NOME.notna()')
        uf_pivot = uf_prep.pivot(index=CHAVES_UF, columns='REDE_NOME', values=METRICAS)
        uf_pivot.columns = [f"{metrica}_{rede}_UF" for metrica, rede in uf_pivot.columns]
        uf_dim = uf_pivot.reset_index()

        REDES_UF = list(MAPA_REDES.values())
        colunas_esperadas_uf = [f"{metrica}_{rede}_UF" for metrica in METRICAS for rede in REDES_UF]
        uf_dim = uf_dim.reindex(columns=CHAVES_UF + colunas_esperadas_uf)
        for metrica in METRICAS:
            uf_dim[f"{metrica}_TOTAL_UF"] = uf_dim[f"{metrica}_TOTAL_UF"].fillna(
                uf_dim[f"{metrica}_PUBLICA_EST_MUN_UF"]
            )

        cache_dimensoes[ano] = (municipio_dim, uf_dim)
        print(f"[SUCESSO] Referências do ano {ano} carregadas em cache.")
        return cache_dimensoes[ano]
    except Exception as e:
        print(f"[ERRO] ao baixar referências do ano {ano}: {e}")
        raise e

def lambda_handler(event, context):
    print("Invocando Lambda Consumer do Kafka...")
    
    # 1. Recupera as mensagens do evento do Kafka (elas vêm agrupadas por partição)
    mensagens_brutas = []
    if 'records' not in event:
        print("[INFO] Evento sem mensagens (records). Cancelando execução.")
        return {"status": "no_records"}

    for particao, records in event['records'].items():
        for record in records:
            try:
                # O valor vem em Base64, precisamos decodificar e ler como JSON
                valor_b64 = record['value']
                valor_decodificado = base64.b64decode(valor_b64).decode('utf-8')
                dados_aluno = json.loads(valor_decodificado)
                
                # Validação de campos obrigatórios
                colunas_obrigatorias = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
                if all(col in dados_aluno for col in colunas_obrigatorias):
                    mensagens_brutas.append(dados_aluno)
            except Exception as e:
                print(f"[ERRO] Erro ao decodificar mensagem: {e}")

    if not mensagens_brutas:
        print("[INFO] Nenhuma mensagem válida decodificada no lote.")
        return {"status": "no_valid_records"}

    print(f"[INFO] Total de registros recebidos no lote: {len(mensagens_brutas)}")

    # 2. Converte para DataFrame Pandas
    df_lote = pd.DataFrame(mensagens_brutas)
    colunas_conversao = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
    
    for col in colunas_conversao:
        # errors='coerce' transforma strings vazias ou lixo em NaN
        # astype('Int64') permite que o Pandas mantenha a coluna como inteira, mesmo com NaNs
        df_lote[col] = pd.to_numeric(df_lote[col], errors='coerce').astype('Int64')
        
    if 'VL_PROFICIENCIA_LP' in df_lote.columns:
        df_lote['VL_PROFICIENCIA_LP'] = pd.to_numeric(df_lote['VL_PROFICIENCIA_LP'], errors='coerce')

    # 3. Processamento Dinâmico Multi-Ano (groupby)
    envios_realizados = 0
    for ano_grupo, df_grupo in df_lote.groupby('NU_ANO_AVALIACAO'):
        ano_grupo = int(ano_grupo)
        
        try:
            # Carrega (ou busca no cache) as referências do ano
            municipio_dim, uf_dim = carregar_referencias_s3(ano_grupo)
            
            # Executa o Join e Enriquecimento
            CHAVES_MUN = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
            CHAVES_UF = ['NU_ANO_AVALIACAO', 'CO_UF', 'TP_SERIE']
            
            df_silver = pd.merge(df_grupo, municipio_dim, on=CHAVES_MUN, how='left')
            df_silver = pd.merge(df_silver, uf_dim, on=CHAVES_UF, how='left')
            
            df_silver['DESVIO_MEDIA_MUNICIPIO'] = df_silver['VL_PROFICIENCIA_LP'] - df_silver['VL_MEDIA_LP_TOTAL_MUNICIPIO']
            df_silver['DESVIO_MEDIA_UF'] = df_silver['VL_PROFICIENCIA_LP'] - df_silver['VL_MEDIA_LP_TOTAL_UF']
            
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
            
            # 4. Gravação direta do micro-lote em formato Parquet no S3
            import time
            timestamp_lote = int(time.time())
            chave_s3_silver = f"prata/ano={ano_grupo}/lote_alunos_{timestamp_lote}.parquet"
            
            parquet_buffer = io.BytesIO()
            df_silver.to_parquet(parquet_buffer, index=False)
            parquet_bytes = parquet_buffer.getvalue()
            
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=chave_s3_silver,
                Body=parquet_bytes
            )
            print(f"[SICESSO] Lote  gravado com sucesso na Silver: s3://{BUCKET_NAME}/{chave_s3_silver} ({len(df_silver)} registros)")
            envios_realizados += 1
            
        except Exception as e:
            print(f"[ERRO] ao processar dados do ano {ano_grupo}: {e}")

    return {
        "status": "success",
        "records_processed": len(mensagens_brutas),
        "files_uploaded": envios_realizados
    }
