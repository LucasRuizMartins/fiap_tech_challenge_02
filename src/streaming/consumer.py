#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Lucas Ruiz 
Description: Consumer do Kafka para processar mensagens do tópico "transactions" e salvar no S3 em formato Parquet.

Uso:
    python -m src.streaming.consumer     

Variáveis de ambiente:
    - KAFKA_SERVER: Endereço do Kafka (default: localhost:9092)
    - TOPIC_NAME: Nome do tópico (default: transactions)
    - BUCKET_NAME: Nome do bucket S3
    - AWS_REGION: Região da AWS
    - AWS_ACCESS_KEY_ID: ID da chave de acesso AWS
    - AWS_SECRET_ACCESS_KEY: Chave de acesso AWS

Saída:
    - Mensagens processadas salvas em s3://<bucket>/prata/ano=<YYYY>/lote_alunos_<timestamp>.parquet

Tratamento de erros:
    - Mensagens com campos obrigatórios ausentes são descartadas
    - Erros de conexão com o Kafka ou S3 são reportados, mas o consumer continua processando
"""

import os
import sys
import json
import time
from pathlib import Path
import pandas as pd
# pyrefly: ignore [missing-import]
from confluent_kafka import Consumer, KafkaError

# Adiciona o diretório raiz do projeto ao PATH do Python para importar src
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from src.data.utils import iniciar_cessao_aws, preparar_dimensoes_silver, enriquecer_alunos_silver

def main():
    # 1. Carrega as variáveis de ambiente (.env)
    import dotenv
    dotenv.load_dotenv(project_root / '.env')

    # Configurações obtidas do .env
    KAFKA_SERVER = os.getenv("SERVER_KAFKA", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "transactions")
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    PATH_BRONZE = project_root / "data" / "bronze"
    PATH_PRATA = project_root / "data" / "prata"

    # Configurações do Micro-batch
    BATCH_SIZE_LIMIT = 10000  # Limite de mensagens por buffer
    BATCH_TIME_LIMIT = 10     # Tempo limite para fechar o buffer (segundos)

    print(f"Iniciando Consumer Dinâmico Multi-Ano...")
    print(f"Servidor Kafka: {KAFKA_SERVER}")
    print(f"Tópico: {TOPIC_NAME}")
    print(f"Bucket S3: {BUCKET_NAME}")

    # Inicializa a sessão da AWS
    try:
        session = iniciar_cessao_aws()
        s3_client = session.client('s3')
    except Exception as e:
        print(f"[Erro]  ao conectar na AWS: {e}")
        sys.exit(1)

   
    # Carrega em memória as tabelas de referência de TODOS os anos disponíveis
    ANOS_DISPONIVEIS = [2024, 2025] #2023 ja foi processado pelo notebook, por isso removi
    dimensoes_anos = {}

    # Pré-carregando tabelas de referência de Municípios/UF de todos os anos
    for ano in ANOS_DISPONIVEIS:
        try:
            m_dim, u_dim = preparar_dimensoes_silver(ano, PATH_BRONZE)
            dimensoes_anos[ano] = (m_dim, u_dim)
            print(f"[SUCESSO]  Tabelas de {ano} carregadas")
        except Exception as e:
            print(f"[AVISO] Não foi possível carregar as dimensões do ano {ano}  {e}")

    if not dimensoes_anos:
        print("[ERRO] Nenhuma tabela de referência foi carregada. O Consumer não pode iniciar.")
        sys.exit(1)

    # Configura e assina o Consumidor Kafka
    consumer = Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'pipeline-silver-group-v1',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])
    print("\nConsumidor ativo e escutando o Kafka...")

    # Limpar buffer e resetar tempo
    buffer_mensagens = []
    ultimo_envio_tempo = time.time()
    total_processado_acumulado = 0

    # Loop contínuo de processamento
    try:
        while True:
            msg = consumer.poll(0.5)
            
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[Erro] Kafka: {msg.error()}")
                        break
                else:
                    dados_aluno_json = json.loads(msg.value().decode('utf-8'))
                    
                    colunas_obrigatorias = ['NU_ANO_AVALIACAO', 'CO_UF', 'CO_MUNICIPIO', 'TP_SERIE']
                    if all(col in dados_aluno_json for col in colunas_obrigatorias):
                        buffer_mensagens.append(dados_aluno_json)
            
            tempo_decorrido = time.time() - ultimo_envio_tempo
            tamanho_lote = len(buffer_mensagens)
            
            # Condições para processar o micro-lote (tamanho ou tempo)
            if tamanho_lote > 0 and (tamanho_lote >= BATCH_SIZE_LIMIT or tempo_decorrido >= BATCH_TIME_LIMIT):
                df_lote_alunos = pd.DataFrame(buffer_mensagens)
                df_lote_alunos = df_lote_alunos.dropna(subset=colunas_obrigatorias)
                
                if not df_lote_alunos.empty:
                    # Garante tipos corretos
                    for col_name in colunas_obrigatorias:
                        df_lote_alunos[col_name] = pd.to_numeric(df_lote_alunos[col_name]).astype(int)
                    if 'VL_PROFICIENCIA_LP' in df_lote_alunos.columns:
                        df_lote_alunos['VL_PROFICIENCIA_LP'] = pd.to_numeric(df_lote_alunos['VL_PROFICIENCIA_LP'])
                    
                    # DIVISÃO DINÂMICA POR ANO:
                    # Agrupa as mensagens do lote pelo ano da avaliação e processa cada um com suas respectivas referências
                    for ano_grupo, df_grupo in df_lote_alunos.groupby('NU_ANO_AVALIACAO'):
                        ano_grupo = int(ano_grupo)
                        
                        if ano_grupo in dimensoes_anos:
                            m_dim, u_dim = dimensoes_anos[ano_grupo]
                            
                            # Enriquecimento usando as tabelas do ano correto
                            df_silver = enriquecer_alunos_silver(df_grupo, m_dim, u_dim)
                            
                            timestamp_atual = int(time.time())
                            nome_arquivo = f"lote_alunos_{timestamp_atual}.parquet"
                            
                            # A. Salva Localmente na pasta de partição correta: data/prata/ano=YYYY/
                            pasta_destino = PATH_PRATA / f"ano={ano_grupo}"
                            pasta_destino.mkdir(parents=True, exist_ok=True)
                            caminho_arquivo_local = pasta_destino / nome_arquivo
                            df_silver.to_parquet(caminho_arquivo_local, index=False)
                            
                            # Salva na Nuvem na partição correta: prata/ano=YYYY/
                            chave_s3 = f"prata/ano={ano_grupo}/{nome_arquivo}"
                            try:
                                import io
                                parquet_buffer = io.BytesIO()
                                df_silver.to_parquet(parquet_buffer, index=False)
                                parquet_bytes = parquet_buffer.getvalue()
                                
                                s3_client.put_object(
                                    Bucket=BUCKET_NAME,
                                    Key=chave_s3,
                                    Body=parquet_bytes
                                )
                                
                                total_processado_acumulado += len(df_silver)
                                print(f"Lote processado dinamicamente para ano={ano_grupo} e enviado para o S3: s3://{BUCKET_NAME}/{chave_s3} "
                                      f"(Lote: {len(df_silver)} | Total Geral: {total_processado_acumulado})")
                            except Exception as e:
                                print(f"Erro ao enviar partição {ano_grupo} para o S3 (salvo apenas localmente): {e}")
                        else:
                            print(f"Aviso: Recebido dados para o ano {ano_grupo}, mas não temos tabelas de referência carregadas para esse ano. Lote descartado.")
                else:
                    print("O lote continha apenas registros inválidos (chaves nulas) e foi descartado.")
                
                # Reseta o buffer para o próximo ciclo
                buffer_mensagens = []
                ultimo_envio_tempo = time.time()

    except KeyboardInterrupt:
        print("\nConsumidor parado pelo usuário via interrupção de terminal.")
    finally:
        consumer.close()
        print("Conexão do consumidor encerrada com sucesso.")

if __name__ == "__main__":
    main()
