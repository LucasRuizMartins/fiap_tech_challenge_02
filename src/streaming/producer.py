#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Lucas Ruiz 
Description: Producer do Kafka para ler os microdados da camada Bronze local e enviar mensagens para o tópico "transactions" do Kafka na AWS.

Uso:
    python -m src.streaming.producer ano limite_registros
    
    Exemplo:
        python -m src.streaming.producer 2024 10000
        python -m src.streaming.producer        # Envia todos os alunos

Variáveis de ambiente:
    - SERVER_KAFKA: Endereço do Kafka (default: localhost:9092)
    - TOPIC_NAME: Nome do tópico (default: transactions)
    - ANO_PROCESSAMENTO: Ano dos microdados (default: 2023)

Entrada:
    - Carrega dados locais de data/bronze/ano=<YYYY>/dados/TS_ALUNO.parquet

Tratamento de erros:
    - Controla o buffer local usando tratamento de BufferError (Queue Full) para evitar estouro de memória
"""

import os
import sys
import json
import time
from pathlib import Path
import pandas as pd
# pyrefly: ignore [missing-import]
from confluent_kafka import Producer

# Adiciona o diretório raiz do projeto ao PATH do Python para importar src
project_root = Path(__file__).resolve().parents[2] #parent[2] sobe dois niveis
sys.path.append(str(project_root))

def delivery_report(err, msg):
    """
    Loga erros de envio do Kafka.
    """
    if err is not None:
        print(f"[ERRO] Falha no envio: {err}")

def main():
    # Carrega as variáveis de ambiente (.env)
    import dotenv
    dotenv.load_dotenv(project_root / '.env')

    KAFKA_SERVER = os.getenv("SERVER_KAFKA", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "transactions")
    ANO = int(os.getenv("ANO_PROCESSAMENTO", "2023"))
    PATH_BRONZE = project_root / "data" / "bronze"

    # Argumentos do terminal: python producer.py [ano] [limite]
    # Exemplo: python producer.py 2024 10000
    limit = None
    if len(sys.argv) > 1:
        arg1 = sys.argv[1]
        if arg1 in [ "2024", "2025"]: #2023 ja foi processado pelo notebook
            ANO = int(arg1)
            # Se tiver um segundo argumento, ele representa o limite de registros
            if len(sys.argv) > 2:
                try:
                    limit = int(sys.argv[2])
                except ValueError:
                    pass
        else:
            # Se não for um ano, assume que o primeiro argumento é o limite de registros para o ano padrão
            try:
                limit = int(arg1)
            except ValueError:
                pass

    caminho_parquet = PATH_BRONZE / f"ano={ANO}/dados/TS_ALUNO.parquet"
    print(f"Iniciando Producer...")
    print(f"Servidor Kafka: {KAFKA_SERVER}")
    print(f"Tópico: {TOPIC_NAME}")
    print(f"Lendo de: {caminho_parquet.name}")

    if not caminho_parquet.exists():
        print(f"[ERRO] Arquivo não encontrado: {caminho_parquet}")
        sys.exit(1)

    # Carrega a base de alunos
    print("[INFO] Carregando arquivo Parquet da Bronze local...")
    df_alunos_source = pd.read_parquet(caminho_parquet)
    
    if limit:
        df_alunos_source = df_alunos_source.head(limit)
        
    total_registros = len(df_alunos_source)
    print(f"Total de registros a serem enviados: {total_registros}")
    #Inicializa o Producer
    producer = Producer({'bootstrap.servers': KAFKA_SERVER})

    print(f"\nIniciando streaming dos dados...")
    
    try:
        for index, row in df_alunos_source.iterrows():
            dados_aluno = row.to_dict()
            dados_aluno_limpo = {}
            for k, v in dados_aluno.items():
                if pd.isna(v):
                    dados_aluno_limpo[k] = None
                elif hasattr(v, 'item'):
                    dados_aluno_limpo[k] = v.item()
                else:
                    dados_aluno_limpo[k] = v

            id_aluno = str(dados_aluno_limpo.get('ID_ALUNO') or index)
            
            # Tratamento de Buffer (Queue Full)
            while True:
                try:
                    producer.produce(
                        TOPIC_NAME,
                        key=id_aluno.encode('utf-8'),
                        value=json.dumps(dados_aluno_limpo).encode('utf-8'),
                        callback=delivery_report
                    )
                    break
                except BufferError:
                    # Se a fila local encher, aguarda a rede enviar as pendentes
                    producer.poll(0.5)

            # Esvazia memória local do client
            if index % 1000 == 0:
                producer.poll(0)
            
            # Log de progresso a cada 10.000 mensagens
            if index % 10000 == 0:
                print(f"[Status] {index}/{total_registros} mensagens enviadas...")

    except KeyboardInterrupt:
        print("\n Envio pausado pelo usuário.")
    finally:
        print("Esvaziando a fila de envios pendentes (flush)...")
        producer.flush()
        print(f"Ingestão de streaming concluída com sucesso!")

if __name__ == "__main__":
    main()
