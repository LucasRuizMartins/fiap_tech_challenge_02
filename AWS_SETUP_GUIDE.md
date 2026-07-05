# Guia de Configuração da AWS e Controle de Custos

Este guia orienta sobre como configurar sua nova conta da AWS de forma segura e econômica para o projeto de Engenharia de Dados, garantindo que você não ultrapasse os créditos de **USD 200** e evite cobranças inesperadas.

---

## 1. Primeiros Passos de Segurança (Crucial)

Nunca utilize a sua conta **Root** (o e-mail principal) para o desenvolvimento do dia a dia. Se a chave da conta Root vazar no GitHub, hackers podem usar sua conta para minerar criptomoedas, gerando contas de milhares de dólares em minutos.

### Passos:
1. **Ative o MFA (Autenticação de Múltiplos Fatores):**
   * No console da AWS, clique no seu nome de usuário no canto superior direito e selecione **Security Credentials**.
   * Ative o MFA para o usuário Root usando um aplicativo como Google Authenticator ou Authy.
2. **Crie um Usuário Administrador (IAM):**
   * Acesse o serviço **IAM** > **Users** > **Create User**.
   * Adicione o usuário ao grupo de Administradores (política `AdministratorAccess`).
   * Salve a senha gerada e configure o MFA para este novo usuário.
3. **Crie o Usuário do Pipeline e suas Chaves de Acesso:**
   * **Criação:** No IAM, vá em **Users** > **Create User**. Dê um nome (ex: `data-pipeline-user`).
   * **Permissões:** Anexe a política `AmazonS3FullAccess` (para salvar os Parquet de Bronze/Prata/Ouro).
   * **Geração das Chaves:** Na aba **Security credentials** > **Access keys** > **Create access key** > Selecione **Local code** > Faça o download do `.csv`.
   * Cole as chaves nas variáveis do arquivo `.env` local.

---

## 2. Configurando Alertas de Faturamento (AWS Budgets)

> [!TIP]
> **Região Recomendada:** Crie todos os recursos na região **`us-east-1` (N. Virginia)**. Ela é a mais barata e garante compatibilidade com os scripts do projeto.

1. No console da AWS, busque por **Billing and Cost Management**.
2. Vá em **Orçamentos** > **Criar orçamento** > **Cost budget**.
3. Configure: período Mensal, método Fixo, valor `200`, nome `Orcamento-Projeto-FIAP`.
4. Adicione alertas de e-mail em: 30%, 50%, 90% e 100% do valor orçado.

---

## 3. Infraestrutura da EC2 com Apache Kafka (Docker)

### Criação da EC2
A instância EC2 foi provisionada via Python/boto3 no notebook `02 - configurar_aws.ipynb` com as seguintes especificações:
- **Tipo:** `t3.small` (2 vCPUs, 2GB RAM)
- **AMI:** Ubuntu 22.04 LTS
- **VPC:** `fiap-msk-vpc` (10.0.0.0/16)
- **Sub-rede:** `fiap-msk-subnet-1` (10.0.1.0/24) — Sub-rede Pública
- **IP Público:** Habilitado
- **IP Privado:** `10.0.1.16` (fixo dentro da VPC)
- **Security Group:** Regras de entrada abertas nas portas `22` (SSH), `9092` (Kafka Público), `9094` (Kafka Privado), `8090` (Kafka UI)

### Docker Compose do Kafka (Dual-Listener)
O Kafka está configurado com **dois listeners** para atender o Producer local (internet pública) e a Lambda (VPC privada):

```yaml
# /home/ubuntu/kafka/docker-compose.yml
version: '3.8'
services:
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka-fiap
    ports:
      - "9092:9092"   # Listener público (Producer local)
      - "9094:9094"   # Listener privado (AWS Lambda)
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://<IP_PUBLICO_EC2>:9092,INTERNAL://10.0.1.16:9094
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: MKU30EVBNTcwNTJENDM2Qk

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-fiap-ui
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: checkout-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9094
```

Para **atualizar o arquivo com permissões de root**, usar `sudo tee` em vez de redirecionamento direto:
```bash
sudo tee /home/ubuntu/kafka/docker-compose.yml <<'EOF'
# ... conteúdo acima ...
EOF
```

Para **iniciar/reiniciar** os contêineres:
```bash
cd /home/ubuntu/kafka
sudo docker-compose down
sudo docker-compose up -d
```

Para verificar se os contêineres estão rodando:
```bash
sudo docker ps
```

---

## 4. Configuração do AWS Lambda Consumer

### Criação da Função Lambda
1. No console da AWS, busque por **Lambda** > **Criar função** > **Criar do zero**.
2. **Nome:** `fiap-kafka-s3-consumer`
3. **Runtime:** Python 3.12
4. **Role de execução:** Criar nova role automaticamente (o nome gerado será algo como `fiap-kafka-s3-consumer-role-XXXX`).

### Adicionar a Camada do Pandas (Layer)
1. Na página da função, role até **Camadas** (Layers) no final > **Adicionar uma camada**.
2. Selecione **Camadas da AWS** > **`AWSSDKPandas-Python312`** > versão mais recente.

### Colar o Código
1. Na aba **Código**, cole o conteúdo de `src/streaming/lambda_function.py`.
2. Clique em **Deploy** para salvar.

### Configurar a Variável de Ambiente
1. Aba **Configuração** > **Variáveis de ambiente** > **Editar**.
2. Adicione: **Chave:** `BUCKET_NAME` | **Valor:** `fiap-postech-challenge-datascience-002`.

---

## 5. Configuração das Permissões IAM da Lambda

A role gerada automaticamente pela Lambda (`fiap-kafka-s3-consumer-role-XXXX`) precisa de 3 políticas:

| Política | Motivo |
|:---|:---|
| `AmazonS3FullAccess` | Leitura das tabelas de referência (Bronze) e gravação na Silver |
| `AWSLambdaVPCAccessExecutionRole` | Criar interfaces de rede (ENIs) nas sub-redes da VPC |
| `AmazonEC2ReadOnlyAccess` | Ler metadados de Security Groups e Sub-redes durante a criação do gatilho |

**Como adicionar no console:**
1. No IAM > **Funções** (Roles) > pesquise pelo nome da role da Lambda.
2. Clique em **Adicionar permissões** > **Anexar políticas**.
3. Busque e selecione cada uma das 3 políticas acima.

---

## 6. Configuração da Rede VPC para a Lambda

Para que a Lambda dentro da VPC consiga se comunicar com as APIs da AWS (S3, STS), é necessário um **NAT Gateway** na sub-rede pública e uma tabela de rotas privada.

### Arquitetura de Rede Final

```
fiap-msk-vpc (10.0.0.0/16)
├── fiap-msk-subnet-1 (10.0.1.0/24) — PÚBLICA
│   ├── EC2 t3.small (IP Privado: 10.0.1.16)
│   ├── NAT Gateway (fiap-nat-gateway)
│   └── Rota: 0.0.0.0/0 → Internet Gateway (igw-xxxx)
│
└── fiap-msk-subnet-2 (10.0.2.0/24) — PRIVADA
    ├── AWS Lambda Consumer (fiap-kafka-s3-consumer)
    └── Rota: 0.0.0.0/0 → NAT Gateway (nat-xxxx)
```

### Passo a Passo para Criar o NAT Gateway

**1. Criar o NAT Gateway:**
- No console da AWS, vá em **VPC** > **Gateways NAT** > **Criar gateway NAT**.
- **Nome:** `fiap-nat-gateway`
- **Sub-rede:** `fiap-msk-subnet-1` (a pública, onde está a EC2)
- **Método de alocação de IP:** `Automático` (a AWS aloca um IP elástico automaticamente)
- Clique em **Criar** e aguarde o status ficar **Disponível** (~2 minutos).

**2. Criar a Tabela de Rotas Privada:**
- Vá em **VPC** > **Tabelas de rotas** > **Criar tabela de rotas**.
- **Nome:** `fiap-private-rt` | **VPC:** `fiap-msk-vpc`
- Clique em **Criar tabela de rotas**.

**3. Associar a subnet-2 à nova tabela:**
- Na tabela `fiap-private-rt` > aba **Associações de sub-rede** > **Editar associações**.
- Marque apenas **`fiap-msk-subnet-2`** e salve.

**4. Adicionar a rota do NAT Gateway:**
- Na tabela `fiap-private-rt` > aba **Rotas** > **Editar rotas** > **Adicionar rota**.
- **Destino:** `0.0.0.0/0` | **Alvo:** `Gateway NAT` > selecione `fiap-nat-gateway`.
- Salve as alterações.

> [!WARNING]
> **Nunca altere** a rota `0.0.0.0/0` da tabela original da `subnet-1` pública. Essa tabela deve continuar apontando para o `Internet Gateway` (igw). Alterar isso derrubaria o acesso SSH à EC2.

### Configurar o Gatilho (Trigger) da Lambda

1. Na Lambda, vá em **Configuração** > **Gatilhos** > **Adicionar gatilho**.
2. Selecione **Apache Kafka (self-managed)**.
3. Preencha os campos:
   - **Bootstrap servers:** `10.0.1.16:9094` *(IP privado da EC2, porta interna 9094)*
   - **Topic:** `transactions`
   - **VPC:** `vpc-0f91f6cc0d6b482f4` (`fiap-msk-vpc`)
   - **Subnets:** Selecione **apenas** `fiap-msk-subnet-2` (a privada)
   - **Security Groups:** Selecione o security group do projeto
   - **Starting position:** `LATEST` ou `EARLIEST`
   - **Batch size:** `10000`
4. Clique em **Adicionar** e aguarde o status mudar para `Enabled`.

---

## 7. Como Rodar o Pipeline Completo

### Pré-requisitos
- EC2 com Docker e Kafka rodando.
- Lambda com gatilho `Enabled` configurado na `subnet-2`.
- NAT Gateway ativo.

### Executar o Producer local
```powershell
# No terminal PowerShell do seu computador:
python -m src.streaming.producer 2024 10000
```

### Monitorar a execução
1. **Logs em tempo real:** Na Lambda > aba **Monitorar** > **Visualizar logs do CloudWatch**.
2. **Validação no S3:** Verifique se novos arquivos `.parquet` aparecem em `prata/ano=2024/`.

---

## 8. Controle de Custos: Como Parar as Cobranças

> [!CAUTION]
> O NAT Gateway custa **U$ 0,045/hora** (~U$ 32/mês) mesmo sem tráfego. **Sempre exclua-o quando não estiver usando a Lambda!**

### Para parar de cobrar (ordem obrigatória):

**Passo 1: Excluir o NAT Gateway**
- VPC > Gateways NAT > selecione `fiap-nat-gateway` > **Ações** > **Excluir gateway NAT**.
- Aguarde o status mudar para `Deleted`.

**Passo 2: Liberar o IP Elástico (se alocado manualmente)**
- VPC > IPs elásticos > selecione o IP > **Ações** > **Liberar endereço IP elástico**.
- *Se você usou a alocação Automática, a AWS gerencia isso automaticamente ao excluir o NAT.*

**Passo 3: Parar a EC2 (opcional)**
- EC2 > Instâncias > selecione sua instância > **Estado da instância** > **Parar**.
- *Instâncias paradas não geram custo de computação (apenas o disco EBS ~U$ 0,10/GB/mês).*

### Alternativa de baixo custo: Consumer Legado na EC2
Se não quiser pagar o NAT Gateway, basta desativar o gatilho da Lambda e rodar o `consumer.py` diretamente na EC2:
```bash
# Na EC2 via SSH:
source .venv/bin/activate
nohup python -u src/streaming/consumer.py > consumer.log 2>&1 &
```

---

## 9. Estimativa de Custos do Projeto

| Recurso | Custo Mensal (24/7) | Custo por Semana Ativa (10h/dia) |
|:---|:---:|:---:|
| EC2 `t3.small` | ~U$ 15,00 | ~U$ 1,50 |
| Amazon S3 (armazenamento + requests) | ~U$ 1,00 | ~U$ 0,25 |
| AWS Lambda (execução) | Praticamente zero | Praticamente zero |
| NAT Gateway | ~U$ 32,00 | ~U$ 3,15 |
| **Total sem NAT** | **~U$ 16,00** | **~U$ 1,75** |
| **Total com NAT** | **~U$ 48,00** | **~U$ 4,90** |

---

[Link oficial para a Base de dados (INEP)](https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao/resultados)