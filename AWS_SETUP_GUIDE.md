# Guia de Configuração da AWS e Controle de Custos

Este guia orienta sobre como configurar sua nova conta da AWS de forma segura e econômica para o seu projeto de Engenharia de Dados ([README.md](file:///c:/Users/deth_/Carmel%20Capital/TECNOLOGIA%20-%20Geral/LUCAS/Estudos/FIAP/POSTECH_AI_SCIENTIST/tech_challenge_02/README.md)), garantindo que você não ultrapasse os créditos de **USD 200** e evite cobranças inesperadas.

---

## 1. Primeiros Passos de Segurança (Crucial)

Nunca utilize a sua conta **Root** (o e-mail principal) para o desenvolvimento do dia a dia ou para rodar seus códigos. Se a chave da conta Root vazar no GitHub, hackers podem usar sua conta para minerar criptomoedas, gerando contas de milhares de dólares em minutos.

### Passos:
1. **Ative o MFA (Autenticação de Múltiplos Fatores):**
   * No console da AWS, clique no seu nome de usuário no canto superior direito e selecione **Security Credentials** (Credenciais de Segurança).
   * Ative o MFA para o usuário Root usando um aplicativo como Google Authenticator ou Authy.
2. **Crie um Usuário Administrador (IAM):**
   * Acesse o serviço **IAM** (Identity and Access Management).
   * Vá em **Users** > **Create User**.
   * Dê um nome (ex: `admin-usuario`).
   * **Marque a opção "Provide user access to the AWS Management Console" (Fornecer acesso ao Console de Gerenciamento da AWS).** Isso é necessário para que você possa usar esse usuário para administrar a AWS pelo navegador sem precisar usar a conta root.
   * Adicione o usuário ao grupo de Administradores (associe a política `AdministratorAccess`).
   * **Uso Programático Sem Chaves Fixas:** Se você quiser usar esse usuário no terminal (AWS CLI) da sua máquina sem criar chaves estáticas de acesso (que são perigosas se vazarem), associe também a política gerenciada `SignInLocalDevelopmentAccess` a ele. Com isso, você poderá autenticar localmente rodando `aws login` no terminal.
   * Salve a senha gerada e configure também o MFA para este novo usuário.
3. **Crie o Usuário do Pipeline e suas Chaves de Acesso:**
   * **Criação do Usuário:** No menu do IAM, vá em **Users** > **Create User**. Dê um nome (ex: `data-pipeline-user`) e **deixe desmarcada** a opção de acesso ao Console (este usuário será exclusivo para código/scripts).
   * **Permissões:** Na tela de permissões, escolha *Attach policies directly* (Anexar políticas diretamente) e selecione:
     * `AmazonS3FullAccess` (para salvar os Parquet de Bronze/Prata/Ouro).
   * **Geração das Chaves (Access Keys):** 
     1. Salve o usuário. Depois, clique no nome dele na lista de usuários.
     2. Vá na aba **Security credentials** (Credenciais de segurança), role até **Access keys** e clique em **Create access key**.
     3. Selecione a opção **Local code** (Código local), aceite os termos e avance.
     4. Na tela final, clique em **Download .csv file** para salvar o *Access Key ID* e a *Secret Access Key* (copie o valor da Secret imediatamente, pois ela não será exibida novamente).
     5. Cole estas chaves nas variáveis apropriadas do seu arquivo `.env` local.

---

## 2. Configurando Alertas de Faturamento (AWS Budgets)

Você deve configurar alertas para monitorar o uso dos seus USD 200 de crédito. O ideal é receber e-mails conforme diferentes faixas de consumo forem atingidas.

> [!TIP]
> **Região Recomendada (Mais Barata):** Crie todos os seus recursos e orçamentos na região **`us-east-1` (N. Virginia)**. Ela é a mais barata e garante que os seus scripts configurados com as variáveis do arquivo `.env` funcionem sem erros de rota.

### Passos:
1. No console da AWS, procure por **Billing and Cost Management** (Faturamento).
2. No menu lateral esquerdo, clique em **Orçamentos** (Budgets) e clique em **Criar orçamento** (Create budget).
3. Escolha **Cost budget** (Orçamento de custo) e clique em Next.
4. Configure os detalhes do orçamento:
   * **Period (Período):** Monthly (Mensal).
   * **Budget effective dates (Datas de vigência):** Escolha o mês atual.
   * **Budgeting method (Método de orçamento):** Fixed (Fixo).
   * **Enter budget amount (Valor do orçamento):** Digite `200` (ou um valor menor como `150` para ter uma margem de segurança).
   * **Budget name:** `Orcamento-Projeto-FIAP`.
5. Clique em Next para configurar os **Limites de Alerta (Alert Thresholds)**. Adicione os alertas apenas com **Notificação de E-mail** (deixando-os como *Nenhuma ação* por segurança):
   * **Alerta 1:** 30% (USD 60) do valor orçado (Custo Real). Digite seu e-mail.
   * **Alerta 2:** 50% (USD 100) do valor orçado (Custo Real). Digite seu e-mail.
   * **Alerta 3:** 90% (USD 180) do valor orçado (Custo Real). Digite seu e-mail.
   * **Alerta 4:** 100% (USD 200) do valor orçado (Custo Real). Digite seu e-mail.
6. Clique em Next. Na tela de **Ações (Actions)**, não selecione nenhuma ação de bloqueio automático por enquanto (pule essa etapa).
7. Avance para a revisão e clique em **Criar Orçamento**.

> [!NOTE]
> **Importante sobre a Ação de Parar o EC2:** Você não conseguirá configurar a ação de desligamento automático de EC2 (Passo 4 abaixo) neste momento, pois a máquina EC2 ainda não foi criada. Crie o orçamento apenas com os alertas de e-mail e, assim que a instância do EC2 estiver rodando, edite este orçamento para associar o desligamento automático.

---



## 3. Otimização de Custos específica para o seu Projeto

Para fazer com que os USD 200 durem os 6 meses (ou o tempo de desenvolvimento do seu projeto), faça as seguintes alterações arquiteturais:

### A. Substitua o Amazon MSK por EC2 ou Docker
* **Por que?** O Amazon MSK custa pelo menos USD 100 a USD 200 por mês, mesmo sem uso ativo, pois mantém máquinas ligadas 24/7.
* **Alternativa nuvem:** Suba uma única instância EC2 do tipo `t3.micro` (que entra no nível gratuito de 12 meses da AWS) ou `t2.small` (custa cerca de USD 15/mês se ficar ligada 24/7). Instale o Kafka manualmente nela.
* **Alternativa local:** Execute o Kafka localmente no seu computador via Docker e envie os dados finais diretamente para o S3 na nuvem.

### B. Cuidado com o AWS Glue
* **Por que?** O Glue cobra por DPU por segundo. Se você engenhar um Job Spark rodando ou errar em um loop infinito, pode consumir USD 50 em poucas horas.
* **Solução:** 
  * Sempre configure o parâmetro `Timeout` nos seus Jobs do Glue para no máximo 10 ou 15 minutos (o padrão da AWS às vezes é de 48 horas!).
  * Reduza o número de DPUs temporárias para o mínimo possível (ex: 2 DPUs para testes).
  * Sempre que terminar de testar, certifique-se de que nenhum Crawler ou Job está em execução.

### C. Ciclo de Vida do S3 (S3 Lifecycle)
* Configure uma regra de ciclo de vida no seu bucket S3 para apagar os arquivos das pastas `bronze`, `silver` e `gold` após 14 ou 30 dias. Como é um projeto de estudos, você não precisa acumular gigabytes de dados históricos pagando armazenamento para sempre.


## 4. É possível criar um bloqueio automático (Hard Limit)?

> [!WARNING]
> A AWS **não possui** um botão nativo simples de "parar tudo" ao atingir um valor, pois interromper serviços de forma abrupta pode corromper bancos de dados ou deletar dados importantes.

No entanto, você pode configurar uma **Ação de Orçamento (Budget Action)** para restringir a conta e impedir a criação de novos recursos, ou desligar instâncias específicas de forma automatizada.

### Alternativa 1: Bloqueio de Permissões via IAM (Recomendado)
Você pode configurar o AWS Budgets para aplicar uma política restritiva que impede qualquer nova criação de recursos quando o orçamento atingir 100%.

1. No passo de criação do orçamento (ou editando um existente), vá até a seção **Configure actions (Configurar ações)**.
2. Adicione uma ação para o limite de 100% (USD 200).
3. Selecione o tipo de ação: **IAM Policy (Política IAM)** ou **Service Control Policy (SCP)**.
4. Você pode aplicar uma política que anexa um `Deny` para criação de novos recursos (ex: negar `ec2:RunInstances`, `msk:CreateCluster`, `glue:StartJobRun`) aos seus usuários desenvolvedores.

### Alternativa 2: Desligamento automático de EC2 (para o Kafka)
Se você optar por rodar o Kafka em uma instância EC2 (em vez do caro MSK):
1. Nas **Ações de Orçamento (Budget Actions)**, escolha a opção **Systems Manager (SSM) action**.
2. Selecione a ação para **Parar instâncias EC2** (*Stop EC2 instances*).
3. Selecione a instância que está rodando o seu Kafka. Ao bater USD 200, a AWS desligará a máquina automaticamente para estancar o consumo de dinheiro.

---