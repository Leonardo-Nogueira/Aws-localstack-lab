#!/bin/bash
set -e

echo "=========================================================================="
echo "INICIANDO PROVISIONAMENTO AUTOMATICO DA INFRAESTRUTURA AWS (LOCALSTACK)"
echo "Regiao Padrao: us-east-1 (N. Virginia)"
echo "=========================================================================="

# ------------------------------------------------------------------------------
# 1. AMAZON S3 (Simple Storage Service) - Armazenamento de Objetos
# Conceito da Prova CLF-C02:
# - S3 armazena OBJETOS (arquivos, PDFs, imagens, backups) em Buckets.
# - Habilitamos o VERSIONING para proteger contra exclusao/sobrescrita acidental!
# ------------------------------------------------------------------------------
echo "-> [1/5] Criando Bucket S3 'order-invoices-bucket' com Versionamento ativo..."
awslocal s3 mb s3://order-invoices-bucket
awslocal s3api put-bucket-versioning \
    --bucket order-invoices-bucket \
    --versioning-configuration Status=Enabled

# ------------------------------------------------------------------------------
# 2. AMAZON DYNAMODB - Banco NoSQL Chave-Valor / Documentos Serverless
# Conceito da Prova CLF-C02:
# - Totalmente gerenciado (Serverless), latencia de milissegundos de 1 digito.
# - PAY_PER_REQUEST (On-Demand): Voce paga por leitura/escrita sem provisionar capacidade fixa.
# ------------------------------------------------------------------------------
echo "-> [2/5] Criando Tabela DynamoDB 'Orders' (On-Demand / Serverless)..."
awslocal dynamodb create-table \
    --table-name Orders \
    --attribute-definitions AttributeName=orderId,AttributeType=S \
    --key-schema AttributeName=orderId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# ------------------------------------------------------------------------------
# 3. AMAZON SQS (Simple Queue Service) + DLQ (Dead-Letter Queue)
# Conceito da Prova CLF-C02:
# - SQS desacopla microsservicos via modelo PULL (polling).
# - DLQ isola mensagens que falharam repetidamente (ex: apos 3 tentativas) para analise.
# ------------------------------------------------------------------------------
echo "-> [3/5] Criando Filas SQS ('payment-queue', 'payment-dlq' e 'invoice-queue')..."
awslocal sqs create-queue --queue-name payment-dlq

PAYMENT_DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/payment-dlq \
    --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Cria a fila principal de pagamentos apontando para a DLQ apos 3 falhas (maxReceiveCount=3)
awslocal sqs create-queue \
    --queue-name payment-queue \
    --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$PAYMENT_DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

awslocal sqs create-queue --queue-name invoice-queue

# ------------------------------------------------------------------------------
# 4. AMAZON SNS (Simple Notification Service) - Padrao FAN-OUT (Pub/Sub)
# Conceito da Prova CLF-C02:
# - SNS e um servico PUSH (Pub/Sub). Um publicador envia 1 mensagem para o Topico SNS,
#   e o SNS distribui copias simultaneas para multiplas filas SQS (Fan-Out)!
# ------------------------------------------------------------------------------
echo "-> [4/5] Criando Topico SNS 'order-events-topic' e conectando as filas SQS (Fan-Out)..."
TOPIC_ARN=$(awslocal sns create-topic --name order-events-topic --query 'TopicArn' --output text)

PAYMENT_QUEUE_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/payment-queue \
    --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

INVOICE_QUEUE_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/invoice-queue \
    --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Inscreve (Subscribe) as duas filas SQS no Topico SNS (RawMessageDelivery=true envia o JSON limpo)
awslocal sns subscribe \
    --topic-arn "$TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$PAYMENT_QUEUE_ARN" \
    --attributes RawMessageDelivery=true

awslocal sns subscribe \
    --topic-arn "$TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$INVOICE_QUEUE_ARN" \
    --attributes RawMessageDelivery=true

# ------------------------------------------------------------------------------
# 5. AWS SSM PARAMETER STORE & AWS SECRETS MANAGER
# Conceito da Prova CLF-C02:
# - SSM Parameter Store: Guarda parametros de configuracao hierarquicos (gratuito no tier padrao).
# - Secrets Manager: Guarda segredos/credenciais criptografados com suporte a ROTACAO AUTOMATICA.
# ------------------------------------------------------------------------------
echo "-> [5/5] Criando configuracao no SSM Parameter Store e segredo no Secrets Manager..."
awslocal ssm put-parameter \
    --name "/ecommerce/config/cashback-percent" \
    --value "0.05" \
    --type "String" \
    --overwrite

awslocal secretsmanager create-secret \
    --name "ecommerce/payment-gateway/api-key" \
    --secret-string '{"gateway":"StripeLocal","apiKey":"sk_test_localstack_998877"}'

echo "=========================================================================="
echo "INFRAESTRUTURA AWS PRONTA NO LOCALSTACK!"
echo "=========================================================================="

