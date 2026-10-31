package com.leonardonogueira.awslab.workers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardonogueira.awslab.domain.OrderCreatedEvent;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.util.Map;

/**
 * PASSO 4 (Worker 1): Consumidor da fila SQS 'payment-queue'.
 *
 * O que voce aprende aqui para a prova CLF-C02 e para System Design:
 * 1. Modelo Pull (SQS): O Spring Cloud AWS faz Long Polling na 'payment-queue'.
 * 2. AWS Secrets Manager: Busca a chave secreta do Gateway de Pagamento ('ecommerce/payment-gateway/api-key').
 * 3. Dead-Letter Queue (DLQ): Se o SKU do produto contiver "FAIL", lancamos excecao!
 *    O SQS tentara entregar 3 vezes (maxReceiveCount=3) e depois movera a mensagem
 *    automaticamente para a fila 'payment-dlq'!
 */
@Component
public class PaymentQueueWorker {

    private static final Logger log = LoggerFactory.getLogger(PaymentQueueWorker.class);
    private static final String DYNAMODB_TABLE = "Orders";
    private static final String SECRET_NAME = "ecommerce/payment-gateway/api-key";

    private final SecretsManagerClient secretsManagerClient;
    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper objectMapper;

    public PaymentQueueWorker(
            SecretsManagerClient secretsManagerClient,
            DynamoDbClient dynamoDbClient,
            ObjectMapper objectMapper) {
        this.secretsManagerClient = secretsManagerClient;
        this.dynamoDbClient = dynamoDbClient;
        this.objectMapper = objectMapper;
    }

    @SqsListener("payment-queue")
    public void processPayment(String messageJson) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(messageJson, OrderCreatedEvent.class);

        log.info("💳 [SQS payment-queue] Recebido pedido '{}' do cliente '{}' no valor de R$ {}",
                event.orderId(), event.customerName(), event.amount());

        // 1. Simula falha proposital para testar a Dead-Letter Queue (payment-dlq) quando o SKU contem "FAIL"
        if (event.productSku() != null && event.productSku().toUpperCase().contains("FAIL")) {
            log.error("💥 [SQS payment-queue -> DLQ Test] Falha simulada no pagamento do pedido '{}' (SKU: {}). " +
                      "Apos 3 falhas, o SQS enviara esta mensagem automaticamente para a 'payment-dlq'!",
                    event.orderId(), event.productSku());
            throw new IllegalStateException("Falha simulada no Gateway de Pagamento para testar a payment-dlq!");
        }

        // 2. Busca a credencial segura do Gateway no AWS Secrets Manager
        String gatewaySecret = secretsManagerClient.getSecretValue(
                GetSecretValueRequest.builder().secretId(SECRET_NAME).build()
        ).secretString();

        log.info("🔐 [AWS Secrets Manager] Credencial recuperada com sucesso ({}) para aprovar o pedido '{}'",
                gatewaySecret, event.orderId());

        // 3. Atualiza o status do pedido no Amazon DynamoDB para "PAID"
        dynamoDbClient.updateItem(UpdateItemRequest.builder()
                .tableName(DYNAMODB_TABLE)
                .key(Map.of("orderId", AttributeValue.fromS(event.orderId())))
                .updateExpression("SET #st = :paidStatus")
                .expressionAttributeNames(Map.of("#st", "status"))
                .expressionAttributeValues(Map.of(":paidStatus", AttributeValue.fromS("PAID")))
                .build());

        log.info("✅ [SQS payment-queue -> DynamoDB] Pagamento APROVADO! Status do pedido '{}' atualizado para 'PAID' no DynamoDB.",
                event.orderId());
    }
}

