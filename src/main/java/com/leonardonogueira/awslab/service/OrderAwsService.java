package com.leonardonogueira.awslab.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardonogueira.awslab.domain.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * PASSO 3 & 4: Servico que integra os pilares da AWS:
 * 1. AWS SSM Parameter Store -> Leitura dinamica de parametro (/ecommerce/config/cashback-percent)
 * 2. Amazon DynamoDB         -> Escrita Condicional Idempotente (attribute_not_exists(orderId))
 * 3. Amazon SNS              -> Publicacao Pub/Sub (Fan-Out para payment-queue e invoice-queue no SQS)
 * 4. Amazon S3 Pre-Signed URL-> Gera link temporario (5 min) assinado criptograficamente para baixar a Nota Fiscal
 */
@Service
public class OrderAwsService {

    private static final Logger log = LoggerFactory.getLogger(OrderAwsService.class);

    private static final String DYNAMODB_TABLE = "Orders";
    private static final String S3_BUCKET = "order-invoices-bucket";
    private static final String SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:000000000000:order-events-topic";
    private static final String SSM_CASHBACK_PARAM = "/ecommerce/config/cashback-percent";

    private final DynamoDbClient dynamoDbClient;
    private final SnsClient snsClient;
    private final SsmClient ssmClient;
    private final S3Presigner s3Presigner;
    private final ObjectMapper objectMapper;

    public OrderAwsService(
            DynamoDbClient dynamoDbClient,
            SnsClient snsClient,
            SsmClient ssmClient,
            S3Presigner s3Presigner,
            ObjectMapper objectMapper) {
        this.dynamoDbClient = dynamoDbClient;
        this.snsClient = snsClient;
        this.ssmClient = ssmClient;
        this.s3Presigner = s3Presigner;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> createOrder(String orderId, String customerName, String productSku, BigDecimal amount) {
        // 1. Busca a porcentagem de cashback no AWS SSM Parameter Store
        String cashbackStr = ssmClient.getParameter(
                GetParameterRequest.builder().name(SSM_CASHBACK_PARAM).build()
        ).parameter().value();

        BigDecimal cashbackRate = new BigDecimal(cashbackStr);
        BigDecimal cashbackAmount = amount.multiply(cashbackRate).setScale(2, RoundingMode.HALF_UP);
        Instant now = Instant.now();

        // 2. Monta o item chave-valor/documento para o Amazon DynamoDB
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("orderId", AttributeValue.fromS(orderId)); // Partition Key (PK)
        item.put("customerName", AttributeValue.fromS(customerName));
        item.put("productSku", AttributeValue.fromS(productSku));
        item.put("amount", AttributeValue.fromN(amount.toPlainString()));
        item.put("cashbackAmount", AttributeValue.fromN(cashbackAmount.toPlainString()));
        item.put("status", AttributeValue.fromS("CREATED"));
        item.put("createdAt", AttributeValue.fromS(now.toString()));

        try {
            dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(DYNAMODB_TABLE)
                    .item(item)
                    .conditionExpression("attribute_not_exists(orderId)")
                    .build());

            log.info("✅ [DynamoDB] Pedido '{}' salvo com sucesso na tabela '{}' (Cashback calculado via SSM: R$ {})",
                    orderId, DYNAMODB_TABLE, cashbackAmount);

        } catch (ConditionalCheckFailedException ex) {
            log.warn("🛑 [DynamoDB Idempotency] Pedido '{}' JA EXISTE no DynamoDB! Retornando pedido existente sem republicar no SNS.", orderId);
            return Map.of(
                    "created", false,
                    "idempotentReplay", true,
                    "message", "Pedido ja existia no DynamoDB (bloqueado por conditionExpression: attribute_not_exists(orderId))",
                    "order", getOrderById(orderId).orElse(Map.of())
            );
        }

        // 3. Publica 1 unico evento no Topico Amazon SNS (que fara Fan-Out para as 2 filas SQS!)
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId, customerName, productSku, amount, cashbackAmount, "CREATED", now
        );

        String payloadJson = toJson(event);
        PublishResponse publishResponse = snsClient.publish(PublishRequest.builder()
                .topicArn(SNS_TOPIC_ARN)
                .message(payloadJson)
                .build());

        log.info("📢 [Amazon SNS] Evento OrderCreated publicado no topico '{}' (SNS MessageId: {}). " +
                 "O SNS distribuiu copias para 'payment-queue' e 'invoice-queue'!",
                SNS_TOPIC_ARN, publishResponse.messageId());

        return Map.of(
                "created", true,
                "idempotentReplay", false,
                "snsMessageId", publishResponse.messageId(),
                "snsTopicArn", SNS_TOPIC_ARN,
                "order", event
        );
    }

    public Optional<Map<String, String>> getOrderById(String orderId) {
        var response = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(DYNAMODB_TABLE)
                .key(Map.of("orderId", AttributeValue.fromS(orderId)))
                .build());

        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }

        Map<String, String> result = new LinkedHashMap<>();
        response.item().forEach((k, v) -> result.put(k, v.s() != null ? v.s() : v.n()));
        return Optional.of(result);
    }

    /**
     * PASSO 4: Gera uma URL Pre-Assinada (S3 Pre-Signed URL) com validade de 5 minutos.
     * Conceito da Prova CLF-C02:
     * - Como permitir que um usuario baixe um arquivo privado do S3 temporariamente sem tornar o Bucket publico?
     * - Resposta: S3 Pre-Signed URL (URL Pre-Assinada)!
     */
    public Optional<Map<String, String>> generateInvoicePresignedUrl(String orderId) {
        Optional<Map<String, String>> orderOpt = getOrderById(orderId);
        if (orderOpt.isEmpty() || !orderOpt.get().containsKey("invoiceS3Key")) {
            return Optional.empty();
        }

        String s3Key = orderOpt.get().get("invoiceS3Key");

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(S3_BUCKET)
                .key(s3Key)
                .build();

        PresignedGetObjectRequest presignedRequest = s3Presigner.presignGetObject(
                GetObjectPresignRequest.builder()
                        .signatureDuration(Duration.ofMinutes(5))
                        .getObjectRequest(getObjectRequest)
                        .build()
        );

        return Optional.of(Map.of(
                "orderId", orderId,
                "s3Bucket", S3_BUCKET,
                "s3Key", s3Key,
                "expiresIn", "5 minutes",
                "presignedUrl", presignedRequest.url().toString()
        ));
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Erro ao serializar evento para JSON", e);
        }
    }
}
