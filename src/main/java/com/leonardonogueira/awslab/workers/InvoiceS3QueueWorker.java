package com.leonardonogueira.awslab.workers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leonardonogueira.awslab.domain.OrderCreatedEvent;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * PASSO 4 (Worker 2): Consumidor da fila SQS 'invoice-queue'.
 *
 * Roda em PARALELO com o PaymentQueueWorker gracas ao Fan-Out do Amazon SNS!
 * O que faz:
 * 1. Gera o arquivo da Nota Fiscal Eletronica do pedido.
 * 2. Faz upload (PutObject) para o Bucket S3 'order-invoices-bucket' (que tem Versioning ativo!).
 * 3. Grava a chave do objeto S3 (invoiceS3Key) e o VersionId gerado pelo S3 na tabela 'Orders' do DynamoDB.
 */
@Component
public class InvoiceS3QueueWorker {

    private static final Logger log = LoggerFactory.getLogger(InvoiceS3QueueWorker.class);
    private static final String S3_BUCKET = "order-invoices-bucket";
    private static final String DYNAMODB_TABLE = "Orders";

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper objectMapper;

    public InvoiceS3QueueWorker(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.objectMapper = objectMapper;
    }

    @SqsListener("invoice-queue")
    public void generateAndUploadInvoice(String messageJson) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(messageJson, OrderCreatedEvent.class);

        String s3ObjectKey = "invoices/NF-" + event.orderId() + ".txt";
        String invoiceContent = """
                ================================================================
                NOTA FISCAL ELETRONICA - AWS LOCALSTACK E-COMMERCE LAB
                ================================================================
                Numero do Pedido (OrderId) : %s
                Cliente                    : %s
                Produto (SKU)              : %s
                Valor Total                : R$ %s
                Cashback Concedido (SSM)   : R$ %s
                Data de Emissao            : %s
                Bucket S3 de Origem        : s3://%s/%s
                ================================================================
                """.formatted(
                event.orderId(),
                event.customerName(),
                event.productSku(),
                event.amount(),
                event.cashbackAmount(),
                event.createdAt(),
                S3_BUCKET,
                s3ObjectKey
        );

        // 1. Faz upload do arquivo da Nota Fiscal para o Amazon S3
        PutObjectResponse putResponse = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(S3_BUCKET)
                        .key(s3ObjectKey)
                        .contentType("text/plain; charset=utf-8")
                        .metadata(Map.of(
                                "order-id", event.orderId(),
                                "customer", event.customerName()
                        ))
                        .build(),
                RequestBody.fromString(invoiceContent, StandardCharsets.UTF_8)
        );

        log.info("🪣 [SQS invoice-queue -> Amazon S3] Nota Fiscal salva em 's3://{}/{}' (S3 VersionId: {})",
                S3_BUCKET, s3ObjectKey, putResponse.versionId());

        // 2. Registra o caminho do arquivo S3 no item do pedido no DynamoDB
        dynamoDbClient.updateItem(UpdateItemRequest.builder()
                .tableName(DYNAMODB_TABLE)
                .key(Map.of("orderId", AttributeValue.fromS(event.orderId())))
                .updateExpression("SET invoiceS3Key = :s3Key, invoiceS3VersionId = :verId")
                .expressionAttributeValues(Map.of(
                        ":s3Key", AttributeValue.fromS(s3ObjectKey),
                        ":verId", AttributeValue.fromS(putResponse.versionId() != null ? putResponse.versionId() : "v1")
                ))
                .build());
    }
}

