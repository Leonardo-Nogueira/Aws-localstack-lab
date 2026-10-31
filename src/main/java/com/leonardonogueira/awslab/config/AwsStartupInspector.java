package com.leonardonogueira.awslab.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PASSO 2: Inspeciona e valida a comunicacao do Spring Boot com os servicos da AWS no LocalStack
 * e alimenta o Console Visual AWS Local em http://localhost:8081
 */
@RestController
public class AwsStartupInspector implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(AwsStartupInspector.class);

    private final SsmClient ssmClient;
    private final SecretsManagerClient secretsManagerClient;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;
    private final SnsClient snsClient;

    public AwsStartupInspector(
            SsmClient ssmClient,
            SecretsManagerClient secretsManagerClient,
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            SqsClient sqsClient,
            SnsClient snsClient) {
        this.ssmClient = ssmClient;
        this.secretsManagerClient = secretsManagerClient;
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.sqsClient = sqsClient;
        this.snsClient = snsClient;
    }

    @Override
    public void run(String... args) {
        log.info("================================================================================");
        log.info("PASSO 2: CONECTANDO SPRING BOOT NA NUVEM AWS LOCAL (LOCALSTACK - us-east-1)");
        log.info("================================================================================");

        try {
            Map<String, Object> summary = inspectResources();
            log.info("✅ [SSM Parameter Store] /ecommerce/config/cashback-percent = {}", summary.get("ssmCashbackPercent"));
            log.info("✅ [AWS Secrets Manager] ecommerce/payment-gateway/api-key  = {}", summary.get("secretsManagerApiKey"));
            log.info("✅ [Amazon S3 Buckets]   Encontrados: {}", summary.get("s3Buckets"));
            log.info("✅ [Amazon DynamoDB]     Tabelas:     {}", summary.get("dynamoDbTables"));
            log.info("✅ [Amazon SNS Topics]   Topicos:     {}", summary.get("snsTopics"));
            log.info("✅ [Amazon SQS Queues]   Filas:       {}", summary.get("sqsQueues"));
            log.info("================================================================================");
            log.info("🖥️  CONSOLE VISUAL AWS LOCAL DISPONIVEL EM: http://localhost:8081");
            log.info("================================================================================");
        } catch (Exception e) {
            log.warn("⚠️ Nao foi possivel conectar ao LocalStack em http://localhost:4566. Certifique-se de rodar 'docker compose up -d'!", e);
        }
    }

    @GetMapping("/api/aws/status")
    public Map<String, Object> inspectResources() {
        String cashbackPercent = ssmClient.getParameter(
                GetParameterRequest.builder().name("/ecommerce/config/cashback-percent").build()
        ).parameter().value();

        String secretJson = secretsManagerClient.getSecretValue(
                GetSecretValueRequest.builder().secretId("ecommerce/payment-gateway/api-key").build()
        ).secretString();

        // S3 Buckets + Objetos dentro de cada bucket
        List<Map<String, Object>> s3Details = new ArrayList<>();
        s3Client.listBuckets().buckets().forEach(b -> {
            var objects = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(b.name()).build())
                    .contents().stream()
                    .map(obj -> Map.of("key", obj.key(), "size", obj.size(), "lastModified", obj.lastModified().toString()))
                    .toList();
            s3Details.add(Map.of("bucketName", b.name(), "objectCount", objects.size(), "objects", objects));
        });

        // DynamoDB Tabelas + Contagem de Itens
        List<Map<String, Object>> dynamoDetails = new ArrayList<>();
        for (String tableName : dynamoDbClient.listTables().tableNames()) {
            var scan = dynamoDbClient.scan(ScanRequest.builder().tableName(tableName).limit(25).build());
            List<Map<String, String>> items = scan.items().stream().map(item -> {
                Map<String, String> simplified = new LinkedHashMap<>();
                item.forEach((k, v) -> simplified.put(k, v.s() != null ? v.s() : (v.n() != null ? v.n() : v.toString())));
                return simplified;
            }).toList();
            dynamoDetails.add(Map.of("tableName", tableName, "itemCount", scan.count(), "items", items));
        }

        // SNS Topics + Subscriptions
        List<String> snsTopics = snsClient.listTopics().topics().stream().map(t -> t.topicArn()).toList();
        List<Map<String, String>> snsSubscriptions = snsClient.listSubscriptions().subscriptions().stream()
                .map(s -> Map.of("topicArn", s.topicArn(), "protocol", s.protocol(), "endpoint", s.endpoint()))
                .toList();

        // SQS Filas + Quantidade de Mensagens Prontas / Em Voo
        List<Map<String, String>> sqsDetails = new ArrayList<>();
        for (String queueUrl : sqsClient.listQueues().queueUrls()) {
            var attrs = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(
                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                            QueueAttributeName.QUEUE_ARN)
                    .build()).attributes();
            String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
            sqsDetails.add(Map.of(
                    "queueName", queueName,
                    "queueUrl", queueUrl,
                    "queueArn", attrs.getOrDefault(QueueAttributeName.QUEUE_ARN, ""),
                    "messagesAvailable", attrs.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"),
                    "messagesInFlight", attrs.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0")
            ));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("region", "us-east-1 (N. Virginia - LocalStack)");
        result.put("ssmCashbackPercent", cashbackPercent);
        result.put("secretsManagerApiKey", secretJson);
        result.put("s3Buckets", s3Details);
        result.put("dynamoDbTables", dynamoDetails);
        result.put("snsTopics", snsTopics);
        result.put("snsSubscriptions", snsSubscriptions);
        result.put("sqsQueues", sqsDetails);
        return result;
    }
}
