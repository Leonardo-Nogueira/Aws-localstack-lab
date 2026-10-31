package com.leonardonogueira.awslab.config;

import io.awspring.cloud.autoconfigure.core.AwsClientBuilderConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.ssm.SsmClient;

import java.net.URI;

/**
 * Configura os clientes AWS (SSM, Secrets Manager, SQS sincrono e S3Presigner) reutilizando
 * o endpoint (http://localhost:4566), regiao (us-east-1) e credenciais definidos no application.yml.
 */
@Configuration
public class AwsClientConfig {

    @Value("${spring.cloud.aws.endpoint:http://localhost:4566}")
    private String awsEndpoint;

    @Value("${spring.cloud.aws.region.static:us-east-1}")
    private String awsRegion;

    @Value("${spring.cloud.aws.credentials.access-key:test}")
    private String accessKey;

    @Value("${spring.cloud.aws.credentials.secret-key:test}")
    private String secretKey;

    @Bean
    @ConditionalOnMissingBean
    public SsmClient ssmClient(AwsClientBuilderConfigurer configurer) {
        return configurer.configure(SsmClient.builder()).build();
    }

    @Bean
    @ConditionalOnMissingBean
    public SecretsManagerClient secretsManagerClient(AwsClientBuilderConfigurer configurer) {
        return configurer.configure(SecretsManagerClient.builder()).build();
    }

    @Bean
    @ConditionalOnMissingBean
    public SqsClient sqsClient(AwsClientBuilderConfigurer configurer) {
        return configurer.configure(SqsClient.builder()).build();
    }

    @Bean
    @ConditionalOnMissingBean
    public S3Presigner s3Presigner() {
        return S3Presigner.builder()
                .endpointOverride(URI.create(awsEndpoint))
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }
}
