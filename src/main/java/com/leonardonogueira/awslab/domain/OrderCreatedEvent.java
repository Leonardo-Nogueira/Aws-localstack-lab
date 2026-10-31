package com.leonardonogueira.awslab.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Representa o Pedido salvo no Amazon DynamoDB e o Evento publicado no Topico Amazon SNS.
 */
public record OrderCreatedEvent(
        String orderId,
        String customerName,
        String productSku,
        BigDecimal amount,
        BigDecimal cashbackAmount,
        String status,
        Instant createdAt
) {
}

