package com.leonardonogueira.awslab.domain;

import java.math.BigDecimal;

/**
 * Payload de entrada para criacao de um novo Pedido via POST /api/orders.
 */
public record CreateOrderRequest(
        String orderId,
        String customerName,
        String productSku,
        BigDecimal amount
) {
}

