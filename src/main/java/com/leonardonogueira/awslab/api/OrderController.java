package com.leonardonogueira.awslab.api;

import com.leonardonogueira.awslab.domain.CreateOrderRequest;
import com.leonardonogueira.awslab.service.OrderAwsService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderAwsService orderAwsService;

    public OrderController(OrderAwsService orderAwsService) {
        this.orderAwsService = orderAwsService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(@RequestBody CreateOrderRequest request) {
        Map<String, Object> result = orderAwsService.createOrder(
                request.orderId(),
                request.customerName(),
                request.productSku(),
                request.amount());

        boolean created = Boolean.TRUE.equals(result.get("created"));
        return ResponseEntity
                .status(created ? HttpStatus.CREATED : HttpStatus.OK)
                .body(result);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<Map<String, String>> getOrder(@PathVariable String orderId) {
        return orderAwsService.getOrderById(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * PASSO 4: Retorna uma S3 Pre-Signed URL temporaria (valida por 5 minutos)
     * para baixar a Nota Fiscal do pedido direto do Bucket privado no Amazon S3!
     */
    @GetMapping("/{orderId}/invoice-url")
    public ResponseEntity<Map<String, String>> getInvoicePresignedUrl(@PathVariable String orderId) {
        return orderAwsService.generateInvoicePresignedUrl(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                        "error", "Nota fiscal ainda nao encontrada para o pedido " + orderId
                )));
    }
}
