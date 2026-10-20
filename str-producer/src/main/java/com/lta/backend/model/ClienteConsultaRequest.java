package com.lta.backend.model;

/**
 * Mensaje de solicitud de consulta de clientes via Kafka.
 */
public record ClienteConsultaRequest(
        String requestId,
        String filtro,
        long timestamp
) {
}
