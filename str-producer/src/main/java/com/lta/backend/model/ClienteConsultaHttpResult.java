package com.lta.backend.model;

import java.util.List;

/**
 * Respuesta HTTP entregada por el endpoint GET /producer/clientes.
 */
public record ClienteConsultaHttpResult(
        String requestId,
        String filtro,
        int total,
        List<Cliente> clientes,
        ClienteKafkaMetadata kafka,
        long latenciaMs,
        String procesadoEn
) {
}
