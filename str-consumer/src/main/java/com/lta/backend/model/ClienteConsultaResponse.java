package com.lta.backend.model;

import java.util.List;

/**
 * Mensaje de respuesta con los clientes encontrados via Kafka.
 */
public record ClienteConsultaResponse(
        String requestId,
        String filtro,
        int total,
        List<Cliente> clientes,
        String procesadoEn
) {
}
