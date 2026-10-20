package com.lta.backend.model;

/**
 * Metadatos de Kafka asociados a la operacion request/reply.
 */
public record ClienteKafkaMetadata(
        String topicSolicitud,
        String topicRespuesta,
        int partition,
        long offset,
        String key
) {
}
