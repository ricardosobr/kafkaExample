package com.lta.backend.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lta.backend.model.ClienteConsultaResponse;
import com.lta.backend.services.ClienteQueryPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listener que consume respuestas del topico cliente-consulta-respuesta
 * y resuelve los futures correlacionados en ClienteQueryPublisher.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ClienteQueryResponseListener {

    private final ObjectMapper objectMapper;
    private final ClienteQueryPublisher clienteQueryPublisher;

    @KafkaListener(topics = "cliente-consulta-respuesta", groupId = "cliente-query-response-group")
    public void recibirRespuesta(ConsumerRecord<String, String> record) {
        log.info("Mensaje de respuesta recibido en topic: {}, partition: {}, offset: {}, key: {}",
                record.topic(), record.partition(), record.offset(), record.key());

        try {
            ClienteConsultaResponse response = objectMapper.readValue(record.value(), ClienteConsultaResponse.class);
            clienteQueryPublisher.completeQuery(response.requestId(), response);
        } catch (Exception e) {
            log.error("Error al deserializar la respuesta de consulta de clientes: {}", e.getMessage(), e);
        }
    }
}
