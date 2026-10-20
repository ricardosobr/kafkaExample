package com.lta.backend.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lta.backend.model.ClienteConsultaRequest;
import com.lta.backend.model.ClienteConsultaResponse;
import com.lta.backend.services.ClienteQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Listener encargado de procesar solicitudes de consulta de clientes
 * y publicar los resultados en el topico de respuesta.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ClienteQueryListener {

    private final ObjectMapper objectMapper;
    private final ClienteQueryService clienteQueryService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "cliente-consulta", groupId = "cliente-query-group")
    public void procesarConsulta(ConsumerRecord<String, String> record) {
        log.info("Mensaje recibido de cliente-consulta -> partition: {}, offset: {}, key: {}, payload: {}",
                record.partition(), record.offset(), record.key(), record.value());

        try {
            ClienteConsultaRequest request = objectMapper.readValue(record.value(), ClienteConsultaRequest.class);
            ClienteConsultaResponse response = clienteQueryService.consultar(request);
            String responseJson = objectMapper.writeValueAsString(response);

            // Publica la respuesta en cliente-consulta-respuesta con key = requestId
            kafkaTemplate.send("cliente-consulta-respuesta", request.requestId(), responseJson)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Error al publicar respuesta en Kafka para requestId={}: {}",
                                    request.requestId(), ex.getMessage(), ex);
                        } else {
                            log.info("Respuesta enviada a topic: {}, partition: {}, offset: {}, key: {}",
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset(),
                                    request.requestId());
                        }
                    });
        } catch (Exception e) {
            log.error("Fallo al deserializar o procesar solicitud de cliente: {}", e.getMessage(), e);
        }
    }
}
