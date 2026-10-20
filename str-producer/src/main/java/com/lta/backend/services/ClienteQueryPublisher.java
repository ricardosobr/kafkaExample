package com.lta.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lta.backend.exceptions.QueryTimeoutException;
import com.lta.backend.model.ClienteConsultaHttpResult;
import com.lta.backend.model.ClienteConsultaRequest;
import com.lta.backend.model.ClienteConsultaResponse;
import com.lta.backend.model.ClienteKafkaMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publicador de consultas de clientes mediante patron Request/Reply con correlacion por requestId.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ClienteQueryPublisher {

    private static final String TOPIC_SOLICITUD = "cliente-consulta";
    private static final String TOPIC_RESPUESTA = "cliente-consulta-respuesta";
    private static final long TIMEOUT_SEGUNDOS = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Mapa concurrente para correlacionar solicitudes pendientes con sus futures de respuesta
    private final ConcurrentMap<String, CompletableFuture<ClienteConsultaResponse>> pendingQueries = new ConcurrentHashMap<>();

    /**
     * Envia una solicitud de consulta a Kafka, espera la respuesta asincrona correlacionada por requestId
     * y retorna el resultado enriquecido con metadatos de Kafka y latencia.
     */
    public ClienteConsultaHttpResult consultar(String filtro) throws Exception {
        String requestId = UUID.randomUUID().toString();
        String filtroNormalizado = (filtro == null) ? "" : filtro;
        long startTime = System.currentTimeMillis();

        ClienteConsultaRequest request = new ClienteConsultaRequest(requestId, filtroNormalizado, startTime);
        String requestJson = objectMapper.writeValueAsString(request);

        CompletableFuture<ClienteConsultaResponse> future = new CompletableFuture<>();
        pendingQueries.put(requestId, future);

        log.info("Publicando consulta de clientes - requestId: {}, filtro: '{}'", requestId, filtroNormalizado);

        SendResult<String, String> sendResult;
        try {
            // Se envia con key = filtro para garantizar que la misma consulta viaje siempre a la misma particion
            sendResult = kafkaTemplate.send(TOPIC_SOLICITUD, filtroNormalizado, requestJson).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            pendingQueries.remove(requestId);
            log.error("Fallo al enviar mensaje al topico {}: {}", TOPIC_SOLICITUD, e.getMessage(), e);
            throw e;
        }

        ClienteConsultaResponse response;
        try {
            // Espera bloqueante con timeout de 10 segundos
            response = future.get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Tiempo de espera agotado ({}s) para requestId: {}", TIMEOUT_SEGUNDOS, requestId);
            throw new QueryTimeoutException(requestId, "Tiempo de espera agotado (" + TIMEOUT_SEGUNDOS + "s) esperando respuesta de Kafka");
        } finally {
            pendingQueries.remove(requestId);
        }

        long latenciaMs = System.currentTimeMillis() - startTime;

        ClienteKafkaMetadata kafkaMetadata = new ClienteKafkaMetadata(
                TOPIC_SOLICITUD,
                TOPIC_RESPUESTA,
                sendResult.getRecordMetadata().partition(),
                sendResult.getRecordMetadata().offset(),
                filtroNormalizado
        );

        return new ClienteConsultaHttpResult(
                response.requestId(),
                response.filtro(),
                response.total(),
                response.clientes(),
                kafkaMetadata,
                latenciaMs,
                response.procesadoEn()
        );
    }

    /**
     * Completa el future asociado a un requestId cuando llega la respuesta desde Kafka.
     */
    public void completeQuery(String requestId, ClienteConsultaResponse response) {
        CompletableFuture<ClienteConsultaResponse> future = pendingQueries.get(requestId);
        if (future != null) {
            future.complete(response);
            log.info("Future completado exitosamente para requestId: {}", requestId);
        } else {
            log.warn("Respuesta recibida para un requestId inexistente o expirado: {}", requestId);
        }
    }
}
