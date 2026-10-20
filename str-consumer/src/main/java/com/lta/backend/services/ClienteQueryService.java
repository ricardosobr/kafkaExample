package com.lta.backend.services;

import com.lta.backend.model.Cliente;
import com.lta.backend.model.ClienteConsultaRequest;
import com.lta.backend.model.ClienteConsultaResponse;
import com.lta.backend.repository.ClienteRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Servicio encargado de procesar consultas de clientes recibidas desde Kafka.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ClienteQueryService {

    private final ClienteRepository clienteRepository;

    public ClienteConsultaResponse consultar(ClienteConsultaRequest request) {
        log.info("Ejecutando consulta de clientes - requestId: {}, filtro: '{}'", request.requestId(), request.filtro());
        List<Cliente> encontrados = clienteRepository.buscar(request.filtro());
        log.info("Coincidencias encontradas: {} para requestId: {}", encontrados.size(), request.requestId());

        return new ClienteConsultaResponse(
                request.requestId(),
                request.filtro(),
                encontrados.size(),
                encontrados,
                Instant.now().toString()
        );
    }
}
