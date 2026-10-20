package com.lta.backend.resources;

import com.lta.backend.exceptions.QueryTimeoutException;
import com.lta.backend.model.ClienteConsultaHttpResult;
import com.lta.backend.model.ErrorResponse;
import com.lta.backend.services.ClienteQueryPublisher;
import com.lta.backend.services.ResponseListener;
import com.lta.backend.services.StringProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;


@Slf4j
@RestController
@RequestMapping("/producer")
public class ProducerController {

    @Autowired
    private StringProducerService producer;

    @Autowired
    private ClienteQueryPublisher clienteQueryPublisher;

    @GetMapping("/clientes")
    public ResponseEntity<?> consultarClientes(@RequestParam(name = "filtro", required = false, defaultValue = "") String filtro) {
        try {
            ClienteConsultaHttpResult resultado = clienteQueryPublisher.consultar(filtro);
            return ResponseEntity.ok(resultado);
        } catch (QueryTimeoutException e) {
            log.warn("Timeout en consulta de clientes con filtro '{}': {}", filtro, e.getMessage());
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                    .body(new ErrorResponse("TIMEOUT", e.getMessage(), e.getRequestId()));
        } catch (Exception e) {
            log.error("Error al procesar consulta de clientes con filtro '{}': {}", filtro, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "ERROR_INTERNO", "message", e.getMessage() != null ? e.getMessage() : "Error"));
        }
    }
    @GetMapping("/menu")
    public String sendMenu(@RequestParam String message) throws InterruptedException {
        return sendAndWait(message);
    }

    @GetMapping("/test")
    public String sendTest(@RequestParam String message) throws InterruptedException {
        return sendAndWait(message);
    }

    // MÉTODO REUTILIZABLE
    private String sendAndWait(String message) throws InterruptedException {

        ResponseListener.response = null;

        producer.sendMessage(message);

        int attempts = 0;

        while(ResponseListener.response == null && attempts < 10){
            Thread.sleep(500);
            attempts++;
        }

        return ResponseListener.response != null
                ? ResponseListener.response
                : "No response (posiblemente fue a partición 0)";
    }
}