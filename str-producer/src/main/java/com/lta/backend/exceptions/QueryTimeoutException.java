package com.lta.backend.exceptions;

/**
 * Excepcion lanzada cuando una consulta a traves de Kafka no recibe respuesta
 * dentro del limite de tiempo establecido.
 */
public class QueryTimeoutException extends RuntimeException {

    private final String requestId;

    public QueryTimeoutException(String requestId, String message) {
        super(message);
        this.requestId = requestId;
    }

    public String getRequestId() {
        return requestId;
    }
}
