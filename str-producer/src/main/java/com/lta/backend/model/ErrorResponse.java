package com.lta.backend.model;

/**
 * Estructura de error retornada ante fallos o tiempos de espera agotados.
 */
public record ErrorResponse(
        String error,
        String message,
        String requestId
) {
}
