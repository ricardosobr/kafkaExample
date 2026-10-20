package com.lta.backend.model;

/**
 * Modelo que representa a un cliente.
 */
public record Cliente(
        Long id,
        String nombre,
        String email,
        String ciudad,
        String telefono
) {
}
