package com.lta.backend.repository;

import com.lta.backend.model.Cliente;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Repositorio en memoria con 12 clientes precargados para demostracion.
 */
@Repository
public class ClienteRepository {

    private final List<Cliente> clientes = new ArrayList<>();

    public ClienteRepository() {
        clientes.add(new Cliente(1L, "Carlos Vegano Martinez", "carlos.vegano@email.com", "Bogota", "+57 300 123 4567"));
        clientes.add(new Cliente(2L, "Maria Lopez", "maria.lopez@veganfood.org", "Medellin", "+57 310 987 6543"));
        clientes.add(new Cliente(3L, "Juan Perez", "juan.perez@empresa.com", "Madrid", "+34 611 222 333"));
        clientes.add(new Cliente(4L, "Laura Gomez", "laura.gomez@tech.co", "Bogota", "+57 320 456 7890"));
        clientes.add(new Cliente(5L, "David Rodriguez", "david.rodriguez@gmail.com", "Barcelona", "+34 622 333 444"));
        clientes.add(new Cliente(6L, "Sofia Fernandez", "sofia.f@consultoria.com", "Cali", "+57 315 234 5678"));
        clientes.add(new Cliente(7L, "Alejandro Ruiz", "alejandro.ruiz@innovacion.es", "Valencia", "+34 633 444 555"));
        clientes.add(new Cliente(8L, "Camila Torres", "camila.torres@startup.co", "Medellin", "+57 301 345 6789"));
        clientes.add(new Cliente(9L, "Mateo Sanchez", "mateo.sanchez@servicios.com", "Sevilla", "+34 644 555 666"));
        clientes.add(new Cliente(10L, "Valentina Diaz", "valentina.diaz@digital.co", "Barranquilla", "+57 312 456 7891"));
        clientes.add(new Cliente(11L, "Gabriel Castro", "gabriel.castro@corp.es", "Bilbao", "+34 655 666 777"));
        clientes.add(new Cliente(12L, "Elena Morales", "elena.morales@global.org", "Cartagena", "+57 318 567 8902"));
    }

    /**
     * Retorna todos los clientes registrados.
     */
    public List<Cliente> todos() {
        return Collections.unmodifiableList(clientes);
    }

    /**
     * Busca clientes aplicando un filtro insensible a mayusculas/minusculas
     * sobre nombre, ciudad o correo electronico.
     */
    public List<Cliente> buscar(String filtro) {
        if (filtro == null || filtro.trim().isEmpty()) {
            return todos();
        }
        String f = filtro.toLowerCase().trim();
        return clientes.stream()
                .filter(c -> (c.nombre() != null && c.nombre().toLowerCase().contains(f)) ||
                             (c.ciudad() != null && c.ciudad().toLowerCase().contains(f)) ||
                             (c.email() != null && c.email().toLowerCase().contains(f)))
                .toList();
    }
}
