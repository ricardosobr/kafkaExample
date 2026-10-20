package com.lta.backend.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;

/**
 * Configuracion administrativa de Kafka para declarar topicos de forma idempotente.
 */
@Configuration
public class KafkaAdminConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    // Configura la conexion administrativa con Kafka
    @Bean
    public KafkaAdmin kafkaAdmin() {
        var configs = new HashMap<String, Object>();
        String bootstrap = (kafkaProperties.getBootstrapServers() != null && !kafkaProperties.getBootstrapServers().isEmpty())
                ? String.join(",", kafkaProperties.getBootstrapServers())
                : "localhost:9092";
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return new KafkaAdmin(configs);
    }

    // Declaracion de topicos para la consulta de clientes:
    // Se configuran 3 particiones y 1 replica porque el cluster de desarrollo cuenta con un unico broker de Kafka.
    @Bean
    public KafkaAdmin.NewTopics clienteTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("cliente-consulta").partitions(3).replicas(1).build(),
                TopicBuilder.name("cliente-consulta-respuesta").partitions(3).replicas(1).build()
        );
    }
}
