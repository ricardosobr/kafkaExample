package com.lta.backend.config;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;

@Configuration
public class KafkaAdminConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    // Configura la conexión administrativa con Kafka
    @Bean
    public KafkaAdmin kafkaAdmin(){
        var configs = new HashMap<String,Object>();
        String bootstrap = (kafkaProperties.getBootstrapServers() != null && !kafkaProperties.getBootstrapServers().isEmpty())
                ? String.join(",", kafkaProperties.getBootstrapServers())
                : "localhost:9092";
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return new KafkaAdmin(configs);
    }

    // Crea automáticamente los tópicos al iniciar la aplicación
    // Réplica 1 porque el clúster local tiene un solo broker de Kafka
    @Bean
    public KafkaAdmin.NewTopics topics(){
        return new KafkaAdmin.NewTopics(
            TopicBuilder.name("str-topic").partitions(2).replicas(1).build(),
            TopicBuilder.name("str-topic-response").partitions(2).replicas(1).build(),
            TopicBuilder.name("cliente-consulta").partitions(3).replicas(1).build(),
            TopicBuilder.name("cliente-consulta-respuesta").partitions(3).replicas(1).build()
        );
    }
}
