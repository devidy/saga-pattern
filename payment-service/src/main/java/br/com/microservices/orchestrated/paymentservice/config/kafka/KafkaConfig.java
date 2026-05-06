package br.com.microservices.orchestrated.paymentservice.config.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    /**
     * Creates a ConsumerFactory for Kafka consumers.
     * This factory is used by Spring to instantiate Kafka consumers for @KafkaListener annotated methods.
     *
     * @return ConsumerFactory configured with consumer properties
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * Builds the configuration map for Kafka consumers.
     * Includes bootstrap servers, group ID, deserializers, and offset reset policy.
     *
     * @return Map of consumer configuration properties
     */
    private Map<String, Object> consumerConfigs() {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return props;
    }

    /**
     * Creates a ProducerFactory for Kafka producers.
     * This factory is used to create Kafka producers for sending messages.
     *
     * @return ProducerFactory configured with producer properties
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    /**
     * Builds the configuration map for Kafka producers.
     * Includes bootstrap servers and serializers for key and value.
     *
     * @return Map of producer configuration properties
     */
    private Map<String, Object> producerConfig() {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

    /**
     * Creates a KafkaTemplate for sending messages to Kafka topics.
     * This template simplifies producer operations and handles serialization.
     *
     * @param producerFactory the ProducerFactory to use for creating producers
     * @return KafkaTemplate configured for sending String messages
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
