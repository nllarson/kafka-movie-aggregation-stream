package com.example.msa.config;

import com.example.msa.avro.MovieTicketSales;
import com.example.msa.avro.YearlyMovieFigures;
import com.example.msa.properties.KafkaProperties;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConfig {
    private final KafkaProperties kafkaProperties;

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        return new CachedSchemaRegistryClient(kafkaProperties.getSchemaRegistryServers(), 500);
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public SpecificAvroSerde<MovieTicketSales> ticketSaleSerde() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaProperties.getSchemaRegistryServers());
        properties.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, kafkaProperties.getSchemaRegistryUserInfo());
        properties.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, kafkaProperties.getSchemaRegistryCredentialsSource());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        properties.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
//        properties.put(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        final SpecificAvroSerde<MovieTicketSales> serde = new SpecificAvroSerde<>();
        serde.configure(properties, false);
        return serde;
    }

    @Bean
    public SpecificAvroSerde<YearlyMovieFigures> movieFiguresSerde() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaProperties.getSchemaRegistryServers());
        properties.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, kafkaProperties.getSchemaRegistryUserInfo());
        properties.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, kafkaProperties.getSchemaRegistryCredentialsSource());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        properties.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        final SpecificAvroSerde<YearlyMovieFigures> serde = new SpecificAvroSerde<>();
        serde.configure(properties,false);
        return serde;
    }

}
