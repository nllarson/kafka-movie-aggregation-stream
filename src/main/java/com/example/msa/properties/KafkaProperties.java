package com.example.msa.properties;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Component
@ConfigurationProperties(prefix = "kafka")
@Data
@Validated
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProperties {

    // common kafka properties
    @NotEmpty
    private List<String> bootstrapServers;

    @NotEmpty
    private List<String> schemaRegistryServers;

    @NotBlank
    private String schemaRegistryCredentialsSource = "USER_INFO";

    @NotBlank
    private String schemaRegistryUserInfo;

    @NotBlank
    private String securityProtocol = "PLAINTEXT";

    // kafka streams properties
    @NotBlank
    private String applicationId;

    @NotBlank
    private String keySerde = KafkaAvroSerde.class.getName();

    @NotBlank
    private String valueSerde = KafkaAvroSerde.class.getName();

    @NotNull
    private Boolean specificAvroReader = Boolean.FALSE;

    // ssl config
    private Ssl ssl;

    // sasl/scram config
    private Sasl sasl;


    @Data
    @Validated
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Ssl {
        @NotBlank private String truststoreLocation;
        @NotBlank private String truststorePassword;
    }

    @Data
    @Validated
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Sasl {
        @NotBlank private String mechanism;
        @NotBlank private String jaasUser;
        @NotBlank private String jaasPassword;
    }

    public static class KafkaAvroSerde extends Serdes.WrapperSerde<Object> {
        public KafkaAvroSerde() {
            super(new KafkaAvroSerializer(), new KafkaAvroDeserializer());
        }
    }

    /**
     * Return the KafkaProperties as java.util.Properties
     * @return Properties
     */
    public Properties toProperties() {
        Map<String, Object> properties = new HashMap<>();

        PropertyMapper propertyMapper = PropertyMapper.get().alwaysApplyingWhenNonNull();

        propertyMapper.from(bootstrapServers)
                .to(value -> properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", value)));
        propertyMapper.from(schemaRegistryServers)
                .to(value -> properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, String.join(",", value)));
        propertyMapper.from(securityProtocol)
                .to(value -> properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, value));

        propertyMapper.from(applicationId)
                .to(value -> properties.put(StreamsConfig.APPLICATION_ID_CONFIG, value));
        propertyMapper.from(keySerde)
                .to(value -> properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, value));
        propertyMapper.from(valueSerde)
                .to(value -> properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, value));
        propertyMapper.from(specificAvroReader)
                .to(value -> properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, value.toString()));

        if (ssl != null) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl.getTruststoreLocation());
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.getTruststorePassword());
        }

        if (sasl != null) {
            properties.put(SaslConfigs.SASL_MECHANISM, sasl.getMechanism());
            properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("%s required username=\"%s\" password=\"%s\";",
                            ScramLoginModule.class.getName(),
                            sasl.getJaasUser(), sasl.getJaasPassword()));
        }

        Properties result = new Properties();
        result.putAll(properties);
        return result;
    }
}
