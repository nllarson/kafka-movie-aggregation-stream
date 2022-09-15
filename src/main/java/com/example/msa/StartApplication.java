package com.example.msa;

import com.example.msa.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@Slf4j
@RequiredArgsConstructor
public class StartApplication implements ApplicationListener<ApplicationStartedEvent> {

    private final ApplicationContext applicationContext;
    private final KafkaProperties kafkaProperties;
    private final StreamsBuilder streamsBuilder;
    private KafkaStreams kafkaStreams;

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        Topology topology = streamsBuilder.build();
        log.debug("topology: \n{}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, kafkaProperties.toProperties());

        // any fatal stream exception should cause the application to shut down
        // this allows the orchestration to restart it
        kafkaStreams.setUncaughtExceptionHandler((thread, exception) -> {
            log.error("fatal streams exception: {} {}", exception.getClass(), exception.getMessage());
            if (kafkaStreams.state().isRunningOrRebalancing()) {
                kafkaStreams.close();
            }
            SpringApplication.exit(applicationContext, () -> 0);
        });

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
