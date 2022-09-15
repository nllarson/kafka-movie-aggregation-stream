package com.example.msa.streams;

import com.example.msa.avro.MovieTicketSales;
import com.example.msa.avro.YearlyMovieFigures;
import com.example.msa.properties.ApplicationProperties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class MovieSalesAggregationStream {

    private final ApplicationProperties applicationProperties;
    private final StreamsBuilder streamsBuilder;
    private final SpecificAvroSerde<MovieTicketSales> ticketSaleSerde;
    private final SpecificAvroSerde<YearlyMovieFigures> movieFiguresSerde;

    @Bean
    public void movieTicketSalesStream() {
        streamsBuilder.stream(
                        applicationProperties.getInputTopic(),
                        Consumed.with(Serdes.String(), ticketSaleSerde))
                .peek((k,v) -> {
                    log.info("Processing Movie Ticket Sale Event for : {}", v.getTitle());
                }).selectKey((k,v) -> v.getReleaseYear())
                .groupBy(
                        (k,v) -> v.getReleaseYear(),
                        Grouped.with(Serdes.Integer(), ticketSaleSerde))
                .aggregate(
                        () -> new YearlyMovieFigures(0, Integer.MAX_VALUE, Integer.MIN_VALUE),
                        ((k,v,aggregate) ->
                                new YearlyMovieFigures(k, Math.min(v.getTotalSales(), aggregate.getMinTotalSales()),
                                        Math.max(v.getTotalSales(), aggregate.getMaxTotalSales()))),
                        Materialized.with(Serdes.Integer(), movieFiguresSerde)
                ).toStream()
                        .peek((k,v) -> {
                            log.info("Aggregation updated : {} - {}", k.toString(), v.toString());
                        })
                .to(applicationProperties.getOutputTopic(), Produced.with(Serdes.Integer(), movieFiguresSerde));
    }
}
