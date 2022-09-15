package com.example.msa.properties;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;


@Component
@ConfigurationProperties(prefix="application")
@Data
@Validated
public class ApplicationProperties {

    @NotBlank
    private String inputTopic;

    @NotBlank
    private String outputTopic;
}
