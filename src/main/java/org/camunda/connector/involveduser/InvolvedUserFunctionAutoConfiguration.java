package org.camunda.connector.involveduser;

import io.camunda.client.CamundaClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.Nullable;

/**
 * Registers InvolvedUserFunction as a Spring bean, with the CamundaClient bean that
 * spring-boot-starter-camunda-connectors auto-configures injected in.
 * <p>
 * This is a Spring Boot auto-configuration (picked up via
 * META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports) rather than a
 * plain @Component on InvolvedUserFunction itself, because @Component only registers a bean when the
 * consuming application's own component scan happens to cover this connector's package - most
 * consuming applications scan only their own base package and have no reason to widen it for a
 * third-party connector. Auto-configuration is discovered by Spring Boot's @EnableAutoConfiguration
 * (included in every @SpringBootApplication) independently of component scanning, so this connector
 * works out of the box in any consuming Spring Boot application.
 */
@AutoConfiguration
public class InvolvedUserFunctionAutoConfiguration {

    @Bean
    public InvolvedUserFunction involvedUserFunction(@Nullable CamundaClient camundaClient) {
        return new InvolvedUserFunction(camundaClient);
    }
}
