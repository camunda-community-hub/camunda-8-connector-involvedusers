package org.camunda.connector.involveduser;

import io.camunda.cherry.definition.RunnerDecorationTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElementTemplateGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ElementTemplateGenerator.class.getName());

    public static void generate() {
        try {
            RunnerDecorationTemplate runnerDecorationTemplate = new RunnerDecorationTemplate(new InvolvedUserFunction());
            runnerDecorationTemplate.generateElementTemplate("./element-templates/", "involveduser-function.json");
        } catch (Exception e) {
            logger.error("Error during generation", e);
        }
    }

    public static void main(String[] args) {
        generate();
    }
}
