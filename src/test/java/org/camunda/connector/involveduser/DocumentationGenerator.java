package org.camunda.connector.involveduser;

import io.camunda.cherry.definition.connector.SdkRunnerCherryConnector;
import org.camunda.connector.cherrytemplate.RunnerParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Generate the documentation in md.
 * InvolvedUser has a single function (no sub-function selection like CalendarAdvance), so the
 * generation is flat: description, inputs, outputs, errors.
 **/
public class DocumentationGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocumentationGenerator.class.getName());

    public static void main(String[] args) {
        DocumentationGenerator documentationGenerator = new DocumentationGenerator();
        documentationGenerator.generate("./doc/", "Functions.md", new InvolvedUserFunction());
    }

    public void generate(String folder, String fileName, InvolvedUserFunction involvedUserFunction) {
        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(folder + fileName))) {
            logger.info("Generating {}/{}", folder, fileName);

            SdkRunnerCherryConnector cherryConnector = new SdkRunnerCherryConnector(involvedUserFunction);
            writeLine(writer, "# " + cherryConnector.getDisplayLabel());
            writeLine(writer, "");
            writeLine(writer, cherryConnector.getDescription());

            writeTitle(writer, "###", "Inputs");
            writeParameters(writer, InvolvedUserInput.allParameters);

            writeTitle(writer, "###", "Outputs");
            writeParameters(writer, InvolvedUserOutput.allParameters);

            writeTitle(writer, "###", "Errors");
            writeErrors(writer, involvedUserFunction.getListBpmnErrors());

            writer.flush();
        } catch (Exception e) {
            logger.error("Exception during generation ", e);
        }
    }

    private void writeLine(BufferedWriter writer, String line) throws IOException {
        writer.write(line);
        writer.newLine();
        logger.info(line);
    }

    private void writeTitle(BufferedWriter writer, String level, String title) throws IOException {
        writer.newLine();
        writer.newLine();
        writer.write(level + " " + title);
        writer.newLine();
        logger.info(level + " " + title);
    }

    private void writeParameters(BufferedWriter writer, List<RunnerParameter> parameters) throws IOException {
        List<Map<String, String>> records = parameters.stream()
                .map(t -> Map.of("Name", t.getName(),
                        "Description", t.label,
                        "Class", t.clazz.getName(),
                        "Level", t.level.name()))
                .toList();
        writeTable(writer, List.of("Name", "Description", "Class", "Level"), records);
        writer.newLine();
    }

    public void writeErrors(BufferedWriter writer, Map<String, String> bpmnErrors) throws IOException {
        List<Map<String, String>> records = bpmnErrors.entrySet().stream()
                .map(t -> Map.of("Name", t.getKey(), "Explanation", t.getValue()))
                .collect(Collectors.toList());
        writeTable(writer, List.of("Name", "Explanation"), records);
        writer.newLine();
    }

    private void writeTable(BufferedWriter writer, List<String> columns, List<Map<String, String>> records) throws IOException {
        Map<String, Integer> sizeColumns = new HashMap<>();
        for (String header : columns) {
            int size = header.length();
            int maxLength = records.stream()
                    .map(m -> m.get(header))
                    .filter(Objects::nonNull)
                    .mapToInt(String::length)
                    .max()
                    .orElse(0);
            sizeColumns.put(header, Math.max(size, maxLength) + 1);
        }
        StringBuffer lineHeader = new StringBuffer();
        StringBuffer lineSeparator = new StringBuffer();
        for (String header : columns) {
            lineHeader.append("| " + complete(header, sizeColumns.get(header), " "));
            lineSeparator.append("|-" + complete("-", sizeColumns.get(header), "-"));
        }
        lineHeader.append("|");
        lineSeparator.append("|");
        writeLine(writer, lineHeader.toString());
        writeLine(writer, lineSeparator.toString());

        for (Map<String, String> record : records) {
            StringBuffer lineRecord = new StringBuffer();
            for (String header : columns) {
                lineRecord.append("| " + complete(record.get(header), sizeColumns.get(header), " "));
            }
            lineRecord.append("|");
            writeLine(writer, lineRecord.toString());
        }
    }

    private String complete(String originalValue, int size, String complement) {
        int remaining = size - originalValue.length();
        if (remaining <= 0) return originalValue;
        return originalValue + complement.repeat(remaining);
    }
}
