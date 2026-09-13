package org.camunda.connector.involveduser.junit;

import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.JobContext;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import org.camunda.connector.involveduser.InvolvedUserFunction;
import org.camunda.connector.involveduser.InvolvedUserInput;
import org.camunda.connector.involveduser.toolbox.InvolvedUserError;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestInput {
    private final Logger logger = LoggerFactory.getLogger(TestInput.class.getName());

    /**
     * When InvolvedUserFunction is instantiated without a CamundaClient (i.e. not run as a Spring
     * bean - the no-arg constructor used for SPI/reflection), it must fail fast with a clear
     * BPMN error rather than a NullPointerException.
     */
    @Test
    public void testNoCamundaClient() {
        InvolvedUserInput involvedUserInput = new InvolvedUserInput();
        involvedUserInput.filterTask = List.of();

        OutboundConnectorContext context = mock(OutboundConnectorContext.class);
        JobContext jobContext = mock(JobContext.class);
        when(context.bindVariables(InvolvedUserInput.class)).thenReturn(involvedUserInput);
        when(context.getJobContext()).thenReturn(jobContext);
        when(jobContext.getProcessInstanceKey()).thenReturn(123456789L);

        InvolvedUserFunction involvedUserFunction = new InvolvedUserFunction();
        try {
            involvedUserFunction.execute(context);
            fail("Expected a ConnectorException since no CamundaClient is available");
        } catch (ConnectorException ce) {
            assertEquals(InvolvedUserError.ERROR_NO_CAMUNDA_CLIENT, ce.getErrorCode());
            logger.info("Received ERROR_NO_CAMUNDA_CLIENT as expected");
        } catch (Exception e) {
            logger.error("testNoCamundaClient", e);
            fail(e.getMessage());
        }
    }

    @Test
    public void testBadInputParameter() {
        OutboundConnectorContext context = mock(OutboundConnectorContext.class);
        when(context.bindVariables(InvolvedUserInput.class)).thenThrow(new RuntimeException("bad type"));

        InvolvedUserFunction involvedUserFunction = new InvolvedUserFunction();
        try {
            involvedUserFunction.execute(context);
            fail("Expected a ConnectorException since bindVariables failed");
        } catch (ConnectorException ce) {
            assertEquals(InvolvedUserError.ERROR_BAD_INPUTPARAMETER, ce.getErrorCode());
            logger.info("Received ERROR_BAD_INPUTPARAMETER as expected");
        } catch (Exception e) {
            logger.error("testBadInputParameter", e);
            fail(e.getMessage());
        }
    }
}
