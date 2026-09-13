package org.camunda.connector.involveduser.toolbox;

import org.camunda.connector.cherrytemplate.RunnerParameter;

import java.util.List;
import java.util.Map;

/**
 * InvolvedUser is a single-function connector (no sub-function selection, unlike CalendarAdvance),
 * so this toolbox is a straightforward pass-through: it turns the declared RunnerParameter constants
 * into the List&lt;Map&gt; shape expected by CherryInput/CherryOutput.
 */
public class ParameterToolbox {

    /**
     * This is a toolbox, only static method
     */
    private ParameterToolbox() {
    }

    public static List<Map<String, Object>> getInputParameters(List<RunnerParameter> parameters) {
        // the parameterNameForCondition argument is unused by RunnerParameter.toMap() itself (it is
        // only meaningful in CalendarAdvance's multi-sub-function toolbox), so null is passed here.
        return parameters.stream().map(t -> t.toMap(null)).toList();
    }

    public static List<Map<String, Object>> getOutputParameters(List<RunnerParameter> parameters) {
        return parameters.stream().map(t -> t.toMap(null)).toList();
    }
}
