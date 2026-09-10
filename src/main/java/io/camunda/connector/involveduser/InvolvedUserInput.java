package io.camunda.connector.involveduser;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.camunda.connector.cherrytemplate.CherryInput;
import io.camunda.connector.cherrytemplate.RunnerParameter;
import io.camunda.connector.involveduser.toolbox.ParameterToolbox;

import java.util.List;
import java.util.Map;

/**
 * the JsonIgnoreProperties is mandatory: the template may contain additional widget to help the designer, especially on the OPTIONAL parameters
 * This avoids the MAPPING Exception
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InvolvedUserInput implements CherryInput {

    /**
     * Attention, each Input here must be added in the InvolvedUserFunction, list of InputVariables
     */
    public static final String FILTER_TASK = "filterTask";

    public static final RunnerParameter parameterFilterTask = new RunnerParameter(
            InvolvedUserInput.FILTER_TASK, // name
            "Filter Task Ids", // label
            Object.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "TaskElementId or List<TaskElementId>. Only compute involved users for these task ids (user task element id). Leave empty to process every active user task of the process instance.")
            .setVisibleInTemplate();

    public static final String FAIL_IF_ERROR = "failIfError";

    public static final RunnerParameter parameterFailifError = new RunnerParameter(
            InvolvedUserInput.FAIL_IF_ERROR, // name
            "Fail when user or group are not found", // label
            Boolean.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "If true, the function fails when a candidate/assignee user, or a group, can't be found. If false, the user is skipped.")
            .setDefaultValue("true")
            .setVisibleInTemplate();

    public static final String MAX_USERS_REPORTED = "maxUsersReported";

    public static final RunnerParameter parameterMaxUsersReported = new RunnerParameter(
            InvolvedUserInput.MAX_USERS_REPORTED, // name
            "Max users reported", // label
            Integer.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Maximum number of involved users reported per task.")
            .setDefaultValue("100")
            .setVisibleInTemplate();

    public static final String ADD_USERS = "addUsers";

    public static final RunnerParameter parameterAddUsers = new RunnerParameter(
            InvolvedUserInput.ADD_USERS, // name
            "Add users", // label
            Object.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Add user in the involved list on each task. Input is a String of UserName, separate by comma, or a List of UserName (String)")
            .setVisibleInTemplate();

    public static final List<RunnerParameter> allParameters = List.of(
            parameterFilterTask,
            parameterFailifError,
            parameterMaxUsersReported,
            parameterAddUsers);

    public List<String> filterTask;
    public Boolean failIfError = true;
    public Integer maxUsersReported = 100;
    public Object addUsers;

    public Object getFilterTask() {
        return filterTask;
    }

    public Boolean getFailIfError() {
        return failIfError;
    }

    public Integer getMaxUsersReported() {
        return maxUsersReported;
    }

    public Object getAddUsers() {
        return addUsers;
    }

    @Override
    public List<Map<String, Object>> getInputParameters() {
        return ParameterToolbox.getInputParameters(allParameters);
    }
}
