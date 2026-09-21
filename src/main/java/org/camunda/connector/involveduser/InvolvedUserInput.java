package org.camunda.connector.involveduser;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.camunda.connector.cherrytemplate.CherryInput;
import org.camunda.connector.cherrytemplate.RunnerParameter;
import org.camunda.connector.involveduser.toolbox.ParameterToolbox;

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

    public static final String INCLUDE_USERS = "includeUsers";

    public static final RunnerParameter parameterIncludeUsers = new RunnerParameter(
            InvolvedUserInput.INCLUDE_USERS, // name
            "Include users", // label
            Object.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Add user in the involved list on each task. Input is a String of UserName, separate by comma, or a List of UserName (String)")
            .setVisibleInTemplate();

    public static final String INCLUDE_GROUPS = "includeGroups";

    public static final RunnerParameter parameterIncludeGroups = new RunnerParameter(
            InvolvedUserInput.INCLUDE_GROUPS, // name
            "Include groups", // label
            Object.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Add every member of these groups in the involved list on each task. Input is a String of GroupId, separate by comma, or a List of GroupId (String)")
            .setVisibleInTemplate();

    public static final String EXCLUDE_GROUPS = "excludeGroups";

    public static final RunnerParameter parameterExcludeGroups = new RunnerParameter(
            InvolvedUserInput.EXCLUDE_GROUPS, // name
            "Exclude groups", // label
            Object.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Candidate groups to ignore when computing the involved users. Input is a String of GroupId, separate by comma, or a List of GroupId (String)")
            .setVisibleInTemplate();

    public static final String EXCLUDE_USERS = "excludeUsers";

    public static final RunnerParameter parameterExcludeUsers = new RunnerParameter(
            InvolvedUserInput.EXCLUDE_USERS, // name
            "Exclude users", // label
            Object.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Users to ignore when computing the involved users (assignee, candidate users, members of candidate groups). Input is a String of UserName, separate by comma, or a List of UserName (String)")
            .setVisibleInTemplate();

    public static final String HTTP_TASK_LIST = "httpTaskList";

    public static final RunnerParameter parameterHttpTaskList = new RunnerParameter(
            InvolvedUserInput.HTTP_TASK_LIST, // name
            "Tasklist base URL", // label
            String.class, // class
            RunnerParameter.Level.OPTIONAL, // level
            "Base URL of your Tasklist, used to build a direct link to each user task (e.g. https://your-camunda-host/tasklist). "
                    + "On Camunda 8 SaaS this is calculated automatically from the cluster's region and cluster id "
                    + "(for example https://jfk-1.api.camunda.io/f9329610-bb97-4ae4-b666-45664111fc66) and this input is ignored - "
                    + "only set it for a Self-Managed cluster, without a trailing slash.")
            .setVisibleInTemplate();

    public static final List<RunnerParameter> allParameters = List.of(
            parameterFilterTask,
            parameterFailifError,
            parameterMaxUsersReported,
            parameterIncludeUsers,
            parameterExcludeUsers,
            parameterIncludeGroups,
            parameterExcludeGroups,
            parameterHttpTaskList);

    public List<String> filterTask;
    public Boolean failIfError = true;
    public Integer maxUsersReported = 100;
    public Object includeUsers;
    public Object includeGroups;
    public Object excludeGroups;
    public Object excludeUsers;
    public String httpTaskList;

    public Object getFilterTask() {
        return filterTask;
    }

    public Boolean getFailIfError() {
        return failIfError;
    }

    public Integer getMaxUsersReported() {
        return maxUsersReported;
    }

    public Object getIncludeUsers() {
        return includeUsers;
    }

    public Object getIncludeGroups() {
        return includeGroups;
    }

    public Object getExcludeGroups() {
        return excludeGroups;
    }

    public Object getExcludeUsers() {
        return excludeUsers;
    }

    public String getHttpTaskList() {
        return httpTaskList;
    }

    @Override
    public List<Map<String, Object>> getInputParameters() {
        return ParameterToolbox.getInputParameters(allParameters);
    }
}
