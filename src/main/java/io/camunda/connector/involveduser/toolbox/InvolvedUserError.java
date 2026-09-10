package io.camunda.connector.involveduser.toolbox;

public class InvolvedUserError {

    public static final String ERROR_BAD_INPUTPARAMETER = "BAD_INPUTPARAMETER";
    public static final String ERROR_BAD_INPUTPARAMETER_EXPLANATION = "During the bind, some input does not have the expected type";

    public static final String ERROR_NO_PROCESSINSTANCE = "ERROR_NO_PROCESSINSTANCE";
    public static final String ERROR_NO_PROCESSINSTANCE_EXPLANATION = "The process instance key can't be found in the job context";

    public static final String ERROR_DURING_OPERATION = "ERROR_DURING_OPERATION";
    public static final String ERROR_DURING_OPERATION_EXPLANATION = "Error during the search of user tasks, groups or users";

    public static final String ERROR_NO_CAMUNDA_CLIENT = "ERROR_NO_CAMUNDA_CLIENT";
    public static final String ERROR_NO_CAMUNDA_CLIENT_EXPLANATION = "No CamundaClient is available to search user tasks, groups or users";

    public static final String CANT_FETCH_USER = "CANT_FETCH_USER";
    public static final String CANT_FETCH_USER_EXPLANATION = "Error during fetch user information";
    public static final String CANT_FETCH_GROUP = "CANT_FETCH_GROUP";
    public static final String CANT_FETCH_GROUP_EXPLANATION = "Error during fetch group information";

    private InvolvedUserError() {
    }
}
