package io.camunda.connector.involveduser;

import io.camunda.client.api.search.response.User;
import io.camunda.client.api.search.response.UserTask;
import io.camunda.connector.cherrytemplate.CherryOutput;
import io.camunda.connector.cherrytemplate.RunnerParameter;
import io.camunda.connector.involveduser.toolbox.ParameterToolbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Output of the InvolvedUser connector.
 * A single output variable "involvedUsers" is produced: a Map keyed by taskId, where each value
 * describes the task (taskName, dueDate, documentation) and the list of involved users
 * (candidateUsers + members of candidateGroups + the assignee, deduplicated), each user record
 * carrying the full user information plus an "assignee" boolean flag.
 * <pre>
 * {
 *   "TaskId_1": {
 *     "taskName": ...,
 *     "dueDate": ...,
 *     "documentation": ...,
 *     "involvedUsers": [
 *        { "userId": ..., "email": ..., "assignee": true|false, ... }
 *     ]
 *   }
 * }
 * </pre>
 */
public class InvolvedUserOutput implements CherryOutput {
    private final Logger logger = LoggerFactory.getLogger(InvolvedUserOutput.class.getName());

    public static final String OUTPUT_DETAIL_TASK_INVOLVED_USERS = "detailTaskInvolvedUsers";
    public static final String OUTPUT_INVOLVED_USERS = "involvedUsers";


    public static final RunnerParameter parameterDetailTaskInvolvedUsers = new RunnerParameter(OUTPUT_DETAIL_TASK_INVOLVED_USERS, // name
            "Detail task involved users", // label
            Map.class, // class
            RunnerParameter.Level.REQUIRED, "Map, keyed by task id, of the task information (taskName, dueDate, documentation) and the list of involved users (assignee, candidate users, members of candidate groups)");

    public static final RunnerParameter parameterInvolvedUsers = new RunnerParameter(OUTPUT_INVOLVED_USERS, // name
            "list of involved users", // label
            List.class, // class
            RunnerParameter.Level.REQUIRED, "list of User, all task included");

    public static final List<RunnerParameter> allParameters = List.of(parameterInvolvedUsers,parameterDetailTaskInvolvedUsers);

    // Fields of one task record
    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_TASK_NAME = "taskName";
    public static final String FIELD_DUE_DATE = "dueDate";
    public static final String FIELD_CREATION_DATE = "creationDate";
    public static final String FIELD_COMPLETION_DATE = "completionDate";
    public static final String FIELD_TASK_KEY = "taskKey";
    public static final String FIELD_ASSIGNEE_USER = "assigneeUser";
    public static final String FIELD_CANDIDATE_USERS = "candidateUsers";

    /**
     * Map<taskId, TaskInvolvedUsers>
     */
    private final Map<Long, Object> detailTaskInvolvedUsers = new LinkedHashMap<>();

    private final List<User> involvedUsers = new ArrayList<>();

    public Map<Long, Object> getDetailTaskInvolvedUsers() {
        return detailTaskInvolvedUsers;
    }

    public List<User> getInvolvedUsers() {
        return involvedUsers;
    }

    public void addTask(UserTask userTask, User assignee, List<User> involvedUsersList) {
        // The task may be an iterate task: it may already exist

        Map<String, Object> taskRecord = new HashMap<>();
        taskRecord.put(FIELD_TASK_ID, userTask.getElementId());
        taskRecord.put(FIELD_TASK_NAME, userTask.getName());
        taskRecord.put(FIELD_DUE_DATE, userTask.getDueDate());
        taskRecord.put(FIELD_CREATION_DATE, userTask.getCreationDate());
        taskRecord.put(FIELD_COMPLETION_DATE, userTask.getCompletionDate());
        taskRecord.put(FIELD_TASK_KEY, userTask.getUserTaskKey());

        if (assignee != null) {
            taskRecord.put(FIELD_ASSIGNEE_USER, assignee);
        }
        if (involvedUsersList != null) {
            taskRecord.put(FIELD_CANDIDATE_USERS, involvedUsersList);
        }


        detailTaskInvolvedUsers.put(userTask.getUserTaskKey(), taskRecord);

        if (assignee != null) {
            if (involvedUsers.stream().noneMatch(user -> user.getUsername().equals(assignee.getUsername()))) {
                involvedUsers.add(assignee);
            }
        }
        if (involvedUsersList != null) {
            for (User user : involvedUsersList) {
                if (involvedUsers.stream().noneMatch(u -> u.getUsername().equals(user.getUsername()))) {
                    involvedUsers.add(user);
                }
            }
        }

        logger.info("Task[{}] TaskName[{}], assignee[{}] involvedUser[{}]",
                userTask.getElementId(),
                userTask.getName(),
                assignee == null ? null : assignee.getUsername() + "(" + assignee.getEmail() + ")",
                involvedUsersList == null ? Collections.emptyList() : involvedUsersList.stream()
                        .map(t -> t.getUsername() + "(" + t.getEmail() + ")")
                        .limit(5)
                        .toList());

    }

    @Override
    public List<Map<String, Object>> getOutputParameters() {
        return ParameterToolbox.getOutputParameters(allParameters);
    }
}
