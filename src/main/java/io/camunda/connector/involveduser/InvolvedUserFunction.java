package io.camunda.connector.involveduser;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.UserTaskState;
import io.camunda.client.api.search.response.GroupUser;
import io.camunda.client.api.search.response.SearchResponse;
import io.camunda.client.api.search.response.User;
import io.camunda.client.api.search.response.UserTask;
import io.camunda.connector.api.annotation.OutboundConnector;
import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.api.outbound.OutboundConnectorFunction;
import io.camunda.connector.cherrytemplate.CherryConnector;
import io.camunda.connector.involveduser.toolbox.InvolvedUserError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * For each active (state=CREATED) user task of the current process instance (optionally restricted to
 * a list of task ids via the "filterTask" input), collects the "involved users": the assignee, the
 * direct candidate users, and every member of every candidate group - then fetches the full user
 * record (via the CamundaClient user search API) for each of them.
 */
@OutboundConnector(name = "InvolvedUserFunction", inputVariables = {
        InvolvedUserInput.FILTER_TASK,
        InvolvedUserInput.ADD_USERS,
        InvolvedUserInput.MAX_USERS_REPORTED,
        InvolvedUserInput.FAIL_IF_ERROR,
}, type = "c-involveduser-function")
@Component
public class InvolvedUserFunction implements OutboundConnectorFunction, CherryConnector {

    private static final String WORKER_LOGO = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0OCA0OCI+PGNpcmNsZSBjeD0iMjQiIGN5PSIxNiIgcj0iOCIgZmlsbD0iIzAwNzJDZSIvPjxwYXRoIGQ9Ik04IDQwYzAtOC44IDcuMi0xNiAxNi0xNnMxNiA3LjIgMTYgMTYiIGZpbGw9IiMwMDcyQ2UiLz48L3N2Zz4=";

    private final Logger logger = LoggerFactory.getLogger(InvolvedUserFunction.class.getName());

    /**
     * The CamundaClient is not reachable from OutboundConnectorContext in this SDK version
     * (verified via javap on io.camunda.connector.api.outbound.OutboundConnectorContext: it only
     * exposes getJobContext() and bindVariables()). So this connector is registered BOTH as an SPI
     * OutboundConnectorFunction (see META-INF/services, using the no-arg constructor below, with no
     * CamundaClient available - kept for the Cherry / element-template tooling reflection) AND as a
     * Spring bean (connector-runtime-spring picks up @OutboundConnector beans too), letting Spring
     * autowire the CamundaClient bean that spring-boot-starter-camunda-connectors auto-configures.
     * ASSUMPTION: Spring-bean-based CamundaClient injection into an OutboundConnectorFunction is not
     * directly verified via javap (no dedicated interface for it exists in connector-core); it is the
     * most plausible mechanism given the verified absence of any CamundaClient accessor on
     * OutboundConnectorContext.
     */
    @Nullable
    private final CamundaClient camundaClient;

    public InvolvedUserFunction() {
        this(null);
    }

    @Autowired
    public InvolvedUserFunction(@Nullable CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    @Override
    public Object execute(OutboundConnectorContext outboundConnectorContext) throws ConnectorException {
        InvolvedUserInput involvedUserInput;
        try {
            involvedUserInput = outboundConnectorContext.bindVariables(InvolvedUserInput.class);
        } catch (Exception e) {
            logger.error("Bad Input Parameters to bindVariables ", e);
            throw new ConnectorException(InvolvedUserError.ERROR_BAD_INPUTPARAMETER, "InvolvedUser can't bind variable [" + e.getMessage() + "]");
        }

        if (camundaClient == null) {
            logger.error("No CamundaClient available: InvolvedUserFunction must be run as a Spring bean so the CamundaClient can be injected");
            throw new ConnectorException(InvolvedUserError.ERROR_NO_CAMUNDA_CLIENT, InvolvedUserError.ERROR_NO_CAMUNDA_CLIENT_EXPLANATION);
        }

        long processInstanceKey = outboundConnectorContext.getJobContext().getProcessInstanceKey();
        if (processInstanceKey == 0) {
            throw new ConnectorException(InvolvedUserError.ERROR_NO_PROCESSINSTANCE, InvolvedUserError.ERROR_NO_PROCESSINSTANCE_EXPLANATION);
        }



        long beginTime = System.currentTimeMillis();
        InvolvedUserOutput output = new InvolvedUserOutput();

        try {

            Map<String, User> cacheUsers = new HashMap<>();
            // cache group -> members, a group used by several tasks should be resolved only once
            Map<String, List<String>> groupMembersCache = new HashMap<>();
            List<String> listAllUsersCache = new ArrayList<>();


            List<String> filterTaskId = getInputFilterTask(involvedUserInput);
            List<User> addUsersAllTasks = getInputAddUser( involvedUserInput, cacheUsers);



            // 1) search every active (CREATED) user task of this process instance
            // ASSUMPTION: "active" is interpreted as UserTaskState.CREATED (the state a user task has
            // once it has been created and is not yet assigned/completed/canceled).
            List<UserTask> userTasks = camundaClient.newUserTaskSearchRequest()
                    .filter(f -> f.processInstanceKey(processInstanceKey).state(UserTaskState.CREATED))
                    .execute()
                    .items();
            logger.info("InvolvedUserFunction: processInstanceKey={} found {} active (CREATED) user task(s) via search",
                    processInstanceKey, userTasks.size());

            /**
             * Let's produce a result task per task
             */

            for (UserTask userTask : userTasks) {
                String taskId = userTask.getElementId();

                if ((!filterTaskId.isEmpty()) && (!filterTaskId.contains(taskId))) {
                    continue;
                }


                String assignee = userTask.getAssignee();
                List<String> candidateUsers = userTask.getCandidateUsers();
                List<String> candidateGroups = userTask.getCandidateGroups();

                // one assignee : don't need to go over
                if (assignee != null && assignee.length() > 0) {
                    User assigneeUser = fetchUser(assignee, cacheUsers, involvedUserInput.getFailIfError());
                    output.addTask(userTask, assigneeUser, addUsersAllTasks);
                    continue;
                }

                // merge all information
                Set<String> involvedUserName = new LinkedHashSet<>();
                if (candidateUsers != null) {
                    involvedUserName.addAll(candidateUsers);
                }
                if (candidateGroups != null) {
                    for (String group : candidateGroups) {
                        List<String> members = groupMembersCache.computeIfAbsent(group,
                                g -> searchGroupMembers(g, involvedUserInput.getFailIfError(), involvedUserInput.getMaxUsersReported()));
                        involvedUserName.addAll(members);
                    }
                }
                if ((candidateGroups == null || candidateGroups.isEmpty())
                        && (candidateUsers == null || candidateUsers.isEmpty())) {
                    if (listAllUsersCache.isEmpty())
                        listAllUsersCache = searchAllUsers(involvedUserInput.getMaxUsersReported());
                    involvedUserName.addAll(listAllUsersCache);
                }


                // Limit the number of users to report: keep only the first maxUsersReported (insertion order)
                if (involvedUserName.size() > involvedUserInput.getMaxUsersReported()) {
                    involvedUserName = involvedUserName.stream()
                            .limit(involvedUserInput.getMaxUsersReported())
                            .collect(Collectors.toCollection(LinkedHashSet::new));
                }
                // Now fetch all the record for all users
                List<User> involvedUsersList = new ArrayList<>();
                for (String userId : involvedUserName) {
                    User user = fetchUser(userId, cacheUsers, involvedUserInput.getFailIfError());
                    if (user != null) {
                        involvedUsersList.add(user);
                    }
                }

                // Add the user from the list - it's acceptable here to be more than the limit because we want these users
                for (User user : addUsersAllTasks) {
                    if (involvedUsersList.stream().noneMatch(u -> u.getUsername().equals(user.getUsername()))) {
                        involvedUsersList.add(user);
                    }
                }

                output.addTask(userTask, null, involvedUsersList);
            }

            logger.info("InvolvedUserFunction End in {} ms, {} task(s)", System.currentTimeMillis() - beginTime, output.getDetailTaskInvolvedUsers().size());
            return output;
        } catch (ConnectorException ce) {
            throw ce;
        } catch (Exception e) {
            logger.error("Error during InvolvedUserFunction execution", e);
            throw new ConnectorException(InvolvedUserError.ERROR_DURING_OPERATION, InvolvedUserError.ERROR_DURING_OPERATION_EXPLANATION + " [" + e.getMessage() + "]");
        }
    }

    /**
     * Resolve the userIds of every member of a candidate group.
     */
    private List<String> searchGroupMembers(String groupId, boolean failIfError, int maxUsersReported) {
        try {
            List<GroupUser> groupUsers = camundaClient.newUsersByGroupSearchRequest(groupId)
                    .execute()
                    .items();
            return groupUsers.stream().map(GroupUser::getUsername).limit(maxUsersReported).toList();
        } catch (Exception e) {
            logger.error("Can't resolve members of group [{}] : {}", groupId, e.getMessage());
            if (failIfError) {
                throw new ConnectorException(InvolvedUserError.CANT_FETCH_GROUP, "GroupId[" + groupId + "] errors :" + e.getMessage());
            }
            return List.of();
        }
    }

    /**
     * Get all users in the organization (via CamundaClient.newUsersSearchRequest()), paging through
     * the search API's cursor until either every user has been fetched or maxUsersReported has been
     * reached - whichever comes first.
     */
    private List<String> searchAllUsers(int maxUsersReported) {
        List<String> allUsers = new ArrayList<>();
        String cursor = null;

        while (allUsers.size() < maxUsersReported) {
            int pageSize = Math.min(maxUsersReported - allUsers.size(), 100);
            String afterCursor = cursor;

            SearchResponse<User> response = camundaClient.newUsersSearchRequest()
                    .page(p -> {
                        p.limit(pageSize);
                        if (afterCursor != null) {
                            p.after(afterCursor);
                        }
                    })
                    .execute();

            List<User> users = response.items();
            if (users.isEmpty()) {
                break;
            }
            for (User user : users) {
                allUsers.add(user.getUsername());
                if (allUsers.size() >= maxUsersReported) {
                    break;
                }
            }

            cursor = response.page().endCursor();
            if (cursor == null || users.size() < pageSize) {
                break;
            }
        }
        return allUsers;
    }

    /**
     * Fetch the User (userId, name, email) for one userId, from the CamundaClient user API.
     * Returns null (never throws) when the user can't be found and failIfError is false.
     */
    private User fetchUser(String userId, Map<String, User> cacheUsers, boolean failIfError) throws
            ConnectorException {
        if (cacheUsers.containsKey(userId)) {
            return cacheUsers.get(userId);
        }
        try {
            User user = camundaClient.newUserGetRequest(userId).execute();
            cacheUsers.put(userId, user);
            return user;
        } catch (Exception e) {
            logger.error("Can't fetch user [{}] : {}", userId, e.getMessage());
            if (failIfError) {
                throw new ConnectorException(InvolvedUserError.CANT_FETCH_USER, "UserId[" + userId + "] errors :" + e.getMessage());
            }
            return null;
        }
    }


    private List<String> getInputFilterTask(InvolvedUserInput involvedUserInput) {
        List<String> filterTaskId = new ArrayList<>();
        Object filterTask = involvedUserInput.getFilterTask();
        if (filterTask instanceof String filterTaskString) {
            filterTaskId.add(filterTaskString);
        }
        if (filterTask instanceof List<?> filterTaskList) {
            for (Object filterTaskObject : filterTaskList) {
                if (filterTaskObject != null) {
                    filterTaskId.add(filterTaskObject.toString());
                }
            }
        }
        logger.info("FilterTask[{}]", filterTaskId);

        return filterTaskId;
    }



    private List<User> getInputAddUser(InvolvedUserInput involvedUserInput, Map<String, User> cacheUsers) throws
            ConnectorException {
        List<User> addUsersAllTasks = new ArrayList<>();

        Object addUsers = involvedUserInput.getAddUsers();
        if (addUsers instanceof List addUserList) {
            for (Object userName : addUserList) {
                User user = fetchUser(userName.toString(), cacheUsers, involvedUserInput.getFailIfError());
                addUsersAllTasks.add(user);
            }
        }
        if (addUsers instanceof String addUserString) {
            StringTokenizer st = new StringTokenizer(addUserString, ",");
            while (st.hasMoreTokens()) {
                User user = fetchUser(st.nextToken(), cacheUsers, involvedUserInput.getFailIfError());
                addUsersAllTasks.add(user);
            }
        }
        logger.info("addUsersAllTasks[{}]", addUsersAllTasks);

        return addUsersAllTasks;
    }

    @Override
    public String getDescription() {
        return "For each active user task of the current process instance (optionally filtered by task id), list the involved users: the assignee, the candidate users, and the members of the candidate groups.";
    }

    @Override
    public String getLogo() {
        return WORKER_LOGO;
    }

    @Override
    public String getCollectionName() {
        return "Users";
    }

    @Override
    public Map<String, String> getListBpmnErrors() {
        Map<String, String> allErrors = new HashMap<>();
        allErrors.put(InvolvedUserError.ERROR_BAD_INPUTPARAMETER, InvolvedUserError.ERROR_BAD_INPUTPARAMETER_EXPLANATION);
        allErrors.put(InvolvedUserError.ERROR_NO_PROCESSINSTANCE, InvolvedUserError.ERROR_NO_PROCESSINSTANCE_EXPLANATION);
        allErrors.put(InvolvedUserError.ERROR_DURING_OPERATION, InvolvedUserError.ERROR_DURING_OPERATION_EXPLANATION);
        allErrors.put(InvolvedUserError.ERROR_NO_CAMUNDA_CLIENT, InvolvedUserError.ERROR_NO_CAMUNDA_CLIENT_EXPLANATION);
        allErrors.put(InvolvedUserError.CANT_FETCH_USER, InvolvedUserError.CANT_FETCH_USER_EXPLANATION);
        allErrors.put(InvolvedUserError.CANT_FETCH_GROUP, InvolvedUserError.CANT_FETCH_GROUP_EXPLANATION);
        return allErrors;
    }

    @Override
    public Class<?> getInputParameterClass() {
        return InvolvedUserInput.class;
    }

    @Override
    public Class<?> getOutputParameterClass() {
        return InvolvedUserOutput.class;
    }

    @Override
    public List<String> getAppliesTo() {
        return null;
    }

    @Override
    public String getElementType() {
        return null;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public String getRelease() {
        return "1.0.0";
    }
}
