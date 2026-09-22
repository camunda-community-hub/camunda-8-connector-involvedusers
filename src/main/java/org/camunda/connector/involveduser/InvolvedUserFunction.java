package org.camunda.connector.involveduser;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.search.enums.UserTaskState;
import io.camunda.client.api.search.response.GroupUser;
import io.camunda.client.api.search.response.SearchResponse;
import io.camunda.client.api.search.response.User;
import io.camunda.client.api.search.response.UserTask;
import io.camunda.client.impl.search.response.UserImpl;
import io.camunda.client.spring.properties.CamundaClientProperties;
import io.camunda.connector.api.annotation.OutboundConnector;
import io.camunda.connector.api.error.ConnectorException;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.api.outbound.OutboundConnectorFunction;
import org.camunda.connector.cherrytemplate.CherryConnector;
import org.camunda.connector.involveduser.toolbox.InvolvedUserError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

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
        InvolvedUserInput.INCLUDE_USERS,
        InvolvedUserInput.INCLUDE_GROUPS,
        InvolvedUserInput.EXCLUDE_GROUPS,
        InvolvedUserInput.EXCLUDE_USERS,
        InvolvedUserInput.MAX_USERS_REPORTED,
        InvolvedUserInput.FAIL_IF_ERROR,
        InvolvedUserInput.HTTP_TASK_LIST,
}, type = "c-involveduser-function")
public class InvolvedUserFunction implements OutboundConnectorFunction, CherryConnector {

    private static final String WORKER_LOGO = "data:image/png;base64,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";

    private final Logger logger = LoggerFactory.getLogger(InvolvedUserFunction.class.getName());

    /**
     * The CamundaClient is not reachable from OutboundConnectorContext in this SDK version
     * (verified via javap on io.camunda.connector.api.outbound.OutboundConnectorContext: it only
     * exposes getJobContext() and bindVariables()). This class is still registered as an SPI
     * OutboundConnectorFunction (see META-INF/services, using the no-arg constructor below, with no
     * CamundaClient available - kept for the Cherry / element-template tooling reflection, which
     * instantiates connectors reflectively rather than through Spring). For actual job execution,
     * InvolvedUserFunctionAutoConfiguration registers this class as a Spring bean instead, passing in
     * the CamundaClient bean that spring-boot-starter-camunda-connectors auto-configures - see that
     * class for why a Spring Boot auto-configuration is used here instead of a plain @Component
     * (component scanning depends on the consuming application's own scanBasePackages, which most
     * consuming apps won't widen just for this connector; auto-configuration works regardless).
     */
    @Nullable
    private final CamundaClient camundaClient;

    // Spring bean auto-configured by camunda-spring-boot-starter, exposing the "camunda.client.*"
    // configuration this connector was started with - in particular getMode() (self-managed vs saas)
    // and getCloud().getRegion()/getClusterId(), used by calculateTaskListUrl() to detect a SaaS
    // connection and derive its Tasklist URL without needing to parse the REST address.
    @Nullable
    private final CamundaClientProperties camundaClientProperties;

    public InvolvedUserFunction() {
        this(null, null);
    }

    public InvolvedUserFunction(@Nullable CamundaClient camundaClient) {
        this(camundaClient, null);
    }

    public InvolvedUserFunction(@Nullable CamundaClient camundaClient,
                                @Nullable CamundaClientProperties camundaClientProperties) {
        this.camundaClient = camundaClient;
        this.camundaClientProperties = camundaClientProperties;
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
            List<String> listAllUsersNameCache = new ArrayList<>();


            List<String> filterTaskId = getInputFilterTask(involvedUserInput);
            List<String> includeUsers = getListOfString(involvedUserInput.getIncludeUsers());
            List<String> excludeUsers = getListOfString(involvedUserInput.getExcludeUsers());
            List<String> includeGroups = getListOfString(involvedUserInput.getIncludeGroups());
            List<String> excludeGroups = getListOfString(involvedUserInput.getExcludeGroups());
            logger.info("InvolvedUserFunction: FilterTaskId [{}] IncludeUsers[{}] ExcludeUsers[{}] includeGroups[{}] excludeGroups[{}]",
                    filterTaskId,
                    includeUsers,
                    excludeUsers,
                    includeGroups,
                    excludeGroups);

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

                User assigneeUser = null;
                Set<String> involvedUsersName = new HashSet<>();
                Set<String> involvedGroupsName = new HashSet<>();
                boolean excludeUsersOperation = true;
                //-----------------------------  one assignee : don't need to go over candidates/groups below
                if (assignee != null && assignee.length() > 0) {
                    involvedUsersName.add(assignee);
                    assigneeUser= fetchUser(assignee, cacheUsers, involvedUserInput.getFailIfError());

                    // Don't use the exclude user: the assigne has the priority.
                    excludeUsersOperation = false;
                }
                // ----------------- special case: no assignee, no candidateUsers or candidateGroups everybody!
                else if ((candidateGroups == null || candidateGroups.isEmpty())
                        && (candidateUsers == null || candidateUsers.isEmpty())) {
                    if (listAllUsersNameCache.isEmpty())
                        listAllUsersNameCache = searchAllUsers(involvedUserInput.getMaxUsersReported());


                    involvedUsersName.addAll(listAllUsersNameCache);


                } else {
                    //---------------------- calculate candidate
                    if (candidateUsers != null) {
                        involvedUsersName.addAll(candidateUsers);
                    }
                    // ------------------ groups
                    if (candidateGroups != null) {
                        involvedGroupsName.addAll(candidateGroups);
                    }
                }

                // At this moment, we got the first set of involvedUsersName, involvedGroupsName

                // Add all includeGroup
                involvedGroupsName.addAll(includeGroups);
                // Remove now the exclude group
                involvedGroupsName.removeAll(excludeGroups);

                // Add all users from groups
                logger.info("InvolvedUserFunction: final InvolvedGroupsName[{}]", involvedGroupsName);
                for (String group : involvedGroupsName) {
                    List<String> members = groupMembersCache.computeIfAbsent(group,
                            g -> searchGroupMembers(g, involvedUserInput.getFailIfError(), involvedUserInput.getMaxUsersReported()));
                    involvedUsersName.addAll(members);
                }

                // Now last: exclude all users
                involvedUsersName.addAll(includeUsers);
                if (excludeUsersOperation) {
                    logger.info("InvolvedUserFunction: Exclude users [{}]", excludeUsers);
                    involvedUsersName.removeAll(excludeUsers);
                }

                // We have the list of users now
                // Limit the number of users to report: keep only the first maxUsersReported (insertion order)
                logger.info("InvolvedUserFunction. Final involvedUsersName.size [{}]. Limit: [{}] . First 5 first in the list [{}]",
                        involvedUsersName.size(),
                        involvedUserInput.getMaxUsersReported(),
                        involvedUsersName.stream().limit(5).toList());

                if (involvedUsersName.size() > involvedUserInput.getMaxUsersReported()) {
                    involvedUsersName = involvedUsersName.stream()
                            .limit(involvedUserInput.getMaxUsersReported())
                            .collect(Collectors.toCollection(LinkedHashSet::new));
                }

                List<User> involvedUsersList = new ArrayList<>();
                for (String userId : involvedUsersName) {
                    User user = fetchUser(userId, cacheUsers, involvedUserInput.getFailIfError());
                    if (user != null) {
                        involvedUsersList.add(user);
                    }
                }

                String urlTask = calculateTaskListUrl(userTask, involvedUserInput);
                logger.info("InvolvedUserFunction; Task Name[{}] Key[{}] involvedUserList (limited) {} candidatesUsers[{}] candidateGroups[{}] involvedGroupName[{}]",
                        userTask.getName(),
                        userTask.getUserTaskKey(),
                        involvedUsersName.stream().limit(5).toList(),
                        candidateUsers == null ? "null" : candidateUsers,
                        candidateGroups == null ? "null" : candidateGroups,
                        involvedGroupsName == null ? "null" : involvedGroupsName);
                output.addTask(userTask, urlTask, assigneeUser, involvedUsersList, candidateUsers, candidateGroups, involvedGroupsName.stream().toList());
            }

            logger.info("InvolvedUserFunction End in {} ms, {} task(s)", System.currentTimeMillis() - beginTime, output.getDetailTaskInvolvedUsers().size());
            return output;
        } catch (
                ConnectorException ce) {
            throw ce;
        } catch (
                Exception e) {
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
            logger.error("InvolvedUserFunction: Can't resolve members of group [{}] : {}", groupId, e.getMessage());
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
     * Builds the base URL used to deep-link to a user task in Tasklist, following the
     * "{tasklist-url}/tasklist/{userTaskKey}" pattern documented at
     * https://docs.camunda.io/docs/components/camunda-integrations/ms-teams/ms-teams-installation/#example-configuration-file
     * <p>
     * On Camunda 8 SaaS, the Tasklist base URL (e.g. https://jfk-1.api.camunda.io/f9329610-bb97-4ae4-b666-45664111fc66)
     * is calculated automatically from CamundaClientProperties.getMode()/getCloud() - the same Spring
     * bean that camunda-spring-boot-starter itself binds "camunda.client.*" onto - no input needed.
     * On a Self-Managed cluster there is no such convention to derive it from, so the "httpTaskList"
     * input is used instead.
     */
    private String calculateTaskListUrl(UserTask userTask, InvolvedUserInput involvedUserInput) {
        String headerTask = "";

        boolean isSaas = camundaClientProperties != null
                && camundaClientProperties.getMode() == CamundaClientProperties.ClientMode.saas
                && camundaClientProperties.getCloud() != null;

        if (isSaas) {
            String region = camundaClientProperties.getCloud().getRegion();
            String clusterId = camundaClientProperties.getCloud().getClusterId();
            headerTask = "https://" + region + ".api.camunda.io/" + clusterId;
        } else if (involvedUserInput.getHttpTaskList() != null && !involvedUserInput.getHttpTaskList().isBlank()) {
            // Self-Managed: use the input as-is (trim a trailing slash to avoid a double "//tasklist").
            headerTask = involvedUserInput.getHttpTaskList().trim();
            if (headerTask.endsWith("/")) {
                headerTask = headerTask.substring(0, headerTask.length() - 1);
            }
        }

        return headerTask + "/tasklist/" + userTask.getUserTaskKey() + "?filter=all-open";
    }

    /**
     * Fetch the User (userId, name, email) for one userId, from the CamundaClient user API.
     * When the user can't be found and failIfError is false, a "shadow" User is built instead of
     * failing: this happens in particular when the cluster runs in OIDC mode, where the native
     * Users API is disabled (403 Forbidden) and no user record can ever be fetched. The shadow user
     * carries the userId as username, and - since usernames are often the user's email address in
     * OIDC setups - populates the email with the userId when it looks like one (contains a "@").
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

            Integer httpCode = e instanceof ClientHttpException clientHttpException ? clientHttpException.code() : null;
            if (httpCode != null && httpCode.intValue() == 403) {
                // SaaS or OIDC env, it's expected to not be allow to get the user details
                logger.info("InvolvedUserFunction: Code 403 on FetchUser: OIDC (SaaS or other) Can't fetch user [{}] : {}, so create a shadow user", userId);
                return getShadowUser(userId, cacheUsers);
            }

            if (failIfError) {
                logger.error("InvolvedUserFunction: Can't fetch user [{}] : {}", userId, e.getMessage());
                throw new ConnectorException(InvolvedUserError.CANT_FETCH_USER, "UserId[" + userId + "] errors :" + e.getMessage());
            }
            logger.info("InvolvedUserFunction: Can't fetch user [{}] : {}, so create a shadow user", userId, e.getMessage());
            return getShadowUser(userId, cacheUsers);
        }
    }

    private User getShadowUser(String userId, Map<String, User> cacheUsers) {
        String shadowEmail = userId.contains("@") ? userId : null;
        User shadowUser = new UserImpl(userId, null, shadowEmail);
        cacheUsers.put(userId, shadowUser);
        return shadowUser;
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
        return filterTaskId;
    }


    /**
     * THe input maybe a List (from a FEEL expression) or a string with , to serate name
     *
     * @param value the value to decode
     * @return a listofstring
     */
    private List<String> getListOfString(Object value) {
        if (value instanceof List valueList) {
            return valueList;
        }
        if (value instanceof String valueString) {
            List<String> listOfStrings = new ArrayList<>();

            StringTokenizer st = new StringTokenizer(valueString, ",");
            while (st.hasMoreTokens()) {
                listOfStrings.add(st.nextToken().trim());
            }
            return listOfStrings;
        }
        return Collections.emptyList();
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
