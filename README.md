[![Community badge: Stable](https://img.shields.io/badge/Lifecycle-Stable-brightgreen)](https://github.com/Camunda-Community-Hub/community/blob/main/extension-lifecycle.md#stable-)
[![Community extension badge](https://img.shields.io/badge/Community%20Extension-An%20open%20source%20community%20maintained%20project-FF4700)](https://github.com/camunda-community-hub/community)
![Compatible with: Camunda Platform 8](https://img.shields.io/badge/Compatible%20with-Camunda%20Platform%208-0072Ce)

# camunda-8-connector-involvedusers

![InvolvedUser.png](InvolvedUser.png)

Windows messenger Icon by Hopstarter (Jojo Mendoza) on <a href="https://icon-icons.com/authors/40-hopstarter-jojo-mendoza">Icon-Icons.com</a>


This connector collects the users involved in the active user tasks of the current process instance:
the task's assignee, its candidate users, and every member of its candidate groups - each deduplicated
and enriched with the full user record (userId, name, email).

## Use the connector

Input:
* `filterTask` (optional) - a list of task ids (BPMN element id, or user task key). When provided,
  only these user tasks are considered; when empty or absent, every active user task of the process
  instance is processed.

Output - a single variable `involvedUsers`, a map keyed by task id:
```json
{
  "TaskId_1": {
    "taskName": "Review",
    "dueDate": "2026-01-20T18:00:00Z",
    "documentation": null,
    "involvedUsers": [
      { "userId": "alice", "email": "alice@example.com", "name": "Alice", "assignee": true },
      { "userId": "bob", "email": "bob@example.com", "name": "Bob", "assignee": false }
    ]
  }
}
```

Notes:
* `documentation` is currently always `null`: the BPMN element documentation is not exposed by the
  Camunda 8 user task search API used by this connector.
* This connector needs a `CamundaClient` to query user tasks, groups and users, so it must run as a
  Spring bean of the connector runtime (`spring-boot-starter-camunda-connectors`), not as a bare SPI
  connector - the runtime autowires the `CamundaClient` bean into it.

# Input, Output, errors

For each active user task of the current process instance (optionally filtered by task id), list the involved users: the assignee, the candidate users, and the members of the candidate groups.


## Inputs
| Name             | Description                           | Class             | Level    |
|------------------|---------------------------------------|-------------------|----------|
| filterTask       | Filter Task Ids                       | java.lang.Object  | OPTIONAL |
| failIfError      | Fail when user or group are not found | java.lang.Boolean | OPTIONAL |
| maxUsersReported | Max users reported                    | java.lang.Integer | OPTIONAL |
| addUsers         | Add users                             | java.lang.Object  | OPTIONAL |



## Outputs
| Name                    | Description                | Class          | Level    |
|-------------------------|----------------------------|----------------|----------|
| involvedUsers           | list of involved users     | java.util.List | REQUIRED |
| detailTaskInvolvedUsers | Detail task involved users | java.util.Map  | REQUIRED |



## Errors
| Name                     | Explanation                                                         |
|--------------------------|---------------------------------------------------------------------|
| ERROR_NO_CAMUNDA_CLIENT  | No CamundaClient is available to search user tasks, groups or users |
| CANT_FETCH_USER          | Error during fetch user information                                 |
| ERROR_DURING_OPERATION   | Error during the search of user tasks, groups or users              |
| CANT_FETCH_GROUP         | Error during fetch group information                                |
| BAD_INPUTPARAMETER       | During the bind, some input does not have the expected type         |
| ERROR_NO_PROCESSINSTANCE | The process instance key can't be found in the job context          |


## How to install this connector?

### Element template

Go to the `element-templates` folder, or download directly:
[involveduser-function.json](element-templates/involveduser-function.json)

### JAR file

Build the project with `mvn package`. Two JARs are produced:
* `camunda-8-connector-involvedusers-<version>.jar` - the plain JAR, without embedded dependencies
  (use this if your application already provides Spring Boot and the Camunda connector runtime)
* `camunda-8-connector-involvedusers-<version>-with-dependencies.jar` - a self-contained JAR bundling
  all dependencies (use this for a standalone deployment)
