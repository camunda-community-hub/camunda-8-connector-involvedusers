[![Community badge: Stable](https://img.shields.io/badge/Lifecycle-Stable-brightgreen)](https://github.com/Camunda-Community-Hub/community/blob/main/extension-lifecycle.md#stable-)
[![Community extension badge](https://img.shields.io/badge/Community%20Extension-An%20open%20source%20community%20maintained%20project-FF4700)](https://github.com/camunda-community-hub/community)
![Compatible with: Camunda Platform 8](https://img.shields.io/badge/Compatible%20with-Camunda%20Platform%208-0072Ce)

# camunda-8-connector-involvedusers

![InvolvedUser.png](InvolvedUser.png)

Windows messenger Icon by Hopstarter (Jojo Mendoza) on <a href="https://icon-icons.com/authors/40-hopstarter-jojo-mendoza">Icon-Icons.com</a>


This connector collects the users involved in the active user tasks of the current process instance:
the task's assignee, its candidate users, and every member of its candidate groups - each deduplicated
and enriched with the full user record (userId, name, email).

## SaaS and OIDC

When the cluster's identity is managed by an external OIDC provider (this is the case for most SaaS
clusters, and for self-managed clusters configured with an enterprise identity provider), Camunda's
native Users API is disabled: a lookup like `newUserGetRequest(userId)` fails with a `403 Forbidden`
(`Users API is disabled because the application is configured in OIDC mode`). In that mode, user
records simply aren't stored in Camunda anymore - they live in the external OIDC provider instead, and
there is no Camunda REST endpoint to read them back.

To keep working in that situation, the connector falls back to a "shadow user" whenever a user (or
group member) can't be fetched and `failIfError` is `false` (the default):
* `userId` / `username` - the raw id that was looked up (assignee, candidate user, or group member).
* `email` - set to that same id when it looks like an email address (contains a `@`), which is the
  common case for OIDC-backed clusters where the username *is* the email. Left `null` otherwise.
* `name` - always `null` (there is no way to recover it once the Users API is disabled).

If `failIfError` is set to `true`, the connector does not build a shadow user for a failed lookup: it
throws instead, failing the job with the `CANT_FETCH_USER` (or `CANT_FETCH_GROUP`) BPMN error.

## Get the result

The connector fills two output variables:

* `detailTaskInvolvedUsers` - a map with one entry per user task, keyed by the task's numeric task
  key (`taskKey`), not by its BPMN element id or name: a multi-instance/"iterate" task creates one
  user task instance per iteration, all sharing the same element id, so the task key is the only way
  to keep every instance distinct. The task's element id and name are still available as fields inside
  the entry. Each entry contains:
  * `taskId`, `taskName`, `dueDate`, `creationDate`, `completionDate`, `taskKey` - the task's own data.
  * `assigneeUser` - present only when the task has an assignee: the assignee's user record.
  * `involvedUsers` - present only when the task has no assignee: the deduplicated list of user
    records resolved from the task's candidate users and the members of its candidate groups.

  A task is therefore reported with either `assigneeUser` (task already assigned - no need to look at
  candidates) or `involvedUsers` (task still up for grabs), never both.

* `involvedUsers` - the flat list of every user involved across *all* tasks (assignees and candidates
  alike), deduplicated by username so a user appearing on several tasks is reported only once.

Since `detailTaskInvolvedUsers` is keyed by task key rather than task name, use a FEEL expression like
this one to collect, say, every distinct task name across all tasks:

```
string join(distinct values(get entries(detailTaskInvolvedUsers).value.taskName), ", ")
```

## How to collect users

Each active user task falls into exactly one of three cases, decided by the task's own `assignee`,
`candidateUsers` and `candidateGroups` fields. `includeUsers`, `includeGroups`, `excludeUsers` and
`excludeGroups` are four *connector-level* inputs (not read from the task) that let you always add or
remove specific people/groups on top of whatever the task itself defines - but, as detailed below, they
don't apply the same way in every case.

### Task has an assignee

The result is exactly:
* the assignee, and
* `includeUsers`, and
* every member of `includeGroups`.

The task's own `candidateUsers`/`candidateGroups` are ignored entirely - once a task is assigned, there
is no "candidate" left to consider. `excludeUsers`/`excludeGroups` and `maxUsersReported` have **no
effect** in this case: the include lists are meant to always add these people regardless of the task,
so there is nothing to cap or filter here.

### Task has no assignee, but candidateUsers and/or candidateGroups

The result is built as:
1. Start from the task's `candidateUsers`.
2. Add every member of `(candidateGroups ∪ includeGroups) − excludeGroups` - i.e. the task's own
   candidate groups plus `includeGroups`, minus any group named in `excludeGroups` (a group listed in
   both `includeGroups` and `excludeGroups` is excluded).
3. Truncate the set to the first `maxUsersReported` entries (insertion order).
4. Add `includeUsers` back in - this happens *after* the truncation, so an explicitly included user is
   never dropped by the size cap.
5. Remove `excludeUsers` - this happens last, so a user listed in both `includeUsers` and `excludeUsers`
   ends up excluded: explicit exclusion always wins.

### Task has no assignee, no candidateUsers or candidateGroups ("everybody")

With nothing on the task to go on, every user in the organization is a candidate (fetched up to
`maxUsersReported`, applied while searching). From that full list:
* every member of `excludeGroups` is removed, then
* every user in `excludeUsers` is removed.

`includeUsers`/`includeGroups` have no effect here either - since everyone is already included, adding
more people to the set changes nothing.


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
|------------------|----------------------------------------|-------------------|----------|
| filterTask       | Filter Task Ids                       | java.lang.Object  | OPTIONAL |
| failIfError      | Fail when user or group are not found | java.lang.Boolean | OPTIONAL |
| maxUsersReported | Max users reported                    | java.lang.Integer | OPTIONAL |
| includeUsers     | Include users                         | java.lang.Object  | OPTIONAL |
| includeGroups    | Include groups                        | java.lang.Object  | OPTIONAL |
| excludeGroups    | Exclude groups                        | java.lang.Object  | OPTIONAL |
| excludeUsers     | Exclude users                         | java.lang.Object  | OPTIONAL |

Includes or exclude groups may be
* String: value separate by comma
```
Customer, Supervisor, Presale
```
a List of String  in FEEL
```
["Customer", "Supervisor", "Presale" ]
```

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
