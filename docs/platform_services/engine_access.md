# Engine access

Every call to an engine or a project goes through a permission check. `Utility.getDatabase`, `getModel`, `getStorage`, `getVectorDatabase`, `getFunctionEngine`, `getVenvEngine`, `getEngine` and `getProject` hand out a proxy, and the proxy asks [EngineAccessGuard](../../src/prerna/engine/impl/pipeline/EngineAccessGuard.java) before it passes a method call to the engine. The check runs on every call, for platform code and for custom reactors alike, and nothing turns it off.

The check answers one question: does whoever is acting hold the permission this method needs on this engine?

## The level each method needs

Every method of the engine and project interfaces carries an [EngineAccess](../../src/prerna/engine/api/EngineAccess.java) annotation with the permission it needs: `READ_ONLY`, `EDIT` or `OWNER`, the same three levels the access screens give people.

```java
@EngineAccess(AccessPermissionEnum.EDIT)
void insertData(String query) throws Exception;

@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
java.sql.Connection getConnection() throws SQLException;
```

- `value` is the level the signed in user needs.
- `projectCode` is the level needed when a project's custom code makes the call itself. It is set on the methods that hand back more than the platform's own callers pass on: a raw connection, or the engine's settings.

| What the method does | Examples | User | Custom code |
| --- | --- | --- | --- |
| Reads | `getEngineId`, `ask`, `embeddings`, `nearestNeighbor`, `list`, `copyToLocal`, `execute`, `getReactor` | `READ_ONLY` | `READ_ONLY` |
| Writes | `insertData`, `removeData`, `commit`, `commitOWL`, `copyToStorage`, `deleteFromStorage`, `addDocument`, `removeDocument`, `addPackage`, `pullRequirementsFile` | `EDIT` | `EDIT` |
| Hands back a connection or runs SQL as given | `getConnection`, `getPreparedStatement`, `execQuery`, `executeInTransaction`, `getQueryUtil`, `getConnectionMetadata` | `READ_ONLY` | `EDIT` |
| Hands back the engine's settings | `getSmssProp`, `getSmssFilePath`, `getProperty`, `getConnectionUrl`, `getDataSource` | `READ_ONLY` | `OWNER` |
| Changes the engine itself | `delete`, `setEngineId`, `setEngineName`, `setDisplayName` | `OWNER` | `OWNER` |
| Reopens or reconfigures the engine | `open`, `setSmssProp`, `closeDataSource`, `getOrigSmssProp`, `addProperty`, `setOwlFilePath`, `setBaseDataEngine`, `setInsightDatabase` | `EDIT` | `OWNER` |
| Closes the engine | `close` | `READ_ONLY` | `OWNER` |
| A project's own runtime | `getInsightDatabase`, `getProjectProperties`, `getProjectPyTranslator`, `compileReactors` | `READ_ONLY` | `EDIT` |

Guarded by [EngineAccessLevelsUnitTests](../../test/prerna/engine/api/EngineAccessLevelsUnitTests.java):

| Rule | Test |
| --- | --- |
| Every method of an engine or project interface has a level, so a new method cannot reach an engine unchecked | `everyMethodOfAnEngineOrProjectInterfaceHasALevel` |
| The level for custom code is one level, above the user's | `theLevelForProjectCodeIsOneLevelThatIsNoLowerThanTheUsers` |
| The methods in the table above keep their levels | `theMethodsThatMatterMostKeepTheirLevels` |

To add a method to an engine interface, give it an `@EngineAccess` with the level the work it does needs: `READ_ONLY` to read, `EDIT` to write or reconfigure, `OWNER` to change the engine itself. `everyMethodOfAnEngineOrProjectInterfaceHasALevel` fails until it has one.

## Who is acting

The check reads the call stack from the proxy down and stops at the first of these it finds:

| Who | How it is recognized |
| --- | --- |
| A project's custom code | A class loaded by the [project sandbox](project_reactor_sandbox.md). The classes of the JDK and of shared libraries are passed over, so a call a project makes through a lambda, a stream or a library is still the project's |
| An engine | An engine's own class, or another proxy call: a vector database calling the model that makes its embeddings, a function engine opening its storage engine, a guardrail calling its judge model |
| The platform loading an engine or project | The `Utility` getter that loads it |
| The platform | Anything else. A reactor a user runs inside an engine's call, such as a tool of a project, is the platform again |

## Calls from the platform

The platform needs the method's level for the signed in user. The user is the one signed in on the thread, or else the user of the insight or room the call carries, which is how a pooled thread names its user.

Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java):

| Rule | Test |
| --- | --- |
| A user calls the methods their level covers: a reader reads, an editor also writes, an owner also deletes | `aUserCallsTheMethodsTheirLevelCovers` |
| A user with no permission is refused every method, with the same answer as for an engine that does not exist | `aUserWithNoPermissionIsRefusedEveryMethod` |
| A refusal names the level the method needs | `aRefusalNamesTheLevelTheMethodNeeds` |
| For a user who can view an engine, the platform reads its settings and gets a connection | `thePlatformReadsSettingsAndGetsAConnectionForAUserWhoCanView` |
| Permission on one engine gives nothing on another | `permissionOnOneEngineGivesNothingOnAnother` |
| A project is checked against the user's permission on the project | `aProjectIsCheckedAgainstTheUsersPermissionOnTheProject` |
| A user off the thread is read from the insight of the call | `aUserOffTheThreadIsReadFromTheInsightOfTheCall` |
| A call with no signed in user is refused, whether or not the security database lists the engine | `aCallWithNoSignedInUserIsRefused`, `aCallWithNoSignedInUserIsRefusedOnAnEngineTheSecurityDatabaseDoesNotList` |
| Methods of `Object`, such as `toString`, are not checked | `methodsOfObjectAreNotChecked` |

A call from the platform also goes through in these cases:

| Case | Why | Test |
| --- | --- | --- |
| An engine is acting | The engine's owner configured the engine it calls | `anEngineCallingTheEngineConfiguredOnItGoesThrough` |
| A reactor a user runs inside an engine's call | It is checked as the user, like any other reactor | `aReactorAUserRunsInsideAnEngineCallIsCheckedAsTheUser` |
| The platform is loading the engine or project | Loading syncs the engine's files, whoever asked for it | `thePlatformLoadingAnEngineGoesThrough` |
| The proxy was handed out for the platform's own use | A feature uses the model a platform setting names for every user of the feature; see [engines the platform uses for everyone](#engines-the-platform-uses-for-everyone) | `aProxyHandedOutForThePlatformsOwnUseServesAnyUser` |
| The platform is running a platform operation | The platform's own work for no user, such as a cluster sync; see [platform operations](#platform-operations) | `aPlatformOperationGoesThroughWithNoSignedInUser`, `aPlatformOperationGoesThroughForAUserWithNoPermission` |
| The method needs `READ_ONLY` and the engine or project is discoverable | Anyone can look at a discoverable engine's details before asking for access | `thePlatformReadsADiscoverableEngineForAUserWithNoPermission` |
| The method needs `READ_ONLY` on a project and the user can open one of its insights | An insight is shared on its own, and opening it reads its project | `thePlatformReadsAProjectForAUserWhoCanOpenOneOfItsInsights` |
| The user is an admin and an admin operation is running | Admin reactors and endpoints work on every engine; see [admin operations](#admin-operations) | `anAdminCallsAnyMethodThroughAnAdminClass` |
| The security database does not list the engine or project | It is being created or deleted | `anEngineTheSecurityDatabaseDoesNotListGoesThrough` |
| The JVM has no security database | The Java worker holds stand-ins that send each call back to the server, which checks it there | `aJvmWithoutTheSecurityDatabaseChecksNothing` |

## Calls from custom reactors

A project's custom code needs the method's level, and the `projectCode` level when it makes the call itself. It has the level in one of two ways:

- the signed in user holds it on the engine, or
- the engine gave the app a permission that covers it; see [giving an app a permission on an engine](#giving-an-app-a-permission-on-an-engine).

On its own project, a project's custom code has `EDIT`.

Guarded by [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java), which loads a class through the project sandbox and has it call an engine behind the real proxy:

| Rule | Test |
| --- | --- |
| A custom reactor reads an engine the user can view | `aCustomReactorReadsAnEngineTheUserCanView` |
| A custom reactor is refused an engine the user cannot view | `aCustomReactorIsRefusedAnEngineTheUserCannotView` |
| A custom reactor needs `EDIT` to write, to run SQL as given, or to hold a connection | `aCustomReactorNeedsEditToWriteOrToHoldAConnection` |
| A custom reactor needs `OWNER` for an engine's settings and its lifecycle | `aCustomReactorNeedsOwnerForAnEnginesSettingsAndItsLifecycle` |
| A refusal tells the reactor which level the call needs | `aRefusalTellsACustomReactorWhichLevelTheCallNeeds` |
| An app's permission stands in for the user's | `anAppsPermissionStandsInForTheUsers` |
| A `READ_ONLY` app permission does not give a connection | `aReadOnlyAppPermissionDoesNotGiveAConnection` |
| An app's permission never reaches an engine's settings | `anAppsPermissionNeverReachesAnEnginesSettings` |
| Another app's permission does not count | `anotherAppsPermissionDoesNotCount` |
| Custom code that belongs to no project has no app permission | `customCodeOfNoProjectHasNoAppPermission` |
| Being an admin gives a custom reactor nothing | `beingAnAdminGivesACustomReactorNothing` |
| A custom reactor runs only for the user signed in on the thread, and with none it is refused | `aCustomReactorWithNoSignedInUserIsRefused` |
| A custom reactor is never checked as the user of the insight the thread names | `aCustomReactorIsNotCheckedAsTheUserOfTheInsightTheThreadNames` |
| A discoverable engine is not readable by a custom reactor | `aDiscoverableEngineIsNotReadableByACustomReactor` |
| An engine the security database does not list is refused to a custom reactor | `anEngineTheSecurityDatabaseDoesNotListIsRefusedToACustomReactor` |
| A call through a lambda and the JDK is still the custom reactor's | `aCallThroughALambdaAndTheJdkIsStillTheCustomReactors` |
| A lambda a custom reactor hands to the platform is still the custom reactor's | `aLambdaACustomReactorHandsToThePlatformIsStillTheCustomReactors` |
| A custom reactor has `EDIT` on its own project | `aCustomReactorHasEditOnItsOwnProject` |
| A custom reactor needs the user's permission on another project | `aCustomReactorNeedsTheUsersPermissionOnAnotherProject` |
| A project's loader names its project | `aProjectsLoaderNamesItsProject` |

The sandbox adds rules of its own, in [SandboxReplacedCallsUnitTests](../../test/prerna/util/SandboxReplacedCallsUnitTests.java) and [SandboxBlockedClassesUnitTests](../../test/prerna/util/SandboxBlockedClassesUnitTests.java):

| Rule | Test |
| --- | --- |
| The query utility an RDBMS engine hands out (`getQueryUtil()`) builds SQL for a custom reactor, and its user name, password, connection properties and connection URL stay with the engine | `anEnginesCredentialsCannotBeReadOrChangedThroughItsQueryUtility` |
| The classes that build the proxy, `EngineProxyFactory` and `PipelineInvocationHandler`, cannot be used by a custom reactor | `platformStateAndInternalHelpersAreBlocked` |
| A custom reactor reads what the thread says about the request it serves and never changes it: the signed in user, the insight, session, route and job, the context project and the thread's whole map | `theSignedInUserCannotBeChanged`, `theRequestTheThreadServesCannotBeChanged` |
| A custom reactor cannot use `InsightStore`, which holds every open insight of every user | `platformStateAndInternalHelpersAreBlocked` |

## Giving an app a permission on an engine

An app often works with an engine for people who have no access to that engine themselves: a form that inserts into a database, a workflow that adds a document to a storage engine. An owner of the engine gives the app a permission on it, and the app's custom reactors then use the engine with that permission, whoever runs them. The owner does it on the engine's Access Control tab, in Apps, or with a call to Monolith, like every other permission:

```
POST /Monolith/api/auth/engine/setEngineProjectPermission
engineId=<engine id>&projectId=<app id>&permission=EDIT
```

With an `EDIT` permission the app's reactors get the connection and run their own transactions:

```java
IRDBMSEngine database = (IRDBMSEngine) Utility.getDatabase(databaseId);
try (Connection connection = database.getConnection();
		PreparedStatement insert = connection.prepareStatement("INSERT INTO ORDERS (ID, ITEM) VALUES (?, ?)")) {
	connection.setAutoCommit(false);
	insert.setString(1, orderId);
	insert.setString(2, item);
	insert.executeUpdate();
	connection.commit();
}
```

| Level | What the app's custom reactors can do |
| --- | --- |
| `READ_ONLY` | Call the engine's read methods: run a query through the platform's query classes, ask a model, list and download from a storage engine, search a vector database |
| `EDIT` | Everything above, and write: get the connection and prepared statements, insert, update and delete, add to and delete from a storage engine, add and remove documents |

An app is never an owner of an engine, so an engine's settings and secrets stay with its owners.

### An app's usage limit on a model

On a model, the app's permission carries an App Limit, set by the model's owners the way a team's limit is: an overall amount for the app (rules that count `SHARED`), an amount for each user of the app (rules that count `EACH_PERSON`), or both. It counts the calls the app's custom reactors make to the model for people whose only way to it is the app: those with no grant of their own and none through a team. A person with a grant of their own, or a team's, counts against their own limits inside the app too. The App Limit comes after Team Limits and before the Public Use Limit, so on a public model the app's users count against the App Limit when one is defined. When the app's overall amount is used up, the model refuses the app's calls for everyone until it resets; when one person's amount is used up, it refuses that person's. With no App Limit defined, the call counts against the Public Use Limit on a public model, then the person's User's Total Model Limit and the General Platform Limit. See [model usage limits](../model_usage_limits/README.md).

```
POST /Monolith/api/auth/engine/setEngineProjectPermission
engineId=<model id>&projectId=<app id>&permission=READ_ONLY&usageLimit={"unlimited": false, "rules": [
    {"metric": "TOKENS", "amount": 1000000, "window": "DAY", "counts": "SHARED"},
    {"metric": "TOKENS", "amount": 50000, "window": "DAY", "counts": "EACH_PERSON"}]}
```

Leaving `usageLimit` out keeps the limit the app has, and an empty `usageLimit` removes it.

The endpoints are Monolith's, beside the ones for people and teams. An owner uses `/auth/engine`, and an admin in admin mode uses `/auth/admin/engine` on any engine:

| Endpoint | Who calls it | Form or query fields | What it does |
| --- | --- | --- | --- |
| `POST /auth/engine/setEngineProjectPermission` | An owner of the engine | `engineId`, `projectId`, `permission`, and optionally `endDate` and `usageLimit`, the App Limit on a model | Gives the app the level, or changes the one it has |
| `POST /auth/engine/removeEngineProjectPermission` | An owner of the engine | `engineId`, `projectId` | Takes the permission away |
| `GET /auth/engine/getEngineProjectPermissions` | An editor or owner of the engine | `engineId` | Lists the apps the engine gives a permission to |
| `POST /auth/admin/engine/setEngineProjectPermission` | An admin | The same fields | The same, on any engine |
| `POST /auth/admin/engine/removeEngineProjectPermission` | An admin | The same fields | The same, on any engine |
| `GET /auth/admin/engine/getEngineProjectPermissions` | An admin | `engineId` | The same, on any engine |

The permission is part of the permission batch model ([prerna.auth.access](../../src/prerna/auth/access/AGENTS.md)): the subject is a `PROJECT` on an `ENGINE` target, saved through `PermissionBatchUtils` and stored in `PROJECTENGINEPERMISSION`. Bulk Access Management sends it as:

```json
[{"target": {"resourceType": "ENGINE", "id": "<engine id>"},
  "changes": [{"action": "ADD",
               "subject": {"category": "PROJECT", "id": "<app id>"},
               "to": {"level": "EDIT", "endDate": null, "usageLimit": null}}]}]
```

`getPermissions` on an engine returns its apps as `projects`, each with its name (the app's display name when it has one), level, end date and App Limit. Only the engine's editors and owners, and admin mode, see them: `allowedPermissionChanges.canViewProjects` says whether the user does, and anyone else gets an empty list. `allowedPermissionChanges.canChangeProjects` says whether the user can change them.

Guarded by [EngineProjectPermissionUnitTests](../../test/prerna/auth/utils/EngineProjectPermissionUnitTests.java), and the endpoints by `EngineAuthorizationResourceTest` and `AdminEngineAuthorizationResourceTest` in Monolith:

| Rule | Test |
| --- | --- |
| An owner gives an app a permission on the engine | `anOwnerGivesAnAppAPermissionOnTheEngine` |
| The permission is for that app and that engine only | `thePermissionIsForThatAppAndThatEngineOnly` |
| Setting again changes the level | `settingAgainChangesTheLevel` |
| An owner takes the permission away | `anOwnerTakesThePermissionAway`, `takingAwayAPermissionThatIsNotThereIsRefused` |
| Only an owner of the engine gives or takes away an app's permission | `onlyAnOwnerOfTheEngineGivesAnAppAPermission`, `onlyAnOwnerOfTheEngineTakesAnAppsPermissionAway` |
| Someone who is not an owner learns nothing about the app or the engine | `someoneWhoIsNotAnOwnerLearnsNothingAboutTheApp` |
| An admin gives, takes away and lists in admin mode, on an engine that exists | `anAdminGivesAndTakesAwayInAdminMode`, `adminModeRefusesAnEngineThatDoesNotExist` |
| An app is never an owner of an engine | `anAppIsNeverAnOwnerOfAnEngine` |
| The level is `READ_ONLY` or `EDIT`, and the app exists | `aLevelThatIsNotOneOfTheLevelsIsRefused`, `anAppThatDoesNotExistIsRefused` |
| An app is given a permission only on an engine | `anAppIsGivenAPermissionOnlyOnAnEngine` |
| An owner gives an app a limit on a model, with an amount for the app and one for each user | `anOwnerGivesAnAppALimitOnAModelWithAnAmountForTheAppAndOneForEachUser` |
| An app's limit is only on a model, and one that cannot be defined is refused | `anAppsLimitIsOnlyOnAModel`, `anAppsLimitThatCannotBeDefinedIsRefused` |
| Changing an app's level keeps its limit, and sending no limit defined removes it | `changingAnAppsLevelKeepsItsLimit`, `sendingNoLimitDefinedRemovesAnAppsLimit` |
| Taking the permission away, or deleting the app, removes its limit | `takingAnAppsPermissionAwayRemovesItsLimit`, `deletingTheAppRemovesItsLimits` |
| An app is in a batch once, and is added once | `anAppIsInABatchOnce`, `aBatchThatAddsAnAppTwiceInARowIsRefusedTheSecondTime` |
| A permission has an optional end date, and one that has ended is not in effect | `anEndDateThatCannotBeReadIsRefused`, `aPermissionThatHasEndedIsNotInEffect` |
| A permission that has ended is removed with its App Limit and recorded as ended | `anEndedAppPermissionTakesItsAppLimitWithIt`, `anEndedGrantNamesWhoHadItAndTheLevel` |
| A batch sent as JSON gives an app a permission | `aBatchSentAsJsonGivesAnAppAPermission` |
| An engine's permissions list its apps, and tell an owner they can change them | `theEnginesPermissionsListItsApps`, `onlyAnOwnerIsToldTheyCanChangeTheApps`, `aProjectsPermissionsHaveNoApps` |
| Only the engine's editors and owners list its apps | `onlyTheEnginesEditorsAndOwnersListItsApps` |
| Someone who can only view the engine sees none of its apps in its permissions | `aReaderSeesNoneOfTheEnginesAppsInItsPermissions` |
| Deleting the engine or the app removes the permission | `deletingTheEngineRemovesThePermissionsItGave`, `deletingTheAppRemovesThePermissionsItWasGiven` |

The App Limit's own rules are guarded where the other limits' are:

| Rule | Test |
| --- | --- |
| A model answering a call an app's custom reactors make knows the app, whether the call went through on the app's permission or on the user's own, and an engine it calls does not | `aModelAnsweringACallMadeWithAnAppsPermissionKnowsTheApp`, `aModelKnowsTheAppMakingTheCallWhenTheUserHasAccessOfTheirOwn` in [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) |
| A call of an app the model gives no permission, a call the platform makes, and a refused call carry no app | `aModelSeesNoAppWhenItGivesTheCallingAppNoPermission`, `aModelSeesNoAppOnACallThePlatformMakes`, `aRefusedCallLeavesNoAppBehind` |
| A call an app makes for a person with no grant is billed to the app's limit, before public use | `resolve_aCallAnAppMakesForAPersonWithNoGrantIsBilledToTheAppsLimit`, `resolve_theAppsLimitComesBeforePublicUse`, `theGuardKeepsTheAppsPathApartFromThePersonsOwn` in [SecurityModelUsageLimitUtilsUnitTests](../../test/prerna/auth/utils/SecurityModelUsageLimitUtilsUnitTests.java) |
| An app with no limit defined passes on to public use, then to the person's all-models default | `resolve_anAppWithNoLimitDefinedPassesOnToPublicUse`, `resolve_anAppWithNoLimitDefinedPassesOnToThePersonsAllModelsDefault` |
| The app's limit is skipped for a person with a grant of their own or a team's | `resolve_anAppsLimitIsSkippedForAPersonWithAGrantOfTheirOwn`, `resolve_anAppsLimitIsSkippedForAPersonWithATeamsGrant` |
| The app's limit is not on a path for a call no app makes, and one app's limit is not another's | `resolve_anAppsLimitIsNotOnAPathForACallNoAppMakes`, `resolve_oneAppsLimitIsNotAnotherApps` |
| The app's overall amount is shared by everyone who uses the app, and the amount for each user stops only that user | `anAppsOverallAmountIsSharedByEveryoneWhoUsesTheApp`, `anAppsAmountForEachUserStopsOnlyThatUser`, `whatOneAppUsesDoesNotCountAgainstAnother` in [ModelUsageGuardUnitTests](../../test/prerna/engine/impl/model/usage/ModelUsageGuardUnitTests.java) |
| The app's counters are filled from the calls billed to the app | `anAppsCountersAreFilledFromTheCallsBilledToTheApp`, `aCounterKeepsItsNameAndAnAppsCounterEndsWithTheApp` |

## Where the answers come from

[SecurityEngineAccessUtils](../../src/prerna/auth/utils/SecurityEngineAccessUtils.java) reads the security database for the check:

- a user's level is their highest grant in effect, given to them directly or to a team they are in, or `READ_ONLY` on a public engine or project (`SecurityEngineUtils.getCurrentUserEngineLevel`, `SecurityProjectUtils.getCurrentUserProjectLevel`);
- an app's permission is its row in `PROJECTENGINEPERMISSION`, while it has not ended.

The check runs on every call, so the answers are kept for 30 seconds. A permission saved on a server is read again by that server at once, and by the other servers of a cluster within 30 seconds.

Guarded by [SecurityEngineAccessUtilsUnitTests](../../test/prerna/auth/utils/SecurityEngineAccessUtilsUnitTests.java):

| Rule | Test |
| --- | --- |
| A user's level on an engine is their highest grant | `aUsersLevelOnAnEngineIsTheirHighestGrant` |
| Everyone can view a public engine | `everyoneCanViewAPublicEngine` |
| A user's level on a project is read from the project's own grants | `aUsersLevelOnAProjectIsReadFromTheProjectsOwnGrants` |
| No user and no id hold nothing | `noUserAndNoIdHoldNothing` |
| A saved permission is read at once | `aSavedPermissionIsReadAtOnce`, `theOwnerOfANewEngineHoldsItAtOnce` |
| An admin is known as one | `anAdminIsKnownAsOne` |
| An engine or project is listed while the security database has it | `anEngineOrProjectIsListedWhileTheSecurityDatabaseHasIt` |
| The platform reads a discoverable engine or project for anyone | `thePlatformReadsADiscoverableEngineForAnyone`, `thePlatformReadsADiscoverableProjectForAnyone` |
| The platform reads a project for a user given one of its insights, or with a public insight | `thePlatformReadsAProjectForAUserGivenOneOfItsInsights`, `thePlatformReadsAProjectWithAPublicInsightForAnyone` |

## Engines the platform uses for everyone

A feature that uses the model a platform setting names, for every user of the feature, gets the model from `InternalUtility.getPlatformModel(engineId)`. The model's pipeline and usage limits apply as on any call, and the access check lets the platform's own calls through whoever the user is. Use it only for an engine id that comes from a platform setting, never for one a user sends. Avatar generation in Collaboration (`COLLAB_AVATAR_ENGINE_ID`) uses it.

Every other feature that names an engine in a setting checks the user's access to it, as Collaboration's text model and classifier do: make the engine public, or give access to the people and teams that use the feature.

## Platform operations

Some work is the platform's own and is done for no user: syncing an engine's files across a cluster, pulling a project from Git. That code declares a platform operation around its steps, and inside it the platform's calls go through whoever is signed in on the thread:

```java
try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
	engine.close();
	pullFiles();
	engine.open(smssProp);
}
```

[PlatformOperation](../../src/prerna/engine/impl/pipeline/PlatformOperation.java) belongs to the thread that began it and ends when it is closed. A thread the work starts begins its own. Keep an operation around the platform's own steps only, never around a pixel or a reactor a user sends.

Only the platform's own code runs one. Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java), [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) and [SandboxBlockedClassesUnitTests](../../test/prerna/util/SandboxBlockedClassesUnitTests.java):

| Rule | Test |
| --- | --- |
| Inside a platform operation the platform's calls go through, with no signed in user or for a user with no permission | `aPlatformOperationGoesThroughWithNoSignedInUser`, `aPlatformOperationGoesThroughForAUserWithNoPermission` |
| An operation ends when it is closed, and closing it again changes nothing | `aPlatformOperationEndsWhenItIsClosed` |
| An operation begun inside another leaves the outer one running, and an outer one closed first is not put back | `aPlatformOperationBegunInsideAnotherLeavesTheOuterOneRunning`, `anOuterPlatformOperationClosedFirstIsNotPutBack` |
| An operation is not carried to a thread the work starts, and another thread cannot close it | `aPlatformOperationIsNotCarriedToAThreadTheWorkStarts`, `aPlatformOperationClosedOnAnotherThreadKeepsRunning` |
| An operation has a name | `aPlatformOperationNeedsAName` |
| A custom reactor cannot load `PlatformOperation` | `platformStateAndInternalHelpersAreBlocked` |
| A platform operation does not cover a call a custom reactor makes | `aPlatformOperationDoesNotCoverACallACustomReactorMakes` |
| A platform method a custom reactor calls begins no operation, and an operation that is running does not cover platform code a custom reactor runs | `aPlatformOperationNeverCoversPlatformCodeACustomReactorRuns` |

The platform runs these operations:

| Operation | Where | What it covers |
| --- | --- | --- |
| `start up`, `shut down` | Monolith `DBLoader` | Loading the settings, the engines and the projects on disk when the server starts, and closing them when it stops |
| `catalog engines and projects` | `AbstractFileWatcher.run` | A watcher thread cataloging the smss files in its folder, at start up and when a new one appears |
| `cluster sync` | `ClusterSyncCloudClient`, which wraps the client `ClusterUtil.getCentralStorageClient()` returns, and `ClusterSyncMethod.apply` | Every push and pull of an engine's or project's files to and from the cluster's cloud storage, whoever's request set it off, and the changes another node published, applied on the listener thread. Guarded by `getCentralStorageClient_runsEachCallAsAPlatformOperation` in [ClusterUtilUnitTests](../../test/prerna/cluster/util/ClusterUtilUnitTests.java) |
| `github sync` | Monolith `GitHubService`, the webhook's sync task | Pulling a project from GitHub after a push to its tracked branch |
| `publish portal` | Monolith `PublicHomeCheckFilter` | Publishing a project's portal and serving its files to a visitor who passed the filter's checks; a public project is served with no session |
| `login count` | Monolith `CACTrackingUtil` | Counting a login in the tracking database the platform keeps for itself |

Work the platform does for a user on another thread is not a platform operation: the thread carries that user (`ThreadStore.setUser`), so the user's permissions are checked there as on the request. The inference log worker, the Java worker's engine requests, agent room naming, background frame caching, the Git reactors' cluster push and Collaboration's import of new mail from a Microsoft Graph notification, which runs as each mailbox owner in turn, all do.

## Admin operations

On an engine or project an admin holds their own permissions, like anyone else. An admin has more only inside an admin operation, where the platform's calls go through for an admin whatever they hold.

An admin operation is running while a class that requires an admin is acting, that is, while one is on the call stack: an admin reactor or admin endpoint, which is a class whose name starts with `Admin`, or `SecurityAdminUtils`. The sandbox uses the same rule to keep those classes from custom reactors (`SemossClassLoader.requiresAdmin`). Being an admin class is the declaration, so an admin reactor does nothing more than check that its user is an admin, as it always does.

So an admin works on someone else's engine through the admin reactors and endpoints, which the screens of admin mode call, and a reactor every user runs gives an admin what their own permissions give. A reactor that serves owners has an admin counterpart for admin mode: `DeleteEngine` deletes an engine its user owns, and `AdminDeleteEngine` deletes any engine for an admin.

Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java) and [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java):

| Rule | Test |
| --- | --- |
| An admin calls any method through an admin class | `anAdminCallsAnyMethodThroughAnAdminClass` |
| Outside an admin class an admin has only their own permissions | `anAdminHasOnlyTheirOwnPermissionsOutsideAnAdminClass` |
| A user who is not an admin gains nothing from an admin class, and neither does a call with no signed in user | `aUserWhoIsNotAnAdminGainsNothingFromAnAdminClass`, `anAdminClassWithNoSignedInUserIsRefused` |
| An admin operation never runs under a custom reactor, such as an admin pixel a custom reactor runs through its insight | `anAdminOperationNeverRunsUnderACustomReactor` |
| Being an admin gives a custom reactor's own calls nothing | `beingAnAdminGivesACustomReactorNothing` |

A reactor every user runs checks the user's own permission, for an admin too, and its admin counterpart does the same work on any engine or project for an admin. The admin counterparts are in `prerna.auth.utils.reactors.admin`:

| Reactor | Its user needs | Admin counterpart |
| --- | --- | --- |
| `DeleteEngine` | Owner | `AdminDeleteEngine` |
| `DeleteDatabase` | Owner | `AdminDeleteDatabase` |
| `DeleteProject` | Owner | `AdminDeleteProject` |
| `DeleteWorkspace` | Owner | `AdminDeleteWorkspace` |
| `DeleteSkill` | Owner | `AdminDeleteSkill` |
| `CloseEngine` | Owner | `AdminCloseEngine` |
| `ExportEngine` | Owner | `AdminExportEngine` |
| `ExportProject` | Owner | `AdminExportProject` |
| `ExportProjectApp` | Owner | `AdminExportProjectApp` |
| `BackupDatabase` | Owner | `AdminBackupDatabase` |
| `CloneEngine` | Owner | `AdminCloneEngine` |
| `ReloadDatabase` | Editor | `AdminReloadDatabase` |
| `RdbmsReconnect` | Editor | `AdminRdbmsReconnect` |
| `GetEngineUserAccessRequest`, `GetProjectUserAccessRequest`, `GetInsightUserAccessRequest` | Editor | `AdminGetEngineUserAccessRequest`, `AdminGetProjectUserAccessRequest`, `AdminGetInsightUserAccessRequest` |
| `GetEngineSMSS`, `GetProjectSMSS` | Owner | `AdminGetEngineSMSS`, `AdminGetProjectSMSS` |

A setting that keeps deleting for admins, such as admin only engine delete, refuses everyone in the reactor every user runs, admins included, and so does the rule that only admins delete a workspace or asset database: an admin deletes through the admin counterpart. Creating something is different: an admin who creates an engine or an app, or uploads one, creates it as themselves, from the screens every user has, so the reactors that create let an admin through the settings that keep adding for admins, and the rule that only admins create a local file system storage engine. Saving an smss file follows the same split in Monolith: `/e-<engineId>/updateSmssFile` and `/project-<projectId>/updateSmssFile` for the owner, `/auth/admin/engine/updateSmssFile` and `/auth/admin/project/updateSmssFile` for an admin.

| Rule | Test |
| --- | --- |
| A reactor every user runs refuses an admin who holds no permission on the resource, and anyone below the level it needs | `theReactorRefusesAnAdminWhoHoldsNoPermissionOnTheResource`, `theReactorRefusesAnEditor` in [EngineAndProjectReactorPermissionsUnitTests](../../test/prerna/reactor/utils/EngineAndProjectReactorPermissionsUnitTests.java); [EngineReactorPermissionsUnitTests](../../test/prerna/reactor/engine/EngineReactorPermissionsUnitTests.java), [DatabaseReactorPermissionsUnitTests](../../test/prerna/reactor/database/DatabaseReactorPermissionsUnitTests.java), [SmssReactorPermissionsUnitTests](../../test/prerna/reactor/security/SmssReactorPermissionsUnitTests.java), [GetProjectUserAccessRequestReactorUnitTests](../../test/prerna/reactor/project/GetProjectUserAccessRequestReactorUnitTests.java), [GetInsightUserAccessRequestReactorUnitTests](../../test/prerna/reactor/insights/GetInsightUserAccessRequestReactorUnitTests.java) |
| While only admins delete, a delete reactor refuses an owner who is not an admin, and an admin who owns the resource | `aDeleteReactorRefusesAnOwnerWhoIsNotAnAdminWhileOnlyAdminsDelete`, `aDeleteReactorRefusesAnAdminWhoOwnsTheResourceWhileOnlyAdminsDelete` |
| An admin reactor refuses an owner who is not an admin, takes the keys of its counterpart and names it | `anAdminReactorRefusesAnOwnerWhoIsNotAnAdmin`, `anAdminReactorTakesTheKeysOfItsCounterpart`, `anAdminReactorNamesItsCounterpart` in [AdminCounterpartReactorsUnitTests](../../test/prerna/auth/utils/reactors/admin/AdminCounterpartReactorsUnitTests.java) |
| An admin reactor works for an admin who holds nothing on the resource | `adminGetEngineUserAccessRequestReturnsTheRequestsForAnAdminWhoHoldsNothing`, `adminCloseEngineClosesAnEngineForAnAdminWhoHoldsNothing` |
| An admin reactor's calls on a loaded engine go through the engine proxy for an admin who holds nothing, and the same admin calling the engine outside an admin reactor is refused | `adminReloadDatabaseReloadsADatabaseForAnAdminWhoHoldsNothing`, `adminRdbmsReconnectClosesTheConnectionsForAnAdminWhoHoldsNothing`, `outsideAnAdminReactorTheAdminCannotCallTheEngine` in [EngineCallsInAdminReactorsUnitTests](../../test/prerna/auth/utils/reactors/admin/EngineCallsInAdminReactorsUnitTests.java) |
| An admin reactor refuses an engine or project that does not exist before it does any work, and an app, agent or skill built into the platform | `adminDeleteEngineRefusesAnEngineThatDoesNotExist`, `adminDeleteProjectRefusesAProjectThatDoesNotExist`, `adminDeleteProjectDeletesNothingWhenOneOfTheProjectsDoesNotExist`, `adminDeleteDatabaseReloadAndExportRefuseAnIdThatDoesNotExist`, `adminDeleteProjectRefusesAnAppBuiltIntoThePlatform`, `adminDeleteWorkspaceAndSkillRefuseAnIdThatDoesNotExist`, `adminDeleteWorkspaceAndSkillRefuseTheOnesBuiltIntoThePlatform` |

## The signed in user on a request

Monolith clears what the thread kept from the request it served before (`StartUpSuccessFilter`), then sets the user of the request thread from its session (`WebUtility.loggingContext`), and clears the thread again when the request ends, health probes included. The insight websocket does the same for each message: the thread carries the socket's user, HTTP session and insight while the message runs, and nothing once it ends. A filter that signs a request in after it started, with an access key, a bearer token, a share token or a scheduler execution id, makes that user the thread's user too (`WebUtility.setSessionUser`). So a thread always carries the user of the request it is serving, and a request with no signed in user carries none.
