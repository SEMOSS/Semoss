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

- `value` is the level every call needs, whoever makes it: the platform for a user, a project's custom code, or another engine's own code.
- `projectCode` is a higher level for a call a project's custom code makes itself. It is set on the methods that hand back more than the platform's own callers pass on, such as a raw connection.

A call meets its level through the signed in user's permission, or through the permission the engine or project gives the app, agent or engine making the call.

| What the method does | Examples | Every call (`value`) | Custom code calling itself (`projectCode`) |
| --- | --- | --- | --- |
| Reads | `getEngineId`, `ask`, `embeddings`, `nearestNeighbor`, `list`, `copyToLocal`, `execute`, `getReactor` | `READ_ONLY` | `READ_ONLY` |
| Writes | `insertData`, `removeData`, `commit`, `commitOWL`, `copyToStorage`, `deleteFromStorage`, `addDocument`, `removeDocument`, `addPackage`, `pullRequirementsFile` | `EDIT` | `EDIT` |
| Hands back a connection or runs SQL as given | `getConnection`, `getPreparedStatement`, `execQuery`, `executeInTransaction`, `getQueryUtil`, `getConnectionMetadata` | `READ_ONLY` | `EDIT` |
| Reads one setting | `getCatalogSubType`, `getPipelineFile`, `getModelName`, `getMaxToolNameLength`, `getMaxTokens`, `getEmbedderEngineId`, `getBucketName`, `isBigData`, `getFunctionType`, `getJudgeModelEngineId` | `READ_ONLY` | `READ_ONLY` |
| Hands back the engine's settings, or picks the file it opens from | `getSmssProp`, `getOrigSmssProp`, `setSmssProp`, `getSmssFilePath`, `setSmssFilePath`, `open`, `getProperty`, `addProperty`, `setOwlFilePath`, `getConnectionUrl`, `getDataSource` | `OWNER` | `OWNER` |
| Reopens the engine, or changes a setting editors change | `reload`, `updateSetting` | `EDIT` | `OWNER` |
| Hands an app's sandbox its secrets | `getSandboxSecretSettings` | `READ_ONLY` | `OWNER` |
| Changes the engine itself | `delete`, `setEngineId`, `setEngineName`, `setDisplayName` | `OWNER` | `OWNER` |
| Reconfigures the engine | `closeDataSource`, `setBaseDataEngine`, `setInsightDatabase` | `EDIT` | `OWNER` |
| Closes the engine | `close` | `READ_ONLY` | `OWNER` |
| A project's own runtime | `getInsightDatabase`, `getProjectProperties`, `getProjectPyTranslator`, `compileReactors` | `READ_ONLY` | `EDIT` |

Guarded by [EngineAccessLevelsUnitTests](../../test/prerna/engine/api/EngineAccessLevelsUnitTests.java):

| Rule | Test |
| --- | --- |
| Every method of an engine or project interface has a level, so a new method cannot reach an engine unchecked | `everyMethodOfAnEngineOrProjectInterfaceHasALevel` |
| The level for custom code is one level, above the level of every call | `theLevelForProjectCodeIsOneLevelAboveTheLevelOfEveryCall` |
| The methods in the table above keep their levels | `theMethodsThatMatterMostKeepTheirLevels` |

To add a method to an engine interface, give it an `@EngineAccess` with the level the work it does needs: `READ_ONLY` to read, `EDIT` to write or reconfigure, `OWNER` to change the engine itself or hand back its settings. `everyMethodOfAnEngineOrProjectInterfaceHasALevel` fails until it has one.

## An engine's settings stay with its owners

An engine's smss holds its secrets: passwords, API keys, tokens and connection strings. So only its owners read or change its settings, or pick the file it opens from, whoever makes the call: the platform for a user, a project's custom code, or another engine. A Read-Only or Editor permission, a user's or one an engine gives another engine, never reaches them. The platform loading an engine, a platform operation and an admin operation still go through, as they do for every method.

Code that needs one harmless setting calls the getter for it, a `READ_ONLY` method that reads that one value from the engine's own settings: a model's name and the longest tool name it takes, a vector database's embedding model, a storage engine's bucket, a database's big data setting, a function's type, a guardrail's judge model, an engine's subtype and pipeline file. A new need gets a getter of its own, never the whole smss.

Editors keep two jobs that touch the smss, through methods that take no file and no free-form setting:

- `reload()` closes the engine and opens it again from its own registered smss file, as Reload Database and a model's metadata updates do.
- `updateSetting(key, value)` saves one setting of `IEngine.SETTINGS_EDITORS_CHANGE`, `PIPELINE` or `MCP_ENABLED`, to the smss file and the open settings, as the guardrail configuration and enabling MCP do. Any other key is refused.

Platform code that only needs where an engine's smss file is, such as the metamodel positions or the local master sync, reads it from the platform registry with `InternalUtility.getEngineSmssPath`, which a project's custom code cannot call.

A few features hand settings on by design:

- An app's sandbox gets the app's sandbox secrets for everyone who can open the app, through `IProject.getSandboxSecretSettings`, which returns only `SANDBOX_SECRETS` and its `SANDBOX_SECRET_*` values. Custom code reads them only for the app's owners.
- A proxy vector database opens a copy of its target with the target's settings, so it needs the target to give it Editor in its Engines section, or the signed in user to own the target ([ProxyVectorDatabaseEngineAccessUnitTests](../../test/prerna/engine/impl/vector/ProxyVectorDatabaseEngineAccessUnitTests.java)).
- A LiveKit room hands the model's keys to the user's Python, so only the model's owners join one.

Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java) and [EngineSettingsUnitTests](../../test/prerna/engine/api/EngineSettingsUnitTests.java):

| Rule | Test |
| --- | --- |
| A viewer or an editor never reads the settings or picks the file the engine opens from | `onlyAnOwnerReadsTheSettingsOrPicksTheFileTheEngineOpensFrom` |
| An owner does | `anOwnerReadsTheSettingsAndPicksTheFileTheEngineOpensFrom` |
| An editor reloads the engine and changes the settings editors change | `anEditorReloadsTheEngineAndChangesOnlyTheSettingsEditorsChange` |
| A permission an engine gives another engine reaches one setting, never the settings | `anEngineThatGaveAnotherEditorStillKeepsItsSettings` |
| A setting editors change is saved to the smss file and the open settings; any other is refused | `aSettingEditorsChangeIsSavedToTheSmssFileAndTheOpenSettings`, `anyOtherSettingIsLeftToTheOwners` |
| Reloading opens the engine from its own file | `reloadingClosesTheEngineAndOpensItFromItsOwnFile` |
| Each getter reads its one setting | `eachGetterReadsItsOneSetting`, `aVectorDatabaseNamesItsEmbedderByEitherKey`, `s3AndGoogleCloudStorageNameTheirBucket` |
| An app hands its sandbox only its sandbox secrets | `aProjectHandsOverOnlyItsSandboxSecrets` |

## Who is acting

The check reads the call stack from the proxy down and stops at the first of these it finds:

| Who | How it is recognized |
| --- | --- |
| A project's custom code | A class loaded by the [project sandbox](project_reactor_sandbox.md). The classes of the JDK and of shared libraries are passed over, so a call a project makes through a lambda, a stream or a library is still the project's |
| An engine | An engine's own class, or another proxy call: a vector database calling the model that makes its embeddings, a function engine opening its storage engine, a guardrail calling its judge model. The engine is the one answering the proxy call that runs on the thread, or the one being opened, so the calls an engine makes from its `open` are its own ([EngineCallPermission](../../src/prerna/engine/impl/pipeline/EngineCallPermission.java)). An engine object that a project's custom code creates or extends and calls itself, with no proxy in between, acts as that project's custom code |
| The platform loading an engine or project | The `Utility` getter that loads it, when no engine being opened comes first |
| The platform | Anything else. A reactor a user runs inside an engine's call, such as a tool of a project, is the platform again |

The platform and engines can be acting inside an [agent's run](#agent-runs), which lets them use the permissions given to the agent as well.

## Calls from the platform

The platform needs the method's level for the signed in user. The user is the one signed in on the thread, or else the user of the insight or room the call carries, which is how a pooled thread names its user.

Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java):

| Rule | Test |
| --- | --- |
| A user calls the methods their level covers: a reader reads, an editor also writes, an owner also deletes | `aUserCallsTheMethodsTheirLevelCovers` |
| A user with no permission is refused every method, with the same answer as for an engine that does not exist | `aUserWithNoPermissionIsRefusedEveryMethod` |
| A refusal names the level the method needs | `aRefusalNamesTheLevelTheMethodNeeds` |
| For a user who can view an engine, the platform gets a connection and reads one setting at a time | `thePlatformGetsAConnectionAndOneSettingAtATimeForAUserWhoCanView` |
| Permission on one engine gives nothing on another | `permissionOnOneEngineGivesNothingOnAnother` |
| A project is checked against the user's permission on the project | `aProjectIsCheckedAgainstTheUsersPermissionOnTheProject` |
| A user off the thread is read from the insight of the call | `aUserOffTheThreadIsReadFromTheInsightOfTheCall` |
| A call with no signed in user is refused, whether or not the security database lists the engine | `aCallWithNoSignedInUserIsRefused`, `aCallWithNoSignedInUserIsRefusedOnAnEngineTheSecurityDatabaseDoesNotList` |
| Methods of `Object`, such as `toString`, are not checked | `methodsOfObjectAreNotChecked` |

A call from the platform also goes through in these cases:

| Case | Why | Test |
| --- | --- | --- |
| A reactor a user runs inside an engine's call | It is checked as the user, like any other reactor | `aReactorAUserRunsInsideAnEngineCallIsCheckedAsTheUser` |
| The platform is loading the engine or project | Loading syncs the engine's files, whoever asked for it | `thePlatformLoadingAnEngineGoesThrough` |
| The proxy was handed out for the platform's own use | A feature uses the model a platform setting names for every user of the feature; see [engines the platform uses for everyone](#engines-the-platform-uses-for-everyone) | `aProxyHandedOutForThePlatformsOwnUseServesAnyUser` |
| The platform is running a platform operation | The platform's own work for no user, such as a cluster sync; see [platform operations](#platform-operations) | `aPlatformOperationGoesThroughWithNoSignedInUser`, `aPlatformOperationGoesThroughForAUserWithNoPermission` |
| The method needs `READ_ONLY` and the engine or project is discoverable | Anyone can look at a discoverable engine's details before asking for access | `thePlatformReadsADiscoverableEngineForAUserWithNoPermission` |
| The method needs `READ_ONLY` on a project and the user can open one of its insights | An insight is shared on its own, and opening it reads its project | `thePlatformReadsAProjectForAUserWhoCanOpenOneOfItsInsights` |
| The user is an admin and an admin operation is running | Admin reactors and endpoints work on every engine; see [admin operations](#admin-operations) | `anAdminCallsAnyMethodThroughAnAdminClass` |
| The security database does not list the engine or project | It is being created or deleted | `anEngineTheSecurityDatabaseDoesNotListGoesThrough` |
| The JVM has no security database | The Java worker holds stand-ins that send each call back to the server, which checks it there | `aJvmWithoutTheSecurityDatabaseChecksNothing` |

## Calls from an engine

An engine's own code calling another engine needs the method's level, and has it in one of three ways:

- the user the call is for holds it on the other engine,
- the other engine gave the calling engine a permission that covers it, or
- an app's custom code made the call the calling engine answers, directly or through other engines' calls, and the other engine gave that app a permission that covers the level the app's own code needs for the method (`projectCode`); see [giving an app, an agent or an engine a permission](#giving-an-app-an-agent-or-an-engine-a-permission), or
- the call is part of an [agent's run](#agent-runs) and the other engine gave the agent a permission that covers it.

The checks on an engine's pipeline are the engine's own calls. When an app's custom reactor asks a model that runs a guardrail, the guardrail goes through on the permission it gives the model, or on the one it gives the app. A guardrail that asks a judge model calls it as the guardrail: the judge model goes through on the permission it gives the guardrail, or on the one it gives the app whose custom reactor started the chain. The app's permission counts at the level the app's own code needs, so an engine between them never gives the app more than calling the engine itself would: with `READ_ONLY` from a database, an app's custom reactor reads it through a model, and needs `EDIT` for the model to get a connection to it for the app. The app is read from the call stack, below the engines' own code, their proxies and the checks of their pipelines. A reactor the user runs, such as a pixel the custom reactor runs through its insight, or the platform loading an engine, between the app's code and the engine leaves the app out, since the call below it is the platform's and is checked as the user.

A call that goes through on the app's permission is the app's call to the engine it reaches, so on a model it counts against the app's App Limit there. One that goes through on the agent's permission counts against the agent's App Limit the same way.

It also goes through when the engine calls itself, when the engine answering is a project, whose own code is the platform's, when the other engine is a venv engine, which only names where its Python runs, and when the method needs `READ_ONLY` and the other engine is public. A platform operation, a proxy handed out for the platform's own use, an admin operation for an admin, and an engine the security database does not list go through as they do for the platform. Reading a discoverable engine through the platform does not apply to an engine's call. An engine calling a project is checked as the platform is.

Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java):

| Rule | Test |
| --- | --- |
| An engine calling an engine that gave it nothing is refused | `anEngineCallingAnEngineThatGaveItNothingIsRefused` |
| An engine calls an engine that gave it a permission covering the method, and the engine it calls knows it | `anEngineCallsAnEngineThatGaveItAPermissionThatCoversTheMethod` |
| A permission given to an app does not cover an engine with the same id | `aPermissionGivenToAnAppDoesNotCoverAnEngineWithTheSameId` |
| An engine calling an engine the user can use goes through | `anEngineCallingAnEngineTheUserCanUseGoesThrough` |
| An engine calling itself goes through | `anEngineCallingItselfGoesThrough` |
| An engine reads a public engine, and needs a permission to write to it | `anEngineReadsAPublicEngineWithNoPermission` |
| An engine uses a venv engine | `anEngineUsesAVenvEngineWithNoPermission` |
| An engine opening calls another engine as itself | `anEngineOpeningCallsAnotherEngineAsItself` |
| A model's call to an engine that gave it nothing is refused, and one the user can use goes through | `aModelsCallToAnEngineThatGaveItNothingIsRefused`, `aModelsCallToAnEngineTheUserCanUseGoesThroughWithNoGrant` in [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) |
| A guardrail on a model's pipeline goes through on the permission it gives the model, or on the one it gives the app whose custom reactor asked the model, and not another app's | `aGuardrailOnAModelsPipelineGoesThroughOnThePermissionItGivesTheModel`, `aGuardrailOnAModelsPipelineGoesThroughOnThePermissionItGivesTheAppBehindTheModel` in [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) |
| The app behind an engine's call needs the level its own code needs for the method | `theAppBehindAnEnginesCallNeedsTheLevelItsOwnCodeNeeds` in [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) |
| A reactor the user runs between the app's code and the engine leaves the app out | `theAppIsLeftOutOfAnEnginesCallWhenAReactorTheUserRunsIsBetweenThem` in [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) |
| An app's custom reactor reaches a guardrail's judge model through a model and the guardrail on its pipeline, each engine going through on the permission it gives the engine calling it or the one it gives the app, and the chain stops at the first engine that gives neither, with the real pipeline and the security database | `eachEngineInTheChainGoesThroughOnThePermissionItGivesTheEngineCallingIt`, `eachEngineInTheChainGoesThroughOnThePermissionItGivesTheApp`, `anEngineThatGivesNeitherTheEngineCallingItNorTheAppAPermissionStopsTheChain`, `anAppTheModelGivesNothingIsRefusedAtTheModel` in [AppEngineChainUnitTests](../../test/prerna/util/AppEngineChainUnitTests.java) |
| In the same chain, taking the app's permission on the model away stops it at the model, a permission the model gives another app does not count, and an agent's run around the app's reactor lends the reactor nothing | `takingTheAppsPermissionAwayOnTheModelStopsTheChainAtTheModel`, `aPermissionTheModelGivesAnotherAppDoesNotCount`, `anAgentRunAroundTheAppsReactorLendsTheReactorNothing` in [AppEngineChainUnitTests](../../test/prerna/util/AppEngineChainUnitTests.java) |

## Calls from custom reactors

A project's custom code needs the method's level, and the `projectCode` level when it makes the call itself. It has the level in one of two ways:

- the signed in user holds it on the engine, or
- the engine or project gave the app a permission that covers it; see [giving an app, an agent or an engine a permission](#giving-an-app-an-agent-or-an-engine-a-permission).

On its own project, a project's custom code has `EDIT`. Custom code running inside an agent's run is checked as its own app, never as the agent. A task a custom reactor submits to a `SignedInTasks` executor runs as the user who submitted it, so its calls are checked the same way; see [work on other threads](project_reactor_sandbox.md#work-on-other-threads).

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
| A custom reactor needs the user's permission on another project, or a permission that project gives its app | `aCustomReactorNeedsTheUsersPermissionOnAnotherProject`, `aCustomReactorReadsAnotherProjectItsAppWasGivenAccessTo` |
| A custom reactor inside an agent's run does not use the permissions given to the agent | `aCustomReactorInsideAnAgentRunDoesNotUseTheAgentsPermission` |
| A project's loader names its project | `aProjectsLoaderNamesItsProject` |
| An engine object a custom reactor creates or extends and calls itself reaches other engines with the user's permission or the app's, like the reactor's own calls | `anEngineObjectACustomReactorDrivesItselfActsAsItsProject` |

The sandbox adds rules of its own, in [SandboxReplacedCallsUnitTests](../../test/prerna/util/SandboxReplacedCallsUnitTests.java) and [SandboxBlockedClassesUnitTests](../../test/prerna/util/SandboxBlockedClassesUnitTests.java):

| Rule | Test |
| --- | --- |
| The query utility an RDBMS engine hands out (`getQueryUtil()`) builds SQL for a custom reactor, and its user name, password, connection properties and connection URL stay with the engine | `anEnginesCredentialsCannotBeReadOrChangedThroughItsQueryUtility` |
| The classes that build the proxy and keep the call's state, `EngineProxyFactory`, `PipelineInvocationHandler`, `EngineCallPermission` and `AgentRunOperation`, cannot be used by a custom reactor | `platformStateAndInternalHelpersAreBlocked` |
| A custom reactor reads what the thread says about the request it serves and never changes it: the signed in user, the insight, session, route and job, the context project and the thread's whole map | `theSignedInUserCannotBeChanged`, `theRequestTheThreadServesCannotBeChanged` |
| A custom reactor cannot use `InsightStore`, which holds every open insight of every user | `platformStateAndInternalHelpersAreBlocked` |

## Giving an app, an agent or an engine a permission

An app often works with an engine for people who have no access to that engine themselves: a form that inserts into a database, a workflow that adds a document to a storage engine. Engines work with each other the same way: a vector database embeds with a model, a guardrail asks a judge model, a model runs the guardrails on its pipeline, a router passes a call to the models it routes to, a function engine opens a storage engine. Projects do too: an agent loads a skill, an app reads another app's insights. An owner of the engine or project gives the app, the agent or the other engine a permission on it, and the app's custom reactors, the agent's runs, or the other engine's own code, then use it with that permission, whoever the call is for. The owner does it on the Access Control tab of the engine or project, on the Resource Delegated Access page in Settings (an admin uses All Resource Delegated Access, in admin mode), or with a call to Monolith, like every other permission. An engine gives a permission to apps, agents and other engines; a project gives one to apps and agents only. The grantee is named by its id and its catalog type, `PROJECT` for an app or an agent:

```
POST /Monolith/api/auth/engine/setEngineGranteePermission
engineId=<engine id>&granteeEngineId=<app id>&granteeEngineType=PROJECT&permission=EDIT
```

```
POST /Monolith/api/auth/engine/setEngineGranteePermission
engineId=<embedding model id>&granteeEngineId=<vector database id>&granteeEngineType=VECTOR&permission=READ_ONLY
```

```
POST /Monolith/api/auth/project/setProjectGranteePermission
projectId=<skill id>&granteeEngineId=<agent id>&granteeEngineType=PROJECT&permission=READ_ONLY
```

With an `EDIT` permission an app's reactors get the connection and run their own transactions:

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

| Level | What the app's custom reactors, the agent's runs, or the other engine's code, can do |
| --- | --- |
| `READ_ONLY` | Call the engine's read methods: run a query through the platform's query classes, ask a model or embed with it, list and download from a storage engine, search a vector database. On a project: read it, such as an agent loading a skill's files or an app opening another app's insights |
| `EDIT` | Everything above, and write: get the connection and prepared statements, insert, update and delete, add to and delete from a storage engine, add and remove documents. On a project: also its runtime, such as compiling its reactors |

A grantee is never an owner of an engine or a project, so their settings and secrets stay with their owners, and an engine or project is never given a permission on itself. A usage limit is only on a model's permission. The one engine whose work needs another engine's settings is a proxy vector database, which opens a copy of its target with them: Editor from the target lets it; see [an engine's settings stay with its owners](#an-engines-settings-stay-with-its-owners).

### The usage limit on a model's permission

On a model, the permission carries the grantee's limit, set by the model's owners the way a team's limit is: the App Limit for an app and the Engine Limit for an engine. It has an overall amount for the grantee (rules that count `SHARED`), an amount for each user (rules that count `EACH_PERSON`), or both. It counts the calls the app's custom reactors, or the engine's own code, make to the model for people whose only way to it is that app or engine: those with no grant of their own and none through a team. A person with a grant of their own, or a team's, counts against their own limits there too. The limit comes after Team Limits and before the Public Use Limit, so on a public model the grantee's users count against its limit when one is defined. When the overall amount is used up, the model refuses the grantee's calls for everyone until it resets; when one person's amount is used up, it refuses that person's. With no limit defined, the call counts against the Public Use Limit on a public model, then the person's User's Total Model Limit and the General Platform Limit. See [model usage limits](../model_usage_limits/README.md).

```
POST /Monolith/api/auth/engine/setEngineGranteePermission
engineId=<model id>&granteeEngineId=<guardrail id>&granteeEngineType=GUARDRAIL&permission=READ_ONLY&usageLimit={"unlimited": false, "rules": [
    {"metric": "TOKENS", "amount": 1000000, "window": "DAY", "counts": "SHARED"},
    {"metric": "TOKENS", "amount": 50000, "window": "DAY", "counts": "EACH_PERSON"}]}
```

Leaving `usageLimit` out keeps the limit the grantee has, and an empty `usageLimit` removes it.

The endpoints are Monolith's, beside the ones for people and teams. An owner uses `/auth/engine` and `/auth/project`, and an admin in admin mode uses `/auth/admin/engine` and `/auth/admin/project` on any engine or project:

| Endpoint | Who calls it | Form or query fields | What it does |
| --- | --- | --- | --- |
| `POST /auth/engine/setEngineGranteePermission` | An owner of the engine | `engineId`, `granteeEngineId`, `granteeEngineType`, `permission`, and optionally `endDate` and `usageLimit`, the grantee's limit on a model | Gives the app or engine the level, or changes the one it has |
| `POST /auth/engine/removeEngineGranteePermission` | An owner of the engine | `engineId`, `granteeEngineId`, `granteeEngineType` | Takes the permission away |
| `GET /auth/engine/getEngineGranteePermissions` | An editor or owner of the engine | `engineId` | Lists the apps and engines the engine gives a permission to |
| `POST /auth/admin/engine/setEngineGranteePermission` | An admin | The same fields | The same, on any engine |
| `POST /auth/admin/engine/removeEngineGranteePermission` | An admin | The same fields | The same, on any engine |
| `GET /auth/admin/engine/getEngineGranteePermissions` | An admin | `engineId` | The same, on any engine |
| `POST /auth/project/setProjectGranteePermission` | An owner of the project | `projectId`, `granteeEngineId`, `granteeEngineType` (`PROJECT`), `permission`, and optionally `endDate` | Gives the app or agent the level, or changes the one it has |
| `POST /auth/project/removeProjectGranteePermission` | An owner of the project | `projectId`, `granteeEngineId`, `granteeEngineType` | Takes the permission away |
| `GET /auth/project/getProjectGranteePermissions` | An editor or owner of the project | `projectId` | Lists the apps, agents and engines the project gives a permission to |
| `POST /auth/admin/project/setProjectGranteePermission`, `removeProjectGranteePermission`, `GET getProjectGranteePermissions` | An admin | The same fields | The same, on any project |
| `GET /auth/permissionBatch/getAllGranteePermissions` | Anyone signed in | `search`, `targetKind`, `targetType`, `granteeKind`, `granteeType`, `limit`, `offset`, all optional | Lists the permissions given on every engine and project the user can edit, for the Resource Delegated Access page |
| `GET /auth/admin/permissionBatch/getAllGranteePermissions` | An admin | The same fields | The same, on every engine and project, for the All Resource Delegated Access page |

The permission is part of the permission batch model ([prerna.auth.access](../../src/prerna/auth/access/AGENTS.md)): the subject is an `ENGINE`, with the grantee's id and catalog type, on an `ENGINE` or `PROJECT` target, saved through `PermissionBatchUtils` and stored in `ENGINE2ENGINEPERMISSION`, with the type of the target giving it (an engine's catalog type, or `PROJECT`) and the grantee's catalog type. Bulk Access Management and the Resource Delegated Access page send it as:

```json
[{"target": {"resourceType": "ENGINE", "id": "<engine id>"},
  "changes": [{"action": "ADD",
               "subject": {"category": "ENGINE", "id": "<app id>", "type": "PROJECT"},
               "to": {"level": "EDIT", "endDate": null, "usageLimit": null}}]}]
```

`getPermissions` on an engine or a project returns its apps, agents and engines as `engines`, each with its type, its name (the display name when it has one), its subtype (an engine's subtype, or a project's type, such as `CODE`, `BLOCKS` or `WORKSPACE` for an agent), and its level, end date and, on a model, limit. Only the editors and owners of the engine or project, and admin mode, see them: `allowedPermissionChanges.canViewEngines` says whether the user does, and anyone else gets an empty list. `allowedPermissionChanges.canChangeEngines` says whether the user can change them.

The same permission shows from the other side, on the Access Control tab of the app, agent or engine it is given to. `getPermissions` on a project or an engine returns `accessToEngines`: the engines and projects that give it a permission, each with its `target`, the engine or project giving it (`resourceType` `ENGINE` or `PROJECT`, its id, type (`PROJECT` for a project), name and subtype (a project's type for a project)), and its `access`, the level, end date and, on a model, the App Limit or Engine Limit the model gives. The engine or project that gives it holds it, and its owners change it on its Access Control tab. The app's or engine's editors and owners, and admin mode, see it: `allowedPermissionChanges.canViewAccessToEngines` says whether the user does, and anyone else gets an empty list.

```json
{"accessToEngines": [{"target": {"resourceType": "ENGINE", "id": "<model id>", "type": "MODEL", "name": "Orders model", "subtype": "OPEN_AI"},
                      "access": {"level": "READ_ONLY", "endDate": null, "usageLimit": null}},
                     {"target": {"resourceType": "PROJECT", "id": "<skill id>", "type": "PROJECT", "name": "Orders skill", "subtype": "SKILL"},
                      "access": {"level": "READ_ONLY", "endDate": null, "usageLimit": null}}]}
```

`getAllGranteePermissions` lists them all in one place: every permission given on the engines and projects the user can edit, or on every one in admin mode, each with its `target` and its `subject` (shaped as above), its `access`, and `canChange`, true for the target's owners and in admin mode. `targetKind` and `granteeKind` take `PROJECT` or `ENGINE`, so the pages list what projects use of projects, what projects use of engines and what engines use of engines apart; any other kind is refused with 400. `targetType` and `granteeType` take a project's type, such as `SKILL` or `WORKSPACE`, or an engine's catalog type, such as `MODEL`; `search` matches the name or id of either side, ignoring case. `total` is how many match, and `limit` and `offset` page them.

```json
{"grants": [{"target": {"resourceType": "PROJECT", "id": "<skill id>", "type": "PROJECT", "name": "Orders skill", "subtype": "SKILL"},
             "subject": {"category": "ENGINE", "id": "<agent id>", "type": "PROJECT", "name": "Orders agent", "subtype": "WORKSPACE"},
             "access": {"level": "READ_ONLY", "endDate": null, "usageLimit": null},
             "canChange": true}],
 "total": 1}
```

Guarded by [EngineGranteePermissionUnitTests](../../test/prerna/auth/utils/EngineGranteePermissionUnitTests.java), and the endpoints by `EngineAuthorizationResourceTest`, `AdminEngineAuthorizationResourceTest`, `ProjectAuthorizationResourceTest`, `AdminProjectAuthorizationResourceTest`, `PermissionBatchResourceTest` and `AdminPermissionBatchResourceTest` in Monolith:

| Rule | Test |
| --- | --- |
| An owner gives an app or another engine a permission on the engine | `anOwnerGivesAnAppAPermissionOnTheEngine`, `anOwnerGivesAnotherEngineAPermission` |
| The permission is for that grantee and that engine only, and an app and an engine with the same id are two grantees | `thePermissionIsForThatAppAndThatEngineOnly`, `anAppAndAnEngineWithTheSameIdAreTwoGrantees` |
| The grantee's type is a catalog type, read ignoring case, and an engine named with another type is refused | `aGranteeTypeIsReadIgnoringCase`, `aGranteeWithNoCatalogTypeIsRefused`, `anEngineNamedWithAnotherCatalogTypeIsRefused` |
| An engine is never given a permission on itself | `anEngineIsNeverGivenAPermissionOnItself` |
| Setting again changes the level | `settingAgainChangesTheLevel` |
| An owner takes the permission away | `anOwnerTakesThePermissionAway`, `takingAwayAPermissionThatIsNotThereIsRefused` |
| Only an owner of the engine gives or takes away the permission | `onlyAnOwnerOfTheEngineGivesAnAppAPermission`, `onlyAnOwnerOfTheEngineTakesAnAppsPermissionAway` |
| Someone who is not an owner learns nothing about the grantee or the engine | `someoneWhoIsNotAnOwnerLearnsNothingAboutTheApp` |
| An admin gives, takes away and lists in admin mode, on an engine that exists | `anAdminGivesAndTakesAwayInAdminMode`, `adminModeRefusesAnEngineThatDoesNotExist` |
| A grantee is never an owner of an engine | `anAppIsNeverAnOwnerOfAnEngine` |
| The level is `READ_ONLY` or `EDIT`, and the grantee exists | `aLevelThatIsNotOneOfTheLevelsIsRefused`, `anAppThatDoesNotExistIsRefused` |
| A project's owner gives an agent a permission on a skill, or an app one on another app, and it is for that project only | `aSkillsOwnerGivesAnAgentAPermissionOnTheSkill`, `anAppIsGivenAPermissionOnAnotherApp` |
| Only a project's owners give or take away its permission, and an admin does in admin mode | `onlyAProjectsOwnerGivesOrTakesAwayItsPermission`, `anAdminGivesAnAgentAPermissionOnAProjectInAdminMode` |
| A project is never given a permission on itself, and its permissions carry no usage limit | `aProjectIsNeverGivenAPermissionOnItselfAndItsGrantsHaveNoLimits` |
| A project's permissions list its agents, and the agent's permissions list the project | `aProjectsPermissionsListItsAgentsAndTheAgentSeesTheProjectItCanUse` |
| Deleting a project removes the permissions it gave | `deletingAProjectRemovesThePermissionsItGave` |
| The Resource Delegated Access pages list the permissions on what the user can edit, or on everything in admin mode, say which can be changed, filter by each side's kind and type and by text, and page | `theGrantsPageListsThePermissionsOnWhatTheUserCanEditAndOnEverythingInAdminMode`, `theGrantsPageFiltersByTypeAndTextAndPages`, `theGrantsPageSplitsWhatProjectsAndEnginesUseByTheKindOfEachSide` |
| An owner gives an app or engine a limit on a model, with an amount for the grantee and one for each user | `anOwnerGivesAnAppALimitOnAModelWithAnAmountForTheAppAndOneForEachUser`, `anOwnerGivesAnEngineALimitOnAModel` |
| The limit is only on a model, and one that cannot be defined is refused | `anAppsLimitIsOnlyOnAModel`, `anAppsLimitThatCannotBeDefinedIsRefused` |
| Changing the level keeps the limit, and sending no limit defined removes it | `changingAnAppsLevelKeepsItsLimit`, `sendingNoLimitDefinedRemovesAnAppsLimit` |
| Taking the permission away, or deleting the grantee, removes its limit | `takingAnAppsPermissionAwayRemovesItsLimit`, `deletingTheAppRemovesItsLimits`, `deletingAnEngineRemovesThePermissionsItWasGivenAndTheirLimits` |
| A grantee is in a batch once, and is added once | `anAppIsInABatchOnce`, `aBatchThatAddsAnAppTwiceInARowIsRefusedTheSecondTime` |
| A permission has an optional end date, and one that has ended is not in effect | `anEndDateThatCannotBeReadIsRefused`, `aPermissionThatHasEndedIsNotInEffect` |
| A permission that has ended is removed with its limit and recorded as ended | `anEndedAppOrEnginePermissionTakesItsLimitWithIt`, `anEndedGrantNamesWhoHadItAndTheLevel` |
| A batch sent as JSON gives an app a permission | `aBatchSentAsJsonGivesAnAppAPermission` |
| An engine's permissions list its apps and engines with their types, and tell an owner they can change them | `theEnginesPermissionsListItsApps`, `theEnginesPermissionsListItsAppsAndEnginesWithTheirTypes`, `onlyAnOwnerIsToldTheyCanChangeTheApps`, `aProjectThatGivesNoPermissionListsNoApps` |
| Only the engine's editors and owners list its grantees | `onlyTheEnginesEditorsAndOwnersListItsApps` |
| Someone who can only view the engine sees none of its grantees in its permissions | `aReaderSeesNoneOfTheEnginesAppsInItsPermissions` |
| An app's permissions list the engines that give it a permission, with each one's level and limit | `anAppsPermissionsListTheEnginesThatGiveItAPermission` |
| An engine's permissions list the engines that give it a permission apart from the ones it gives one to, and an app with the same id sees none of them | `anEnginesPermissionsListTheEnginesThatGiveItAPermissionApartFromTheOnesItGivesOne`, `aPermissionGivenToAnEngineIsNotListedForAnAppWithTheSameId` |
| Only the editors and owners of the app or engine, and admin mode, see the engines that give it a permission | `onlyTheEditorsAndOwnersOfAnAppOrEngineSeeTheEnginesThatGiveItAPermission` |
| Deleting the engine or the grantee removes the permission | `deletingTheEngineRemovesThePermissionsItGave`, `deletingTheAppRemovesThePermissionsItWasGiven` |

The limit's own rules are guarded where the other limits' are:

| Rule | Test |
| --- | --- |
| A model answering a call an app's custom reactors make knows the app, whether the call went through on the app's permission or on the user's own, and an engine it calls with a permission knows the model | `aModelAnsweringACallMadeWithAnAppsPermissionKnowsTheApp`, `aModelKnowsTheAppMakingTheCallWhenTheUserHasAccessOfTheirOwn` in [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java) |
| A call of an app the model gives no permission, a call the platform makes, and a refused call carry no grantee | `aModelSeesNoAppWhenItGivesTheCallingAppNoPermission`, `aModelSeesNoAppOnACallThePlatformMakes`, `aRefusedCallLeavesNoAppBehind` |
| A call an app or engine makes for a person with no grant is billed to its limit, before public use | `resolve_aCallAnAppMakesForAPersonWithNoGrantIsBilledToTheAppsLimit`, `resolve_aCallAnEngineMakesForAPersonWithNoGrantIsBilledToTheEnginesLimit`, `resolve_theAppsLimitComesBeforePublicUse`, `theGuardKeepsTheAppsPathApartFromThePersonsOwn` in [SecurityModelUsageLimitUtilsUnitTests](../../test/prerna/auth/utils/SecurityModelUsageLimitUtilsUnitTests.java) |
| A grantee with no limit defined passes on to public use, then to the person's all-models default | `resolve_anAppWithNoLimitDefinedPassesOnToPublicUse`, `resolve_anAppWithNoLimitDefinedPassesOnToThePersonsAllModelsDefault` |
| The limit is skipped for a person with a grant of their own or a team's | `resolve_anAppsLimitIsSkippedForAPersonWithAGrantOfTheirOwn`, `resolve_anAppsLimitIsSkippedForAPersonWithATeamsGrant` |
| The limit is not on a path for a call no grantee makes, one grantee's limit is not another's, and a grantee that does not exist is refused | `resolve_anAppsLimitIsNotOnAPathForACallNoAppMakes`, `resolve_oneAppsLimitIsNotAnotherApps`, `resolve_anAppThatDoesNotExistIsRefused`, `resolve_anEngineThatDoesNotExistIsRefused` |
| The overall amount is shared by everyone who uses the grantee, and the amount for each user stops only that user | `anAppsOverallAmountIsSharedByEveryoneWhoUsesTheApp`, `anAppsAmountForEachUserStopsOnlyThatUser`, `whatOneAppUsesDoesNotCountAgainstAnother`, `aCallAnEngineMakesForAPersonWithNoGrantIsBilledToTheEnginesLimit`, `anEnginesUsedUpLimitNamesTheEngine` in [ModelUsageGuardUnitTests](../../test/prerna/engine/impl/model/usage/ModelUsageGuardUnitTests.java) |
| The grantee's counters are filled from the calls billed to it | `anAppsCountersAreFilledFromTheCallsBilledToTheApp`, `aCounterKeepsItsNameAndAnAppsCounterEndsWithTheAppAndItsType` |

## Agent runs

An agent is a project of type `WORKSPACE`. Its runs use what it was given: a skill, a model, a database or another agent whose owner gave the agent a permission is open to the agent's runs for everyone who may use the agent, as an app's custom reactors use what was given to the app. So an agent's owner can hand it a skill its users cannot open themselves.

`AgentRunner` declares the run around its work, after it reads the agent and checks that the person may use it:

```java
try (AgentRunOperation run = AgentRunOperation.begin(agentId)) {
	stageSkills();
	harness.execute();
}
```

A run that names a workspace, explicitly or through its room, checks access to
that agent before selecting or checking the model. A workspace that is missing,
is not an agent, or the person may not use fails with
`You do not have access to workspace <id>`. The server log includes the run, room
and workspace ids. A run with no workspace continues on the person's own permissions.

Inside it, a call the person's own access does not cover goes through when the engine or project called gave the agent a permission that covers the call's level (`value`). This covers the platform's Java code on the run's thread: loading the agent's model and skills, the tools the harness runs, and the engines those calls reach, where an engine's call counts the agent as the app behind it. On a model a call that goes through this way counts against the agent's App Limit.

[AgentRunOperation](../../src/prerna/engine/impl/pipeline/AgentRunOperation.java) belongs to the run's thread:

- A thread the run starts does not carry it. The run's own parallel tool calls begin it again for the same agent, and so does the thread that names a new room, so a model given to the agent names the room too. Each begins it through `AgentUseAccess.beginAgentRun`, which begins nothing for a person who may not use the agent.
- Python reaches the platform on threads of its own, so whatever Python does is checked as the person.
- A project's custom code is checked as its own app, even inside a run, and cannot load `AgentRunOperation`.
- Automations act as the person who runs them. An agent an automation starts runs as that agent, with the agent's permissions.

Who may use an agent is read by [AgentUseAccess](../../src/prerna/reactor/agent/AgentUseAccess.java): a person who can view it, or, in a sub-agent's room, a person who can view an agent above it when each agent on the way was given the one below it. When a sub-agent is spawned, its room records the agents it was started under (`subagent_agent_chain`): the ones its parent room records, then the agent whose run spawned it, unless the sub-agent runs that same agent. The check reads only that list and the grants, so it works after the parent run ends or the server restarts, and it never loads another room. So an agent given another agent spawns it as a sub-agent for people who cannot view the sub-agent themselves.

A skill the agent was not given, and the person cannot view, is left out of the run; that is checked before the skill is loaded. The run shows it as a failed `load_skill` tool call that names the skill and tells the person how to give the agent access to it.

Guarded by [EngineAccessGuardUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessGuardUnitTests.java) and [SandboxEngineAccessUnitTests](../../test/prerna/util/SandboxEngineAccessUnitTests.java):

| Rule | Test |
| --- | --- |
| An agent's run calls an engine that gave the agent a permission, and the model knows the agent | `anAgentRunCallsAnEngineThatGaveTheAgentAPermission` |
| An agent's run reads a project that gave the agent a permission | `anAgentRunReadsAProjectThatGaveTheAgentAPermission` |
| A permission given to an agent on an engine does not cover a project with the same id | `aPermissionGivenToAnAgentOnAnEngineDoesNotCoverAProjectWithTheSameId` |
| An engine's call inside an agent's run counts the agent as the app behind it | `anEngineCallInsideAnAgentRunCountsTheAgentAsTheAppBehindIt` |
| A run ends when it is closed, and a run begun inside another puts the outer one back | `anAgentRunEndsWhenItIsClosedAndAnInnerRunPutsBackTheOuterOne` |
| A run is not carried to a thread it starts | `anAgentRunIsNotCarriedToAThreadItStarts` |
| A custom reactor inside a run does not use the agent's permissions | `aCustomReactorInsideAnAgentRunDoesNotUseTheAgentsPermission` |

End to end, with the run begun through `AgentUseAccess.beginAgentRun`, the real engine proxy and the grants saved in the security database, in [AgentEngineChainUnitTests](../../test/prerna/util/AgentEngineChainUnitTests.java):

| Use case | Test |
| --- | --- |
| The person's own access needs no agent and no grant | `thePersonsOwnAccessNeedsNoAgentAndNoGrant` |
| An agent's run asks a model the agent was given, and nothing outside the run can | `anAgentsRunAsksAModelTheAgentWasGivenAndNothingElseCanAskIt` |
| A person who cannot view the agent gets none of its access | `aPersonWhoCannotViewTheAgentGetsNoneOfItsAccess` |
| A Read-Only grant lets the agent ask the model but not change it | `aReadOnlyGrantLetsTheAgentAskTheModelButNotChangeIt` |
| An agent's run reads a skill the agent was given | `anAgentsRunReadsASkillTheAgentWasGiven` |
| A sub-agent's run uses the sub-agent's grants and never its parent's | `aSubAgentsRunUsesTheSubAgentsGrantsAndNeverItsParents` |
| A sub-agent its parent was not given gets none of its access | `aSubAgentItsParentWasNotGivenGetsNoneOfItsAccess` |
| A thread the run starts asks as the person only | `aThreadTheRunStartsAsksAsThePersonOnly` |
| Taking the agent's permission away stops its next call | `takingTheAgentsPermissionAwayStopsItsNextCall` |

## Where the answers come from

[SecurityEngineAccessUtils](../../src/prerna/auth/utils/SecurityEngineAccessUtils.java) reads the security database for the check:

- a user's level is their highest grant in effect, given to them directly or to a team they are in, or `READ_ONLY` on a public engine or project (`SecurityEngineUtils.getCurrentUserEngineLevel`, `SecurityProjectUtils.getCurrentUserProjectLevel`), and the same query says where it comes from: their own grant, the team whose grant gives the highest level, with its id and type, or the engine or project being public (`getUserAccess`, a [UserAccess](../../src/prerna/auth/access/UserAccess.java)); when their own grant and a team's give the same level, it is their own;
- an app's, agent's or engine's permission is its row in `ENGINE2ENGINEPERMISSION`, for the engine or the project called, while it has not ended;
- whether an engine is public, for an engine's read of it.

The check runs on every call, so the answers are kept for 30 seconds. A permission saved on a server is read again by that server at once, and by the other servers of a cluster within 30 seconds.

Guarded by [SecurityEngineAccessUtilsUnitTests](../../test/prerna/auth/utils/SecurityEngineAccessUtilsUnitTests.java):

| Rule | Test |
| --- | --- |
| A user's level on an engine is their highest grant | `aUsersLevelOnAnEngineIsTheirHighestGrant` |
| A user's access says whether it is their own grant, a team's grant or public | `aUsersAccessSaysWhetherItIsTheirOwnGrantATeamsGrantOrPublic` |
| Everyone can view a public engine | `everyoneCanViewAPublicEngine` |
| A user's level on a project is read from the project's own grants | `aUsersLevelOnAProjectIsReadFromTheProjectsOwnGrants` |
| No user and no id hold nothing | `noUserAndNoIdHoldNothing` |
| A saved permission is read at once | `aSavedPermissionIsReadAtOnce`, `theOwnerOfANewEngineHoldsItAtOnce` |
| An admin is known as one | `anAdminIsKnownAsOne` |
| An engine or project is listed while the security database has it | `anEngineOrProjectIsListedWhileTheSecurityDatabaseHasIt` |
| The platform reads a discoverable engine or project for anyone | `thePlatformReadsADiscoverableEngineForAnyone`, `thePlatformReadsADiscoverableProjectForAnyone` |
| The platform reads a project for a user given one of its insights, or with a public insight | `thePlatformReadsAProjectForAUserGivenOneOfItsInsights`, `thePlatformReadsAProjectWithAPublicInsightForAnyone` |

## Why a call went through, on the Activity Log

The access check says why it lets each call through ([EngineAccessDecision](../../src/prerna/engine/impl/pipeline/EngineAccessDecision.java)), and every row an engine or project call writes to the Activity Log (`AUDIT_LOGS`) carries it: `ACCESS_REASON`, a short name, and `ACCESS_DETAILS`, JSON with the specifics. Calls made for a person and the platform's own work both carry one; rows written before the columns existed have none.

| Reason | When | Details |
| --- | --- | --- |
| `DIRECT` | The person's own grant covers the call | `level` |
| `TEAM` | A team's grant gives the person's highest level | `level`, `teamId`, `teamType` |
| `PUBLIC` | The engine or project is public and the call only reads it | `level` |
| `DELEGATED` | The engine or project gave an app, an agent or another engine a permission that covers the call | `level`, `granteeId`, `granteeType`, `via` (`APP_CODE`, `AGENT_RUN` or `ENGINE_CODE`), and `callingEngineId` when an engine's own code made the call |
| `OWN_PROJECT` | An app's custom code calls its own project | `appId` |
| `INSIGHT` | The platform reads a project through one of its insights the person can open | |
| `DISCOVERABLE` | The platform reads a discoverable engine or project | |
| `ADMIN` | An admin, while an admin reactor or endpoint is acting | |
| `PLATFORM` | The platform's own work | `rule`: `LOADING`, `PLATFORM_USE`, `PLATFORM_OPERATION` (with `operation`, its name), `ENGINE_ITSELF`, `PROJECT_ENGINE`, `VENV` or `BEING_CREATED_OR_DELETED` |
| `REFUSED` | Nothing let the call through | `levelNeeded`, `via` (`APP_CODE`, `AGENT_RUN`, `ENGINE_CODE` or `PLATFORM`), `appId`, `agentId`, `callingEngineId`, and `attempt` |

A refused call is a failed row with reason `REFUSED` and nothing of what was sent: its request and response read as not tracked. [EngineAccessRefusals](../../src/prerna/engine/impl/pipeline/EngineAccessRefusals.java) counts the refusals of each person on each engine or project in a session (per day for a call with no session), and `attempt` on the row is that count. The first refusal in a session also records an `AUTHORIZATION_DENIED` audit event on the engine or project with the method, the details above and what was sent, which the audit trail keeps sanitized like the details of every event (each text value at most 2,000 characters, secrets scrubbed), with `requestLength`, the length of the request as sent. A resource's Audit Trail tab lists only the events its owners see, which leaves refusals out, so only admins read them, on the Audit Trail page. Methods the Activity Log never keeps (`@IgnoreEngineLogging`) are neither logged nor counted when refused.

The Activity Log filters by reason (`accessReason` in `AuditLogsReport`'s `search`, and a filter option list of the same name), and its CSV and PDF exports carry the reason and details.

Guarded by [EngineAccessReasonUnitTests](../../test/prerna/engine/impl/pipeline/EngineAccessReasonUnitTests.java) and [AuditLogsJDBCAppenderUnitTests](../../test/prerna/logging/AuditLogsJDBCAppenderUnitTests.java):

| Rule | Test |
| --- | --- |
| The user's own grant says DIRECT with its level | `theUsersOwnGrantSaysDirectWithItsLevel` |
| A team's grant says TEAM with the team | `aTeamsGrantSaysTeamWithTheTeam` |
| A public engine says PUBLIC | `aPublicEngineSaysPublic` |
| An agent's run says DELEGATED with the agent | `anAgentsRunSaysDelegatedWithTheAgent` |
| An engine's own code using its permission says DELEGATED with the calling engine | `anEnginesOwnCodeUsingItsPermissionSaysDelegatedWithTheCallingEngine` |
| The platform's own work says PLATFORM with what it was | `thePlatformsOwnWorkSaysPlatformWithWhatItWas`, `anEngineBeingCreatedOrDeletedSaysPlatform` |
| A read through the platform says DISCOVERABLE or INSIGHT | `aReadThroughThePlatformSaysDiscoverableOrInsight` |
| An admin in an admin class says ADMIN | `anAdminInAnAdminClassSaysAdmin` |
| A refusal carries the level needed and who made the call | `aRefusalCarriesTheLevelNeededAndWhoMadeTheCall` |
| A method of Object is not checked and carries no reason | `aMethodOfObjectIsNotChecked` |
| A logged call's row carries its reason and details | `aLoggedCallsRowCarriesItsReasonAndDetails`, `aRowKeepsWhyTheCallWasAllowedAndTheDetails` |
| A refused call logs a failed row with nothing sent, and only the first in a session is audited | `aRefusedCallLogsAFailedRowWithNothingSentAndOnlyTheFirstInASessionIsAudited` |

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
| `publish portal` | Monolith `PublicHomeCheckFilter` | Publishing a project's portal and serving its files to a visitor who passed the filter's checks; a project with its own login page is served to signed out visitors |
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
| While a custom reactor is acting, the user is an admin nowhere: `SecurityAdminUtils.userIsAdmin` answers no and `getInstance` returns nothing, so an admin reactor it runs through its insight refuses | `anAdminIsNotAnAdminWhileACustomReactorIsActing` in [SandboxActsAsTheUserUnitTests](../../test/prerna/util/SandboxActsAsTheUserUnitTests.java) |

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
