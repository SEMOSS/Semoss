# Project reactor sandbox

Custom reactors in a project run in a sandbox. The sandbox keeps a project's code to its own work: it reaches platform data through the same reactors, engines and helpers every user reaches it through, and platform state, other users' data, the host and the network stay with the platform.

Three class loaders load a project's code, and all three apply the same rules:

| Code | Loader |
| --- | --- |
| The project's compiled classes | [SemossClassLoader](../../src/prerna/util/SemossClassLoader.java) |
| The project's jars | [SemossProjectJarClassLoader](../../src/prerna/util/SemossProjectJarClassLoader.java) |
| The project's Maven dependencies | [SemossJarClassLoader](../../src/prerna/util/SemossJarClassLoader.java) |

To load a project and compile its reactors, see [writing a custom reactor](../how-to-guides/writing_custom_reactors.md#deploying-and-using-custom-reactors).

## How a rule is applied

A rule takes one of two forms.

- **A class that cannot be used.** `SemossClassLoader.isReflectionApiBlocked` decides by name. A custom class that names a blocked class fails with `NoClassDefFoundError` when that line runs. `Class.forName` with a blocked name throws `SecurityException`.
- **A call that is replaced.** `SemossClassLoader.applyBytecodeTransformations` rewrites the class before it loads. The call is swapped for a sandboxed version in [SandboxedJavaExecution](../../src/prerna/util/SandboxedJavaExecution.java) that returns a safe value, keeps a path inside the user's folder, or throws `SecurityException`.

A method reference behaves exactly like the call it stands for: `File::getAbsolutePath` runs the sandboxed version, `System::getenv` returns `null`, and `System::loadLibrary` throws `SecurityException` when it is used. A method handle constant to a replaced or refused call is refused when the class loads.

A third rule decides which libraries a project shares with the platform; see [libraries](#libraries).

Every rule below names the test that guards it. The tests build a small class, load it through the sandbox the way a project's reactors load, and check what the sandbox does with it.

## Classes that cannot be used

Guarded by [SandboxBlockedClassesUnitTests](../../test/prerna/util/SandboxBlockedClassesUnitTests.java). `aCustomClassCannotNameABlockedClassOrFindItByName` loads a class that uses a blocked class both ways, and `classesCustomReactorsRelyOnStayOpen` keeps the list from growing past its purpose.

| Group | Classes | Test |
| --- | --- | --- |
| Platform state and internal helpers | `prerna.util.DIHelper`, `prerna.util.SystemEngineRegistry`, `prerna.security.SecretRedactor`, `prerna.util.InternalUtility`, the classes that build the proxy that checks every call to an engine, `prerna.engine.impl.pipeline.EngineProxyFactory` and `PipelineInvocationHandler`, `prerna.engine.impl.pipeline.PlatformOperation`, which marks the platform's own work for that check, `prerna.om.InsightStore`, which holds every open insight of every user, and the classes that set up the system databases when the server starts, `prerna.util.LocalMasterConceptIdHash`, `SystemProjectSeeder` and `SystemDatabaseSchemaUtils` | `platformStateAndInternalHelpersAreBlocked` |
| The secret store | `prerna.io.connector.secrets.*` | `theSecretStoreIsBlocked` |
| Classes that require an admin | Every `prerna` class whose name starts with `Admin`, and `prerna.auth.utils.SecurityAdminUtils` | `classesThatRequireAnAdminAreBlocked`, `everyAdminNamedClassInThePlatformSourceIsBlocked` |
| Reflection | `java.lang.reflect.*`, `java.lang.invoke.MethodHandles` and its `Lookup`, `sun.misc.Unsafe`, `jdk.internal.misc.Unsafe` | `reflectionIsBlocked` |
| Classes that call methods by name | `java.beans.*` | `classesThatCallMethodsByNameAreBlocked` |
| Script engines, compilers and shells | `javax.script.*`, `jdk.nashorn.*`, `org.codehaus.groovy.*`, `javax.tools.*`, `com.sun.tools.*`, `jdk.jshell.*` | `scriptEnginesCompilersAndShellsAreBlocked` |
| Processes | `java.lang.ProcessBuilder`, `java.lang.ProcessHandle` | `processesAreBlocked` |
| JVM internals | `java.lang.management.*`, `javax.management.*`, `jdk.jfr.*`, `java.lang.instrument.*`, `com.sun.tools.attach.*` | `jvmInternalsAreBlocked` |
| Native code | `java.lang.foreign.*` | `nativeCodeIsBlocked` |
| Remote lookups | `javax.naming.*`, `java.rmi.*`, `javax.sql.rowset.*` | `remoteLookupsAreBlocked` |
| Directory and Kerberos sign-ins | `javax.security.auth.login.*`, `com.sun.security.auth.*`, `javax.security.sasl.*`, `org.ietf.jgss.*` | `directoryAndKerberosSignInsAreBlocked` |
| Sockets and servers | `java.net.Socket`, `ServerSocket`, `DatagramSocket`, `MulticastSocket`, the socket and datagram channels in `java.nio.channels`, `javax.net.*`, `java.util.logging.SocketHandler`, `com.sun.net.httpserver.*`, `javax.print.*`, and `java.net.NetworkInterface`, which lists the host's network interfaces | `socketsAreBlocked` |
| HTTP clients other than the platform helper | `java.net.http.*`, `java.net.URLConnection`, `HttpURLConnection`, `JarURLConnection`, `javax.net.ssl.HttpsURLConnection` | `httpClientsOtherThanThePlatformHelperAreBlocked` |
| Hooks into every HTTP call of the JVM | `java.net.ProxySelector`, `Authenticator`, `CookieHandler`, `CookieManager`, `ResponseCache`, `javax.net.ssl.SSLContext`, `HostnameVerifier` | `hooksIntoEveryHttpCallOfTheJvmAreBlocked` |
| Threads and thread pools | `java.lang.Thread`, `java.util.concurrent.ThreadPoolExecutor`, `ScheduledThreadPoolExecutor`, `ForkJoinPool` | `threadsAndThreadPoolsAreBlocked` |
| Mail transports | `jakarta.mail.Transport`, `Store`, `Service`, `Folder`; mail is sent through `EmailUtility` | `mailIsSentThroughThePlatformHelper` |
| Java deserialization | `java.io.ObjectInputStream` | `javaDeserializationIsBlocked` |

A blocked class is blocked with its nested classes. A package is blocked with its sub-packages.

## Calls that are replaced

Guarded by [SandboxReplacedCallsUnitTests](../../test/prerna/util/SandboxReplacedCallsUnitTests.java) and [SemossClassLoaderUnitTests](../../test/prerna/util/SemossClassLoaderUnitTests.java). Each test first checks that the call is gone from the rewritten class, then runs the class.

| Call | What the reactor gets | Test |
| --- | --- | --- |
| `System.load`, `System.loadLibrary`, `Runtime.load`, `Runtime.loadLibrary` | `SecurityException` | `nativeLibrariesCannotBeLoaded` |
| `System.exit`, `Runtime.halt`, `Runtime.exec` | `SecurityException` | `theJvmCannotBeStoppedAndProcessesCannotBeStarted` |
| `System.getenv`, `System.getProperties`, `Thread.getAllStackTraces` | `null` | `environmentVariablesAllPropertiesAndOtherThreadsAreHidden` |
| `System.getProperty` | The value for a listed property, such as `java.version` or `file.separator`, and `null` for the rest | `onlyListedSystemPropertiesCanBeRead` |
| `System.setProperty`, `clearProperty`, `setProperties` | `SecurityException` | `systemPropertiesCannotBeChanged` |
| `System.setIn`, `setOut`, `setErr` | `SecurityException` | `theStandardStreamsCannotBeReplaced` |
| `Security.addProvider`, `insertProviderAt`, `removeProvider`, `setProperty` | `SecurityException` | `securityProvidersCannotBeChanged` |
| `DriverManager.registerDriver`, `deregisterDriver`, `setLogWriter`, `setLogStream` | `SecurityException` | `registeredJdbcDriversCannotBeChanged` |
| `TimeZone.setDefault`, `Locale.setDefault` | `SecurityException` | `theDefaultTimeZoneAndLocaleCannotBeChanged` |
| The calls that change who the signed in user is: `Insight.setUser`, `ThreadStore.setUser` and `setThreadMapObject`, `User.setAccessToken`, `setGlobalAccessToken`, `dropAccessToken` and `setPrimaryLogin`, and `AccessToken.setId`, `setProvider`, `setUserGroups`, `setUserGroupType` and `setLocked` | `SecurityException` | `theSignedInUserCannotBeChanged` |
| The `ThreadStore` calls that change which request the thread serves: `setInsightId`, `setSessionId`, `setRouteId`, `setJobId`, `setSchedulerMode`, `setLocalHostname`, `setLocalProtocol`, `setLocalPort`, `setContextProjectIdOverride`, `setContextProjectNameOverride`, `clearContextProjectOverride`, `remove`, and `getTheadMapObject`, which hands out the thread's map itself | `SecurityException` | `theRequestTheThreadServesCannotBeChanged` |
| `ThreadStore.getSessionId`, since the session id is all a browser needs to act as the user | `SecurityException` | `theSessionIdIsNotHandedToAProject` |
| A static method of a class that works on a system database directly: every class of `prerna.auth.utils`, `prerna.usertracking`, `prerna.theme`, `prerna.masterdatabase`, `prerna.engine.logging`, `prerna.engine.impl.model.inferencetracking`, `prerna.prompt`, `prerna.notifications`, `prerna.collaboration`, `prerna.reactor.scheduler`, `prerna.reactor.agent.run` and `prerna.reactor.automation.run`, the appenders `AuditLogsJDBCAppender` and `ServerLogsJDBCAppender`, and `MicrosoftGraphUserLookup`. A project reaches the system databases through the platform's reactors, which check the user first, and through `CustomReactorSecurity`, which answers what the signed in user may do and finds people in the organization; see [What custom reactors use instead](#what-custom-reactors-use-instead). The security utilities still answer what a user may do when a custom reactor asks them directly: their methods named `userCan...`, `userIs...`, `userHas...`, `userGroupCan...`, `userGroupIs...`, `adminOnly...` and `adminSet...` run, and are deprecated in custom reactors; see [Compile output](#compile-output) | `SecurityException` | `aClassThatWorksOnASystemDatabaseCannotBeCalledDirectly`, `theSecurityUtilitiesStillAnswerWhatAUserMayDo`, `aCustomReactorAsksAboutPeopleAndPermissionsThroughCustomReactorSecurity`, and `everyClassThatAsksForASystemDatabaseIsClosedToAProjectsDirectCalls` in `SandboxBlockedClassesUnitTests`, which reads the platform source so a new class is covered |
| Extending one of those classes, or creating an object of a `prerna.auth.utils` class | The class is refused when it loads | `aClassThatWorksOnASystemDatabaseCannotBeExtended`, `aSecurityUtilityCannotBeCreated` |
| Extending a query wrapper of `prerna.rdf.engine.wrappers`, which holds the connection, statement and result set of the engine it reads | The class is refused when it loads | `aQueryWrapperCannotBeExtended` |
| The `InetAddress` calls that look up a host name or probe an address: `getByName`, `getAllByName`, `isReachable` | `SecurityException` | `hostNamesAreNotLookedUpAndAddressesAreNotProbed` |
| The `java.util.logging` calls that put a project's handler or filter on the JVM's own logging, where it would see what every user's work logs, or take the platform's off: `Logger.addHandler`, `removeHandler`, `setFilter` and `setParent`, and `LogManager.reset`, `readConfiguration` and `updateConfiguration`. A library still sets its own logger's level and reads the local host name when it starts | `SecurityException` | `theJvmsOwnLoggingGetsNoHandlerOrFilterOfAProjects`, `whatLibrariesDoWhenTheyStartIsLeftAlone` |
| `Class.newInstance()` | The new object, when project code could create it by name, and `SecurityException` for a blocked class or a class that works on a system database | `classNewInstanceAppliesTheBlockList` |
| A URL handed to the JDK, the platform or a shared library, which would fetch it, such as `ImageIO.read(url)` or `mapper.readValue(url, type)` | The local file it names, under the path rule, and `SecurityException` for any other address; `new URL(base, path)` works out an address as before | `aUrlHandedToAnotherClassIsALocalFile`, `withAUserRootAFileUrlHandedToAnotherClassStaysInsideIt`, `aUrlIsStillWorkedOutFromAnother` |
| The calls that read an RDBMS engine's credentials from its query utility or change where it connects, on any class of `prerna.util.sql`: `getUsername`, `getPassword`, `getAdditionalProps`, `getConnectionUrl`, `setHostname`, `setPort`, `setDatabase`, `setSchema`, `setUsername`, `setPassword`, `setAdditionalProps`, `setConnectionUrl`. JSON written from a query utility, such as a reactor's output, leaves the credentials out: they are `transient`, with the cloud keys and tokens of the Athena and BigQuery utilities, and their getters carry `@JsonIgnore` | `SecurityException` | `anEnginesCredentialsCannotBeReadOrChangedThroughItsQueryUtility`, and `anEnginesCredentialsStayOutOfTheJsonWrittenFromItsQueryUtility` and `aCloudQueryUtilitysSecretStaysOutOfItsJson` in `AbstractSqlQueryUtilUnitTests` |
| Assigning a static field of a JDK, platform or shared library class, such as `SchedulerListener.schedulerLeader` or `PDDeviceCMYK.INSTANCE` | `SecurityException` | `aStaticFieldOfThePlatformCannotBeAssigned` |
| Extending a class that has refused calls, or inherits them, such as `User`, `AccessToken`, `Insight`, `TimeZone` or `SimpleTimeZone` | The class is refused when it loads | `aClassWithRefusedCallsCannotBeExtended` |
| `URL.openStream`, `openConnection`, `getContent`, `setURLStreamHandlerFactory` | `SecurityException` | `aUrlCannotOpenAConnection` |
| `Files.createSymbolicLink`, `FileChannel.open` | `SecurityException` | `symbolicLinksAndFileChannelsAreRefused` |
| `Class.forName(name)` and `Class.forName(name, initialize, loader)` | The class, found through the project's own sandbox loader, when its name is open, and `SecurityException` when it is blocked; a loader that is passed in is not used | `aClassLoaderPassedToForNameIsIgnored`, `folderReactorsCannotLoadTheRedactorByName`, `forNameFindsTheProjectsOwnClasses` in `SandboxClassLoadersUnitTests` |
| `new File(...)`, `new FileInputStream(...)`, `new FileOutputStream(...)`, `Paths.get`, `Path.of` | With `CHROOT_ENABLE` on, a path inside the user's folder | `withAUserRootEveryPathStaysInsideIt` |
| `File.getAbsolutePath`, `getCanonicalPath`, `getParent`, `getParentFile`, `toPath` | With `CHROOT_ENABLE` on, the path as seen from the user's folder | `withAUserRootAFileReportsItsPathFromThatRoot` |
| `Utility.getDIHelperProperty` | The RDF_Map value, and `null` for every key the RDF_Map editor hides | `folderReactorsReadSettingsButNotSecretSettings`, `theSandboxedSettingGetterHidesWhatTheRdfMapEditorHides` |
| `Utility.loadProperties` | The file's values, read under the same path rule as `new File(...)` | `withoutAChrootLoadPropertiesReadsTheFileAsBefore`, `withAChrootLoadPropertiesStaysInsideIt` |
| `InternalUtility.getLocalProperty`, the values the desktop tools keep in memory | Refused with the rest of `InternalUtility`: `NoClassDefFoundError` | `folderReactorsCannotReadTheDIHelperLocalMap` |
| A method reference to any call above | The same result as the call itself | `aMethodReferenceToARewrittenMethodRunsTheSandboxedVersion`, `aMethodReferenceBehavesLikeTheCallItStandsFor` |
| A method handle constant to any call above, or to any other call the sandbox changes, such as a file call or a call on a JSON mapper | The class is refused when it loads; a handle to the project's own method is left alone | `aMethodHandleConstantToARefusedCallIsRefused`, `aMethodHandleConstantToABlockedUtilityMethodIsRefused`, `aMethodHandleConstantToASystemDatabaseUtilityIsRefused`, `aMethodHandleConstantToACallTheSandboxChangesIsRefused`, `aMethodHandleConstantToAProjectsOwnMethodIsAllowed` |

A refused call is refused whichever platform class the call names it through, so `SimpleTimeZone.setDefault` is refused like `TimeZone.setDefault`, and `EscherContainerRecord.setMaxNumberOfChildren` like `EscherRecord.setMaxNumberOfChildren`.

## Compile output

`CompileAppReactors` returns what the compiler wrote, followed by a line for each call in the compiled classes that the sandbox treats differently, in the compiler's own format with the file and line of the call. The compile succeeds as before. Guarded by [SandboxCompileNoticesUnitTests](../../test/prerna/util/SandboxCompileNoticesUnitTests.java).

| Call | Line |
| --- | --- |
| A security question a custom reactor asks the security utilities directly, such as `SecurityInsightUtils.userCanEditInsight` | `[WARNING]`: the call is deprecated in custom reactors, with the `CustomReactorSecurity` method to call instead, such as `CustomReactorSecurity.userCanEditInsight` |
| A static call to a class that works on a system database, such as `SecurityEngineUtils.addEngineUser` or `SecurityQueryUtils.searchForUser` | `[ERROR]`: the call is not available to custom reactors and throws when it runs, with the `CustomReactorSecurity` method to call instead when there is one, such as `CustomReactorSecurity.searchUsers` |

## Files

With `CHROOT_ENABLE` on, every file a project's code opens is inside the user's folder. A path outside the folder is read as a path from the folder, the way a path from `/` is read inside a chroot. The rule is applied where a file is opened, so a path that was worked out with `resolve`, `getParent` or `toFile` is checked when it is used. Guarded by [SandboxFilesUnitTests](../../test/prerna/util/SandboxFilesUnitTests.java).

| Rule | Test |
| --- | --- |
| A file opened by name is the one inside the user's folder: `FileReader`, `FileWriter`, `RandomAccessFile`, `PrintWriter`, `PrintStream`, `Formatter`, `ZipFile` and `JarFile`, along with `File`, `FileInputStream`, `FileOutputStream`, `Paths.get` and `Path.of` | `aFileOpenedByNameIsTheOneInsideTheUsersFolder`, `withAUserRootEveryPathStaysInsideIt` in `SandboxReplacedCallsUnitTests` |
| A `File` or `Path` that project code hands to the JDK, the platform or a shared library is put under the rule first, so `Files.readString(path)`, `new FileReader(file)` and `mapper.readTree(file)` open the file inside the user's folder | `aPathWorkedOutByNavigationIsCheckedWhereItIsUsed`, `aFileHandedToASharedLibraryIsTheOneInsideTheUsersFolder` |
| The `File` methods that read or change the file system, such as `exists`, `delete`, `listFiles`, `mkdirs` and `renameTo`, act on the file inside the user's folder | `aPathWorkedOutByNavigationIsCheckedWhereItIsUsed` |
| A method reference to a file call, such as `Files::readString`, `File::delete` or `FileReader::new`, follows the same rule | `aMethodReferenceToAFileCallFollowsTheSameRule` |
| Working out a path, such as `Path.resolve(Path)` or `relativize`, gives the same result as outside the sandbox | `workingOutAPathIsLeftAlone` |
| A shared library or platform call that takes a path as text follows the rule: `new XSSFWorkbook(path)`, `OPCPackage.open(path)`, `new FileDataSource(path)`, `MimeBodyPart.attachFile(path)`, and `Utility.writeResultToFile`, `writeResultToJson`, `writeWorkbook`, `folderHasAnyFiles`, `folderIsNotEmpty` and `getUniqueFilePath` | `aCallThatTakesAPathAsTextGetsThePathRule`, `aPlatformHelperThatTakesAPathChecksTheUsersFolder`, `aFileHandedToASharedLibraryIsTheOneInsideTheUsersFolder` |
| A project class that extends a class that opens a file by name follows the rule in its constructor | `aSubclassThatOpensAFileByNameGetsThePathRule` |
| A zip or jar file opened as a file system by its address, `FileSystems.newFileSystem(uri, env)` with a `jar:file:` address, is the one inside the user's folder; any other address is refused | `aZipFileSystemOpenedByItsAddressIsTheOneInsideTheUsersFolder` |
| A project class cannot extend `java.io.File` | `aProjectClassCannotExtendFile` |
| File work happens on the thread that runs the reactor. With `CHROOT_ENABLE` on, a thread that does not carry the user's folder, such as one from an executor or a parallel stream, cannot open files | `withChrootOnAThreadWithoutTheUsersFolderCannotOpenFiles` |
| `java.util.logging.FileHandler` cannot be used | `logFilesAreWrittenByThePlatform` in `SandboxBlockedClassesUnitTests` |
| With `CHROOT_ENABLE` off, as on a developer machine, a reactor opens files exactly as named, and the platform logs a warning that this would typically require `CHROOT_ENABLE` for security | `withChrootOffAReactorStillRunsAndThePlatformWarns`, `withNoUsersFolderAFileOpenedByNameIsTheOneNamed` |

A project reaches a user's folder in one of two ways, by what the user may do with it. Someone who can edit the project works on the project's own folder, through a symlink. Anyone else, through a public app or a read-only permission, gets a copy that nobody can write to. The copy holds the project's files without its `java`, `classes` and `portals` folders, since the server compiles and loads custom reactors and serves portals from the project's own folder, and without its `.git` folder, which a copy nobody can change has no use for; what an app has for the people who only view it goes in its other folders, such as `public`. The copy is made again when the project's files change, and a change of the user's permission is followed the next time the project is loaded. Whichever folder of a project a reactor asks for, the whole project is placed by this one rule ([SymlinkHelperUnitTests](../../test/prerna/util/SymlinkHelperUnitTests.java): `aReadOnlyCopyLeavesOutTheReactorSourceTheClassesThePortalsAndTheGitFolder`, `aReadOnlyCopyKeepsEveryOtherFileOfTheProject`, `nobodyCanWriteToAViewersCopy`, `aViewersCopyIsMadeAgainInTheSameFolder`, `someoneWhoWasAnEditorGetsACopyInPlaceOfTheLink`, `someoneWhoBecomesAnEditorLosesTheCopySoTheProjectCanBeLinked`, `aChangedProjectIsToldFromTheOneThatWasCopied`, `aFolderOfAProjectIsTracedToTheProject`, `aProjectFolderThatCannotBePutInTheChrootIsLoggedAndSkipped`).

## XML

A project's code parses and transforms XML with the standard `javax.xml` and `org.xml.sax` APIs. The sandbox gives it the JDK's own parsers with everything that reads another document switched off: external DTDs and entities, external schemas and stylesheets, XInclude, and Java calls from a stylesheet. The settings are applied again right before each use, so a setting the project changes has no effect. The helpers are in [SandboxedXml](../../src/prerna/util/SandboxedXml.java), guarded by [SandboxXmlUnitTests](../../test/prerna/util/SandboxXmlUnitTests.java).

| Rule | Test |
| --- | --- |
| A DOM parser does not read an external entity or DTD | `aDomParserDoesNotReadAnExternalEntity` |
| A feature or attribute the project sets on a factory, such as turning external entities back on, has no effect | `aSettingAProjectChangesHasNoEffect` |
| A factory lookup returns the JDK's own factory, whatever implementation the lookup names | `aLookupThatNamesAnotherParserGetsTheJdksOwn` |
| A SAX parser's reader stays hardened whatever is set on it, including when a filter or a transformer parses through it; `SAXParser.getParser` is refused | `aSaxReaderStaysHardenedWhateverIsSetOnIt`, `theSax1ParserUnderASaxParserIsRefused` |
| A stylesheet cannot call Java or read another document with `document()`, `xsl:import` or `xsl:include` | `aStylesheetCannotCallJavaOrReadOtherDocuments` |
| A StAX parser does not read external entities | `aStreamParserDoesNotReadExternalEntities` |
| A schema is read from a local file or a stream, and does not fetch what it imports | `aSchemaIsReadFromALocalFile` |
| `XPath.evaluate` over an `InputSource` parses it with a hardened parser | `anXpathOverASourceUsesAHardenedParser` |
| A document named by address, as text, an `InputSource`, a `Source` or a `Result`, is a local file; a network address is refused, and the content is fetched with `HttpHelperUtility` instead | `aDocumentNamedByANetworkAddressIsNotFetched` |
| With `CHROOT_ENABLE` on, a document named by a path is the one inside the user's folder | `aDocumentNamedByAPathIsTheOneInsideTheUsersFolder` |
| An `InputSource` or `Source` a project's resolver returns follows the same two rules | `aResolverCannotPointTheParserAtAnotherAddress` |
| A subclass of `InputSource`, `StreamSource` and the like that overrides `getSystemId` is refused | `aSourceThatAnswersForItsOwnAddressIsRefused` |
| The DOM load classes (`LSParser`, `LSInput`, `LSResourceResolver`), `javax.xml.catalog` and the SAX1 `ParserFactory` cannot be used; `LSSerializer` is open | `xmlParsersTheSandboxDoesNotSetUpAreBlocked` in `SandboxBlockedClassesUnitTests` |

## Class loaders

Every class loader a project's code can get or create is a sandbox loader, so a class reaches the project only through the rules above. Guarded by [SandboxClassLoadersUnitTests](../../test/prerna/util/SandboxClassLoadersUnitTests.java).

| Rule | Test |
| --- | --- |
| `new URLClassLoader(...)` and `URLClassLoader.newInstance(...)` create a `SemossProjectJarClassLoader` under the project's own sandbox loader, whatever parent is passed | `aNewUrlClassLoaderIsASandboxLoaderUnderTheProjectLoader` |
| A loader the project creates reads jars and folders on the local disk | `aLoaderForARemoteJarIsRefused` |
| With `CHROOT_ENABLE` on, those jars and folders are inside the user's folder | `withAUserRootALoaderReadsJarsInsideIt` |
| A call that returns a class loader, such as `getClassLoader()`, `ClassLoader.getSystemClassLoader()`, `getPlatformClassLoader()` or `getParent()`, a field that holds one, and a cast to `ClassLoader` all give the project's own sandbox loader | `callsThatReturnALoaderReturnTheProjectsSandboxLoader` |
| A cast to `URLClassLoader` or `SecureClassLoader` passes for a sandbox loader only | `aPlatformLoaderCastToAUrlClassLoaderIsRefused` |
| `loadClass(name)` on a loader applies the block list | `loadClassOnALoaderAppliesTheBlockList` |
| A project class that extends `ClassLoader` or `SecureClassLoader` is loaded as a subclass of [SandboxedClassLoader](../../src/prerna/util/SandboxedClassLoader.java), and its parent is the project's sandbox loader, whatever parent its constructor passes | `aCustomClassLoaderIsMovedUnderTheSandbox` |
| Every class such a loader defines is rewritten first, so the rules on this page apply to it | `classesACustomClassLoaderDefinesAreSandboxed` |
| Such a loader cannot define classes in `prerna.*` packages | `aCustomClassLoaderCannotDefinePlatformPackageClasses` |
| Such a loader supplies its classes through `findClass`; `loadClass` stays with the sandbox | `aCustomClassLoaderCannotOverrideLoadClass` |
| A project class that extends `URLClassLoader` is loaded as a subclass of `SemossProjectJarClassLoader` under the project's sandbox loader | `aCustomUrlClassLoaderIsMovedUnderTheSandbox` |
| The classes that make up the sandbox (`SandboxedJavaExecution`, `SandboxedXml`, `SandboxedClassLoading`, `SemossClassLoader`, `SemossProjectJarClassLoader`, `SemossJarClassLoader`, `SandboxedClassLoader`, and the library classes `SandboxedPdfBox`, `SandboxedHikari`, `SandboxedJackson`, `SandboxedJackson3`, `SandboxedGson`, `SandboxedLog4j`, `SandboxedPoi` and `SandboxedMail`) are reached only through the calls the sandbox writes into a class | `customCodeCannotNameTheSandboxClasses` |
| A method reference to a loader call gives the sandbox loader like the call does, and a method handle constant to one is refused when the class loads | `aMethodReferenceToALoaderCallReturnsTheSandboxLoader`, `aMethodHandleConstantToALoaderCallIsRefused` |

## Libraries

A project shares a short list of the platform's libraries. Every other library on the platform classpath that a project class names is loaded as the project's own copy, rewritten like the project's own code, so the rules on this page apply to the library as well. A copy is made for the classes the project's code names itself; a library the platform uses on the reactor's behalf stays the platform's own. Guarded by [SandboxLibrariesUnitTests](../../test/prerna/util/SandboxLibrariesUnitTests.java).

| Shared with the platform | Packages |
| --- | --- |
| JSON | `com.google.gson` with its `annotations`, `reflect` and `stream` packages, `org.json.*`, `com.fasterxml.jackson.*`, `tools.jackson.*` |
| Excel and Office files | `org.apache.poi.*`, `com.github.pjfanning.xlsx.*`, and the schema classes in `org.openxmlformats.schemas.*`, `com.microsoft.schemas.*`, `org.etsi.uri.*` and `org.w3.x2000.*` |
| Logging | The log4j API: `org.apache.logging.log4j` with its `message` and `spi` packages, and the types its logging methods take |
| Database connection pools | `com.zaxxer.hikari` |
| Tuples | `org.javatuples.*` |
| PDF files | `org.apache.pdfbox.*` and `org.apache.fontbox.*` |
| Mail and attachments | `jakarta.mail.*`, `jakarta.activation.*` |

An admin shares more packages with the RDF_Map setting `PROJECT_REACTOR_SHARED_PACKAGES`, a comma separated list of package names such as `org.apache.commons.lang3, com.mycompany.shared`. The block list applies to an approved package like any other.

| Rule | Test |
| --- | --- |
| A class of a shared library, a `prerna` class and a JDK class are the platform's own | `aSharedLibraryClassIsThePlatformsOwn` |
| A class of any other library is the project's own sandboxed copy | `anyOtherLibraryClassIsTheProjectsOwnCopy`, `eachProjectGetsItsOwnCopy` |
| A copy works like the library and follows the sandbox rules: environment variables are hidden from it, and its files stay inside the user's folder | `aLibraryCopyWorksAndFollowsTheSandboxRules` |
| A library compiled for any Java version, back to Java 1.1, loads and runs as a copy, `finally` blocks from old compilers included | `aLibraryCompiledForAnOldJavaRunsInTheSandbox`, `aFinallyBlockFromAnOldCompilerRunsInTheSandbox` |
| A library class that needs a blocked class, such as a mail transport, cannot be loaded | `aLibraryCopyThatTheSandboxRefusesCannotBeLoaded` |
| An admin approves more packages in RDF_Map, under the block list | `anAdminCanApproveMorePackages`, `anApprovedPackageStaysUnderTheBlockList` |
| A project's Maven dependencies share the same libraries and copy the rest | `mavenDependenciesShareTheSameLibrariesAndCopyTheRest` |
| Jackson reads and writes JSON, and the calls and the `@JsonTypeInfo` settings that create objects of classes the data names are refused: `enableDefaultTyping`, `activateDefaultTyping`, `setDefaultTyping`, `TypeFactory.constructFromCanonical` and `findClass`, and `use = Id.CLASS` or `Id.MINIMAL_CLASS` | `aJacksonMapperStillReadsAndWritesJson`, `jacksonCannotCreateObjectsOfClassesTheDataNames`, `aJacksonAnnotationThatNamesClassesInTheJsonIsRefused`, `aJacksonAnnotationThatNamesTypesByALogicalNameIsAllowed` |
| Gson and Jackson read and build a project's own classes, and JSON data: maps, lists, text, numbers, dates, enums and the JSON tree. A mapper reaches into no object of a class outside the project and builds none, whatever its settings, filters and mix-ins: a platform object is written through its own methods and built through its own constructors. Each call a project makes on a `Gson`, or on a Jackson 2 or Jackson 3 `ObjectMapper` or a mapper class that extends one, runs on a twin of the mapper that has the same settings and this rule, so it holds for a mapper the project builds any way it likes and for one the platform hands it ([SandboxJsonUnitTests](../../test/prerna/util/SandboxJsonUnitTests.java)) | `aCallOnAMapperRunsOnItsTwin`, `gsonWritesAndReadsJsonData`, `gsonDoesNotReachIntoAPlatformObjectOrBuildOne`, `gsonReadsAndWritesAProjectsOwnClass`, `aFilterAProjectAddsToAGsonDoesNotOpenPlatformObjects`, `aJacksonMapperWritesAndReadsJsonData`, `aJacksonMapperDoesNotReachIntoAPlatformObjectOrBuildOne`, `aJacksonMapperReadsAndWritesAProjectsOwnClass`, `aSettingAProjectChangesOnAMapperHoldsForItsLaterCalls`, `aMapperToldToReadFieldsDoesNotReachIntoAPlatformObject`, `aJackson3MapperFollowsTheSameRule` |
| A Jackson 2 parser or generator that reads or writes an object through the mapper it is bound to (`readValueAs`, `readValuesAs`, `writeObject`, `writePOJO`, `writeObjectField`, `writePOJOField`), and a call on a mapper held as an `ObjectCodec`, use the twin as well | `aParserOrGeneratorBoundToAMapperFollowsTheSameRule`, `aCallThatReadsOrWritesAnObjectThroughAParsersMapperRunsOnTheTwin` |
| A JSON tree holds JSON data, in Jackson 2 and Jackson 3. The calls that put an object of any class into a tree or a token stream are refused: `pojoNode`, `rawValueNode`, `putPOJO`, `putRawValue`, `addPOJO`, `insertPOJO`, `addRawValue` and `JsonGenerator.writeEmbeddedObject`, and `POJONode`, `RawValue` and `TokenBuffer` cannot be used. An object of the project's own goes into a tree through `mapper.valueToTree`, which runs on the twin | `aJsonTreeHoldsJsonDataOnly`, and `theJacksonClassesThatReachIntoAnyObjectAreBlocked` in `SandboxBlockedClassesUnitTests` |
| The mapper is the only way to Jackson's reflection. The classes Jackson reads, sets and calls the members of any class with, and builds its bean readers and writers from, cannot be used: `com.fasterxml.jackson.databind.introspect.*` except `VisibilityChecker`, `deser.impl.*`, `ser.impl.*` except `SimpleFilterProvider` and `SimpleBeanPropertyFilter`, the serializer and deserializer factories, providers, contexts and caches, the bean serializer and deserializer classes, the value instantiators and the settable properties, with the same classes of Jackson 3 under `tools.jackson.databind`. `BeanDescription.instantiateBean` is refused. A project writes its own serializers and deserializers from `StdSerializer`, `StdDeserializer` and the other classes of the `std` packages | `theJacksonClassesThatReachIntoAnyObjectAreBlocked`, `theJsonClassesCustomReactorsRelyOnStayOpen` in `SandboxBlockedClassesUnitTests`, `aMapperDoesNotCreateAnObjectOfAnyClassOnRequest` |
| jackson-jr, `com.fasterxml.jackson.jr.*`, is a project's own sandboxed copy | `jacksonJrIsAProjectsOwnCopy` |
| A project class cannot extend a Jackson mapper | `aJsonMapperCannotBeExtended` |
| A reactor gets its logger from `LogManager.getLogger`; `LogManager.setFactory` and `shutdown` are refused | `aReactorStillGetsALogger`, `platformLoggingCannotBeReplacedOrShutDown` |
| A HikariCP pool connects through its JDBC URL; `setDataSourceJNDI`, `setDataSourceClassName` and the same settings in a `Properties` are refused | `aConnectionPoolCannotLookUpItsDataSourceByName`, `aConnectionPoolBuiltFromPropertiesFollowsTheSameRule` |
| A pool keeps the password it connects with to itself: `getPassword` and `getDataSourceProperties` are refused, on a pool the project builds and on one an engine hands out | `aConnectionPoolKeepsItsPasswordToItself` |
| PDFBox reads and writes PDFs, and a path it is given as text, such as `PDDocument.save(String)` or `new RandomAccessReadBufferedFile(String)`, is the one inside the user's folder | `pdfBoxWritesAndReadsPdfsInsideTheUsersFolder` |
| The PDFBox calls that change what every document in the JVM shares, or free memory other code may still read, are refused: `IOUtils.unmap`, `COSName.clearResources`, `ResourceCacheFactory.setResourceCacheCreateFunction`, `FontMappers.set`, `SecurityProvider.setProvider`, `SecurityHandlerFactory.registerHandler`, `getInstance` of `CFFISOAdobeCharset`, `CFFExpertCharset` and `CFFExpertSubsetCharset`, and `Overlay.overlay`, which reads file paths out of a map; `Overlay.overlayDocuments` takes the documents themselves | `pdfBoxCannotChangeWhatEveryUserShares` |
| A page size such as `PDRectangle.A4`, the media box of a new `PDPage`, and a page size PDFBox hands back are the project's own copy | `aPageSizeEveryDocumentSharesStaysTheSame` |
| The POI calls that change what every document shares, run a project's code in other users' requests, or keep a setting on a server thread are refused, and a project class cannot extend a POI class that has them: the size, record and zip bomb limits, such as `ZipSecureFile.setMinInflateRatio`, `IOUtils.setByteArrayMaxOverride` and each `setMaxRecordLength`; the temp file settings and `TempFile.setTempFileCreationStrategy`; `addProvider` and `removeProvider` of `WorkbookFactory`, `ExtractorFactory` and `SlideShowFactory`; `registerFunction`; `DrawFactory.setDefaultFactory`; `SheetUtil.setFailoverFunction`; `ThreadLocalUtil.registerCleaner` and `clearAllThreadLocals`; `CryptoFunctions.registerBouncyCastle` and `RelationshipTransformService.registerDsigProvider`; `LocaleUtil.setUserLocale` and `setUserTimeZone`; `Biff8EncryptionKey.setCurrentUserPassword`; and the event extractor preferences. A workbook with a password opens through `WorkbookFactory.create(file, password)` | `poiCannotChangeWhatEveryUserShares`, `aPoiClassWithRefusedCallsCannotBeExtended`, `anEncryptedWorkbookOpensWithItsPassword` |

## HTTP requests

A project's custom reactors make HTTP requests through [HttpHelperUtility](../../src/prerna/security/HttpHelperUtility.java). Every request it sends, for any caller, follows the [request lists](../development_guides/configuration_and_environment.md) an admin sets in RDF_Map: `BLACK_LIST_DOMAINS` and `WHITE_LIST_DOMAINS`. The lists are checked on the host of the URL, on every redirect, and on every address the host resolves to. Two clients send the requests, alike except for certificates: `getSystemClient` checks the server's certificate and host name against the JVM's truststore, for sign ins, and `getCustomClient`, which the request methods use unless they are given a client, accepts self-signed and internal certificates (`onlyTheCustomClientTrustsASelfSignedCertificate`). Both take the JVM's proxy settings. The scheduler's calls to its own endpoint, `SCHEDULER_ENDPOINT`, go through `HttpHelperUtility.getSchedulerClient` and are outside the lists, so a list that closes localhost leaves scheduled jobs running (`theSchedulerReachesItsOwnEndpointWhenTheListsCloseLocalhost`). Guarded by [UrlAccessRulesUnitTests](../../test/prerna/security/UrlAccessRulesUnitTests.java) and [HttpHelperUtilityUnitTests](../../test/prerna/security/HttpHelperUtilityUnitTests.java).

Requests from a project's code follow these rules as well, guarded by [SandboxHttpUnitTests](../../test/prerna/util/SandboxHttpUnitTests.java):

| Rule | Test |
| --- | --- |
| A project reactor reaches localhost and any other host the lists leave open. An admin closes localhost with `BLACK_LIST_DOMAINS`, for example `localhost, 127.0.0.0/8, ::1` | `aProjectReactorCallsLocalhostUnlessTheAdminBlacklistsIt`, `anAdminClosesLocalhostWithTheBlacklist` in `UrlAccessRulesUnitTests` |
| A project reactor does not reach link local addresses, which include the cloud metadata service, or the wildcard address | `aProjectReactorCannotCallTheCloudMetadataService`, `aProjectReactorCannotReachTheMetadataServiceOrTheWildcardAddress` in `UrlAccessRulesUnitTests` |
| A project reactor uses the request methods; `getCustomClient`, `getSchedulerClient` and `getSystemClient` are refused | `aProjectReactorCannotBuildItsOwnClient` |
| With `CHROOT_ENABLE` on, a file a project reactor downloads or uploads through the helper, and a keystore it names, are inside the user's folder | `withAUserRootADownloadIsSavedInsideIt` |

## Acting as the user

A custom reactor works for the user who runs it, on the request it serves, with what that user holds. What lets someone act as the user anywhere else stays with the platform. Guarded by [SandboxActsAsTheUserUnitTests](../../test/prerna/util/SandboxActsAsTheUserUnitTests.java).

| Rule | Test |
| --- | --- |
| An admin's rights belong to the platform's own reactors and endpoints. While a project's custom code is on the call stack, `SecurityAdminUtils.userIsAdmin` answers no and `getInstance` returns nothing, so a custom reactor an admin runs, and every pixel it runs through its insight, has the admin's own permissions | `anAdminIsNotAnAdminWhileACustomReactorIsActing` |
| A share token for the user's session is created on the user's own request: `GetShareSessionToken` and `GetShareAuthToken` refuse while a custom reactor is acting | `aCustomReactorCannotHaveAShareTokenMadeForTheUsersSession` |
| The session id stays with the platform: `ThreadStore.getSessionId` is refused, and the folder that holds a session's files is named by a hash of the id | `theSessionIdIsNotHandedToAProject` in `SandboxReplacedCallsUnitTests`, `aSessionsFolderNameDoesNotTellTheSessionId` |
| The signed in user cannot be changed or built: the calls that set who a user is are refused, `User`, `AccessToken` and `Insight` cannot be extended, and a JSON mapper builds none of them | `theSignedInUserCannotBeChanged`, `aClassWithRefusedCallsCannotBeExtended` in `SandboxReplacedCallsUnitTests`, `gsonDoesNotReachIntoAPlatformObjectOrBuildOne`, `aJacksonMapperDoesNotReachIntoAPlatformObjectOrBuildOne` in `SandboxJsonUnitTests` |
| An engine object a custom reactor creates or extends and calls itself reaches other engines with the user's permission or the app's; see [engine access](engine_access.md#who-is-acting) | `anEngineObjectACustomReactorDrivesItselfActsAsItsProject` in `SandboxEngineAccessUnitTests` |

## Jars and platform packages

| Rule | Test |
| --- | --- |
| Classes in a project's jars get the same rules as the classes in its folder | `jarReactorsGetTheSameSandbox` |
| A loader that sits under the sandbox asks the sandbox for every class, so the block list applies to it | `aChildLoaderAskingTheSandboxForABlockedClassIsRefused` |
| A project cannot define classes in `prerna.*` packages | `jarsCannotDefinePlatformPackageClasses` |
| A lambda whose body is in the project's own class runs as written, with its body rewritten like any other method | `aLambdaWhoseBodyIsInTheClassIsAllowed` |

## What custom reactors use instead

| Need | Use |
| --- | --- |
| An RDF_Map setting | `Utility.getDIHelperProperty(key)` |
| An engine or project | `Utility.getDatabase`, `Utility.getModel`, `Utility.getStorage`, `Utility.getProject` and the other typed getters. Every call on what they return is checked against the signed in user's permission or the permission the engine gave the app; see [engine access](engine_access.md#calls-from-custom-reactors) |
| An engine for people who have no access to it | A permission the engine's owner gives the app; see [giving an app or an engine a permission on an engine](engine_access.md#giving-an-app-or-an-engine-a-permission-on-an-engine) |
| Whether the user may do something | `CustomReactorSecurity`, with the signed in user from `requireUser()`: `userCanViewEngine(user, engineId)`, `userCanEditEngine` and `userIsEngineOwner`; `userCanViewProject`, `userCanEditProject` and `userIsProjectOwner`; `userCanViewInsight(user, projectId, insightId)` and `userCanEditInsight`; `userIsExporter(user)` and `userIsPublisher(user)`; and `adminSetExporter()`, whether only exporters may export |
| People in the organization | `CustomReactorSecurity.searchUsers(user, searchTerm, limit, offset)`, which searches the Microsoft directory when the server uses it for user searches and the security database otherwise, as the user search endpoint does; a fifth argument of `false` searches the security database. Each person comes back with `id`, `type`, `name` and `email`, up to 100 a page and 1000 in all. A directory search uses the user's Microsoft login, unless the server searches with application credentials |
| A list of engines, projects, members or permissions, or a change to one | The platform's reactors, run through Pixel, which check the user first |
| JSON for a platform object | Its values, such as `gson.toJson(noun.getValue())`, or a map the reactor fills from the object's getters |
| An image, a document or JSON from a web address | Fetch it with `HttpHelperUtility`, then read it from the stream or the saved file |
| SQL for an engine's dialect | `engine.getQueryUtil()`, with `EDIT` on the engine |
| An HTTP call | [HttpHelperUtility](../../src/prerna/security/HttpHelperUtility.java); see [calling web services](../how-to-guides/writing_custom_reactors.md#calling-web-services) |
| XML from a web address | Fetch the text with `HttpHelperUtility`, then parse it from a `StringReader` |
| An email | [EmailUtility](../../src/prerna/util/EmailUtility.java) |
| A wait between attempts | `TimeUnit.MILLISECONDS.sleep(...)` |
| A library the platform has | Name it as usual; the project gets the shared class or its own sandboxed copy |
| An Excel workbook with a password, `.xls` or `.xlsx` | `WorkbookFactory.create(file, password)`, or the form that takes an `InputStream`. POI holds the password only while that call opens the workbook |
| Cell values formatted for a locale | `new DataFormatter(locale)`, which formats with that locale for that formatter only |
| A properties file | `Utility.loadProperties(path)` |
| An admin action | The admin reactors, run by an admin through Pixel |

## Adding a rule

A rule and its test land together.

1. Write the test first: add the class name to the matching group in `SandboxBlockedClassesUnitTests`, or the call to the matching test in `SandboxReplacedCallsUnitTests`, and watch it fail.
2. Add the name to `BLOCKED_JDK_PACKAGES`, `BLOCKED_JDK_CLASSES`, `BLOCKED_PLATFORM_CLASSES` or `BLOCKED_PLATFORM_PACKAGES` in `SemossClassLoader`, or the call to `BLOCKED_CALLS`. A call that takes a path as text goes in `PATH_TEXT_ARGUMENTS` with a test in `SandboxFilesUnitTests`. A rule about class loaders goes in `SandboxClassLoadersUnitTests`. A call that needs a sandboxed version instead of a refusal gets a method in `SandboxedJavaExecution` and a rewrite in `SandboxMethodVisitor`. A package whose classes work on a system database directly goes in `SYSTEM_DATABASE_PACKAGES`; `everyClassThatAsksForASystemDatabaseIsClosedToAProjectsDirectCalls` fails until it does.
3. Add the row to this page with the test's name.

A library joins the shared list with a sandbox class of its own in `prerna.util`, such as `SandboxedPdfBox`, written after checking what the library's public methods do for their caller. The class holds every rule for the library: its packages, the classes and calls a project cannot use, the calls that take a path as text, the constructors it replaces, and the methods that stand in for them. `SemossClassLoader` adds the class's tables to its own and lists the class in `SANDBOX_CLASSES`. A library that only needs its packages shared, such as Gson, is listed in `SemossClassLoader` directly. Each rule gets a test in `SandboxLibrariesUnitTests`.
