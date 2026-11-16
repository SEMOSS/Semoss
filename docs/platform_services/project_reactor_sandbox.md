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
| Platform state and internal helpers | `prerna.util.DIHelper`, `prerna.util.SystemEngineRegistry`, `prerna.security.SecretRedactor`, `prerna.util.InternalUtility`, the classes that build the proxy that checks every call to an engine, `prerna.engine.impl.pipeline.EngineProxyFactory` and `PipelineInvocationHandler`, `prerna.engine.impl.pipeline.PlatformOperation`, which marks the platform's own work for that check, and `prerna.om.InsightStore`, which holds every open insight of every user | `platformStateAndInternalHelpersAreBlocked` |
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
| Sockets and servers | `java.net.Socket`, `ServerSocket`, `DatagramSocket`, `MulticastSocket`, the socket and datagram channels in `java.nio.channels`, `javax.net.*`, `java.util.logging.SocketHandler`, `com.sun.net.httpserver.*`, `javax.print.*` | `socketsAreBlocked` |
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
| The calls that read an RDBMS engine's credentials from its query utility or change where it connects, on any class of `prerna.util.sql`: `getUsername`, `getPassword`, `getAdditionalProps`, `getConnectionUrl`, `setHostname`, `setPort`, `setDatabase`, `setSchema`, `setUsername`, `setPassword`, `setAdditionalProps`, `setConnectionUrl` | `SecurityException` | `anEnginesCredentialsCannotBeReadOrChangedThroughItsQueryUtility` |
| Assigning a static field of a JDK, platform or shared library class, such as `SchedulerListener.schedulerLeader` or `PDDeviceCMYK.INSTANCE` | `SecurityException` | `aStaticFieldOfThePlatformCannotBeAssigned` |
| Extending a class that has refused calls, such as `User`, `AccessToken`, `Insight` or `TimeZone` | The class is refused when it loads | `aClassWithRefusedCallsCannotBeExtended` |
| `URL.openStream`, `openConnection`, `getContent`, `setURLStreamHandlerFactory` | `SecurityException` | `aUrlCannotOpenAConnection` |
| `Files.createSymbolicLink`, `FileChannel.open` | `SecurityException` | `symbolicLinksAndFileChannelsAreRefused` |
| `Class.forName(name)` and `Class.forName(name, initialize, loader)` | The class, found through the project's own sandbox loader, when its name is open, and `SecurityException` when it is blocked; a loader that is passed in is not used | `aClassLoaderPassedToForNameIsIgnored`, `folderReactorsCannotLoadTheRedactorByName`, `forNameFindsTheProjectsOwnClasses` in `SandboxClassLoadersUnitTests` |
| `new File(...)`, `new FileInputStream(...)`, `new FileOutputStream(...)`, `Paths.get`, `Path.of` | With `CHROOT_ENABLE` on, a path inside the user's folder | `withAUserRootEveryPathStaysInsideIt` |
| `File.getAbsolutePath`, `getCanonicalPath`, `getParent`, `getParentFile`, `toPath` | With `CHROOT_ENABLE` on, the path as seen from the user's folder | `withAUserRootAFileReportsItsPathFromThatRoot` |
| `Utility.getDIHelperProperty` | The RDF_Map value, and `null` for every key the RDF_Map editor hides | `folderReactorsReadSettingsButNotSecretSettings`, `theSandboxedSettingGetterHidesWhatTheRdfMapEditorHides` |
| `Utility.loadProperties` | The file's values, read under the same path rule as `new File(...)` | `withoutAChrootLoadPropertiesReadsTheFileAsBefore`, `withAChrootLoadPropertiesStaysInsideIt` |
| `InternalUtility.getLocalProperty`, the values the desktop tools keep in memory | Refused with the rest of `InternalUtility`: `NoClassDefFoundError` | `folderReactorsCannotReadTheDIHelperLocalMap` |
| A method reference to any call above | The same result as the call itself | `aMethodReferenceToARewrittenMethodRunsTheSandboxedVersion`, `aMethodReferenceBehavesLikeTheCallItStandsFor` |
| A method handle constant to any call above | The class is refused when it loads | `aMethodHandleConstantToARefusedCallIsRefused`, `aMethodHandleConstantToABlockedUtilityMethodIsRefused` |

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
| The classes that make up the sandbox (`SandboxedJavaExecution`, `SandboxedXml`, `SemossClassLoader`, `SemossProjectJarClassLoader`, `SemossJarClassLoader`, `SandboxedClassLoader`, and the library classes `SandboxedPdfBox`, `SandboxedHikari`, `SandboxedJackson`, `SandboxedLog4j`, `SandboxedPoi` and `SandboxedMail`) are reached only through the calls the sandbox writes into a class | `customCodeCannotNameTheSandboxClasses` |
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
| A reactor gets its logger from `LogManager.getLogger`; `LogManager.setFactory` and `shutdown` are refused | `aReactorStillGetsALogger`, `platformLoggingCannotBeReplacedOrShutDown` |
| A HikariCP pool connects through its JDBC URL; `setDataSourceJNDI`, `setDataSourceClassName` and the same settings in a `Properties` are refused | `aConnectionPoolCannotLookUpItsDataSourceByName`, `aConnectionPoolBuiltFromPropertiesFollowsTheSameRule` |
| PDFBox reads and writes PDFs, and a path it is given as text, such as `PDDocument.save(String)` or `new RandomAccessReadBufferedFile(String)`, is the one inside the user's folder | `pdfBoxWritesAndReadsPdfsInsideTheUsersFolder` |
| The PDFBox calls that change what every document in the JVM shares, or free memory other code may still read, are refused: `IOUtils.unmap`, `COSName.clearResources`, `ResourceCacheFactory.setResourceCacheCreateFunction`, `FontMappers.set`, `SecurityProvider.setProvider`, `SecurityHandlerFactory.registerHandler`, `getInstance` of `CFFISOAdobeCharset`, `CFFExpertCharset` and `CFFExpertSubsetCharset`, and `Overlay.overlay`, which reads file paths out of a map; `Overlay.overlayDocuments` takes the documents themselves | `pdfBoxCannotChangeWhatEveryUserShares` |
| A page size such as `PDRectangle.A4`, the media box of a new `PDPage`, and a page size PDFBox hands back are the project's own copy | `aPageSizeEveryDocumentSharesStaysTheSame` |

## HTTP requests

A project's custom reactors make HTTP requests through [HttpHelperUtility](../../src/prerna/security/HttpHelperUtility.java). Every request it sends, for any caller, follows the [request lists](../development_guides/configuration_and_environment.md) an admin sets in RDF_Map: `BLACK_LIST_DOMAINS` and `WHITE_LIST_DOMAINS`. The lists are checked on the host of the URL, on every redirect, and on every address the host resolves to. Two clients send the requests, alike except for certificates: `getSystemClient` checks the server's certificate and host name against the JVM's truststore, for sign ins, and `getCustomClient`, which the request methods use unless they are given a client, accepts self-signed and internal certificates (`onlyTheCustomClientTrustsASelfSignedCertificate`). Both take the JVM's proxy settings. The scheduler's calls to its own endpoint, `SCHEDULER_ENDPOINT`, go through `HttpHelperUtility.getSchedulerClient` and are outside the lists, so a list that closes localhost leaves scheduled jobs running (`theSchedulerReachesItsOwnEndpointWhenTheListsCloseLocalhost`). Guarded by [UrlAccessRulesUnitTests](../../test/prerna/security/UrlAccessRulesUnitTests.java) and [HttpHelperUtilityUnitTests](../../test/prerna/security/HttpHelperUtilityUnitTests.java).

Requests from a project's code follow these rules as well, guarded by [SandboxHttpUnitTests](../../test/prerna/util/SandboxHttpUnitTests.java):

| Rule | Test |
| --- | --- |
| A project reactor reaches localhost and any other host the lists leave open. An admin closes localhost with `BLACK_LIST_DOMAINS`, for example `localhost, 127.0.0.0/8, ::1` | `aProjectReactorCallsLocalhostUnlessTheAdminBlacklistsIt`, `anAdminClosesLocalhostWithTheBlacklist` in `UrlAccessRulesUnitTests` |
| A project reactor does not reach link local addresses, which include the cloud metadata service, or the wildcard address | `aProjectReactorCannotCallTheCloudMetadataService`, `aProjectReactorCannotReachTheMetadataServiceOrTheWildcardAddress` in `UrlAccessRulesUnitTests` |
| A project reactor uses the request methods; `getCustomClient`, `getSchedulerClient` and `getSystemClient` are refused | `aProjectReactorCannotBuildItsOwnClient` |
| With `CHROOT_ENABLE` on, a file a project reactor downloads or uploads through the helper, and a keystore it names, are inside the user's folder | `withAUserRootADownloadIsSavedInsideIt` |

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
| An engine for people who have no access to it | A permission the engine's owner gives the app; see [giving an app a permission on an engine](engine_access.md#giving-an-app-a-permission-on-an-engine) |
| SQL for an engine's dialect | `engine.getQueryUtil()`, with `EDIT` on the engine |
| An HTTP call | [HttpHelperUtility](../../src/prerna/security/HttpHelperUtility.java); see [calling web services](../how-to-guides/writing_custom_reactors.md#calling-web-services) |
| XML from a web address | Fetch the text with `HttpHelperUtility`, then parse it from a `StringReader` |
| An email | [EmailUtility](../../src/prerna/util/EmailUtility.java) |
| A wait between attempts | `TimeUnit.MILLISECONDS.sleep(...)` |
| A library the platform has | Name it as usual; the project gets the shared class or its own sandboxed copy |
| A properties file | `Utility.loadProperties(path)` |
| An admin action | The admin reactors, run by an admin through Pixel |

## Adding a rule

A rule and its test land together.

1. Write the test first: add the class name to the matching group in `SandboxBlockedClassesUnitTests`, or the call to the matching test in `SandboxReplacedCallsUnitTests`, and watch it fail.
2. Add the name to `BLOCKED_JDK_PACKAGES`, `BLOCKED_JDK_CLASSES`, `BLOCKED_PLATFORM_CLASSES` or `BLOCKED_PLATFORM_PACKAGES` in `SemossClassLoader`, or the call to `BLOCKED_CALLS`. A call that takes a path as text goes in `PATH_TEXT_ARGUMENTS` with a test in `SandboxFilesUnitTests`. A rule about class loaders goes in `SandboxClassLoadersUnitTests`. A call that needs a sandboxed version instead of a refusal gets a method in `SandboxedJavaExecution` and a rewrite in `SandboxMethodVisitor`.
3. Add the row to this page with the test's name.

A library joins the shared list with a sandbox class of its own in `prerna.util`, such as `SandboxedPdfBox`, written after checking what the library's public methods do for their caller. The class holds every rule for the library: its packages, the classes and calls a project cannot use, the calls that take a path as text, the constructors it replaces, and the methods that stand in for them. `SemossClassLoader` adds the class's tables to its own and lists the class in `SANDBOX_CLASSES`. A library that only needs its packages shared, such as Gson, is listed in `SemossClassLoader` directly. Each rule gets a test in `SandboxLibrariesUnitTests`.
