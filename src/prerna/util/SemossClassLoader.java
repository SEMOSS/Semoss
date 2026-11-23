/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * A custom class loader for dynamically loading project-specific classes in
 * Semoss. This class loader allows the application to load compiled Java
 * classes (.class files) from a specified folder that is not on the default
 * application classpath. This is essential for loading custom reactors and
 * other project-specific code at runtime.
 */
public class SemossClassLoader extends ClassLoader {

	private static final Logger classLogger = LogManager.getLogger(SemossClassLoader.class);

	/**
	 * Platform classes project reactor code can never use: DIHelper holds every
	 * loaded engine and the RDF_Map, SystemEngineRegistry holds the system
	 * databases, SecretRedactor holds the secret values engines loaded,
	 * InternalUtility holds the helpers that act with the platform's authority,
	 * EngineProxyFactory and PipelineInvocationHandler build the proxy that checks
	 * every call to an engine, PlatformOperation marks the platform's own work,
	 * which that check lets through, and InsightStore holds every open insight of
	 * every user, with the user it belongs to. LocalMasterConceptIdHash,
	 * SystemProjectSeeder and SystemDatabaseSchemaUtils work on the system
	 * databases for the platform's own start up.
	 */
	private static final Set<String> BLOCKED_PLATFORM_CLASSES = Set.of("prerna.util.DIHelper",
			"prerna.util.SystemEngineRegistry", "prerna.security.SecretRedactor", "prerna.util.InternalUtility",
			"prerna.engine.impl.pipeline.EngineProxyFactory", "prerna.engine.impl.pipeline.PipelineInvocationHandler",
			"prerna.engine.impl.pipeline.PlatformOperation", "prerna.engine.impl.pipeline.EngineCallPermission",
			"prerna.om.InsightStore",
			"prerna.util.LocalMasterConceptIdHash", "prerna.util.SystemProjectSeeder",
			"prerna.util.SystemDatabaseSchemaUtils");

	/**
	 * Platform packages project reactor code can never use, as name prefixes: the
	 * secret store connectors hand out the stored secrets of any engine or insight.
	 */
	private static final List<String> BLOCKED_PLATFORM_PACKAGES = List.of("prerna.io.connector.secrets.");

	/**
	 * The platform packages whose classes work on a system database directly, as
	 * internal name prefixes: the security database, the local master, the
	 * scheduler, themes, user tracking, prompts, notifications, collaboration, the
	 * audit logs and the model inference logs. Their static methods read and write
	 * the database for whoever calls them, and the reactors and endpoints that
	 * call them check the user first. Project reactor code reaches the system
	 * databases through those reactors and through
	 * {@link prerna.auth.CustomReactorSecurity}, so it cannot call a static method
	 * of these classes or extend one.
	 */
	private static final List<String> SYSTEM_DATABASE_PACKAGES = List.of("prerna/auth/utils/", "prerna/usertracking/",
			"prerna/theme/", "prerna/masterdatabase/", "prerna/engine/logging/",
			"prerna/engine/impl/model/inferencetracking/", "prerna/prompt/", "prerna/notifications/",
			"prerna/collaboration/", "prerna/reactor/scheduler/", "prerna/reactor/agent/run/",
			"prerna/reactor/automation/run/");

	/**
	 * Single classes that work on a system database for whoever calls them, out of
	 * a package whose other classes do not, by internal name: the two JDBC log
	 * appenders, and the Microsoft directory lookup, which adds directory users to
	 * the security database. Project reactor code searches the directory through
	 * {@link prerna.auth.CustomReactorSecurity}.
	 */
	private static final Set<String> SYSTEM_DATABASE_CLASSES = Set.of("prerna/logging/AuditLogsJDBCAppender",
			"prerna/logging/ServerLogsJDBCAppender", "prerna/io/connector/ms/MicrosoftGraphUserLookup");

	/**
	 * The package of the security utilities, as an internal name prefix. Project
	 * reactor code creates no object of their classes.
	 */
	private static final String SECURITY_UTILITIES_PACKAGE = "prerna/auth/utils/";

	/**
	 * How the name of a security utility method starts when project reactor code
	 * can still call it directly: each answers a question about what a user may
	 * do, or about a rule an admin set, and changes nothing. These direct calls are
	 * deprecated in custom reactors, which ask
	 * {@link prerna.auth.CustomReactorSecurity} instead, and the compile output
	 * says so beside each one.
	 */
	private static final List<String> ACCESS_QUESTION_PREFIXES = List.of("userCan", "userIs", "userHas",
			"userGroupCan", "userGroupIs", "adminOnly", "adminSet");

	/**
	 * The method of {@link prerna.auth.CustomReactorSecurity} a custom reactor
	 * calls in place of a direct call, by the internal name of the class that
	 * declares the direct call and the method's name. The compile output names it
	 * beside each such call.
	 */
	static final Map<String, String> CUSTOM_REACTOR_SECURITY_REPLACEMENTS = Map.ofEntries(
			Map.entry("prerna/auth/utils/SecurityEngineUtils.userCanViewEngine", "userCanViewEngine"),
			Map.entry("prerna/auth/utils/SecurityEngineUtils.userCanEditEngine", "userCanEditEngine"),
			Map.entry("prerna/auth/utils/SecurityEngineUtils.userIsOwner", "userIsEngineOwner"),
			Map.entry("prerna/auth/utils/SecurityProjectUtils.userCanViewProject", "userCanViewProject"),
			Map.entry("prerna/auth/utils/SecurityProjectUtils.userCanEditProject", "userCanEditProject"),
			Map.entry("prerna/auth/utils/SecurityProjectUtils.userIsOwner", "userIsProjectOwner"),
			Map.entry("prerna/auth/utils/SecurityInsightUtils.userCanViewInsight", "userCanViewInsight"),
			Map.entry("prerna/auth/utils/SecurityInsightUtils.userCanEditInsight", "userCanEditInsight"),
			Map.entry("prerna/auth/utils/SecurityQueryUtils.userIsExporter", "userIsExporter"),
			Map.entry("prerna/auth/utils/SecurityQueryUtils.userIsPublisher", "userIsPublisher"),
			Map.entry("prerna/auth/utils/AbstractSecurityUtils.adminSetExporter", "adminSetExporter"),
			Map.entry("prerna/auth/utils/SecurityQueryUtils.searchForUser", "searchUsers"),
			Map.entry("prerna/io/connector/ms/MicrosoftGraphUserLookup.searchUsers", "searchUsers"));

	/**
	 * The package of the wrappers that iterate a query's results, as an internal
	 * name prefix. A wrapper holds the connection, statement and result set of the
	 * engine it reads, so a project class cannot extend one.
	 */
	private static final String QUERY_WRAPPER_PACKAGE = "prerna/rdf/engine/wrappers/";

	/**
	 * The package every platform class is in.
	 */
	private static final String PLATFORM_PACKAGE = "prerna.";

	/**
	 * How the name of a platform class that requires an admin starts. Project
	 * reactor code can never use one.
	 */
	private static final String ADMIN_CLASS_PREFIX = "Admin";

	/**
	 * The platform class behind every admin action, blocked with the classes whose
	 * name starts with {@link #ADMIN_CLASS_PREFIX}.
	 */
	private static final String ADMIN_UTILS_CLASS = "prerna.auth.utils.SecurityAdminUtils";

	/**
	 * JDK packages project reactor code can never use, as name prefixes. Each acts
	 * for whoever calls it, without the caller naming a class the rest of the block
	 * list covers: it calls methods or builds objects by name (java.beans), reaches
	 * into the JVM and other processes (javax.management, jdk.jfr,
	 * java.lang.instrument, com.sun.tools), compiles or runs source (javax.tools,
	 * jdk.jshell), calls native code (java.lang.foreign), looks up remote objects
	 * (javax.naming, java.rmi, javax.sql.rowset), signs in to a directory or
	 * Kerberos server (javax.security.auth.login, com.sun.security.auth,
	 * javax.security.sasl, org.ietf.jgss), or opens network connections
	 * (java.net.http, javax.net, com.sun.net.httpserver, javax.print), or fetches
	 * the XML documents a catalog names (javax.xml.catalog).
	 */
	private static final List<String> BLOCKED_JDK_PACKAGES = List.of("java.beans.", "javax.management.", "jdk.jfr.",
			"java.lang.instrument.", "com.sun.tools.", "javax.tools.", "jdk.jshell.", "java.lang.foreign.",
			"javax.naming.", "java.rmi.", "javax.sql.rowset.", "javax.security.auth.login.", "com.sun.security.auth.",
			"javax.security.sasl.", "org.ietf.jgss.", "java.net.http.", "javax.net.", "com.sun.net.httpserver.",
			"javax.print.", "javax.xml.catalog.");

	/**
	 * JDK classes project reactor code can never use, by full name; their nested
	 * classes are blocked with them. ProcessHandle lists every process on the host
	 * with its command line. The channel, handler and connection classes open
	 * network connections. FileHandler writes log files wherever its pattern
	 * points. The DOM load classes and the SAX1 ParserFactory read an XML document
	 * with a parser the sandbox does not set up. ProxySelector, Authenticator,
	 * CookieHandler, CookieManager and ResponseCache install a hook the whole JVM's
	 * HTTP traffic then goes through. NetworkInterface lists the host's network
	 * interfaces and addresses.
	 */
	private static final Set<String> BLOCKED_JDK_CLASSES = Set.of("java.lang.ProcessHandle",
			"java.nio.channels.AsynchronousSocketChannel", "java.nio.channels.AsynchronousServerSocketChannel",
			"java.util.logging.SocketHandler", "java.util.logging.FileHandler", "org.w3c.dom.ls.LSParser",
			"org.w3c.dom.ls.LSInput", "org.w3c.dom.ls.LSResourceResolver", "org.xml.sax.helpers.ParserFactory",
			"java.net.URLConnection", "java.net.HttpURLConnection", "java.net.JarURLConnection",
			"java.net.ProxySelector", "java.net.Authenticator", "java.net.CookieHandler", "java.net.CookieManager",
			"java.net.ResponseCache", "java.net.NetworkInterface");

	/**
	 * The access token setters that change who a signed in user is: the id, the
	 * login type, the groups and the lock.
	 */
	private static final Set<String> IDENTITY_SETTERS = Set.of("setId", "setProvider", "setUserGroups",
			"setUserGroupType", "setLocked");

	/**
	 * The ThreadStore methods that change what the thread says about the request it
	 * serves: the signed in user, the insight, session, route and job, the context
	 * project, the local host details and scheduler mode, the whole map at once,
	 * and removing it. Project reactor code reads the thread's values and never
	 * changes them, since the engine access check reads the user from there. The
	 * session id is refused as well: it is all a browser needs to act as the user.
	 */
	private static final Set<String> THREAD_STORE_CHANGES = Set.of("setUser", "setThreadMapObject",
			"getTheadMapObject", "getSessionId", "setInsightId", "setSessionId", "setRouteId", "setJobId", "setSchedulerMode",
			"setLocalHostname", "setLocalProtocol", "setLocalPort", "setContextProjectIdOverride",
			"setContextProjectNameOverride", "clearContextProjectOverride", "remove");

	/**
	 * Calls project reactor code cannot make, by owner then method name. Each is
	 * replaced with a call that throws, whichever platform class the call names it
	 * through, and a project class cannot extend an owner or a class that inherits
	 * from one.
	 * They load native code (load, loadLibrary), open a network connection
	 * (URL.getContent), change something for every user of the JVM (its standard
	 * streams, URL handlers, security providers, registered JDBC drivers, time zone
	 * and locale), change who the signed in user is or which request the thread
	 * serves, or hand out the SAX1 parser under a SAX parser. They look up a host
	 * name or probe an address, which sends a request of the project's choosing to
	 * a name server or a host (InetAddress), or put a handler or filter of the
	 * project's on the JVM's own logging, which then sees what every user's work
	 * logs, or take the platform's off (the java.util.logging Logger and
	 * LogManager). The calls each shared library cannot make are listed in its
	 * sandbox class and added here.
	 */
	private static final Map<String, Set<String>> BLOCKED_CALLS = withLibraryRules(Map.ofEntries(
			Map.entry("java/lang/System", Set.of("load", "loadLibrary", "setIn", "setOut", "setErr")),
			Map.entry("java/lang/Runtime", Set.of("load", "loadLibrary")),
			Map.entry("java/net/URL", Set.of("getContent", "setURLStreamHandlerFactory")),
			Map.entry("java/security/Security",
					Set.of("addProvider", "insertProviderAt", "removeProvider", "setProperty")),
			Map.entry("java/sql/DriverManager",
					Set.of("registerDriver", "deregisterDriver", "setLogWriter", "setLogStream")),
			Map.entry("java/util/TimeZone", Set.of("setDefault")),
			Map.entry("java/net/InetAddress", Set.of("getByName", "getAllByName", "isReachable")),
			Map.entry("java/util/logging/Logger", Set.of("addHandler", "removeHandler", "setFilter", "setParent")),
			Map.entry("java/util/logging/LogManager", Set.of("reset", "readConfiguration", "updateConfiguration")),
			Map.entry("java/util/Locale", Set.of("setDefault")), Map.entry("prerna/om/Insight", Set.of("setUser")),
			Map.entry("prerna/om/ThreadStore", THREAD_STORE_CHANGES),
			Map.entry("prerna/auth/User",
					Set.of("setAccessToken", "setGlobalAccessToken", "dropAccessToken", "setPrimaryLogin")),
			Map.entry("prerna/auth/AccessToken", IDENTITY_SETTERS),
			Map.entry("prerna/auth/ReadOnlyAccessToken", IDENTITY_SETTERS),
			Map.entry("javax/xml/parsers/SAXParser", Set.of("getParser"))),
			List.of(SandboxedLog4j.BLOCKED_CALLS, SandboxedHikari.BLOCKED_CALLS, SandboxedJackson.BLOCKED_CALLS,
					SandboxedPdfBox.BLOCKED_CALLS, SandboxedPoi.BLOCKED_CALLS));

	/**
	 * Every method name an owner in {@link #BLOCKED_CALLS} refuses, so a call by
	 * any other name needs no look at the superclasses of the class it names.
	 */
	private static final Set<String> BLOCKED_CALL_NAMES = union(List.copyOf(BLOCKED_CALLS.values()));

	/**
	 * Whether a call, as {@code owner.name}, reaches a refused method through a
	 * superclass of the class it names, worked out once per call.
	 */
	private static final Map<String, Boolean> INHERITED_BLOCKED_CALLS = new ConcurrentHashMap<>();

	/**
	 * Whether a class, by internal name, is or inherits from an owner in
	 * {@link #BLOCKED_CALLS}, worked out once per class.
	 */
	private static final Map<String, Boolean> INHERITS_BLOCKED_CALLS = new ConcurrentHashMap<>();

	/**
	 * The package of the query utilities an RDBMS engine hands out
	 * ({@code getQueryUtil()}), as an internal name prefix.
	 */
	private static final String QUERY_UTIL_PACKAGE = "prerna/util/sql/";

	/**
	 * The methods of the query utilities project reactor code cannot call, on any
	 * class of {@link #QUERY_UTIL_PACKAGE}: the ones that read the engine's user
	 * name, password, connection properties and connection URL, and the ones that
	 * change where the engine connects.
	 */
	private static final Set<String> QUERY_UTIL_CONNECTION_MEMBERS = Set.of("getUsername", "getPassword",
			"getAdditionalProps", "getConnectionUrl", "setHostname", "setPort", "setDatabase", "setSchema",
			"setUsername", "setPassword", "setAdditionalProps", "setConnectionUrl");

	/**
	 * Library classes project reactor code can never use, by full name; their
	 * nested classes are blocked with them. Each shared library lists its own in
	 * its sandbox class.
	 */
	private static final Set<String> BLOCKED_LIBRARY_CLASSES = union(List.of(SandboxedMail.BLOCKED_CLASSES));

	/**
	 * The library packages project reactor code shares with the platform, as name
	 * prefixes: org.json and tuples, which need no other rule, and the packages of
	 * Jackson, POI, PDFBox and the mail APIs, listed in their sandbox classes. Any
	 * other library on the platform classpath that a project class names is loaded
	 * as the project's own sandboxed copy.
	 */
	private static final List<String> SHARED_LIBRARY_PACKAGES = concat(List.of(List.of("org.json.", "org.javatuples."),
			SandboxedJackson.SHARED_PACKAGES, SandboxedPoi.SHARED_PACKAGES, SandboxedPdfBox.SHARED_PACKAGES,
			SandboxedMail.SHARED_PACKAGES));

	/**
	 * Library packages shared without their sub-packages, whose other packages hold
	 * the library's internals: Gson, which needs no other rule, and the log4j API
	 * and HikariCP, listed in their sandbox classes.
	 */
	private static final Set<String> SHARED_LIBRARY_EXACT_PACKAGES = union(List.of(Set.of("com.google.gson",
			"com.google.gson.annotations", "com.google.gson.reflect", "com.google.gson.stream"),
			SandboxedLog4j.SHARED_EXACT_PACKAGES, SandboxedHikari.SHARED_EXACT_PACKAGES));

	/**
	 * Single classes shared out of a package that is not, listed in the sandbox
	 * class of their library.
	 */
	private static final Set<String> SHARED_LIBRARY_CLASSES = union(List.of(SandboxedLog4j.SHARED_CLASSES));

	/**
	 * Single classes kept out of a shared package, so a project gets its own
	 * sandboxed copy, listed in the sandbox class of their library.
	 */
	private static final Set<String> UNSHARED_LIBRARY_CLASSES = union(
			List.of(SandboxedLog4j.UNSHARED_CLASSES, SandboxedJackson.UNSHARED_CLASSES));

	/**
	 * Internal name of the sandbox utility class whose static methods replace calls
	 * in the transformed bytecode.
	 */
	private static final String SANDBOX = "prerna/util/SandboxedJavaExecution";

	/**
	 * The oldest class file version the sandbox writes: Java 5, the first in which
	 * {@code ldc} loads a class constant, which the rewritten calls use to pass the
	 * calling class. A class compiled for an older Java, such as a library built
	 * for Java 1.1, is written at this version and checked by the same verifier.
	 */
	private static final int OLDEST_WRITTEN_VERSION = Opcodes.V1_5;

	/**
	 * Internal name of the sandbox class whose static methods harden the JDK's XML
	 * parsers and check the documents they are pointed at.
	 */
	private static final String SANDBOX_XML = "prerna/util/SandboxedXml";

	/**
	 * Internal name of the sandbox class whose static methods stand in for class
	 * loading: finding a class by name, handing out a loader, and creating a
	 * loader for local jars.
	 */
	private static final String SANDBOX_CLASS_LOADING = "prerna/util/SandboxedClassLoading";

	private static final String CLASS_LOADER = "java/lang/ClassLoader";
	private static final String SECURE_CLASS_LOADER = "java/security/SecureClassLoader";
	private static final String URL_CLASS_LOADER = "java/net/URLClassLoader";
	private static final String SANDBOXED_CLASS_LOADER = "prerna/util/SandboxedClassLoader";
	private static final String PROJECT_JAR_CLASS_LOADER = "prerna/util/SemossProjectJarClassLoader";
	private static final String CLASS_LOADER_DESCRIPTOR = "Ljava/lang/ClassLoader;";
	private static final String CLASS_DESCRIPTOR = "Ljava/lang/Class;";

	/**
	 * The JDK loader classes a project class can extend, each with the sandbox
	 * class it extends instead once rewritten.
	 */
	private static final Map<String, String> LOADER_BASES = Map.of(CLASS_LOADER, SANDBOXED_CLASS_LOADER,
			SECURE_CLASS_LOADER, SANDBOXED_CLASS_LOADER, URL_CLASS_LOADER, PROJECT_JAR_CLASS_LOADER);

	/**
	 * The classes that make up the sandbox. Project reactor code cannot name them:
	 * only the calls the sandbox itself writes into a class reach them.
	 */
	private static final Set<String> SANDBOX_CLASSES = Set.of(SANDBOX, SANDBOX_XML, SANDBOX_CLASS_LOADING,
			"prerna/util/SemossClassLoader",
			PROJECT_JAR_CLASS_LOADER, "prerna/util/SemossJarClassLoader", SANDBOXED_CLASS_LOADER,
			SandboxedPdfBox.INTERNAL_NAME, SandboxedHikari.INTERNAL_NAME, SandboxedJackson.INTERNAL_NAME,
			SandboxedJackson3.INTERNAL_NAME, SandboxedGson.INTERNAL_NAME, "prerna/util/SandboxedLog4j",
			"prerna/util/SandboxedPoi", "prerna/util/SandboxedMail");

	/**
	 * The JSON mapper classes that read and set an object's members by reflection,
	 * each with the sandbox class whose {@code guarded} method stands in for a
	 * mapper before project reactor code calls it. A call on a mapper class that
	 * extends one of them is treated the same, and a project class cannot extend
	 * one.
	 */
	private static final Map<String, String> JSON_MAPPER_GUARDS = Map.of(SandboxedGson.GSON,
			SandboxedGson.INTERNAL_NAME, SandboxedJackson.OBJECT_MAPPER, SandboxedJackson.INTERNAL_NAME,
			SandboxedJackson.OBJECT_CODEC, SandboxedJackson.INTERNAL_NAME, SandboxedJackson3.OBJECT_MAPPER,
			SandboxedJackson3.INTERNAL_NAME);

	/**
	 * How the internal name of a class that can be a JSON mapper starts.
	 */
	private static final List<String> JSON_MAPPER_PACKAGES = List.of("com/google/gson/", "com/fasterxml/jackson/",
			"tools/jackson/");

	/**
	 * The mapper class in {@link #JSON_MAPPER_GUARDS} a class is or extends, by
	 * internal name, worked out once per class; the empty text for a class that is
	 * no mapper.
	 */
	private static final Map<String, String> JSON_MAPPER_BASES = new ConcurrentHashMap<>();

	private static final String FILE = "java/io/File";
	private static final String FILE_DESCRIPTOR = "Ljava/io/File;";
	private static final String PATH_DESCRIPTOR = "Ljava/nio/file/Path;";
	private static final String STRING_DESCRIPTOR = "Ljava/lang/String;";
	private static final String URL_DESCRIPTOR = "Ljava/net/URL;";
	private static final String URI_DESCRIPTOR = "Ljava/net/URI;";
	private static final String INPUT_SOURCE_DESCRIPTOR = "Lorg/xml/sax/InputSource;";
	private static final String SOURCE_DESCRIPTOR = "Ljavax/xml/transform/Source;";

	/**
	 * A sandbox method that takes a value and gives back the value to use in its
	 * place.
	 *
	 * @param helper the internal name of the sandbox class
	 * @param method the method
	 * @param type   the descriptor of the value
	 */
	private record ValueRule(String helper, String method, String type) {
	}

	private static final ValueRule PATH_TEXT_RULE = new ValueRule(SANDBOX, "sandboxedPath", STRING_DESCRIPTOR);
	private static final ValueRule FILE_RULE = new ValueRule(SANDBOX, "sandboxedFileValue", FILE_DESCRIPTOR);
	private static final ValueRule SYSTEM_ID_RULE = new ValueRule(SANDBOX_XML, "systemId", STRING_DESCRIPTOR);
	private static final ValueRule SYSTEM_URL_RULE = new ValueRule(SANDBOX_XML, "systemUrl", URL_DESCRIPTOR);
	private static final ValueRule FILE_SYSTEM_URI_RULE = new ValueRule(SANDBOX, "sandboxedFileSystemUri",
			URI_DESCRIPTOR);
	private static final ValueRule INPUT_SOURCE_RULE = new ValueRule(SANDBOX_XML, "inputSource",
			INPUT_SOURCE_DESCRIPTOR);
	private static final ValueRule SOURCE_RULE = new ValueRule(SANDBOX_XML, "source", SOURCE_DESCRIPTOR);

	/**
	 * The rule for each kind of argument that project code hands to the JDK, the
	 * platform or a shared library, by the argument's descriptor: files and paths
	 * follow the path rule, a URL is a local file under the path rule, since the
	 * class it is handed to would fetch it, and XML sources and results have their
	 * system id checked.
	 */
	private static final Map<String, ValueRule> ARGUMENT_RULES = Map.of(FILE_DESCRIPTOR, FILE_RULE, PATH_DESCRIPTOR,
			new ValueRule(SANDBOX, "sandboxedPathValue", PATH_DESCRIPTOR), URL_DESCRIPTOR,
			new ValueRule(SANDBOX, "sandboxedUrlValue", URL_DESCRIPTOR), INPUT_SOURCE_DESCRIPTOR, INPUT_SOURCE_RULE,
			SOURCE_DESCRIPTOR, SOURCE_RULE, "[" + SOURCE_DESCRIPTOR,
			new ValueRule(SANDBOX_XML, "sources", "[" + SOURCE_DESCRIPTOR), "Ljavax/xml/transform/Result;",
			new ValueRule(SANDBOX_XML, "result", "Ljavax/xml/transform/Result;"));

	/**
	 * The XML factory lookups, each with the sandbox method that hands out the
	 * JDK's own factory, hardened. A lookup that could pick another parser off the
	 * classpath is answered the same way.
	 */
	private static final Map<String, String> XML_FACTORY_LOOKUPS = Map.of("javax/xml/parsers/DocumentBuilderFactory",
			"documentBuilderFactory", "javax/xml/parsers/SAXParserFactory", "saxParserFactory",
			"javax/xml/transform/TransformerFactory", "transformerFactory", "javax/xml/validation/SchemaFactory",
			"schemaFactory", "javax/xml/stream/XMLInputFactory", "xmlInputFactory", "javax/xml/xpath/XPathFactory",
			"xpathFactory", "org/xml/sax/helpers/XMLReaderFactory", "xmlReader");

	private static final Set<String> XML_FACTORY_LOOKUP_METHODS = Set.of("newInstance", "newDefaultInstance",
			"newNSInstance", "newDefaultNSInstance", "newFactory", "newDefaultFactory", "createXMLReader");

	private static final Set<String> TRANSFORMER_FACTORY_USES = Set.of("newTransformer", "newTemplates",
			"getAssociatedStylesheet", "newTransformerHandler", "newTemplatesHandler", "newXMLFilter");

	/**
	 * The calls that put an XML factory, parser, reader or validator to use, by
	 * owner. The sandbox applies its XML settings to the object right before each
	 * one, so a setting the project changed has no effect.
	 */
	private static final Map<String, Set<String>> XML_USES = Map.ofEntries(
			Map.entry("javax/xml/parsers/DocumentBuilderFactory", Set.of("newDocumentBuilder")),
			Map.entry("javax/xml/parsers/SAXParserFactory", Set.of("newSAXParser")),
			Map.entry("javax/xml/parsers/SAXParser", Set.of("parse")),
			Map.entry("org/xml/sax/XMLReader", Set.of("parse")), Map.entry("org/xml/sax/XMLFilter", Set.of("parse")),
			Map.entry("org/xml/sax/helpers/XMLFilterImpl", Set.of("parse")),
			Map.entry("javax/xml/transform/TransformerFactory", TRANSFORMER_FACTORY_USES),
			Map.entry("javax/xml/transform/sax/SAXTransformerFactory", TRANSFORMER_FACTORY_USES),
			Map.entry("javax/xml/validation/SchemaFactory", Set.of("newSchema")),
			Map.entry("javax/xml/validation/Validator", Set.of("validate")),
			Map.entry("javax/xml/stream/XMLInputFactory", Set.of("createXMLStreamReader", "createXMLEventReader")),
			Map.entry("javax/xml/xpath/XPathFactory", Set.of("newXPath")));

	/**
	 * The type the sandbox method for an {@link #XML_USES} owner takes, where it is
	 * not the owner itself.
	 */
	private static final Map<String, String> XML_USE_TYPES = Map.of("javax/xml/transform/sax/SAXTransformerFactory",
			"javax/xml/transform/TransformerFactory", "org/xml/sax/XMLFilter", "org/xml/sax/XMLReader",
			"org/xml/sax/helpers/XMLFilterImpl", "org/xml/sax/XMLReader");

	private static final Set<String> XPATH_CLASSES = Set.of("javax/xml/xpath/XPath", "javax/xml/xpath/XPathExpression");

	/**
	 * Calls that take the address of an XML document as text or as a URL, or the
	 * address of a zip file to open as a file system as a URI, by owner, then
	 * method name and descriptor, then the position of the argument. The address
	 * has to be a local file, and follows the path rule.
	 */
	private static final Map<String, Map<String, Integer>> SYSTEM_ID_ARGUMENTS = Map.ofEntries(
			Map.entry("javax/xml/parsers/DocumentBuilder",
					Map.of("parse(Ljava/lang/String;)Lorg/w3c/dom/Document;", 0,
							"parse(Ljava/io/InputStream;Ljava/lang/String;)Lorg/w3c/dom/Document;", 1)),
			Map.entry("javax/xml/parsers/SAXParser",
					Map.of("parse(Ljava/lang/String;Lorg/xml/sax/HandlerBase;)V", 0,
							"parse(Ljava/lang/String;Lorg/xml/sax/helpers/DefaultHandler;)V", 0,
							"parse(Ljava/io/InputStream;Lorg/xml/sax/HandlerBase;Ljava/lang/String;)V", 2,
							"parse(Ljava/io/InputStream;Lorg/xml/sax/helpers/DefaultHandler;Ljava/lang/String;)V", 2)),
			Map.entry("org/xml/sax/XMLReader", Map.of("parse(Ljava/lang/String;)V", 0)),
			Map.entry("org/xml/sax/XMLFilter", Map.of("parse(Ljava/lang/String;)V", 0)),
			Map.entry("org/xml/sax/helpers/XMLFilterImpl", Map.of("parse(Ljava/lang/String;)V", 0)),
			Map.entry("org/xml/sax/Parser", Map.of("parse(Ljava/lang/String;)V", 0)),
			Map.entry("javax/xml/stream/XMLInputFactory", Map.of(
					"createXMLStreamReader(Ljava/lang/String;Ljava/io/InputStream;)Ljavax/xml/stream/XMLStreamReader;",
					0, "createXMLStreamReader(Ljava/lang/String;Ljava/io/Reader;)Ljavax/xml/stream/XMLStreamReader;", 0,
					"createXMLEventReader(Ljava/lang/String;Ljava/io/InputStream;)Ljavax/xml/stream/XMLEventReader;", 0,
					"createXMLEventReader(Ljava/lang/String;Ljava/io/Reader;)Ljavax/xml/stream/XMLEventReader;", 0)),
			Map.entry("javax/xml/validation/SchemaFactory",
					Map.of("newSchema(Ljava/net/URL;)Ljavax/xml/validation/Schema;", 0)),
			Map.entry("org/w3c/dom/ls/LSSerializer", Map.of("writeToURI(Lorg/w3c/dom/Node;Ljava/lang/String;)Z", 1)),
			Map.entry("org/w3c/dom/ls/LSOutput", Map.of("setSystemId(Ljava/lang/String;)V", 0)),
			Map.entry("java/nio/file/FileSystems",
					Map.of("newFileSystem(Ljava/net/URI;Ljava/util/Map;)Ljava/nio/file/FileSystem;", 0,
							"newFileSystem(Ljava/net/URI;Ljava/util/Map;Ljava/lang/ClassLoader;)Ljava/nio/file/FileSystem;",
							0)));

	/**
	 * Name prefixes of the JDK classes outside {@code java.*}.
	 */
	private static final List<String> JDK_PREFIXES = List.of("javax/", "jdk/", "com/sun/", "sun/", "org/w3c/",
			"org/xml/");

	/**
	 * Classes whose File, Path and URL arguments are used to work out a path or an
	 * address, such as {@code Path.resolve(Path)} or {@code new URL(URL, String)},
	 * and are not opened. Those arguments are left as they are, and the path that
	 * results is checked where it is used.
	 */
	private static final Set<String> PATH_ARITHMETIC_CLASSES = Set.of("java/nio/file/Path", "java/nio/file/Paths",
			FILE, "java/net/URL");

	/**
	 * The File methods that read or change the file system. The file they are
	 * called on is put under the path rule first.
	 */
	private static final Set<String> FILE_IO_METHODS = Set.of("exists", "canRead", "canWrite", "canExecute",
			"isDirectory", "isFile", "isHidden", "lastModified", "length", "createNewFile", "delete", "deleteOnExit",
			"list", "listFiles", "mkdir", "mkdirs", "renameTo", "setLastModified", "setReadOnly", "setWritable",
			"setReadable", "setExecutable", "getTotalSpace", "getFreeSpace", "getUsableSpace", "getCanonicalFile");

	/**
	 * Calls that take a path as text, by owner, then method name, then the position
	 * of the path argument. The text is put under the path rule before the call.
	 * They are the JDK classes that open a file by name and the platform helpers
	 * that read or write a file for their caller, and the shared library methods
	 * that open a file by name, listed in their sandbox classes.
	 */
	private static final Map<String, Map<String, Integer>> PATH_TEXT_ARGUMENTS = withLibraryRules(Map.ofEntries(
			Map.entry("java/io/FileInputStream", Map.of("<init>", 0)),
			Map.entry("java/io/FileOutputStream", Map.of("<init>", 0)),
			Map.entry("java/io/FileReader", Map.of("<init>", 0)), Map.entry("java/io/FileWriter", Map.of("<init>", 0)),
			Map.entry("java/io/RandomAccessFile", Map.of("<init>", 0)),
			Map.entry("java/io/PrintWriter", Map.of("<init>", 0)),
			Map.entry("java/io/PrintStream", Map.of("<init>", 0)),
			Map.entry("java/util/Formatter", Map.of("<init>", 0)),
			Map.entry("java/util/zip/ZipFile", Map.of("<init>", 0)),
			Map.entry("java/util/jar/JarFile", Map.of("<init>", 0)),
			Map.entry("prerna/util/Utility", Map.of("writeResultToFile", 0, "writeResultToJson", 0, "writeWorkbook", 1,
					"folderHasAnyFiles", 0, "folderIsNotEmpty", 0, "getUniqueFilePath", 0))),
			List.of(SandboxedPoi.PATH_TEXT_ARGUMENTS, SandboxedPdfBox.PATH_TEXT_ARGUMENTS,
					SandboxedMail.PATH_TEXT_ARGUMENTS));

	/**
	 * The two forms of {@code loadClass}, as name and descriptor. A custom loader
	 * supplies its classes through {@code findClass} and cannot override these.
	 */
	private static final Set<String> LOAD_CLASS_METHODS = Set.of("loadClass(Ljava/lang/String;)Ljava/lang/Class;",
			"loadClass(Ljava/lang/String;Z)Ljava/lang/Class;");

	/**
	 * The descriptors of the {@code defineClass} methods of the JDK loader classes.
	 */
	private static final Set<String> DEFINE_CLASS_DESCRIPTORS = Set.of("(Ljava/lang/String;[BII)Ljava/lang/Class;",
			"([BII)Ljava/lang/Class;", "(Ljava/lang/String;[BIILjava/security/ProtectionDomain;)Ljava/lang/Class;",
			"(Ljava/lang/String;Ljava/nio/ByteBuffer;Ljava/security/ProtectionDomain;)Ljava/lang/Class;",
			"(Ljava/lang/String;[BIILjava/security/CodeSource;)Ljava/lang/Class;",
			"(Ljava/lang/String;Ljava/nio/ByteBuffer;Ljava/security/CodeSource;)Ljava/lang/Class;");

	/**
	 * The methods the sandbox rewrites or blocks, by owner, along with the ones in
	 * {@link #BLOCKED_CALLS}. A method reference or method handle constant that
	 * points at one of them would run it without the rewrite, so a class holding
	 * one is refused.
	 */
	private static final Map<String, Set<String>> SANDBOXED_MEMBERS = Map.ofEntries(
			Map.entry("java/lang/System",
					Set.of("exit", "getenv", "getProperty", "getProperties", "setProperty", "clearProperty",
							"setProperties")),
			Map.entry("java/lang/Runtime", Set.of("halt", "exec")),
			Map.entry("java/lang/Class", Set.of("forName", "getClassLoader", "newInstance")),
			Map.entry("java/lang/ClassLoader",
					Set.of("getSystemClassLoader", "getPlatformClassLoader", "getParent", "loadClass", "defineClass",
							"findSystemClass")),
			Map.entry("java/security/SecureClassLoader", Set.of("getParent", "loadClass", "defineClass")),
			Map.entry("java/net/URLClassLoader",
					Set.of("<init>", "newInstance", "getParent", "loadClass", "defineClass")),
			Map.entry("java/lang/Thread", Set.of("getAllStackTraces")),
			Map.entry("java/nio/file/Files", Set.of("createSymbolicLink")),
			Map.entry("java/nio/channels/FileChannel", Set.of("open")),
			Map.entry("java/net/URL", Set.of("openStream", "openConnection")),
			Map.entry("java/nio/file/Paths", Set.of("get")), Map.entry("java/nio/file/Path", Set.of("of")),
			Map.entry("java/io/File",
					Set.of("<init>", "getParentFile", "getParent", "getAbsolutePath", "getCanonicalPath", "toPath")),
			Map.entry("java/io/FileInputStream", Set.of("<init>")),
			Map.entry("java/io/FileOutputStream", Set.of("<init>")), Map.entry("prerna/util/Utility",
					Set.of("getDIHelperProperty", "loadProperties")));

	private String folder = null;

	private final String projectId;

	private final List<String> approvedPackages = approvedLibraryPackages();

	/**
	 * Constructs a new SemossClassLoader with a specified parent class loader.
	 *
	 * @param parent The parent class loader.
	 */
	public SemossClassLoader(ClassLoader parent) {
		this(parent, (String) null);
	}

	/**
	 * Constructs the loader for the custom classes of one project.
	 *
	 * @param parent    the parent class loader
	 * @param projectId the id of the project whose classes this loader loads, or
	 *                  null when the classes belong to no project
	 */
	public SemossClassLoader(ClassLoader parent, String projectId) {
		super(parent);
		this.projectId = projectId;
	}

	/**
	 * Constructs a new SemossClassLoader and registers additional system properties
	 * that reactor code loaded by this instance is permitted to read.
	 *
	 * <p>
	 * Map semantics - for each entry:
	 * <ul>
	 * <li>Non-null value: reactor always receives this value for the key (platform
	 * override, decoupled from the real system property).</li>
	 * <li>Null value: the real {@code System.getProperty(key)} value is returned
	 * (allow-list the key, expose its actual value).</li>
	 * </ul>
	 *
	 * @param parent               the parent class loader
	 * @param additionalProperties extra properties to expose to reactor code;
	 *                             {@code null} is treated as an empty map
	 */
	public SemossClassLoader(ClassLoader parent, Map<String, String> additionalProperties) {
		super(parent);
		this.projectId = null;
		SandboxedJavaExecution.addPlatformProperties(additionalProperties);
	}

	/**
	 * The project whose custom classes this loader loads.
	 *
	 * @return the project id, or null when the classes belong to no project
	 */
	String getProjectId() {
		return this.projectId;
	}

	/**
	 * Sets the base folder from which to load .class files.
	 * 
	 * @param folder The absolute path to the directory containing the compiled
	 *               classes.
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}

	/**
	 * Finds and loads the class from a .class file. This method is called when a
	 * class is not found in the parent class loader's path.
	 *
	 * @param name The fully qualified name of the class to load.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found.
	 */
	private Class<?> getClass(String name) throws ClassNotFoundException {
		// We are getting a name that looks like
		// prerna.project.MyCustomClass
		// and we have to convert it into the .class file name
		// like prerna/project/MyCustomClass.class
		String file = name.replace('.', File.separatorChar) + ".class";
		byte[] b = null;
		try {
			// This loads the byte code data from the file
			b = loadClassData(file);
			// defineClass is inherited from the ClassLoader class
			// and converts the byte array into a Class
			if (b != null) {
				b = applyBytecodeTransformations(b);
				Class<?> c = defineClass(name, b, 0, b.length);
				resolveClass(c);
				return c;
			}
		} catch (IOException e) {
			classLogger.error("Failed to read class file for '{}' from folder '{}'", name, folder, e);
		} catch (SecurityException e) {
			throw new ClassNotFoundException("Class '" + name + "' blocked by sandbox: " + e.getMessage(), e);
		} catch (IllegalArgumentException e) {
			// ASM throws this when the class file was compiled with a newer Java version
			// than the ASM library supports ("Unsupported class file major version N").
			// Deny the load rather than allowing untransformed bytecode through.
			throw new ClassNotFoundException(
					"Class '" + name + "' cannot be sandbox-transformed (unsupported class file version"
							+ " - upgrade asm-commons in pom.xml): " + e.getMessage(),
					e);
		}
		return null;
	}

	/**
	 * Returns true if the given class name belongs to a JDK API that provides
	 * private reflective access to fields or objects. Custom project reactor code
	 * must not be able to use these APIs to bypass the SystemEngineRegistry access
	 * controls.
	 *
	 * Blocked: - java.lang.reflect.* direct Field/Method/Constructor reflection -
	 * java.lang.invoke.MethodHandles privateLookupIn() gives equivalent access -
	 * java.lang.invoke.MethodHandles$Lookup the lookup object itself -
	 * sun.misc.Unsafe objectFieldOffset+getObject reads any field -
	 * jdk.internal.misc.Unsafe same capability, internal variant - the platform
	 * classes in {@link #BLOCKED_PLATFORM_CLASSES} and their nested classes - the
	 * platform packages in {@link #BLOCKED_PLATFORM_PACKAGES} - every platform
	 * class that requires an admin - the JDK packages in
	 * {@link #BLOCKED_JDK_PACKAGES} - the JDK classes in
	 * {@link #BLOCKED_JDK_CLASSES} and their nested classes
	 *
	 * @param name fully-qualified class name
	 * @return true if this class must not be loaded by project reactor code
	 */
	static boolean isReflectionApiBlocked(String name) {
		for (String blocked : BLOCKED_PLATFORM_CLASSES) {
			if (name.equals(blocked) || name.startsWith(blocked + "$")) {
				return true;
			}
		}
		for (String blocked : BLOCKED_PLATFORM_PACKAGES) {
			if (name.startsWith(blocked)) {
				return true;
			}
		}
		if (requiresAdmin(name)) {
			return true;
		}
		for (String blocked : BLOCKED_JDK_PACKAGES) {
			if (name.startsWith(blocked)) {
				return true;
			}
		}
		for (String blocked : BLOCKED_JDK_CLASSES) {
			if (name.equals(blocked) || name.startsWith(blocked + "$")) {
				return true;
			}
		}
		for (String blocked : BLOCKED_LIBRARY_CLASSES) {
			if (name.equals(blocked) || name.startsWith(blocked + "$")) {
				return true;
			}
		}
		if (SandboxedJackson.isBlocked(name)) {
			return true;
		}
		// Reflection / field access
		if (name.startsWith("java.lang.reflect.")) {
			return true;
		}
		if (name.equals("java.lang.invoke.MethodHandles")) {
			return true;
		}
		if (name.equals("java.lang.invoke.MethodHandles$Lookup")) {
			return true;
		}
		if (name.equals("sun.misc.Unsafe")) {
			return true;
		}
		if (name.equals("jdk.internal.misc.Unsafe")) {
			return true;
		}

		// Process execution
		if (name.equals("java.lang.ProcessBuilder")) {
			return true;
		}

		// Script engines - execute arbitrary code bypassing bytecode transformation
		if (name.startsWith("javax.script.")) {
			return true;
		}
		if (name.startsWith("jdk.nashorn.")) {
			return true;
		}
		if (name.startsWith("org.codehaus.groovy.")) {
			return true;
		}

		// Raw networking - data exfiltration / internal service access
		if (name.equals("java.net.Socket")) {
			return true;
		}
		if (name.equals("java.net.ServerSocket")) {
			return true;
		}
		if (name.equals("java.net.DatagramSocket")) {
			return true;
		}
		if (name.equals("java.net.MulticastSocket")) {
			return true;
		}
		if (name.startsWith("java.nio.channels.SocketChannel")) {
			return true;
		}
		if (name.startsWith("java.nio.channels.ServerSocketChannel")) {
			return true;
		}
		if (name.startsWith("java.nio.channels.DatagramChannel")) {
			return true;
		}

		// Thread creation - fork bombs, classloader replacement, context leaks
		if (name.equals("java.lang.Thread")) {
			return true;
		}
		if (name.startsWith("java.util.concurrent.ThreadPoolExecutor")) {
			return true;
		}
		if (name.startsWith("java.util.concurrent.ScheduledThreadPoolExecutor")) {
			return true;
		}
		if (name.startsWith("java.util.concurrent.ForkJoinPool")) {
			return true;
		}

		// Deserialization gadget chains
		if (name.equals("java.io.ObjectInputStream")) {
			return true;
		}

		// JVM diagnostics / classpath disclosure
		if (name.startsWith("java.lang.management.")) {
			return true;
		}

		return false;
	}

	/**
	 * True for a loader that applies the sandbox to the classes it loads.
	 *
	 * @param loader a class loader, or null
	 * @return true if the loader is one of the sandbox loaders
	 */
	static boolean isSandboxLoader(ClassLoader loader) {
		return loader instanceof SemossClassLoader || loader instanceof SemossProjectJarClassLoader
				|| loader instanceof SemossJarClassLoader || loader instanceof SandboxedClassLoader;
	}

	/**
	 * The class file a project's own loader asked to define, rewritten by the
	 * sandbox.
	 *
	 * @param classBytes the array holding the class file
	 * @param offset     where the class file starts
	 * @param length     how long the class file is
	 * @return the sandboxed class file
	 * @throws SecurityException if the class is in a {@code prerna.*} package or
	 *                           the sandbox refuses it
	 */
	static byte[] sandboxedDefinition(byte[] classBytes, int offset, int length) {
		byte[] original = Arrays.copyOfRange(classBytes, offset, offset + length);
		String internalName = new ClassReader(original).getClassName();
		if (internalName.startsWith("prerna/")) {
			throw new SecurityException("Classes in 'prerna.*' packages cannot be defined in project reactors: "
					+ internalName.replace('/', '.'));
		}
		return applyBytecodeTransformations(original);
	}

	/**
	 * The class file a project's own loader asked to define, rewritten by the
	 * sandbox.
	 *
	 * @param classBytes the buffer holding the class file
	 * @return the sandboxed class file
	 * @throws SecurityException if the class is in a {@code prerna.*} package or
	 *                           the sandbox refuses it
	 */
	static byte[] sandboxedDefinition(ByteBuffer classBytes) {
		byte[] original = new byte[classBytes.remaining()];
		classBytes.get(original);
		return sandboxedDefinition(original, 0, original.length);
	}

	/**
	 * True for a platform class that requires an admin: by convention its name
	 * starts with {@code Admin}, such as the admin reactors and the admin
	 * endpoints, plus SecurityAdminUtils. Project reactor code can never use one,
	 * and an admin has more than their own permissions on an engine only while
	 * one is acting.
	 *
	 * @param name fully-qualified class name
	 * @return true if only an admin may use the class
	 */
	public static boolean requiresAdmin(String name) {
		if (!name.startsWith(PLATFORM_PACKAGE)) {
			return false;
		}
		if (name.equals(ADMIN_UTILS_CLASS) || name.startsWith(ADMIN_UTILS_CLASS + "$")) {
			return true;
		}
		return name.startsWith(ADMIN_CLASS_PREFIX, name.lastIndexOf('.') + 1);
	}

	/**
	 * The library packages an admin approved for sharing with project reactor code,
	 * on top of the built-in list: the comma separated package names in the RDF_Map
	 * setting {@link Constants#PROJECT_REACTOR_SHARED_PACKAGES}.
	 *
	 * @return the approved packages as name prefixes
	 */
	static List<String> approvedLibraryPackages() {
		String setting = Utility.getDIHelperProperty(Constants.PROJECT_REACTOR_SHARED_PACKAGES);
		if (setting == null || setting.isBlank()) {
			return List.of();
		}
		List<String> packages = new ArrayList<>();
		for (String entry : setting.split(",")) {
			String packageName = entry.trim();
			if (!packageName.isEmpty()) {
				packages.add(packageName.endsWith(".") ? packageName : packageName + ".");
			}
		}
		return List.copyOf(packages);
	}

	/**
	 * True when a call is refused: the class it names, or a superclass of that
	 * class on the platform's side, is an owner in {@link #BLOCKED_CALLS} that
	 * refuses the method. A call named through a subclass, such as
	 * {@code SimpleTimeZone.setDefault}, is refused like one named through the
	 * class that declares the method.
	 *
	 * @param owner the internal name of the class the call names
	 * @param name  the method
	 * @return true if the call is refused
	 */
	static boolean isBlockedCall(String owner, String name) {
		if (!BLOCKED_CALL_NAMES.contains(name)) {
			return false;
		}
		Set<String> declared = BLOCKED_CALLS.get(owner);
		if (declared != null && declared.contains(name)) {
			return true;
		}
		return INHERITED_BLOCKED_CALLS.computeIfAbsent(owner + "." + name, call -> {
			for (String ancestor : platformSuperclasses(owner)) {
				Set<String> refused = BLOCKED_CALLS.get(ancestor);
				if (refused != null && refused.contains(name)) {
					return true;
				}
			}
			return false;
		});
	}

	/**
	 * True when a class is an owner in {@link #BLOCKED_CALLS} or inherits from one
	 * on the platform's side. A project class cannot extend it, since the class
	 * would hand the refused methods on.
	 *
	 * @param internalName the internal name of the class
	 * @return true if the class has refused calls of its own or inherited
	 */
	static boolean inheritsBlockedCalls(String internalName) {
		if (BLOCKED_CALLS.containsKey(internalName)) {
			return true;
		}
		return INHERITS_BLOCKED_CALLS.computeIfAbsent(internalName,
				type -> platformSuperclasses(type).stream().anyMatch(BLOCKED_CALLS::containsKey));
	}

	/**
	 * The JSON mapper class a class is or extends: Gson, or the Jackson 2 or
	 * Jackson 3 mapper.
	 *
	 * @param internalName the internal name of the class a call names, or null
	 * @return the internal name of the mapper class, or null for a class that is
	 *         no mapper
	 */
	static String jsonMapperBase(String internalName) {
		if (internalName == null) {
			return null;
		}
		boolean candidate = false;
		for (String mapperPackage : JSON_MAPPER_PACKAGES) {
			candidate = candidate || internalName.startsWith(mapperPackage);
		}
		if (!candidate) {
			return null;
		}
		String base = JSON_MAPPER_BASES.computeIfAbsent(internalName, type -> {
			if (JSON_MAPPER_GUARDS.containsKey(type)) {
				return type;
			}
			for (String ancestor : platformSuperclasses(type)) {
				if (JSON_MAPPER_GUARDS.containsKey(ancestor)) {
					return ancestor;
				}
			}
			return "";
		});
		return base.isEmpty() ? null : base;
	}

	/**
	 * The parser or generator class of {@link SandboxedJackson#CODEC_USES} a call
	 * reads or writes an object through.
	 *
	 * @param owner the internal name of the class the call names
	 * @param name  the method
	 * @return the internal name of the parser or generator class, or null when the
	 *         call is no such call
	 */
	static String jsonCodecUser(String owner, String name) {
		if (!owner.startsWith("com/fasterxml/jackson/")) {
			return null;
		}
		for (Map.Entry<String, Set<String>> use : SandboxedJackson.CODEC_USES.entrySet()) {
			if (use.getValue().contains(name)
					&& (use.getKey().equals(owner) || platformSuperclasses(owner).contains(use.getKey()))) {
				return use.getKey();
			}
		}
		return null;
	}

	/**
	 * True for a platform class that works on a system database directly.
	 *
	 * @param internalName the internal name of a class
	 * @return true if project reactor code cannot call its static methods or
	 *         extend it
	 */
	static boolean isSystemDatabaseClass(String internalName) {
		int nested = internalName.indexOf('$');
		String outer = nested < 0 ? internalName : internalName.substring(0, nested);
		if (SYSTEM_DATABASE_CLASSES.contains(outer)) {
			return true;
		}
		for (String systemPackage : SYSTEM_DATABASE_PACKAGES) {
			if (internalName.startsWith(systemPackage)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * True for a static call project reactor code cannot make: a method of a class
	 * that works on a system database directly. The security utilities still
	 * answer the questions whose names start with one of
	 * {@link #ACCESS_QUESTION_PREFIXES}, which are deprecated in custom reactors.
	 *
	 * @param owner the internal name of the class the call names
	 * @param name  the static method
	 * @return true if the call is refused
	 */
	static boolean isRefusedSystemDatabaseCall(String owner, String name) {
		return isSystemDatabaseClass(owner) && !isAccessQuestion(owner, name);
	}

	/**
	 * True for a security utility method that answers a question and changes
	 * nothing, by its name.
	 *
	 * @param owner the internal name of the class the call names
	 * @param name  the method
	 * @return true if the method is one of the security questions
	 */
	private static boolean isAccessQuestion(String owner, String name) {
		if (!isSecurityUtility(owner)) {
			return false;
		}
		for (String question : ACCESS_QUESTION_PREFIXES) {
			if (name.startsWith(question)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * One line the compile output adds about a call in a project class.
	 *
	 * @param source  the source file of the class, as a path from the package
	 *                root, such as {@code custom/reactors/CheckReactor.java}
	 * @param line    the line of the call, or 0 when the class has no line numbers
	 * @param refused true when the call throws when it runs, false when it is
	 *                deprecated
	 * @param message what the call does in a custom reactor and what to call
	 *                instead
	 */
	record CompileNotice(String source, int line, boolean refused, String message) {
	}

	/**
	 * What the compile output says about a compiled project class: each direct call
	 * it makes to a security question, which is deprecated in custom reactors, and
	 * each static call to a class that works on a system database, which throws
	 * when it runs. A notice names the {@link prerna.auth.CustomReactorSecurity}
	 * method to call instead when there is one.
	 *
	 * @param classBytes a compiled project class
	 * @return the notices, in the order of the calls
	 */
	static List<CompileNotice> compileNotices(byte[] classBytes) {
		ClassReader reader = new ClassReader(classBytes);
		String className = reader.getClassName();
		int packageEnd = className.lastIndexOf('/') + 1;
		String packagePath = className.substring(0, packageEnd);
		int nested = className.indexOf('$', packageEnd);
		String defaultSource = (nested < 0 ? className : className.substring(0, nested)) + ".java";
		List<CompileNotice> notices = new ArrayList<>();
		reader.accept(new ClassVisitor(Opcodes.ASM9) {
			private String source = defaultSource;

			@Override
			public void visitSource(String file, String debug) {
				if (file != null) {
					this.source = packagePath + file;
				}
			}

			@Override
			public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
					String[] exceptions) {
				return new MethodVisitor(Opcodes.ASM9) {
					private int line;

					@Override
					public void visitLineNumber(int lineNumber, Label start) {
						this.line = lineNumber;
					}

					@Override
					public void visitMethodInsn(int opcode, String owner, String called, String calledDescriptor,
							boolean isInterface) {
						if (opcode == Opcodes.INVOKESTATIC && isSystemDatabaseClass(owner)) {
							notices.add(compileNotice(source, this.line, owner, called));
						}
					}
				};
			}
		}, ClassReader.SKIP_FRAMES);
		return notices;
	}

	/**
	 * The compile notice for one static call to a class that works on a system
	 * database.
	 *
	 * @param source the source file the call is in
	 * @param line   the line of the call
	 * @param owner  the internal name of the class the call names
	 * @param name   the method
	 * @return the notice
	 */
	private static CompileNotice compileNotice(String source, int line, String owner, String name) {
		String call = owner.substring(owner.lastIndexOf('/') + 1).replace('$', '.') + "." + name;
		String replacement = customReactorSecurityReplacement(owner, name);
		String instead = replacement == null ? null : "; call CustomReactorSecurity." + replacement + " instead";
		if (isRefusedSystemDatabaseCall(owner, name)) {
			return new CompileNotice(source, line, true,
					call + " is not available to custom reactors and throws when it runs" + (instead == null
							? "; the platform's reactors, run through Pixel, read and change this data"
							: instead));
		}
		return new CompileNotice(source, line, false, call + " is deprecated in custom reactors" + (instead == null
				? "; CustomReactorSecurity answers the permission questions custom reactors ask"
				: instead));
	}

	/**
	 * The {@link prerna.auth.CustomReactorSecurity} method that replaces a direct
	 * call, looked up on the class the call names and then on its superclasses.
	 *
	 * @param owner the internal name of the class the call names
	 * @param name  the method
	 * @return the replacing method's name, or null when there is none
	 */
	private static String customReactorSecurityReplacement(String owner, String name) {
		String replacement = CUSTOM_REACTOR_SECURITY_REPLACEMENTS.get(owner + "." + name);
		if (replacement != null) {
			return replacement;
		}
		for (String ancestor : platformSuperclasses(owner)) {
			replacement = CUSTOM_REACTOR_SECURITY_REPLACEMENTS.get(ancestor + "." + name);
			if (replacement != null) {
				return replacement;
			}
		}
		return null;
	}

	/**
	 * True for a class of the security utilities package itself, without its
	 * sub-packages.
	 *
	 * @param internalName the internal name of a class
	 * @return true if the class is a security utility
	 */
	private static boolean isSecurityUtility(String internalName) {
		return internalName.startsWith(SECURITY_UTILITIES_PACKAGE)
				&& internalName.indexOf('/', SECURITY_UTILITIES_PACKAGE.length()) < 0;
	}

	/**
	 * The superclasses of a class the platform loads, as internal names, nearest
	 * first. The class is looked at without being initialized. A class the
	 * platform cannot load, such as a project's own, has none.
	 *
	 * @param internalName the internal name of the class
	 * @return its superclasses
	 */
	private static List<String> platformSuperclasses(String internalName) {
		List<String> superclasses = new ArrayList<>();
		if (internalName.startsWith("[")) {
			return superclasses;
		}
		try {
			Class<?> type = Class.forName(internalName.replace('/', '.'), false,
					SemossClassLoader.class.getClassLoader());
			for (Class<?> parent = type.getSuperclass(); parent != null; parent = parent.getSuperclass()) {
				superclasses.add(parent.getName().replace('.', '/'));
			}
		} catch (ClassNotFoundException | LinkageError e) {
			classLogger.debug("No platform class {} to look up refused calls on", internalName);
		}
		return superclasses;
	}

	/**
	 * One rule table out of the loader's own entries and the entries each shared
	 * library's sandbox class lists. A class with entries in two places stops the
	 * loader from starting, so each class's rules live in one place.
	 *
	 * @param <V>       the type of an entry
	 * @param own       the loader's own entries
	 * @param libraries the entries of each library's sandbox class
	 * @return the combined table
	 */
	private static <V> Map<String, V> withLibraryRules(Map<String, V> own, List<Map<String, V>> libraries) {
		Map<String, V> all = new HashMap<>(own);
		for (Map<String, V> library : libraries) {
			for (Map.Entry<String, V> entry : library.entrySet()) {
				if (all.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
					throw new IllegalStateException(entry.getKey() + " has sandbox rules in two places");
				}
			}
		}
		return Collections.unmodifiableMap(all);
	}

	/**
	 * Every name in a set of name sets.
	 *
	 * @param sets the sets
	 * @return their union
	 */
	private static Set<String> union(List<Set<String>> sets) {
		Set<String> all = new HashSet<>();
		for (Set<String> set : sets) {
			all.addAll(set);
		}
		return Collections.unmodifiableSet(all);
	}

	/**
	 * Every name in a list of name lists, in order.
	 *
	 * @param lists the lists
	 * @return the names
	 */
	private static List<String> concat(List<List<String>> lists) {
		List<String> all = new ArrayList<>();
		for (List<String> list : lists) {
			all.addAll(list);
		}
		return Collections.unmodifiableList(all);
	}

	/**
	 * True when project reactor code uses the platform's own copy of a class: a
	 * {@code java.*} or {@code prerna.*} class, or a class of a shared library.
	 *
	 * @param name             fully-qualified class name
	 * @param approvedPackages the packages an admin approved, as name prefixes
	 * @return true if the class is shared with the platform
	 */
	static boolean isSharedWithPlatform(String name, List<String> approvedPackages) {
		if (name.startsWith("java.") || name.startsWith(PLATFORM_PACKAGE)) {
			return true;
		}
		int nested = name.indexOf('$');
		String outer = nested < 0 ? name : name.substring(0, nested);
		if (UNSHARED_LIBRARY_CLASSES.contains(outer)) {
			return false;
		}
		for (String unshared : SandboxedJackson.UNSHARED_PACKAGES) {
			if (name.startsWith(unshared)) {
				return false;
			}
		}
		if (SHARED_LIBRARY_CLASSES.contains(outer)) {
			return true;
		}
		for (String shared : SHARED_LIBRARY_PACKAGES) {
			if (name.startsWith(shared)) {
				return true;
			}
		}
		for (String approved : approvedPackages) {
			if (name.startsWith(approved)) {
				return true;
			}
		}
		int packageEnd = outer.lastIndexOf('.');
		return packageEnd > 0 && SHARED_LIBRARY_EXACT_PACKAGES.contains(outer.substring(0, packageEnd));
	}

	/**
	 * The rewritten class file of a library class that a project gets its own copy
	 * of. A library on the platform classpath that is not shared is loaded this
	 * way, so the sandbox rules apply to it like they do to the project's own code.
	 *
	 * @param name             fully-qualified class name
	 * @param platform         the loader of the platform classpath, or null
	 * @param approvedPackages the packages an admin approved, as name prefixes
	 * @return the sandboxed class file, or null when the class is shared with the
	 *         platform, is a JDK class, or is not on the platform classpath
	 * @throws ClassNotFoundException if the class file cannot be read, or the
	 *                                sandbox refuses it
	 */
	static byte[] sandboxedLibraryCopy(String name, ClassLoader platform, List<String> approvedPackages)
			throws ClassNotFoundException {
		if (platform == null || isSharedWithPlatform(name, approvedPackages)) {
			return null;
		}
		String classFile = name.replace('.', '/') + ".class";
		if (ClassLoader.getPlatformClassLoader().getResource(classFile) != null) {
			return null;
		}
		URL resource = platform.getResource(classFile);
		if (resource == null || "jrt".equals(resource.getProtocol())) {
			return null;
		}
		byte[] original;
		try (InputStream in = resource.openStream()) {
			original = in.readAllBytes();
		} catch (IOException e) {
			throw new ClassNotFoundException("Could not read library class '" + name + "'", e);
		}
		try {
			return applyBytecodeTransformations(original);
		} catch (SecurityException e) {
			throw new ClassNotFoundException("Class '" + name + "' blocked by sandbox: " + e.getMessage(), e);
		} catch (IllegalArgumentException e) {
			throw new ClassNotFoundException("Class '" + name + "' cannot be sandbox-transformed: " + e.getMessage(),
					e);
		}
	}

	/**
	 * Applies the block list to the two-argument form as well, which is the one a
	 * child loader, such as the loader of a project's jars, calls on its parent. A
	 * library that is not shared with the platform is loaded as the project's own
	 * sandboxed copy.
	 */
	@Override
	protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		if (isReflectionApiBlocked(name)) {
			throw new ClassNotFoundException("Access to '" + name + "' is not permitted in project reactors");
		}
		synchronized (getClassLoadingLock(name)) {
			Class<?> loaded = findLoadedClass(name);
			if (loaded == null) {
				byte[] copy = sandboxedLibraryCopy(name, getParent(), this.approvedPackages);
				if (copy == null) {
					return super.loadClass(name, resolve);
				}
				loaded = defineClass(name, copy, 0, copy.length);
			}
			if (resolve) {
				resolveClass(loaded);
			}
			return loaded;
		}
	}

	/**
	 * Overrides the default class loading strategy. Blocks access to reflection
	 * APIs that could be used to bypass SystemEngineRegistry access controls, then
	 * attempts the parent class loader (standard delegation), and falls back to
	 * loading from the custom folder location if not found.
	 *
	 * @param name The fully qualified name of the class.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class is blocked, or could not be found
	 *                                in either the parent loader or the custom
	 *                                path.
	 */
	@Override
	public Class<?> loadClass(String name) throws ClassNotFoundException {
		if (isReflectionApiBlocked(name)) {
			throw new ClassNotFoundException("Access to '" + name + "' is not permitted in project reactors");
		}
		Class retClass = null;
		// see if it is already loaded or in the classpath
		try {
			retClass = super.loadClass(name);
		} catch (ClassNotFoundException e) {
			if (e.getCause() != null) {
				throw e;
			}
		}

		if (retClass == null) {
			if (name != null && name.startsWith("prerna.")) {
				classLogger.warn(
						"Project reactor requested prerna.* class '{}' - not found in application classpath, denying load from project folder",
						name);
				throw new ClassNotFoundException(
						"Classes in 'prerna.*' packages cannot be loaded from project reactor folders: " + name);
			}
			classLogger.info("Project Specific Class " + name);
			retClass = getClass(name);
		}
		return retClass;
	}

	/**
	 * Reads the raw bytecode of a class file from the custom folder location.
	 * 
	 * TODO: Need to incorporate loading jars. Not right now
	 *
	 * @param name The relative path of the .class file to load.
	 * @return A byte array containing the class data.
	 * @throws IOException If there is a problem reading the file.
	 */
	private byte[] loadClassData(String name) throws IOException {
		FileInputStream stream = null;
		DataInputStream in = null;
		byte buff[] = null;
		try {
			stream = new FileInputStream(new File(folder + "/" + name));
			int size = stream.available();
			buff = new byte[size];
			in = new DataInputStream(stream);
			// Reading the binary data
			in.readFully(buff);
		} finally {
			try {
				if (stream != null) {
					stream.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close FileInputStream for class file '{}' in folder '{}'", name, folder,
						e);
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close DataInputStream for class file '{}' in folder '{}'", name, folder,
						e);
			}
		}
		return buff;
	}

	/**
	 * Finds the specified class. This method is overridden from ClassLoader. In
	 * this implementation, it delegates to the parent's findClass method.
	 *
	 * @param name The fully qualified name of the class.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found.
	 */
	@Override
	protected Class findClass(String name) throws ClassNotFoundException {
		return super.findClass(name);
	}

	// =========================================================================
	// Bytecode transformation
	// =========================================================================

	/**
	 * Maps each intercepted internal class name to its constructor descriptor(s),
	 * and for each descriptor the replacement factory method name and descriptor.
	 * The factories are in {@link SandboxedJavaExecution}, or in the shared
	 * library's sandbox class that {@link #REDIRECT_FACTORY_OWNERS} names.
	 *
	 * <p>
	 * Layout:
	 * {@code classInternalName -> constructorDescriptor -> [factoryName, factoryDescriptor]}
	 *
	 * <p>
	 * Every constructor overload of each type must be present so that a suppressed
	 * {@code NEW} always finds a matching factory entry. Constructors that carry no
	 * path information (e.g. {@code FileDescriptor}-based ones) are mapped to
	 * passthrough factories.
	 */
	private static final Map<String, Map<String, String[]>> CONSTRUCTOR_REDIRECTS;

	/**
	 * The class that holds the factory methods for a redirected class, by internal
	 * name: {@link SandboxedClassLoading} for the URL class loader, and a shared
	 * library's sandbox class for the redirects it lists. Every other redirect goes
	 * to {@link SandboxedJavaExecution}.
	 */
	private static final Map<String, String> REDIRECT_FACTORY_OWNERS;

	static {
		Map<String, Map<String, String[]>> m = new HashMap<>();

		// java.io.File - all four public constructors
		Map<String, String[]> file = new HashMap<>();
		file.put("(Ljava/lang/String;)V", new String[] { "file", "(Ljava/lang/String;)Ljava/io/File;" });
		file.put("(Ljava/lang/String;Ljava/lang/String;)V",
				new String[] { "file", "(Ljava/lang/String;Ljava/lang/String;)Ljava/io/File;" });
		file.put("(Ljava/io/File;Ljava/lang/String;)V",
				new String[] { "file", "(Ljava/io/File;Ljava/lang/String;)Ljava/io/File;" });
		file.put("(Ljava/net/URI;)V", new String[] { "file", "(Ljava/net/URI;)Ljava/io/File;" });
		m.put("java/io/File", file);

		// java.io.FileInputStream - all three public constructors
		Map<String, String[]> fis = new HashMap<>();
		fis.put("(Ljava/lang/String;)V",
				new String[] { "fileInputStream", "(Ljava/lang/String;)Ljava/io/FileInputStream;" });
		fis.put("(Ljava/io/File;)V", new String[] { "fileInputStream", "(Ljava/io/File;)Ljava/io/FileInputStream;" });
		fis.put("(Ljava/io/FileDescriptor;)V",
				new String[] { "fileInputStream", "(Ljava/io/FileDescriptor;)Ljava/io/FileInputStream;" });
		m.put("java/io/FileInputStream", fis);

		// java.io.FileOutputStream - all five public constructors
		Map<String, String[]> fos = new HashMap<>();
		fos.put("(Ljava/lang/String;)V",
				new String[] { "fileOutputStream", "(Ljava/lang/String;)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/lang/String;Z)V",
				new String[] { "fileOutputStream", "(Ljava/lang/String;Z)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/io/File;)V", new String[] { "fileOutputStream", "(Ljava/io/File;)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/io/File;Z)V",
				new String[] { "fileOutputStream", "(Ljava/io/File;Z)Ljava/io/FileOutputStream;" });
		fos.put("(Ljava/io/FileDescriptor;)V",
				new String[] { "fileOutputStream", "(Ljava/io/FileDescriptor;)Ljava/io/FileOutputStream;" });
		m.put("java/io/FileOutputStream", fos);

		Map<String, String[]> loader = new HashMap<>();
		for (String arguments : List.of("[Ljava/net/URL;", "[Ljava/net/URL;Ljava/lang/ClassLoader;",
				"[Ljava/net/URL;Ljava/lang/ClassLoader;Ljava/net/URLStreamHandlerFactory;",
				"Ljava/lang/String;[Ljava/net/URL;Ljava/lang/ClassLoader;",
				"Ljava/lang/String;[Ljava/net/URL;Ljava/lang/ClassLoader;Ljava/net/URLStreamHandlerFactory;")) {
			loader.put("(" + arguments + ")V", new String[] { "urlClassLoader",
					"(" + arguments + CLASS_DESCRIPTOR + ")Ljava/net/URLClassLoader;" });
		}
		m.put(URL_CLASS_LOADER, loader);

		Map<String, String> owners = new HashMap<>();
		owners.put(URL_CLASS_LOADER, SANDBOX_CLASS_LOADING);
		for (Map.Entry<String, Map<String, Map<String, String[]>>> library : Map
				.of(SandboxedHikari.INTERNAL_NAME, SandboxedHikari.CONSTRUCTOR_REDIRECTS, SandboxedPdfBox.INTERNAL_NAME,
						SandboxedPdfBox.CONSTRUCTOR_REDIRECTS)
				.entrySet()) {
			for (Map.Entry<String, Map<String, String[]>> redirect : library.getValue().entrySet()) {
				if (m.putIfAbsent(redirect.getKey(), redirect.getValue()) != null) {
					throw new IllegalStateException(redirect.getKey() + " has constructor redirects in two places");
				}
				owners.put(redirect.getKey(), library.getKey());
			}
		}

		CONSTRUCTOR_REDIRECTS = Collections.unmodifiableMap(m);
		REDIRECT_FACTORY_OWNERS = Collections.unmodifiableMap(owners);
	}

	/**
	 * Applies all bytecode transformations to a raw class byte array:
	 * <ul>
	 * <li>Redirects {@code new File(...)}, {@code new FileInputStream(String)}, and
	 * {@code new FileOutputStream(String[,boolean])} through
	 * {@link SandboxedJavaExecution} factory methods that enforce the sandbox
	 * root.</li>
	 * <li>Redirects {@code Paths.get(...)} and {@code Path.of(...)} through
	 * sandboxed equivalents.</li>
	 * <li>Replaces {@code System.getenv()} / {@code System.getenv(String)} with
	 * {@code null}, hiding host environment variables.</li>
	 * <li>Replaces all {@code Runtime.exec(...)} overloads with a call that throws
	 * {@link SecurityException}.</li>
	 * <li>Writes a class compiled for Java 1.4 or older at
	 * {@link #OLDEST_WRITTEN_VERSION}, so the class constants the rewritten calls
	 * pass are valid in it.</li>
	 * </ul>
	 * <p>
	 * Stack-map frames are rebuilt from scratch for a class file of Java 6 or
	 * newer, since the replacements change the frame layout; a type that is not on
	 * the classpath counts as {@code Object} when two types meet. An older class
	 * file has no frames, so only its stack and local sizes are recomputed, which
	 * also keeps the {@code jsr} and {@code ret} instructions older compilers write
	 * for {@code finally} blocks.
	 *
	 * @param original raw bytecode of the class to transform
	 * @return transformed bytecode
	 */
	static byte[] applyBytecodeTransformations(byte[] original) {
		ClassReader cr = new ClassReader(original);
		Map<String, Integer> methodLocals = new HashMap<>();
		cr.accept(new ClassVisitor(Opcodes.ASM9) {
			@Override
			public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
					String[] exceptions) {
				methodLocals.put(name + descriptor, 0);
				return new MethodVisitor(Opcodes.ASM9) {
					@Override
					public void visitMaxs(int maxStack, int maxLocals) {
						methodLocals.put(name + descriptor, maxLocals);
					}
				};
			}
		}, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
		int frames = cr.readUnsignedShort(6) < Opcodes.V1_6 ? ClassWriter.COMPUTE_MAXS : ClassWriter.COMPUTE_FRAMES;
		ClassWriter cw = new ClassWriter(cr, frames) {
			@Override
			protected String getCommonSuperClass(String type1, String type2) {
				try {
					return super.getCommonSuperClass(type1, type2);
				} catch (Exception e) {
					return "java/lang/Object";
				}
			}
		};
		cr.accept(new SandboxClassVisitor(cw, methodLocals, approvedLibraryPackages()), ClassReader.SKIP_FRAMES);
		return cw.toByteArray();
	}

	// -------------------------------------------------------------------------
	// ASM visitor - class level
	// -------------------------------------------------------------------------

	private static final class SandboxClassVisitor extends ClassVisitor {

		private final Map<String, Integer> methodLocals;
		private final Set<String> declaredMethods;
		private final List<String> approvedPackages;
		private final List<Handle> references = new ArrayList<>();
		private String className;
		private String originalSuper;
		private String sandboxSuper;
		private boolean isInterface;

		SandboxClassVisitor(ClassVisitor cv, Map<String, Integer> methodLocals, List<String> approvedPackages) {
			super(Opcodes.ASM9, cv);
			this.methodLocals = methodLocals;
			this.declaredMethods = methodLocals.keySet();
			this.approvedPackages = approvedPackages;
		}

		/**
		 * Moves a class that extends a JDK class loader onto the sandbox class that
		 * stands in for it, and refuses a class that extends a sandbox class or
		 * overrides {@code loadClass}. A class compiled for Java 1.4 or older is
		 * written at {@link #OLDEST_WRITTEN_VERSION}.
		 */
		@Override
		public void visit(int version, int access, String name, String signature, String superName,
				String[] interfaces) {
			refuseSandboxClass(superName);
			if (superName != null && (inheritsBlockedCalls(superName) || FILE.equals(superName)
					|| isSystemDatabaseClass(superName) || superName.startsWith(QUERY_WRAPPER_PACKAGE)
					|| jsonMapperBase(superName) != null)) {
				throw new SecurityException(
						"Extending " + superName.replace('/', '.') + " is not permitted in project reactors");
			}
			if (interfaces != null) {
				for (String implemented : interfaces) {
					refuseSandboxClass(implemented);
				}
			}
			this.className = name;
			this.isInterface = (access & Opcodes.ACC_INTERFACE) != 0;
			this.originalSuper = superName;
			this.sandboxSuper = superName == null ? null : LOADER_BASES.get(superName);
			int writtenVersion = (version & 0xFFFF) < OLDEST_WRITTEN_VERSION ? OLDEST_WRITTEN_VERSION : version;
			if (this.sandboxSuper == null) {
				super.visit(writtenVersion, access, name, signature, superName, interfaces);
				return;
			}
			for (String loadClass : LOAD_CLASS_METHODS) {
				if (this.declaredMethods.contains(loadClass)) {
					throw new SecurityException("A class loader in a project reactor supplies its classes through "
							+ "findClass; overriding loadClass is not permitted");
				}
			}
			super.visit(writtenVersion, access, name, null, this.sandboxSuper, interfaces);
		}

		@Override
		public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
			return typingByDataRefused(descriptor, super.visitAnnotation(descriptor, visible));
		}

		@Override
		public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
			return new FieldVisitor(Opcodes.ASM9, super.visitField(access, name, descriptor, signature, value)) {
				@Override
				public AnnotationVisitor visitAnnotation(String annotation, boolean visible) {
					return typingByDataRefused(annotation, super.visitAnnotation(annotation, visible));
				}
			};
		}

		@Override
		public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
				String[] exceptions) {
			MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
			int argumentSlots = (Type.getArgumentsAndReturnSizes(descriptor) >> 2)
					- ((access & Opcodes.ACC_STATIC) != 0 ? 1 : 0);
			int spareLocals = Math.max(argumentSlots, this.methodLocals.getOrDefault(name + descriptor, 0));
			String returned = Type.getReturnType(descriptor).getDescriptor();
			ValueRule returnRule = INPUT_SOURCE_DESCRIPTOR.equals(returned) ? INPUT_SOURCE_RULE
					: SOURCE_DESCRIPTOR.equals(returned) ? SOURCE_RULE : null;
			return new SandboxMethodVisitor(mv, this, this.className, this.originalSuper, this.sandboxSuper,
					this.declaredMethods, spareLocals, returnRule);
		}

		/**
		 * True when a class is on the platform's side of the sandbox: a JDK class, a
		 * platform class or a shared library class. A file a project hands to one of
		 * them is opened outside the rewrite, so it is put under the path rule first.
		 *
		 * @param owner the internal name of the class a call names
		 * @return true if the class is not project code
		 */
		boolean isPlatformSide(String owner) {
			if (owner.startsWith("[")) {
				return false;
			}
			for (String prefix : JDK_PREFIXES) {
				if (owner.startsWith(prefix)) {
					return true;
				}
			}
			return isSharedWithPlatform(owner.replace('/', '.'), this.approvedPackages);
		}

		/**
		 * True when a method reference points at a call the sandbox would change if
		 * project code made it directly: a rewritten or refused call, a constructor the
		 * sandbox replaces, a call that takes a path as text, or a call into the JDK,
		 * the platform or a shared library that takes a file or a path or returns a
		 * class loader.
		 *
		 * @param handle the method reference's target
		 * @return true if the reference has to run through the rewrite
		 */
		boolean isRewritten(Handle handle) {
			int tag = handle.getTag();
			if (tag != Opcodes.H_INVOKESTATIC && tag != Opcodes.H_INVOKEVIRTUAL && tag != Opcodes.H_INVOKEINTERFACE
					&& tag != Opcodes.H_NEWINVOKESPECIAL) {
				return false;
			}
			String owner = handle.getOwner();
			if (owner.equals(this.className)) {
				return false;
			}
			if (isSandboxedMember(handle)) {
				return true;
			}
			if (!isPlatformSide(owner)) {
				return false;
			}
			String descriptor = handle.getDesc();
			if (CONSTRUCTOR_REDIRECTS.containsKey(owner) || PATH_TEXT_ARGUMENTS.containsKey(owner)
					|| LOADER_BASES.containsKey(owner) || FILE.equals(owner) || XML_FACTORY_LOOKUPS.containsKey(owner)
					|| XML_USES.containsKey(owner) || SYSTEM_ID_ARGUMENTS.containsKey(owner)
					|| XPATH_CLASSES.contains(owner) || jsonMapperBase(owner) != null
					|| jsonCodecUser(owner, handle.getName()) != null
					|| descriptor.endsWith(")" + CLASS_LOADER_DESCRIPTOR)
					|| descriptor.endsWith(")" + SandboxedPdfBox.PD_RECTANGLE_DESCRIPTOR)) {
				return true;
			}
			for (String argument : ARGUMENT_RULES.keySet()) {
				if (descriptor.contains(argument)) {
					return true;
				}
			}
			return false;
		}

		/**
		 * Stands in for a method reference the sandbox has to rewrite: a static method
		 * of this class that makes the same call. The method is written through the
		 * rewrite like any other, so the reference behaves exactly as the call would.
		 *
		 * @param handle the method reference's target
		 * @return a handle to the method of this class that makes the call
		 */
		Handle throughRewrite(Handle handle) {
			String ownerDescriptor = "L" + handle.getOwner() + ";";
			String descriptor = handle.getDesc();
			if (handle.getTag() == Opcodes.H_NEWINVOKESPECIAL) {
				descriptor = descriptor.substring(0, descriptor.lastIndexOf(')') + 1) + ownerDescriptor;
			} else if (handle.getTag() != Opcodes.H_INVOKESTATIC) {
				descriptor = "(" + ownerDescriptor + descriptor.substring(1);
			}
			String method = "sandbox$reference$" + this.references.size();
			this.references.add(handle);
			return new Handle(Opcodes.H_INVOKESTATIC, this.className, method, descriptor, this.isInterface);
		}

		/**
		 * Adds the methods {@link #throughRewrite(Handle)} handed out.
		 */
		@Override
		public void visitEnd() {
			int visibility = this.isInterface ? Opcodes.ACC_PUBLIC : Opcodes.ACC_PRIVATE;
			for (int i = 0; i < this.references.size(); i++) {
				Handle handle = this.references.get(i);
				boolean constructs = handle.getTag() == Opcodes.H_NEWINVOKESPECIAL;
				boolean isStatic = handle.getTag() == Opcodes.H_INVOKESTATIC;
				String ownerDescriptor = "L" + handle.getOwner() + ";";
				String descriptor = handle.getDesc();
				if (constructs) {
					descriptor = descriptor.substring(0, descriptor.lastIndexOf(')') + 1) + ownerDescriptor;
				} else if (!isStatic) {
					descriptor = "(" + ownerDescriptor + descriptor.substring(1);
				}
				MethodVisitor mv = visitMethod(visibility | Opcodes.ACC_STATIC | Opcodes.ACC_SYNTHETIC,
						"sandbox$reference$" + i, descriptor, null, null);
				mv.visitCode();
				if (constructs) {
					mv.visitTypeInsn(Opcodes.NEW, handle.getOwner());
					mv.visitInsn(Opcodes.DUP);
				}
				int local = 0;
				for (Type argument : Type.getArgumentTypes(descriptor)) {
					mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), local);
					local += argument.getSize();
				}
				if (constructs) {
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, handle.getOwner(), "<init>", handle.getDesc(), false);
				} else {
					int opcode = isStatic ? Opcodes.INVOKESTATIC
							: handle.getTag() == Opcodes.H_INVOKEINTERFACE ? Opcodes.INVOKEINTERFACE
									: Opcodes.INVOKEVIRTUAL;
					mv.visitMethodInsn(opcode, handle.getOwner(), handle.getName(), handle.getDesc(),
							handle.isInterface());
				}
				mv.visitInsn(Type.getReturnType(descriptor).getOpcode(Opcodes.IRETURN));
				mv.visitMaxs(0, 0);
				mv.visitEnd();
			}
			super.visitEnd();
		}
	}

	/**
	 * Refuses a Jackson {@code @JsonTypeInfo} that names classes in the JSON, which
	 * would have a mapper create objects of whatever class the data asks for.
	 *
	 * @param descriptor the annotation's descriptor
	 * @param next       the visitor the annotation goes on to
	 * @return a visitor that throws on such a setting, or {@code next} for any
	 *         other annotation
	 */
	private static AnnotationVisitor typingByDataRefused(String descriptor, AnnotationVisitor next) {
		if (!SandboxedJackson.JSON_TYPE_INFO.equals(descriptor)) {
			return next;
		}
		return new AnnotationVisitor(Opcodes.ASM9, next) {
			@Override
			public void visitEnum(String name, String enumDescriptor, String value) {
				if ("use".equals(name) && SandboxedJackson.JSON_TYPE_BY_CLASS_NAME.contains(value)) {
					throw new SecurityException(
							"@JsonTypeInfo that names classes in the JSON is not permitted in project reactors");
				}
				super.visitEnum(name, enumDescriptor, value);
			}
		};
	}

	// -------------------------------------------------------------------------
	// ASM visitor - method level
	// -------------------------------------------------------------------------

	/**
	 * Rewrites individual bytecode instructions inside each method body.
	 *
	 * <h3>Constructor redirect strategy</h3>
	 * <p>
	 * Java compilers always emit
	 * {@code NEW T; DUP; [push args]; INVOKESPECIAL T.<init>}. We suppress the
	 * {@code NEW} and its companion {@code DUP}, then at {@code INVOKESPECIAL} time
	 * we emit {@code INVOKESTATIC SandboxedJavaExecution.factory(args)} instead. The
	 * factory returns an already-initialized instance, leaving the stack in exactly
	 * the same state as the original sequence would have.
	 *
	 * <p>
	 * Nesting (e.g. {@code new FileInputStream(new File("x"))}) is handled
	 * correctly by maintaining a LIFO stack of suppressed {@code NEW} types: the
	 * innermost {@code INVOKESPECIAL} always matches the innermost suppressed
	 * {@code NEW}.
	 */
	private static final class SandboxMethodVisitor extends MethodVisitor {

		/**
		 * LIFO stack of internal class names for {@code NEW} instructions we have
		 * suppressed. Each entry is waiting for its matching
		 * {@code INVOKESPECIAL <init>}.
		 */
		private final Deque<String> pendingNews = new ArrayDeque<>();

		/**
		 * Set to {@code true} immediately after suppressing a {@code NEW} so that the
		 * very next {@code DUP} instruction (which always immediately follows
		 * {@code NEW} in compiler-generated bytecode) is also suppressed.
		 */
		private boolean suppressNextDup = false;

		private final SandboxClassVisitor owner;
		private final String className;
		private final String originalSuper;
		private final String sandboxSuper;
		private final Set<String> declaredMethods;

		/**
		 * The first local variable slot the method does not use, free for holding
		 * arguments while one under them is put under the path rule.
		 */
		private final int spareLocals;

		/**
		 * The rule for what the method returns, when it returns an XML source that a
		 * parser may have asked for, such as a resolver does.
		 */
		private final ValueRule returnRule;

		SandboxMethodVisitor(MethodVisitor mv, SandboxClassVisitor owner, String className, String originalSuper,
				String sandboxSuper, Set<String> declaredMethods, int spareLocals, ValueRule returnRule) {
			super(Opcodes.ASM9, mv);
			this.owner = owner;
			this.spareLocals = spareLocals;
			this.returnRule = returnRule;
			this.className = className;
			this.originalSuper = originalSuper;
			this.sandboxSuper = sandboxSuper;
			this.declaredMethods = declaredMethods;
		}

		@Override
		public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
			return typingByDataRefused(descriptor, super.visitAnnotation(descriptor, visible));
		}

		@Override
		public AnnotationVisitor visitParameterAnnotation(int parameter, String descriptor, boolean visible) {
			return typingByDataRefused(descriptor, super.visitParameterAnnotation(parameter, descriptor, visible));
		}

		@Override
		public void visitTypeInsn(int opcode, String type) {
			refuseSandboxClass(type);
			if (opcode == Opcodes.NEW && isSecurityUtility(type)) {
				throw new SecurityException(
						"Creating " + type.replace('/', '.') + " is not permitted in project reactors");
			}
			if (opcode == Opcodes.NEW && CONSTRUCTOR_REDIRECTS.containsKey(type)) {
				pendingNews.push(type);
				suppressNextDup = true;
				// Do NOT emit the NEW - factory method replaces the whole sequence.
			} else {
				suppressNextDup = false;
				super.visitTypeInsn(opcode, type);
				if (opcode == Opcodes.CHECKCAST) {
					if (CLASS_LOADER.equals(type)) {
						keepLoaderInSandbox();
					} else if (LOADER_BASES.containsKey(type)) {
						super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_CLASS_LOADING, "requireSandboxLoader",
								"(" + CLASS_LOADER_DESCRIPTOR + ")" + CLASS_LOADER_DESCRIPTOR, false);
						super.visitTypeInsn(Opcodes.CHECKCAST, type);
					}
				}
			}
		}

		/**
		 * Keeps a class loader read from a field in the sandbox, hands out a copy of a
		 * page size PDFBox shares, refuses a field of a sandbox class, and replaces an
		 * assignment to a static field of a JDK, platform or shared library class,
		 * which every user of the JVM would see, with a call that throws.
		 */
		@Override
		public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
			suppressNextDup = false;
			refuseSandboxClass(owner);
			if (opcode == Opcodes.PUTSTATIC && this.owner.isPlatformSide(owner)) {
				super.visitInsn(Type.getType(descriptor).getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				super.visitLdcInsn(owner.substring(owner.lastIndexOf('/') + 1) + "." + name);
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockCall", "(Ljava/lang/String;)V", false);
				return;
			}
			super.visitFieldInsn(opcode, owner, name, descriptor);
			if ((opcode == Opcodes.GETFIELD || opcode == Opcodes.GETSTATIC)
					&& CLASS_LOADER_DESCRIPTOR.equals(descriptor)) {
				keepLoaderInSandbox();
			} else if (opcode == Opcodes.GETSTATIC && SandboxedPdfBox.PD_RECTANGLE_DESCRIPTOR.equals(descriptor)) {
				unsharePageSize();
			}
		}

		/**
		 * With a class loader on top of the stack, swaps it for the sandbox loader of
		 * this class unless it is a sandbox loader already.
		 */
		private void keepLoaderInSandbox() {
			super.visitLdcInsn(Type.getObjectType(this.className));
			super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_CLASS_LOADING, "sandboxLoader",
					"(" + CLASS_LOADER_DESCRIPTOR + CLASS_DESCRIPTOR + ")" + CLASS_LOADER_DESCRIPTOR, false);
		}

		/**
		 * With a PDFBox rectangle on top of the stack, swaps it for a copy when it is a
		 * page size every document shares.
		 */
		private void unsharePageSize() {
			super.visitMethodInsn(Opcodes.INVOKESTATIC, SandboxedPdfBox.INTERNAL_NAME, "unsharedPageSize",
					"(" + SandboxedPdfBox.PD_RECTANGLE_DESCRIPTOR + ")" + SandboxedPdfBox.PD_RECTANGLE_DESCRIPTOR,
					false);
		}

		/**
		 * Writes a call as it is, then keeps the class loader it returns, if it returns
		 * one, in the sandbox, and swaps a page size PDFBox shares that it returns for
		 * a copy.
		 */
		private void callKeepingResultInSandbox(int opcode, String owner, String name, String descriptor, boolean itf) {
			super.visitMethodInsn(opcode, owner, name, descriptor, itf);
			if (descriptor.endsWith(")" + CLASS_LOADER_DESCRIPTOR)) {
				keepLoaderInSandbox();
			} else if (descriptor.endsWith(")" + SandboxedPdfBox.PD_RECTANGLE_DESCRIPTOR)) {
				unsharePageSize();
			}
		}

		/**
		 * The sandbox class a loader call moves to when it names a JDK loader class, or
		 * the same owner when it names a project class.
		 */
		private String loaderOwner(String owner) {
			return LOADER_BASES.getOrDefault(owner, owner);
		}

		/**
		 * True when this class declares a method itself, which makes a call to it a
		 * call to the project's own code.
		 */
		private boolean declaresItself(String owner, String name, String descriptor) {
			return owner.equals(this.className) && this.declaredMethods.contains(name + descriptor);
		}

		@Override
		public void visitInsn(int opcode) {
			// Suppress the DUP that always immediately follows a suppressed NEW.
			if (opcode == Opcodes.DUP && suppressNextDup) {
				suppressNextDup = false;
				return;
			}
			suppressNextDup = false;
			if (opcode == Opcodes.ARETURN && this.returnRule != null) {
				apply(this.returnRule);
			}
			super.visitInsn(opcode);
		}

		@Override
		public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean itf) {
			suppressNextDup = false;
			refuseSandboxClass(owner);

			// ------------------------------------------------------------------
			// Constructor redirect: replace NEW+DUP+INVOKESPECIAL with a static
			// factory call whose return type is the constructed object.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESPECIAL && "<init>".equals(name) && !pendingNews.isEmpty()
					&& owner.equals(pendingNews.peek())) {
				pendingNews.pop();
				Map<String, String[]> ctorMap = CONSTRUCTOR_REDIRECTS.get(owner);
				String[] factory = ctorMap != null ? ctorMap.get(descriptor) : null;
				if (factory != null) {
					if (URL_CLASS_LOADER.equals(owner)) {
						super.visitLdcInsn(Type.getObjectType(this.className));
					}
					// factory[0] = method name, factory[1] = method descriptor
					super.visitMethodInsn(Opcodes.INVOKESTATIC, REDIRECT_FACTORY_OWNERS.getOrDefault(owner, SANDBOX),
							factory[0], factory[1], false);
				} else {
					// Unknown overload for a sandboxed type - pop args and block.
					for (Type argType : Type.getArgumentTypes(descriptor)) {
						super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
					}
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockUnknownConstructor", "()V", false);
					super.visitInsn(Opcodes.ACONST_NULL); // unreachable; satisfies verifier
				}
				return;
			}

			String xmlFactory = opcode == Opcodes.INVOKESTATIC ? XML_FACTORY_LOOKUPS.get(owner) : null;
			if (xmlFactory != null && XML_FACTORY_LOOKUP_METHODS.contains(name)) {
				Type[] arguments = Type.getArgumentTypes(descriptor);
				boolean keepsLanguage = "schemaFactory".equals(xmlFactory) && arguments.length > 0;
				for (int i = arguments.length - 1; i >= (keepsLanguage ? 1 : 0); i--) {
					super.visitInsn(arguments[i].getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_XML, xmlFactory + (name.contains("NS") ? "NS" : ""),
						(keepsLanguage ? "(" + STRING_DESCRIPTOR + ")" : "()")
								+ Type.getReturnType(descriptor).getDescriptor(),
						false);
				return;
			}
			if (opcode == Opcodes.INVOKEINTERFACE && XPATH_CLASSES.contains(owner)
					&& descriptor.contains(INPUT_SOURCE_DESCRIPTOR)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_XML, name,
						"(L" + owner + ";" + descriptor.substring(1), false);
				return;
			}
			if (opcode == Opcodes.INVOKEVIRTUAL && "javax/xml/parsers/SAXParser".equals(owner)
					&& "getXMLReader".equals(name)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_XML, "xmlReader",
						"(Ljavax/xml/parsers/SAXParser;)Lorg/xml/sax/XMLReader;", false);
				return;
			}

			keepPathsInSandbox(opcode, owner, name, descriptor);

			if (opcode == Opcodes.INVOKESPECIAL && "<init>".equals(name) && this.sandboxSuper != null
					&& owner.equals(this.originalSuper)) {
				super.visitLdcInsn(Type.getObjectType(this.className));
				super.visitMethodInsn(Opcodes.INVOKESPECIAL, this.sandboxSuper, name,
						descriptor.replace(")V", CLASS_DESCRIPTOR + ")V"), false);
				return;
			}
			if ("defineClass".equals(name) && DEFINE_CLASS_DESCRIPTORS.contains(descriptor)
					&& opcode != Opcodes.INVOKESTATIC && !declaresItself(owner, name, descriptor)) {
				super.visitMethodInsn(opcode, loaderOwner(owner), "defineSandboxedClass", descriptor, false);
				return;
			}
			if ("findSystemClass".equals(name) && "(Ljava/lang/String;)Ljava/lang/Class;".equals(descriptor)
					&& opcode != Opcodes.INVOKESTATIC && !declaresItself(owner, name, descriptor)) {
				super.visitMethodInsn(opcode, loaderOwner(owner), "findSandboxedSystemClass", descriptor, false);
				return;
			}
			if (opcode == Opcodes.INVOKESPECIAL && this.sandboxSuper != null && owner.equals(this.originalSuper)) {
				callKeepingResultInSandbox(opcode, this.sandboxSuper, name, descriptor, itf);
				return;
			}
			if (opcode == Opcodes.INVOKESTATIC && URL_CLASS_LOADER.equals(owner) && "newInstance".equals(name)) {
				super.visitLdcInsn(Type.getObjectType(this.className));
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_CLASS_LOADING, "urlClassLoader",
						descriptor.replace(")", CLASS_DESCRIPTOR + ")"), false);
				return;
			}
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/lang/Class".equals(owner) && "newInstance".equals(name)
					&& "()Ljava/lang/Object;".equals(descriptor)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_CLASS_LOADING, "sandboxedNewInstance",
						"(" + CLASS_DESCRIPTOR + ")Ljava/lang/Object;", false);
				return;
			}
			if (opcode == Opcodes.INVOKEVIRTUAL && LOADER_BASES.containsKey(owner) && "loadClass".equals(name)
					&& "(Ljava/lang/String;)Ljava/lang/Class;".equals(descriptor)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_CLASS_LOADING, "sandboxedLoadClass",
						"(" + CLASS_LOADER_DESCRIPTOR + "Ljava/lang/String;)Ljava/lang/Class;", false);
				return;
			}

			if (isBlockedCall(owner, name) || isQueryUtilConnectionMember(owner, name)
					|| (opcode == Opcodes.INVOKESTATIC && isRefusedSystemDatabaseCall(owner, name))
					|| (SandboxedJackson.BLOCKED_CALLS_ON_ANY_CLASS.contains(name)
							&& !declaresItself(owner, name, descriptor))) {
				blockCall(opcode, owner, name, descriptor);
				return;
			}

			// ------------------------------------------------------------------
			// Block System.exit / Runtime.halt - would kill the entire JVM.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "exit".equals(name)) {
				super.visitInsn(Opcodes.POP); // pop the int status arg
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockExit", "()V", false);
				return;
			}
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/lang/Runtime".equals(owner) && "halt".equals(name)) {
				super.visitInsn(Opcodes.POP); // pop the int status arg
				super.visitInsn(Opcodes.POP); // pop the Runtime instance
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockExit", "()V", false);
				return;
			}

			// ------------------------------------------------------------------
			// Sandbox Class.forName - the single-arg overload is safe because it
			// uses the calling class's classloader (SemossClassLoader), which
			// already enforces isReflectionApiBlocked. We still route it through
			// sandboxedForName so the block-list is checked before the load.
			//
			// The three-arg overload (String, boolean, ClassLoader) is dangerous:
			// the caller could pass ClassLoader.getSystemClassLoader() to bypass
			// SemossClassLoader entirely. We strip the extra args and fall back to
			// the sandboxed single-arg version.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/Class".equals(owner) && "forName".equals(name)) {
				if ("(Ljava/lang/String;ZLjava/lang/ClassLoader;)Ljava/lang/Class;".equals(descriptor)) {
					// Stack: [..., className(String), initialize(boolean), loader(ClassLoader)]
					super.visitInsn(Opcodes.POP); // pop the ClassLoader
					super.visitInsn(Opcodes.POP); // pop the boolean (initialize)
					// className is now on top - fall through to sandboxedForName
				}
				// Stack: [..., className(String)]
				super.visitLdcInsn(Type.getObjectType(this.className));
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX_CLASS_LOADING, "sandboxedForName",
						"(Ljava/lang/String;" + CLASS_DESCRIPTOR + ")Ljava/lang/Class;", false);
				return;
			}

			// ------------------------------------------------------------------
			// Block System.getenv - return null instead of exposing env vars.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "getenv".equals(name)) {
				if ("(Ljava/lang/String;)Ljava/lang/String;".equals(descriptor)) {
					super.visitInsn(Opcodes.POP); // discard the String key argument
				}
				// Both overloads return a reference - push null.
				super.visitInsn(Opcodes.ACONST_NULL);
				return;
			}

			// ------------------------------------------------------------------
			// Sandbox System.getProperty - route through SandboxedJavaExecution which
			// allows the default safe set + any platform-registered properties,
			// and blocks everything else.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "getProperty".equals(name)) {
				if ("(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".equals(descriptor)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetProperty",
							"(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;", false);
				} else {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetProperty",
							"(Ljava/lang/String;)Ljava/lang/String;", false);
				}
				return;
			}
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner) && "getProperties".equals(name)) {
				super.visitInsn(Opcodes.ACONST_NULL);
				return;
			}

			// ------------------------------------------------------------------
			// Block System.setProperty / clearProperty / setProperties -
			// would affect JVM-wide state for all users.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/System".equals(owner)
					&& ("setProperty".equals(name) || "clearProperty".equals(name) || "setProperties".equals(name))) {
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockSystemMutation", "()V", false);
				// setProperty returns String, clearProperty returns String, setProperties is
				// void
				if (!descriptor.endsWith(")V")) {
					super.visitInsn(Opcodes.ACONST_NULL);
				}
				return;
			}

			// ------------------------------------------------------------------
			// Block Thread.getAllStackTraces - reveals stack traces of all
			// threads across all users (information disclosure).
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/lang/Thread".equals(owner)
					&& "getAllStackTraces".equals(name)) {
				super.visitInsn(Opcodes.ACONST_NULL);
				return;
			}

			// ------------------------------------------------------------------
			// Block Files.createSymbolicLink - reactor code could create a
			// symlink inside the chroot pointing to a path outside it, then
			// read through it via the admin-symlink path in sanitize().
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/file/Files".equals(owner)
					&& "createSymbolicLink".equals(name)) {
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockSymlinkCreation", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake Path return (unreachable)
				return;
			}

			// ------------------------------------------------------------------
			// Block FileChannel.open - NIO channel bypass for file access.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/channels/FileChannel".equals(owner)
					&& "open".equals(name)) {
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockFileChannel", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake FileChannel return (unreachable)
				return;
			}

			// ------------------------------------------------------------------
			// Block URL.openStream / URL.openConnection - HTTP calls to
			// arbitrary external endpoints (data exfiltration).
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/net/URL".equals(owner)
					&& ("openStream".equals(name) || "openConnection".equals(name))) {
				super.visitInsn(Opcodes.POP); // pop the URL instance
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockNetworkAccess", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake return (unreachable)
				return;
			}

			// ------------------------------------------------------------------
			// Redirect Paths.get(String, String...) through the sandbox.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/file/Paths".equals(owner) && "get".equals(name)
					&& "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;".equals(descriptor)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "pathsGet",
						"(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;", false);
				return;
			}

			// ------------------------------------------------------------------
			// Redirect Path.of(String, String...) through the sandbox.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKESTATIC && "java/nio/file/Path".equals(owner) && "of".equals(name)
					&& "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;".equals(descriptor)) {
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "pathOf",
						"(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;", false);
				return;
			}

			// ------------------------------------------------------------------
			// Sandbox File.getParentFile / File.getParent - return null if the
			// parent would escape the chroot, matching natural JVM behaviour at
			// the filesystem root.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/io/File".equals(owner)) {
				if ("getParentFile".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetParentFile",
							"(Ljava/io/File;)Ljava/io/File;", false);
					return;
				}
				if ("getParent".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetParent",
							"(Ljava/io/File;)Ljava/lang/String;", false);
					return;
				}
				if ("getAbsolutePath".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetAbsolutePath",
							"(Ljava/io/File;)Ljava/lang/String;", false);
					return;
				}
				if ("getCanonicalPath".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedGetCanonicalPath",
							"(Ljava/io/File;)Ljava/lang/String;", false);
					return;
				}
				if ("toPath".equals(name)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedFileToPath",
							"(Ljava/io/File;)Ljava/nio/file/Path;", false);
					return;
				}
			}

			// ------------------------------------------------------------------
			// Block Runtime.exec - pop all args + instance, then throw.
			// ------------------------------------------------------------------
			if (opcode == Opcodes.INVOKEVIRTUAL && "java/lang/Runtime".equals(owner) && "exec".equals(name)) {
				// Pop constructor args right-to-left (top of stack first).
				for (Type argType : Type.getArgumentTypes(descriptor)) {
					super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
				}
				super.visitInsn(Opcodes.POP); // pop the Runtime instance
				super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockRuntimeExec", "()V", false);
				super.visitInsn(Opcodes.ACONST_NULL); // fake Process return (unreachable)
				return;
			}

			if (opcode == Opcodes.INVOKESTATIC && "prerna/util/Utility".equals(owner)) {
				if ("getDIHelperProperty".equals(name) && "(Ljava/lang/String;)Ljava/lang/String;".equals(descriptor)) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedPlatformProperty",
							"(Ljava/lang/String;)Ljava/lang/String;", false);
					return;
				}
				if ("loadProperties".equals(name) && ("(Ljava/lang/String;)Ljava/util/Properties;".equals(descriptor)
						|| "(Ljava/io/File;)Ljava/util/Properties;".equals(descriptor))) {
					super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "sandboxedLoadProperties", descriptor, false);
					return;
				}
			}

			callKeepingResultInSandbox(opcode, owner, name, descriptor, itf);
		}

		/**
		 * With a call's arguments on the stack, puts the files and paths among them
		 * under the path rule before the call is written:
		 * <ul>
		 * <li>a {@code File} or {@code Path} argument of a call into the JDK, the
		 * platform or a shared library, which opens it outside the rewrite;</li>
		 * <li>the text argument of a call listed in
		 * {@link SemossClassLoader#PATH_TEXT_ARGUMENTS};</li>
		 * <li>the file itself, for a {@code File} method that reads or changes the file
		 * system.</li>
		 * </ul>
		 * The arguments above the first one that needs it are moved into spare local
		 * variables and put back in order.
		 *
		 * @param opcode     how the call is made
		 * @param owner      the internal name of the class the call names
		 * @param name       the method
		 * @param descriptor the method's descriptor
		 */
		private void keepPathsInSandbox(int opcode, String owner, String name, String descriptor) {
			Type[] arguments = Type.getArgumentTypes(descriptor);
			boolean onFile = FILE.equals(owner) && FILE_IO_METHODS.contains(name);
			boolean opensArguments = this.owner.isPlatformSide(owner)
					&& (onFile || !PATH_ARITHMETIC_CLASSES.contains(owner));
			Map<String, Integer> pathText = PATH_TEXT_ARGUMENTS.get(owner);
			int textArgument = pathText == null ? -1 : pathText.getOrDefault(name, -1);
			Map<String, Integer> systemIds = SYSTEM_ID_ARGUMENTS.get(owner);
			int systemIdArgument = systemIds == null ? -1 : systemIds.getOrDefault(name + descriptor, -1);

			ValueRule[] rules = new ValueRule[arguments.length];
			int first = arguments.length;
			for (int i = arguments.length - 1; i >= 0; i--) {
				String type = arguments[i].getDescriptor();
				if (i == textArgument && STRING_DESCRIPTOR.equals(type)) {
					rules[i] = PATH_TEXT_RULE;
				} else if (i == systemIdArgument && STRING_DESCRIPTOR.equals(type)) {
					rules[i] = SYSTEM_ID_RULE;
				} else if (i == systemIdArgument && URL_DESCRIPTOR.equals(type)) {
					rules[i] = SYSTEM_URL_RULE;
				} else if (i == systemIdArgument && URI_DESCRIPTOR.equals(type)) {
					rules[i] = FILE_SYSTEM_URI_RULE;
				} else if (opensArguments) {
					rules[i] = ARGUMENT_RULES.get(type);
				}
				if (rules[i] != null) {
					first = i;
				}
			}
			boolean onObject = opcode == Opcodes.INVOKEVIRTUAL || opcode == Opcodes.INVOKEINTERFACE;
			boolean xmlUse = onObject && XML_USES.getOrDefault(owner, Set.of()).contains(name);
			String mapperBase = onObject ? jsonMapperBase(owner) : null;
			String mapperGuard = mapperBase == null ? null : JSON_MAPPER_GUARDS.get(mapperBase);
			if (onObject && mapperBase == null) {
				mapperBase = jsonCodecUser(owner, name);
				mapperGuard = mapperBase == null ? null : SandboxedJackson.INTERNAL_NAME;
			}
			boolean onReceiver = xmlUse || mapperBase != null || (onFile && opcode == Opcodes.INVOKEVIRTUAL);
			if (first == arguments.length && !onReceiver) {
				return;
			}

			int moved = onReceiver ? 0 : first + 1;
			int[] slots = new int[arguments.length];
			int slot = this.spareLocals;
			for (int i = moved; i < arguments.length; i++) {
				slots[i] = slot;
				slot += arguments[i].getSize();
			}
			for (int i = arguments.length - 1; i >= moved; i--) {
				super.visitVarInsn(arguments[i].getOpcode(Opcodes.ISTORE), slots[i]);
			}
			if (xmlUse) {
				String type = XML_USE_TYPES.getOrDefault(owner, owner);
				apply(new ValueRule(SANDBOX_XML, "hardened", "L" + type + ";"));
				if (!type.equals(owner)) {
					super.visitTypeInsn(Opcodes.CHECKCAST, owner);
				}
			} else if (mapperBase != null) {
				apply(new ValueRule(mapperGuard, "guarded", "L" + mapperBase + ";"));
				if (!mapperBase.equals(owner)) {
					super.visitTypeInsn(Opcodes.CHECKCAST, owner);
				}
			} else {
				apply(onReceiver ? FILE_RULE : rules[first]);
			}
			for (int i = moved; i < arguments.length; i++) {
				super.visitVarInsn(arguments[i].getOpcode(Opcodes.ILOAD), slots[i]);
				if (rules[i] != null) {
					apply(rules[i]);
				}
			}
		}

		/**
		 * Swaps the value on top of the stack for the one a sandbox rule gives back.
		 *
		 * @param rule the rule for the value
		 */
		private void apply(ValueRule rule) {
			super.visitMethodInsn(Opcodes.INVOKESTATIC, rule.helper(), rule.method(),
					"(" + rule.type() + ")" + rule.type(), false);
		}

		/**
		 * Replaces a call with one that throws, taking the call's arguments off the
		 * stack and leaving what the call would have returned.
		 *
		 * @param opcode     how the call was made
		 * @param owner      the internal name of the class the call names
		 * @param name       the method
		 * @param descriptor the method's descriptor
		 */
		private void blockCall(int opcode, String owner, String name, String descriptor) {
			for (Type argType : Type.getArgumentTypes(descriptor)) {
				super.visitInsn(argType.getSize() == 2 ? Opcodes.POP2 : Opcodes.POP);
			}
			if (opcode != Opcodes.INVOKESTATIC) {
				super.visitInsn(Opcodes.POP);
			}
			super.visitLdcInsn(owner.substring(owner.lastIndexOf('/') + 1) + "." + name);
			super.visitMethodInsn(Opcodes.INVOKESTATIC, SANDBOX, "blockCall", "(Ljava/lang/String;)V", false);
			pushDefault(Type.getReturnType(descriptor));
		}

		/**
		 * Makes a method reference behave like the call it stands for. A reference to a
		 * call the sandbox would change, such as {@code File::delete},
		 * {@code Files::readString} or {@code System::loadLibrary}, is pointed at a
		 * method of this class that makes the call, and that method is rewritten like
		 * any other. Lambdas whose body is in the class itself pass, since the body is
		 * rewritten already.
		 */
		@Override
		public void visitInvokeDynamicInsn(String name, String descriptor, Handle bootstrapMethodHandle,
				Object... bootstrapMethodArguments) {
			refuseSandboxedConstant(bootstrapMethodHandle);
			Object[] arguments = bootstrapMethodArguments.clone();
			for (int i = 0; i < arguments.length; i++) {
				if (arguments[i] instanceof Handle handle) {
					refuseSandboxClass(handle.getOwner());
					if (this.owner.isRewritten(handle)) {
						arguments[i] = this.owner.throughRewrite(handle);
					}
					continue;
				}
				refuseSandboxedConstant(arguments[i]);
				refuseRewrittenConstant(arguments[i]);
			}
			super.visitInvokeDynamicInsn(name, descriptor, bootstrapMethodHandle, arguments);
		}

		/**
		 * Refuses a method handle constant that points at a call the sandbox would
		 * change if project code made it directly, since a handle runs the call as it
		 * is.
		 */
		@Override
		public void visitLdcInsn(Object value) {
			refuseSandboxedConstant(value);
			refuseRewrittenConstant(value);
			super.visitLdcInsn(value);
		}

		/**
		 * Throws when a constant is a method handle to a call the sandbox rewrites, or
		 * a dynamic constant built from one.
		 *
		 * @param constant a loaded constant or the argument of one
		 * @throws SecurityException when the constant points at a rewritten call
		 */
		private void refuseRewrittenConstant(Object constant) {
			if (constant instanceof Handle handle) {
				if (this.owner.isRewritten(handle)) {
					throw new SecurityException("A method handle to " + handle.getOwner().replace('/', '.') + "."
							+ handle.getName() + " is not permitted in project reactors; call it directly instead");
				}
			} else if (constant instanceof ConstantDynamic dynamic) {
				for (int i = 0; i < dynamic.getBootstrapMethodArgumentCount(); i++) {
					refuseRewrittenConstant(dynamic.getBootstrapMethodArgument(i));
				}
			}
		}

		/**
		 * Pushes the zero value of a return type, after a call that always throws, so
		 * the stack still matches what the original call left.
		 *
		 * @param returnType the replaced method's return type
		 */
		private void pushDefault(Type returnType) {
			switch (returnType.getSort()) {
			case Type.VOID:
				break;
			case Type.LONG:
				super.visitInsn(Opcodes.LCONST_0);
				break;
			case Type.FLOAT:
				super.visitInsn(Opcodes.FCONST_0);
				break;
			case Type.DOUBLE:
				super.visitInsn(Opcodes.DCONST_0);
				break;
			case Type.OBJECT:
			case Type.ARRAY:
				super.visitInsn(Opcodes.ACONST_NULL);
				break;
			default:
				super.visitInsn(Opcodes.ICONST_0);
				break;
			}
		}
	}

	/**
	 * True when a method handle points at a method the sandbox rewrites or refuses.
	 *
	 * @param handle a method handle constant
	 * @return true if a direct call to the method would be rewritten or refused
	 */
	private static boolean isSandboxedMember(Handle handle) {
		Set<String> members = SANDBOXED_MEMBERS.get(handle.getOwner());
		return (members != null && members.contains(handle.getName()))
				|| isBlockedCall(handle.getOwner(), handle.getName())
				|| (handle.getTag() == Opcodes.H_INVOKESTATIC
						&& isRefusedSystemDatabaseCall(handle.getOwner(), handle.getName()))
				|| isQueryUtilConnectionMember(handle.getOwner(), handle.getName())
				|| SandboxedJackson.BLOCKED_CALLS_ON_ANY_CLASS.contains(handle.getName());
	}

	/**
	 * True for a call that reads an RDBMS engine's credentials from its query
	 * utility, or changes where the engine connects.
	 *
	 * @param owner the internal name of the class the call names
	 * @param name  the method
	 * @return true if the call is one project code cannot make
	 */
	private static boolean isQueryUtilConnectionMember(String owner, String name) {
		return owner.startsWith(QUERY_UTIL_PACKAGE) && QUERY_UTIL_CONNECTION_MEMBERS.contains(name);
	}

	/**
	 * Throws when a constant is a method handle to a sandboxed method, or a dynamic
	 * constant built from one.
	 *
	 * @param constant a bootstrap argument or loaded constant
	 * @throws SecurityException when the constant points at a sandboxed method
	 */
	private static void refuseSandboxedConstant(Object constant) {
		if (constant instanceof Handle handle) {
			refuseSandboxClass(handle.getOwner());
			if (isSandboxedMember(handle)) {
				throw new SecurityException("A method reference to " + handle.getOwner().replace('/', '.') + "."
						+ handle.getName() + " is not permitted in project reactors; call it directly instead");
			}
		} else if (constant instanceof ConstantDynamic dynamic) {
			refuseSandboxedConstant(dynamic.getBootstrapMethod());
			for (int i = 0; i < dynamic.getBootstrapMethodArgumentCount(); i++) {
				refuseSandboxedConstant(dynamic.getBootstrapMethodArgument(i));
			}
		}
	}

	/**
	 * Throws when a project class names one of the classes that make up the
	 * sandbox.
	 *
	 * @param internalName the internal name of a class a project class uses, or
	 *                     null
	 * @throws SecurityException when the class is a sandbox class
	 */
	private static void refuseSandboxClass(String internalName) {
		if (internalName == null) {
			return;
		}
		int nested = internalName.indexOf('$');
		String outer = nested < 0 ? internalName : internalName.substring(0, nested);
		if (SANDBOX_CLASSES.contains(outer)) {
			throw new SecurityException(outer.replace('/', '.') + " is not permitted in project reactors");
		}
	}

}
