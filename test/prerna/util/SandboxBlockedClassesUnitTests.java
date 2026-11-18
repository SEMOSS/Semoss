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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * One test per group of classes a project's custom reactors can never use. The
 * sandbox page in the docs lists each group with the test here that guards it.
 */
class SandboxBlockedClassesUnitTests {

	private static final Pattern ASKS_FOR_A_SYSTEM_DATABASE = Pattern
			.compile("SystemEngineRegistry\\.get\\w*Db\\(\\)");

	@TempDir
	Path folder;

	@ParameterizedTest
	@ValueSource(strings = { "java.lang.reflect.Method", "java.lang.reflect.Field", "java.lang.reflect.Constructor",
			"java.lang.reflect.Proxy", "java.lang.invoke.MethodHandles", "java.lang.invoke.MethodHandles$Lookup",
			"sun.misc.Unsafe", "jdk.internal.misc.Unsafe" })
	void reflectionIsBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.beans.Statement", "java.beans.Expression", "java.beans.EventHandler",
			"java.beans.XMLDecoder", "java.beans.XMLEncoder", "java.beans.Beans", "java.beans.Introspector",
			"java.beans.PropertyDescriptor" })
	void classesThatCallMethodsByNameAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "javax.script.ScriptEngineManager", "jdk.nashorn.api.scripting.NashornScriptEngine",
			"org.codehaus.groovy.control.CompilationUnit", "javax.tools.ToolProvider", "javax.tools.JavaCompiler",
			"com.sun.tools.javac.api.JavacTool", "jdk.jshell.JShell" })
	void scriptEnginesCompilersAndShellsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.lang.ProcessBuilder", "java.lang.ProcessHandle", "java.lang.ProcessHandle$Info" })
	void processesAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.lang.management.ManagementFactory", "javax.management.MBeanServer",
			"javax.management.loading.MLet", "javax.management.remote.JMXConnectorFactory", "jdk.jfr.Recording",
			"jdk.jfr.consumer.RecordingFile", "java.lang.instrument.Instrumentation",
			"com.sun.tools.attach.VirtualMachine" })
	void jvmInternalsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.lang.foreign.Linker", "java.lang.foreign.MemorySegment",
			"java.lang.foreign.SymbolLookup" })
	void nativeCodeIsBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "javax.naming.InitialContext", "javax.naming.directory.InitialDirContext",
			"javax.naming.ldap.InitialLdapContext", "java.rmi.Naming", "java.rmi.registry.LocateRegistry",
			"java.rmi.server.UnicastRemoteObject", "javax.sql.rowset.RowSetProvider", "javax.sql.rowset.JdbcRowSet" })
	void remoteLookupsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "javax.security.auth.login.LoginContext", "javax.security.auth.login.Configuration",
			"com.sun.security.auth.module.LdapLoginModule", "com.sun.security.auth.module.Krb5LoginModule",
			"javax.security.sasl.Sasl", "org.ietf.jgss.GSSManager" })
	void directoryAndKerberosSignInsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.net.Socket", "java.net.ServerSocket", "java.net.DatagramSocket",
			"java.net.MulticastSocket", "java.nio.channels.SocketChannel", "java.nio.channels.ServerSocketChannel",
			"java.nio.channels.DatagramChannel", "java.nio.channels.AsynchronousSocketChannel",
			"java.nio.channels.AsynchronousServerSocketChannel", "javax.net.SocketFactory",
			"javax.net.ServerSocketFactory", "javax.net.ssl.SSLSocket", "javax.net.ssl.SSLSocketFactory",
			"javax.net.ssl.SSLServerSocket", "javax.net.ssl.SSLEngine", "java.util.logging.SocketHandler",
			"com.sun.net.httpserver.HttpServer", "com.sun.net.httpserver.SimpleFileServer",
			"javax.print.PrintServiceLookup", "java.net.NetworkInterface" })
	void socketsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.net.http.HttpClient", "java.net.http.HttpRequest", "java.net.http.HttpResponse",
			"java.net.http.HttpClient$Builder", "java.net.http.WebSocket", "java.net.URLConnection",
			"java.net.HttpURLConnection", "java.net.JarURLConnection", "javax.net.ssl.HttpsURLConnection" })
	void httpClientsOtherThanThePlatformHelperAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.net.ProxySelector", "java.net.Authenticator", "java.net.CookieHandler",
			"java.net.CookieManager", "java.net.ResponseCache", "javax.net.ssl.SSLContext",
			"javax.net.ssl.HostnameVerifier" })
	void hooksIntoEveryHttpCallOfTheJvmAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.lang.Thread", "java.util.concurrent.ThreadPoolExecutor",
			"java.util.concurrent.ThreadPoolExecutor$AbortPolicy", "java.util.concurrent.ScheduledThreadPoolExecutor",
			"java.util.concurrent.ForkJoinPool" })
	void threadsAndThreadPoolsAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "jakarta.mail.Transport", "jakarta.mail.Store", "jakarta.mail.Service",
			"jakarta.mail.Folder" })
	void mailIsSentThroughThePlatformHelper(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "org.w3c.dom.ls.LSParser", "org.w3c.dom.ls.LSInput", "org.w3c.dom.ls.LSResourceResolver",
			"org.xml.sax.helpers.ParserFactory", "javax.xml.catalog.CatalogManager",
			"javax.xml.catalog.CatalogResolver" })
	void xmlParsersTheSandboxDoesNotSetUpAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@Test
	void logFilesAreWrittenByThePlatform() {
		assertTrue(SemossClassLoader.isReflectionApiBlocked("java.util.logging.FileHandler"));
	}

	@Test
	void javaDeserializationIsBlocked() {
		assertTrue(SemossClassLoader.isReflectionApiBlocked("java.io.ObjectInputStream"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna.util.DIHelper", "prerna.util.SystemEngineRegistry",
			"prerna.security.SecretRedactor", "prerna.util.InternalUtility", "prerna.util.InternalUtility$Nested",
			"prerna.engine.impl.pipeline.EngineProxyFactory", "prerna.engine.impl.pipeline.PipelineInvocationHandler",
			"prerna.engine.impl.pipeline.PlatformOperation", "prerna.engine.impl.pipeline.EngineCallPermission",
			"prerna.om.InsightStore",
			"prerna.util.LocalMasterConceptIdHash", "prerna.util.SystemProjectSeeder",
			"prerna.util.SystemDatabaseSchemaUtils" })
	void platformStateAndInternalHelpersAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna.io.connector.secrets.SecretsFactory", "prerna.io.connector.secrets.ISecrets",
			"prerna.io.connector.secrets.SecretsUtility",
			"prerna.io.connector.secrets.hashicorp.vault.HashiCorpVaultUtil" })
	void theSecretStoreIsBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna.auth.utils.SecurityAdminUtils", "prerna.auth.utils.SecurityAdminUtils$Nested",
			"prerna.auth.utils.AdminSecurityGroupUtils", "prerna.auth.utils.reactors.admin.AdminGetEngineSMSSReactor",
			"prerna.auth.utils.reactors.admin.AdminGetEngineSMSSReactor$1", "prerna.theme.AdminThemeUtils",
			"prerna.reactor.agent.run.AdminAccess",
			"prerna.semoss.web.services.local.auth.AdminUserAuthorizationResource" })
	void classesThatRequireAnAdminAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@Test
	void everyAdminNamedClassInThePlatformSourceIsBlocked() throws Exception {
		Path source = Path.of("src");
		assertTrue(Files.isDirectory(source), "run from the Semoss folder");

		List<String> adminClasses;
		try (Stream<Path> files = Files.walk(source)) {
			adminClasses = files.filter(file -> {
				String fileName = file.getFileName().toString();
				return fileName.startsWith("Admin") && fileName.endsWith(".java");
			}).map(file -> {
				String relative = source.relativize(file).toString().replace('\\', '/');
				return relative.substring(0, relative.length() - ".java".length()).replace('/', '.');
			}).toList();
		}

		assertFalse(adminClasses.isEmpty());
		for (String adminClass : adminClasses) {
			assertTrue(SemossClassLoader.isReflectionApiBlocked(adminClass), adminClass);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "com.fasterxml.jackson.databind.introspect.AnnotatedField",
			"com.fasterxml.jackson.databind.introspect.AnnotatedMethod",
			"com.fasterxml.jackson.databind.introspect.BasicBeanDescription",
			"com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector",
			"com.fasterxml.jackson.databind.ser.BeanSerializerFactory",
			"com.fasterxml.jackson.databind.ser.DefaultSerializerProvider$Impl",
			"com.fasterxml.jackson.databind.deser.BeanDeserializerFactory",
			"com.fasterxml.jackson.databind.deser.DefaultDeserializationContext$Impl",
			"com.fasterxml.jackson.databind.deser.std.StdValueInstantiator",
			"com.fasterxml.jackson.databind.deser.SettableBeanProperty",
			"com.fasterxml.jackson.databind.deser.impl.FieldProperty",
			"tools.jackson.databind.introspect.AnnotatedMethod", "tools.jackson.databind.ser.BeanSerializerFactory",
			"tools.jackson.databind.deser.DeserializationContextExt",
			"tools.jackson.databind.deser.std.StdValueInstantiator",
			"tools.jackson.databind.deser.bean.BeanDeserializer", "com.fasterxml.jackson.databind.node.POJONode",
			"com.fasterxml.jackson.databind.util.RawValue", "com.fasterxml.jackson.databind.util.TokenBuffer",
			"tools.jackson.databind.node.POJONode", "tools.jackson.databind.util.RawValue",
			"tools.jackson.databind.util.TokenBuffer" })
	void theJacksonClassesThatReachIntoAnyObjectAreBlocked(String name) {
		assertTrue(SemossClassLoader.isReflectionApiBlocked(name), name);
	}

	@ParameterizedTest
	@ValueSource(strings = { "com.fasterxml.jackson.databind.ObjectMapper",
			"com.fasterxml.jackson.databind.JsonNode", "com.fasterxml.jackson.databind.node.ObjectNode",
			"com.fasterxml.jackson.databind.module.SimpleModule",
			"com.fasterxml.jackson.databind.ser.std.StdSerializer",
			"com.fasterxml.jackson.databind.deser.std.StdDeserializer",
			"com.fasterxml.jackson.databind.introspect.VisibilityChecker$Std",
			"com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider",
			"com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter",
			"com.fasterxml.jackson.databind.json.JsonMapper", "com.fasterxml.jackson.core.JsonParser",
			"com.fasterxml.jackson.annotation.JsonProperty", "tools.jackson.databind.ObjectMapper",
			"tools.jackson.databind.ser.std.StdSerializer", "tools.jackson.databind.deser.std.StdDeserializer",
			"com.google.gson.Gson", "com.google.gson.GsonBuilder" })
	void theJsonClassesCustomReactorsRelyOnStayOpen(String name) {
		assertFalse(SemossClassLoader.isReflectionApiBlocked(name), name);
	}

	@Test
	void everyClassThatAsksForASystemDatabaseIsClosedToAProjectsDirectCalls() throws Exception {
		Path source = Path.of("src");
		assertTrue(Files.isDirectory(source), "run from the Semoss folder");
		List<String> open = List.of("prerna/util/SystemEngineRegistry", "prerna/auth/PasswordRequirements");

		List<String> askers;
		try (Stream<Path> files = Files.walk(source)) {
			askers = files.filter(file -> file.getFileName().toString().endsWith(".java")).filter(file -> {
				try {
					String text = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
					return ASKS_FOR_A_SYSTEM_DATABASE.matcher(text).find();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}).map(file -> {
				String relative = source.relativize(file).toString().replace('\\', '/');
				return relative.substring(0, relative.length() - ".java".length());
			}).toList();
		}

		assertFalse(askers.isEmpty());
		for (String asker : askers) {
			boolean reactor = asker.endsWith("Reactor");
			boolean closed = SemossClassLoader.isSystemDatabaseClass(asker)
					|| SemossClassLoader.isReflectionApiBlocked(asker.replace('/', '.'));
			assertTrue(closed || reactor || open.contains(asker), asker);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "java.lang.String", "java.util.ArrayList", "java.io.File", "java.net.URL", "java.net.URI",
			"java.net.URLEncoder", "java.sql.Connection", "java.sql.DriverManager", "java.security.MessageDigest",
			"java.security.Security", "javax.security.auth.x500.X500Principal", "javax.sql.DataSource",
			"java.util.concurrent.ConcurrentHashMap", "java.util.concurrent.TimeUnit",
			"java.util.concurrent.ThreadLocalRandom", "java.time.Duration", "java.lang.invoke.LambdaMetafactory",
			"javax.xml.parsers.DocumentBuilderFactory", "javax.xml.transform.TransformerFactory",
			"org.xml.sax.InputSource", "org.w3c.dom.ls.LSSerializer", "org.w3c.dom.ls.DOMImplementationLS",
			"prerna.util.Utility", "prerna.reactor.AbstractReactor", "prerna.security.HttpHelperUtility",
			"prerna.security.HttpStatusException", "prerna.util.EmailUtility", "jakarta.mail.Session",
			"jakarta.mail.internet.MimeMessage", "prerna.auth.utils.SecurityEngineUtils",
			"prerna.reactor.agent.admin.RunSummary", "custom.reactors.AdminPanelReactor" })
	void classesCustomReactorsRelyOnStayOpen(String name) {
		assertFalse(SemossClassLoader.isReflectionApiBlocked(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna/auth/utils/SecurityAdminUtils",
			"prerna/auth/utils/reactors/admin/AdminGetEngineSMSSReactor", "prerna/io/connector/secrets/SecretsFactory",
			"java/net/http/HttpClient", "java/beans/XMLDecoder", "javax/naming/InitialContext",
			"javax/management/MBeanServer", "javax/net/ssl/SSLSocketFactory", "java/lang/ProcessHandle" })
	void aCustomClassCannotNameABlockedClassOrFindItByName(String internalName) throws Exception {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe()
				.method("name", mv -> mv.visitLdcInsn(Type.getObjectType(internalName))).method("find", mv -> {
					mv.visitLdcInsn(internalName.replace('/', '.'));
					mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Class", "forName",
							"(Ljava/lang/String;)Ljava/lang/Class;", false);
				}).bytes());

		assertThrows(NoClassDefFoundError.class, () -> SandboxProbe.run(probe, "name"));
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "find"));
	}
}
