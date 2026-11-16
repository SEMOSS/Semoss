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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * One test per call the sandbox replaces in a project's custom reactors. Each
 * checks that the call is gone from the class once the sandbox has rewritten
 * it, then runs the class and checks what the call does instead. The sandbox
 * page in the docs lists each rule with the test here that guards it.
 */
class SandboxReplacedCallsUnitTests {

	private static final String STRING_TO_VOID = "(Ljava/lang/String;)V";

	@TempDir
	Path folder;

	@AfterEach
	void clearSandboxRoot() {
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
	}

	@ParameterizedTest
	@CsvSource({ "java/lang/System, load, (Ljava/lang/String;)V, true",
			"java/lang/System, loadLibrary, (Ljava/lang/String;)V, true",
			"java/lang/Runtime, load, (Ljava/lang/String;)V, false",
			"java/lang/Runtime, loadLibrary, (Ljava/lang/String;)V, false" })
	void nativeLibrariesCannotBeLoaded(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@CsvSource({ "java/lang/System, setIn, (Ljava/io/InputStream;)V, true",
			"java/lang/System, setOut, (Ljava/io/PrintStream;)V, true",
			"java/lang/System, setErr, (Ljava/io/PrintStream;)V, true" })
	void theStandardStreamsCannotBeReplaced(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@CsvSource({ "java/security/Security, addProvider, (Ljava/security/Provider;)I, true",
			"java/security/Security, insertProviderAt, (Ljava/security/Provider;I)I, true",
			"java/security/Security, removeProvider, (Ljava/lang/String;)V, true",
			"java/security/Security, setProperty, (Ljava/lang/String;Ljava/lang/String;)V, true" })
	void securityProvidersCannotBeChanged(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@CsvSource({ "java/sql/DriverManager, registerDriver, (Ljava/sql/Driver;)V, true",
			"java/sql/DriverManager, deregisterDriver, (Ljava/sql/Driver;)V, true",
			"java/sql/DriverManager, setLogWriter, (Ljava/io/PrintWriter;)V, true",
			"java/sql/DriverManager, setLogStream, (Ljava/io/PrintStream;)V, true" })
	void registeredJdbcDriversCannotBeChanged(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@CsvSource({ "prerna/om/Insight, setUser, (Lprerna/auth/User;)V, false",
			"prerna/om/ThreadStore, setUser, (Lprerna/auth/User;)V, true",
			"prerna/om/ThreadStore, setThreadMapObject, (Ljava/util/Map;)V, true",
			"prerna/auth/User, setAccessToken, (Lprerna/auth/AccessToken;)V, false",
			"prerna/auth/User, setGlobalAccessToken, (Lprerna/auth/AccessToken;)V, false",
			"prerna/auth/User, dropAccessToken, (Ljava/lang/String;)Z, false",
			"prerna/auth/User, dropAccessToken, (Lprerna/auth/AuthProvider;)Z, false",
			"prerna/auth/User, setPrimaryLogin, (Lprerna/auth/AuthProvider;)V, false",
			"prerna/auth/AccessToken, setId, (Ljava/lang/String;)V, false",
			"prerna/auth/AccessToken, setProvider, (Lprerna/auth/AuthProvider;)V, false",
			"prerna/auth/AccessToken, setUserGroups, (Ljava/util/Set;)V, false",
			"prerna/auth/AccessToken, setUserGroupType, (Ljava/lang/String;)V, false",
			"prerna/auth/AccessToken, setLocked, (Ljava/lang/Boolean;)V, false",
			"prerna/auth/ReadOnlyAccessToken, setId, (Ljava/lang/String;)V, false" })
	void theSignedInUserCannotBeChanged(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@CsvSource({ "setInsightId, (Ljava/lang/String;)V", "setSessionId, (Ljava/lang/String;)V",
			"setRouteId, (Ljava/lang/String;)V", "setJobId, (Ljava/lang/String;)V",
			"setSchedulerMode, (Ljava/lang/Boolean;)V", "setLocalHostname, (Ljava/lang/String;)V",
			"setLocalProtocol, (Ljava/lang/String;)V", "setLocalPort, (Ljava/lang/Integer;)V",
			"setContextProjectIdOverride, (Ljava/lang/String;)V",
			"setContextProjectNameOverride, (Ljava/lang/String;)V", "clearContextProjectOverride, ()V",
			"getTheadMapObject, ()Ljava/util/Map;", "remove, ()V" })
	void theRequestTheThreadServesCannotBeChanged(String name, String descriptor) throws Exception {
		assertRefused("prerna/om/ThreadStore", name, descriptor, true);
	}

	@ParameterizedTest
	@CsvSource({ "prerna/util/sql/AbstractSqlQueryUtil, getPassword, ()Ljava/lang/String;, false",
			"prerna/util/sql/AbstractSqlQueryUtil, getUsername, ()Ljava/lang/String;, false",
			"prerna/util/sql/AbstractSqlQueryUtil, getConnectionUrl, ()Ljava/lang/String;, false",
			"prerna/util/sql/AbstractSqlQueryUtil, getAdditionalProps, ()Ljava/lang/String;, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setPassword, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setUsername, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setHostname, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setPort, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setDatabase, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setSchema, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setConnectionUrl, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AbstractSqlQueryUtil, setAdditionalProps, (Ljava/lang/String;)V, false",
			"prerna/util/sql/AnsiSqlQueryUtil, getPassword, ()Ljava/lang/String;, false",
			"prerna/util/sql/PostgresQueryUtil, getPassword, ()Ljava/lang/String;, false",
			"prerna/util/sql/AthenaQueryUtil, getUsername, ()Ljava/lang/String;, false" })
	void anEnginesCredentialsCannotBeReadOrChangedThroughItsQueryUtility(String owner, String name,
			String descriptor, boolean isStatic) throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@ValueSource(strings = { "prerna/auth/User", "prerna/auth/AccessToken", "prerna/om/Insight",
			"prerna/om/ThreadStore", "java/util/TimeZone", "com/zaxxer/hikari/HikariConfig",
			"org/apache/logging/log4j/LogManager" })
	void aClassWithRefusedCallsCannotBeExtended(String owner) {
		byte[] subclass = new SandboxProbe("custom/sandbox/Extended", owner).bytes();

		assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(subclass));
	}

	@ParameterizedTest
	@CsvSource({ "java/util/TimeZone, setDefault, (Ljava/util/TimeZone;)V, true",
			"java/util/SimpleTimeZone, setDefault, (Ljava/util/TimeZone;)V, true",
			"java/util/Locale, setDefault, (Ljava/util/Locale;)V, true",
			"java/util/Locale, setDefault, (Ljava/util/Locale$Category;Ljava/util/Locale;)V, true" })
	void theDefaultTimeZoneAndLocaleCannotBeChanged(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		assertRefused(owner, name, descriptor, isStatic);
	}

	@ParameterizedTest
	@CsvSource({ "java/net/URL, getContent, ()Ljava/lang/Object;, false",
			"java/net/URL, getContent, ([Ljava/lang/Class;)Ljava/lang/Object;, false",
			"java/net/URL, openStream, ()Ljava/io/InputStream;, false",
			"java/net/URL, openConnection, ()Ljava/net/URLConnection;, false",
			"java/net/URL, openConnection, (Ljava/net/Proxy;)Ljava/net/URLConnection;, false",
			"java/net/URL, setURLStreamHandlerFactory, (Ljava/net/URLStreamHandlerFactory;)V, true" })
	void aUrlCannotOpenAConnection(String owner, String name, String descriptor, boolean isStatic) throws Exception {
		Class<?> probe = rewritten(owner, name, descriptor, isStatic);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@CsvSource({ "java/lang/System, exit, (I)V, true", "java/lang/Runtime, halt, (I)V, false",
			"java/lang/Runtime, exec, (Ljava/lang/String;)Ljava/lang/Process;, false",
			"java/lang/Runtime, exec, ([Ljava/lang/String;)Ljava/lang/Process;, false",
			"java/lang/Runtime, exec, ([Ljava/lang/String;[Ljava/lang/String;Ljava/io/File;)Ljava/lang/Process;, false" })
	void theJvmCannotBeStoppedAndProcessesCannotBeStarted(String owner, String name, String descriptor,
			boolean isStatic) throws Exception {
		Class<?> probe = rewritten(owner, name, descriptor, isStatic);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@CsvSource({ "java/lang/System, getenv, (Ljava/lang/String;)Ljava/lang/String;, true",
			"java/lang/System, getenv, ()Ljava/util/Map;, true",
			"java/lang/System, getProperties, ()Ljava/util/Properties;, true",
			"java/lang/Thread, getAllStackTraces, ()Ljava/util/Map;, true" })
	void environmentVariablesAllPropertiesAndOtherThreadsAreHidden(String owner, String name, String descriptor,
			boolean isStatic) throws Throwable {
		Class<?> probe = rewritten(owner, name, descriptor, isStatic);

		assertNull(SandboxProbe.run(probe, "run"));
	}

	@Test
	void onlyListedSystemPropertiesCanBeRead() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("javaVersion", readProperty("java.version"))
						.method("userHome", readProperty("user.home"))
						.method("classPath", readProperty("java.class.path")).bytes());

		assertEquals(System.getProperty("java.version"), SandboxProbe.run(probe, "javaVersion"));
		assertNull(SandboxProbe.run(probe, "userHome"));
		assertNull(SandboxProbe.run(probe, "classPath"));
	}

	@ParameterizedTest
	@CsvSource({ "java/lang/System, setProperty, (Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;, true",
			"java/lang/System, clearProperty, (Ljava/lang/String;)Ljava/lang/String;, true",
			"java/lang/System, setProperties, (Ljava/util/Properties;)V, true" })
	void systemPropertiesCannotBeChanged(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		Class<?> probe = rewritten(owner, name, descriptor, isStatic);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@CsvSource({
			"java/nio/file/Files, createSymbolicLink, (Ljava/nio/file/Path;Ljava/nio/file/Path;[Ljava/nio/file/attribute/FileAttribute;)Ljava/nio/file/Path;, true",
			"java/nio/channels/FileChannel, open, (Ljava/nio/file/Path;[Ljava/nio/file/OpenOption;)Ljava/nio/channels/FileChannel;, true" })
	void symbolicLinksAndFileChannelsAreRefused(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		Class<?> probe = rewritten(owner, name, descriptor, isStatic);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aClassLoaderPassedToForNameIsIgnored() throws Exception {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitLdcInsn("prerna.util.DIHelper");
			mv.visitInsn(Opcodes.ICONST_0);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Class", "forName",
					"(Ljava/lang/String;ZLjava/lang/ClassLoader;)Ljava/lang/Class;", false);
		}).bytes());

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aMethodReferenceBehavesLikeTheCallItStandsFor() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("getenv", viaReference("getenv", "(Ljava/lang/String;)Ljava/lang/String;"))
						.method("loadLibrary", viaReference("loadLibrary", STRING_TO_VOID)).bytes());

		assertNull(SandboxProbe.run(probe, "getenv"));
		SecurityException refused = assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "loadLibrary"));
		assertEquals("System.loadLibrary is not permitted in project reactors", refused.getMessage());
	}

	/**
	 * A method body that makes a method reference to a {@code System} method that
	 * takes a String, then uses it on "PATH".
	 */
	private static Consumer<MethodVisitor> viaReference(String method, String descriptor) {
		Handle metafactory = new Handle(Opcodes.H_INVOKESTATIC, "java/lang/invoke/LambdaMetafactory", "metafactory",
				"(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
						+ "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)"
						+ "Ljava/lang/invoke/CallSite;",
				false);
		boolean returnsValue = !descriptor.endsWith(")V");
		return mv -> {
			mv.visitInvokeDynamicInsn(returnsValue ? "apply" : "accept",
					returnsValue ? "()Ljava/util/function/Function;" : "()Ljava/util/function/Consumer;", metafactory,
					org.objectweb.asm.Type
							.getType(returnsValue ? "(Ljava/lang/Object;)Ljava/lang/Object;" : "(Ljava/lang/Object;)V"),
					new Handle(Opcodes.H_INVOKESTATIC, "java/lang/System", method, descriptor, false),
					org.objectweb.asm.Type.getType(descriptor));
			mv.visitLdcInsn("PATH");
			if (returnsValue) {
				mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/function/Function", "apply",
						"(Ljava/lang/Object;)Ljava/lang/Object;", true);
			} else {
				mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/function/Consumer", "accept",
						"(Ljava/lang/Object;)V", true);
				mv.visitInsn(Opcodes.ACONST_NULL);
			}
		};
	}

	@Test
	void aMethodHandleConstantToARefusedCallIsRefused() {
		byte[] constant = new SandboxProbe().method("run", mv -> mv.visitLdcInsn(new Handle(Opcodes.H_INVOKESTATIC,
				"java/sql/DriverManager", "registerDriver", "(Ljava/sql/Driver;)V", false))).bytes();

		assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(constant));
	}

	@Test
	void withAUserRootEveryPathStaysInsideIt() throws Throwable {
		Path root = Files.createDirectories(this.folder.resolve("user-root")).toRealPath();
		Path outside = Files.writeString(this.folder.toRealPath().resolve("outside.txt"), "outside");
		String outsidePath = outside.toString();
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("file", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "java/io/File");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(outsidePath);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/File", "<init>", STRING_TO_VOID, false);
		}).method("pathsGet", pathFactory("java/nio/file/Paths", "get", false, outsidePath))
				.method("pathOf", pathFactory("java/nio/file/Path", "of", true, outsidePath)).method("read", mv -> {
					mv.visitTypeInsn(Opcodes.NEW, "java/io/FileInputStream");
					mv.visitInsn(Opcodes.DUP);
					mv.visitLdcInsn(outsidePath);
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/FileInputStream", "<init>", STRING_TO_VOID,
							false);
				}).method("write", mv -> {
					mv.visitTypeInsn(Opcodes.NEW, "java/io/FileOutputStream");
					mv.visitInsn(Opcodes.DUP);
					mv.visitLdcInsn(outsidePath);
					mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/FileOutputStream", "<init>", STRING_TO_VOID,
							false);
				}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(root.toString());

		assertTrue(((File) SandboxProbe.run(probe, "file")).toPath().startsWith(root));
		assertTrue(((Path) SandboxProbe.run(probe, "pathsGet")).startsWith(root));
		assertTrue(((Path) SandboxProbe.run(probe, "pathOf")).startsWith(root));
		assertThrows(FileNotFoundException.class, () -> SandboxProbe.run(probe, "read"));
		assertThrows(FileNotFoundException.class, () -> SandboxProbe.run(probe, "write"));
		assertEquals("outside", Files.readString(outside));
	}

	@Test
	void withAUserRootAFileReportsItsPathFromThatRoot() throws Throwable {
		Path root = Files.createDirectories(this.folder.resolve("user-root")).toRealPath();
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("absolutePath", fileGetter("getAbsolutePath", "()Ljava/lang/String;"))
						.method("canonicalPath", fileGetter("getCanonicalPath", "()Ljava/lang/String;"))
						.method("parent", fileGetter("getParent", "()Ljava/lang/String;"))
						.method("parentFile", fileGetter("getParentFile", "()Ljava/io/File;"))
						.method("toPath", fileGetter("toPath", "()Ljava/nio/file/Path;")).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(root.toString());

		assertEquals(File.separator + "notes.txt", SandboxProbe.run(probe, "absolutePath"));
		assertEquals(File.separator + "notes.txt", SandboxProbe.run(probe, "canonicalPath"));
		assertEquals(root.toString(), SandboxProbe.run(probe, "parent"));
		assertEquals(root.toFile(), SandboxProbe.run(probe, "parentFile"));
		assertNotNull(SandboxProbe.run(probe, "toPath"));
	}

	/**
	 * Checks that a call is replaced with one that throws and names the call.
	 */
	private void assertRefused(String owner, String name, String descriptor, boolean isStatic) throws Exception {
		Class<?> probe = rewritten(owner, name, descriptor, isStatic);

		SecurityException refused = assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
		String call = owner.substring(owner.lastIndexOf('/') + 1) + "." + name;
		assertEquals(call + " is not permitted in project reactors", refused.getMessage());
	}

	private Class<?> rewritten(String owner, String name, String descriptor, boolean isStatic) throws Exception {
		return SandboxProbe.loadCall(this.folder, owner, name, descriptor, isStatic);
	}

	private static Consumer<MethodVisitor> readProperty(String key) {
		return mv -> {
			mv.visitLdcInsn(key);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "getProperty",
					"(Ljava/lang/String;)Ljava/lang/String;", false);
		};
	}

	private static Consumer<MethodVisitor> pathFactory(String owner, String name, boolean isInterface, String path) {
		return mv -> {
			mv.visitLdcInsn(path);
			mv.visitInsn(Opcodes.ICONST_0);
			mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/String");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, owner, name,
					"(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;", isInterface);
		};
	}

	/**
	 * A method body that asks {@code new File("notes.txt")} one question about
	 * where it is.
	 */
	private static Consumer<MethodVisitor> fileGetter(String getter, String descriptor) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "java/io/File");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn("notes.txt");
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/File", "<init>", STRING_TO_VOID, false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/File", getter, descriptor, false);
		};
	}
}
