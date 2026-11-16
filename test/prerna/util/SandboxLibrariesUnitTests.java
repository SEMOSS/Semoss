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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.zaxxer.hikari.HikariConfig;

/**
 * One test per rule on the libraries a project's custom reactors use: which
 * ones are shared with the platform, how every other one is loaded as the
 * project's own sandboxed copy, and the calls on shared libraries that are
 * refused. The sandbox page in the docs lists each rule with the test here that
 * guards it.
 */
class SandboxLibrariesUnitTests {

	private static final String JSON_TYPE_INFO = "Lcom/fasterxml/jackson/annotation/JsonTypeInfo;";
	private static final String JSON_TYPE_ID = "Lcom/fasterxml/jackson/annotation/JsonTypeInfo$Id;";
	private static final String OBJECT_MAPPER = "com/fasterxml/jackson/databind/ObjectMapper";
	private static final String PD_DOCUMENT = "org/apache/pdfbox/pdmodel/PDDocument";
	private static final String PD_PAGE = "org/apache/pdfbox/pdmodel/PDPage";
	private static final String PD_RECTANGLE = "org/apache/pdfbox/pdmodel/common/PDRectangle";
	private static final String PD_RECTANGLE_DESCRIPTOR = "L" + PD_RECTANGLE + ";";

	@TempDir
	Path folder;

	private Properties savedCoreProp;

	@BeforeEach
	void saveRdfMap() {
		this.savedCoreProp = DIHelper.getInstance().getCoreProp();
		DIHelper.getInstance().setCoreProp(new Properties());
	}

	@AfterEach
	void restoreRdfMap() {
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		DIHelper.getInstance().setCoreProp(this.savedCoreProp);
	}

	@ParameterizedTest
	@ValueSource(strings = { "com.google.gson.Gson", "com.google.gson.JsonObject", "com.google.gson.reflect.TypeToken",
			"com.google.gson.stream.JsonReader", "org.json.JSONObject", "com.fasterxml.jackson.databind.ObjectMapper",
			"com.fasterxml.jackson.core.JsonParser", "com.fasterxml.jackson.annotation.JsonProperty",
			"org.apache.poi.ss.usermodel.Workbook", "org.apache.poi.xssf.usermodel.XSSFWorkbook",
			"org.apache.logging.log4j.LogManager", "org.apache.logging.log4j.Logger",
			"org.apache.logging.log4j.message.ParameterizedMessage", "org.apache.logging.log4j.spi.ExtendedLogger",
			"org.apache.logging.log4j.util.Supplier", "com.zaxxer.hikari.HikariDataSource", "org.javatuples.Pair",
			"jakarta.mail.Session", "jakarta.mail.internet.MimeMessage", "jakarta.activation.DataHandler",
			"org.apache.pdfbox.Loader", "org.apache.pdfbox.io.IOUtils", "org.apache.fontbox.ttf.TTFParser",
			"prerna.util.Utility", "javax.sql.DataSource", "org.w3c.dom.Document", "java.util.ArrayList" })
	void aSharedLibraryClassIsThePlatformsOwn(String name) throws Exception {
		assertSame(Class.forName(name), SandboxProbe.loader(this.folder).loadClass(name));
	}

	@ParameterizedTest
	@ValueSource(strings = { "org.apache.commons.lang3.StringUtils", "org.apache.commons.io.FileUtils",
			"com.google.common.collect.ImmutableList", "org.apache.commons.text.StringSubstitutor",
			"org.apache.hc.client5.http.impl.classic.HttpClients", "org.jsoup.Jsoup",
			"org.apache.logging.log4j.core.LoggerContext", "org.apache.logging.log4j.util.LoaderUtil",
			"org.apache.logging.log4j.util.PropertiesUtil", "org.apache.logging.log4j.message.ThreadDumpMessage",
			"com.google.gson.internal.UnsafeAllocator", "com.zaxxer.hikari.util.UtilityElf",
			"com.fasterxml.jackson.databind.util.ClassUtil" })
	void anyOtherLibraryClassIsTheProjectsOwnCopy(String name) throws Exception {
		SemossClassLoader sandbox = SandboxProbe.loader(this.folder);

		Class<?> copy = sandbox.loadClass(name);

		assertSame(sandbox, copy.getClassLoader());
		assertNotSame(Class.forName(name), copy);
		assertSame(copy, sandbox.loadClass(name));
	}

	@Test
	void aLibraryCopyThatTheSandboxRefusesCannotBeLoaded() {
		SemossClassLoader sandbox = SandboxProbe.loader(this.folder);

		assertThrows(NoClassDefFoundError.class, () -> sandbox.loadClass("com.sun.mail.smtp.SMTPTransport"));
	}

	@Test
	void eachProjectGetsItsOwnCopy() throws Exception {
		String name = "org.apache.commons.lang3.StringUtils";

		assertNotSame(SandboxProbe.loader(this.folder).loadClass(name),
				SandboxProbe.loader(this.folder).loadClass(name));
	}

	@Test
	void aLibraryCopyWorksAndFollowsTheSandboxRules() throws Throwable {
		Path root = Files.createDirectories(this.folder.resolve("user-root")).toRealPath();
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("capitalize", mv -> {
			mv.visitLdcInsn("sandbox");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/commons/lang3/StringUtils", "capitalize",
					"(Ljava/lang/String;)Ljava/lang/String;", false);
		}).method("readEnv", mv -> {
			mv.visitLdcInsn("PATH");
			mv.visitLdcInsn("hidden");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/commons/lang3/SystemUtils", "getEnvironmentVariable",
					"(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;", false);
		}).method("file", mv -> {
			mv.visitInsn(Opcodes.ICONST_1);
			mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/String");
			mv.visitInsn(Opcodes.DUP);
			mv.visitInsn(Opcodes.ICONST_0);
			mv.visitLdcInsn(File.separator + "outside.txt");
			mv.visitInsn(Opcodes.AASTORE);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/commons/io/FileUtils", "getFile",
					"([Ljava/lang/String;)Ljava/io/File;", false);
		}).method("parse", mv -> {
			mv.visitLdcInsn("<html><body><p>read me</p></body></html>");
			mv.visitLdcInsn("https://example.invalid/");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/jsoup/Jsoup", "parse",
					"(Ljava/lang/String;Ljava/lang/String;)Lorg/jsoup/nodes/Document;", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "org/jsoup/nodes/Document", "text", "()Ljava/lang/String;",
					false);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(root.toString());

		assertEquals("Sandbox", SandboxProbe.run(probe, "capitalize"));
		assertEquals("hidden", SandboxProbe.run(probe, "readEnv"));
		assertTrue(((File) SandboxProbe.run(probe, "file")).toPath().startsWith(root));
		assertEquals("read me", SandboxProbe.run(probe, "parse"));
	}

	@ParameterizedTest
	@ValueSource(ints = { Opcodes.V1_1, Opcodes.V1_4, Opcodes.V1_5 })
	void aLibraryCompiledForAnOldJavaRunsInTheSandbox(int version) throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe(version).method("findClass", mv -> {
			mv.visitLdcInsn("java.lang.String");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Class", "forName",
					"(Ljava/lang/String;)Ljava/lang/Class;", false);
		}).bytes());

		assertEquals(String.class, SandboxProbe.run(probe, "findClass"));
	}

	@ParameterizedTest
	@ValueSource(ints = { Opcodes.V1_1, Opcodes.V1_4, Opcodes.V1_5 })
	void aFinallyBlockFromAnOldCompilerRunsInTheSandbox(int version) throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe(version).method("finallyBlock", mv -> {
			Label subroutine = new Label();
			Label after = new Label();
			mv.visitJumpInsn(Opcodes.JSR, subroutine);
			mv.visitJumpInsn(Opcodes.GOTO, after);
			mv.visitLabel(subroutine);
			mv.visitVarInsn(Opcodes.ASTORE, 0);
			mv.visitVarInsn(Opcodes.RET, 0);
			mv.visitLabel(after);
			mv.visitLdcInsn("finally ran");
		}).bytes());

		assertEquals("finally ran", SandboxProbe.run(probe, "finallyBlock"));
	}

	@Test
	void anAdminCanApproveMorePackages() throws Exception {
		DIHelper.getInstance().getCoreProp().setProperty(Constants.PROJECT_REACTOR_SHARED_PACKAGES,
				"org.apache.commons.lang3, org.apache.commons.text.");
		SemossClassLoader sandbox = SandboxProbe.loader(this.folder);

		assertSame(Class.forName("org.apache.commons.lang3.StringUtils"),
				sandbox.loadClass("org.apache.commons.lang3.StringUtils"));
		assertSame(Class.forName("org.apache.commons.text.StringSubstitutor"),
				sandbox.loadClass("org.apache.commons.text.StringSubstitutor"));
		assertSame(sandbox, sandbox.loadClass("org.apache.commons.io.FileUtils").getClassLoader());
	}

	@Test
	void anApprovedPackageStaysUnderTheBlockList() {
		DIHelper.getInstance().getCoreProp().setProperty(Constants.PROJECT_REACTOR_SHARED_PACKAGES,
				"java.lang.reflect, prerna.util");
		SemossClassLoader sandbox = SandboxProbe.loader(this.folder);

		assertThrows(ClassNotFoundException.class, () -> sandbox.loadClass("java.lang.reflect.Method"));
		assertThrows(ClassNotFoundException.class, () -> sandbox.loadClass("prerna.util.DIHelper"));
	}

	@Test
	void mavenDependenciesShareTheSameLibrariesAndCopyTheRest() throws Exception {
		SemossJarClassLoader maven = new SemossJarClassLoader(Map.of());

		assertSame(Class.forName("com.google.gson.Gson"), maven.loadClass("com.google.gson.Gson"));
		assertSame(maven, maven.loadClass("org.apache.commons.lang3.StringUtils").getClassLoader());
		assertThrows(ClassNotFoundException.class, () -> maven.loadClass("java.lang.reflect.Method"));
	}

	@ParameterizedTest
	@CsvSource({
			"com/fasterxml/jackson/databind/ObjectMapper, enableDefaultTyping, ()Lcom/fasterxml/jackson/databind/ObjectMapper;",
			"com/fasterxml/jackson/databind/ObjectMapper, activateDefaultTyping, (Lcom/fasterxml/jackson/databind/jsontype/PolymorphicTypeValidator;)Lcom/fasterxml/jackson/databind/ObjectMapper;",
			"com/fasterxml/jackson/databind/ObjectMapper, activateDefaultTypingAsProperty, (Lcom/fasterxml/jackson/databind/jsontype/PolymorphicTypeValidator;Lcom/fasterxml/jackson/databind/ObjectMapper$DefaultTyping;Ljava/lang/String;)Lcom/fasterxml/jackson/databind/ObjectMapper;",
			"com/fasterxml/jackson/databind/ObjectMapper, setDefaultTyping, (Lcom/fasterxml/jackson/databind/jsontype/TypeResolverBuilder;)Lcom/fasterxml/jackson/databind/ObjectMapper;",
			"custom/sandbox/OwnMapper, activateDefaultTyping, (Lcom/fasterxml/jackson/databind/jsontype/PolymorphicTypeValidator;)Lcom/fasterxml/jackson/databind/ObjectMapper;",
			"tools/jackson/databind/cfg/MapperBuilder, activateDefaultTyping, (Ltools/jackson/databind/jsontype/PolymorphicTypeValidator;)Ltools/jackson/databind/cfg/MapperBuilder;",
			"com/fasterxml/jackson/databind/type/TypeFactory, constructFromCanonical, (Ljava/lang/String;)Lcom/fasterxml/jackson/databind/JavaType;",
			"com/fasterxml/jackson/databind/type/TypeFactory, findClass, (Ljava/lang/String;)Ljava/lang/Class;",
			"tools/jackson/databind/type/TypeFactory, findClass, (Ljava/lang/String;)Ljava/lang/Class;" })
	void jacksonCannotCreateObjectsOfClassesTheDataNames(String owner, String name, String descriptor)
			throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, owner, name, descriptor, false);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "CLASS", "MINIMAL_CLASS" })
	void aJacksonAnnotationThatNamesClassesInTheJsonIsRefused(String use) {
		byte[] annotated = new SandboxProbe().annotation(JSON_TYPE_INFO, "use", JSON_TYPE_ID, use).bytes();

		assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(annotated));
	}

	@Test
	void aJacksonAnnotationThatNamesTypesByALogicalNameIsAllowed() {
		byte[] annotated = new SandboxProbe().annotation(JSON_TYPE_INFO, "use", JSON_TYPE_ID, "NAME").bytes();

		assertTrue(SemossClassLoader.applyBytecodeTransformations(annotated).length > 0);
	}

	@Test
	void aJacksonMapperStillReadsAndWritesJson() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("roundTrip", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, OBJECT_MAPPER);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, OBJECT_MAPPER, "<init>", "()V", false);
			mv.visitLdcInsn("{\"name\":\"sandbox\"}");
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, OBJECT_MAPPER, "readTree",
					"(Ljava/lang/String;)Lcom/fasterxml/jackson/databind/JsonNode;", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/fasterxml/jackson/databind/JsonNode", "toString",
					"()Ljava/lang/String;", false);
		}).bytes());

		assertEquals("{\"name\":\"sandbox\"}", SandboxProbe.run(probe, "roundTrip"));
	}

	@ParameterizedTest
	@CsvSource({
			"org/apache/logging/log4j/LogManager, setFactory, (Lorg/apache/logging/log4j/spi/LoggerContextFactory;)V",
			"org/apache/logging/log4j/LogManager, shutdown, ()V" })
	void platformLoggingCannotBeReplacedOrShutDown(String owner, String name, String descriptor) throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, owner, name, descriptor, true);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aReactorStillGetsALogger() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("logger", mv -> {
			mv.visitLdcInsn("custom.sandbox");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/logging/log4j/LogManager", "getLogger",
					"(Ljava/lang/String;)Lorg/apache/logging/log4j/Logger;", false);
		}).bytes());

		assertInstanceOf(org.apache.logging.log4j.Logger.class, SandboxProbe.run(probe, "logger"));
	}

	@ParameterizedTest
	@CsvSource({ "com/zaxxer/hikari/HikariConfig, setDataSourceJNDI",
			"com/zaxxer/hikari/HikariConfig, setDataSourceClassName",
			"com/zaxxer/hikari/HikariDataSource, setDataSourceJNDI",
			"com/zaxxer/hikari/HikariDataSource, setDataSourceClassName" })
	void aConnectionPoolCannotLookUpItsDataSourceByName(String owner, String name) throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, owner, name, "(Ljava/lang/String;)V", false);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aConnectionPoolBuiltFromPropertiesFollowsTheSameRule() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("empty", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, "com/zaxxer/hikari/HikariConfig");
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "com/zaxxer/hikari/HikariConfig", "<init>", "()V", false);
		}).bytes());
		Properties byUrl = new Properties();
		byUrl.setProperty("jdbcUrl", "jdbc:h2:mem:sandbox");
		Properties byLookup = new Properties();
		byLookup.setProperty("datasourcejndi", "ldap://example.invalid/pool");
		Properties byClassName = new Properties();
		byClassName.setProperty("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");

		assertInstanceOf(HikariConfig.class, SandboxProbe.run(probe, "empty"));
		assertEquals("jdbc:h2:mem:sandbox", SandboxedHikari.hikariConfig(byUrl).getJdbcUrl());
		assertThrows(SecurityException.class, () -> SandboxedHikari.hikariConfig(byLookup));
		assertThrows(SecurityException.class, () -> SandboxedHikari.hikariConfig(byClassName));
	}

	@Test
	void pdfBoxWritesAndReadsPdfsInsideTheUsersFolder() throws Throwable {
		Path root = Files.createDirectories(this.folder.resolve("user-root")).toRealPath();
		String path = File.separator + "letter.pdf";
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("roundTrip", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, PD_DOCUMENT);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, PD_DOCUMENT, "<init>", "()V", false);
			mv.visitVarInsn(Opcodes.ASTORE, 0);
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitTypeInsn(Opcodes.NEW, PD_PAGE);
			mv.visitInsn(Opcodes.DUP);
			mv.visitFieldInsn(Opcodes.GETSTATIC, PD_RECTANGLE, "A4", PD_RECTANGLE_DESCRIPTOR);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, PD_PAGE, "<init>", "(" + PD_RECTANGLE_DESCRIPTOR + ")V", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_DOCUMENT, "addPage", "(L" + PD_PAGE + ";)V", false);
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitLdcInsn(path);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_DOCUMENT, "save", "(Ljava/lang/String;)V", false);
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_DOCUMENT, "close", "()V", false);
			mv.visitTypeInsn(Opcodes.NEW, "java/io/File");
			mv.visitInsn(Opcodes.DUP);
			mv.visitLdcInsn(path);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/io/File", "<init>", "(Ljava/lang/String;)V", false);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/pdfbox/Loader", "loadPDF",
					"(Ljava/io/File;)L" + PD_DOCUMENT + ";", false);
			mv.visitVarInsn(Opcodes.ASTORE, 0);
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_DOCUMENT, "getNumberOfPages", "()I", false);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false);
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_DOCUMENT, "close", "()V", false);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(root.toString());

		assertEquals(1, SandboxProbe.run(probe, "roundTrip"));
		assertTrue(Files.exists(root.resolve("letter.pdf")));
	}

	@ParameterizedTest
	@CsvSource({ "org/apache/pdfbox/io/IOUtils, unmap, (Ljava/nio/ByteBuffer;)V, true",
			"org/apache/pdfbox/cos/COSName, clearResources, ()V, true",
			"org/apache/pdfbox/pdmodel/ResourceCacheFactory, setResourceCacheCreateFunction, (Lorg/apache/pdfbox/pdmodel/ResourceCacheCreateFunction;)V, true",
			"org/apache/pdfbox/pdmodel/font/FontMappers, set, (Lorg/apache/pdfbox/pdmodel/font/FontMapper;)V, true",
			"org/apache/pdfbox/pdmodel/encryption/SecurityProvider, setProvider, (Ljava/security/Provider;)V, true",
			"org/apache/pdfbox/pdmodel/encryption/SecurityHandlerFactory, registerHandler, (Ljava/lang/String;Ljava/lang/Class;Ljava/lang/Class;)V, false",
			"org/apache/pdfbox/multipdf/Overlay, overlay, (Ljava/util/Map;)Lorg/apache/pdfbox/pdmodel/PDDocument;, false",
			"org/apache/fontbox/cff/CFFISOAdobeCharset, getInstance, ()Lorg/apache/fontbox/cff/CFFISOAdobeCharset;, true",
			"org/apache/fontbox/cff/CFFExpertCharset, getInstance, ()Lorg/apache/fontbox/cff/CFFExpertCharset;, true",
			"org/apache/fontbox/cff/CFFExpertSubsetCharset, getInstance, ()Lorg/apache/fontbox/cff/CFFExpertSubsetCharset;, true" })
	void pdfBoxCannotChangeWhatEveryUserShares(String owner, String name, String descriptor, boolean isStatic)
			throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, owner, name, descriptor, isStatic);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aPageSizeEveryDocumentSharesStaysTheSame() throws Throwable {
		float a4Width = PDRectangle.A4.getUpperRightX();
		float letterWidth = PDRectangle.LETTER.getUpperRightX();
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("changeA4", mv -> {
			mv.visitFieldInsn(Opcodes.GETSTATIC, PD_RECTANGLE, "A4", PD_RECTANGLE_DESCRIPTOR);
			changeWidth(mv);
		}).method("changeANewPage", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, PD_PAGE);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, PD_PAGE, "<init>", "()V", false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_PAGE, "getMediaBox", "()" + PD_RECTANGLE_DESCRIPTOR, false);
			changeWidth(mv);
		}).method("changeAPageWithoutABox", mv -> {
			mv.visitTypeInsn(Opcodes.NEW, PD_PAGE);
			mv.visitInsn(Opcodes.DUP);
			mv.visitTypeInsn(Opcodes.NEW, "org/apache/pdfbox/cos/COSDictionary");
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/pdfbox/cos/COSDictionary", "<init>", "()V", false);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, PD_PAGE, "<init>", "(Lorg/apache/pdfbox/cos/COSDictionary;)V",
					false);
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_PAGE, "getMediaBox", "()" + PD_RECTANGLE_DESCRIPTOR, false);
			changeWidth(mv);
		}).bytes());

		assertEquals(1f, ((PDRectangle) SandboxProbe.run(probe, "changeA4")).getUpperRightX());
		assertEquals(1f, ((PDRectangle) SandboxProbe.run(probe, "changeANewPage")).getUpperRightX());
		assertEquals(1f, ((PDRectangle) SandboxProbe.run(probe, "changeAPageWithoutABox")).getUpperRightX());
		assertEquals(a4Width, PDRectangle.A4.getUpperRightX());
		assertEquals(letterWidth, PDRectangle.LETTER.getUpperRightX());
	}

	/**
	 * With a rectangle on the stack, sets its upper right x to 1 and leaves the
	 * rectangle on the stack.
	 *
	 * @param mv the method being written
	 */
	private static void changeWidth(MethodVisitor mv) {
		mv.visitInsn(Opcodes.DUP);
		mv.visitInsn(Opcodes.FCONST_1);
		mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, PD_RECTANGLE, "setUpperRightX", "(F)V", false);
	}
}
