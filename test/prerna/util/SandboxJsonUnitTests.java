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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.ReflectionAccessFilter.FilterResult;

import prerna.auth.User;

/**
 * The JSON libraries a project shares with the platform read and build the
 * project's own classes and plain JSON data, and never reach into a platform
 * object or build one. Each test names the rule it guards; the rules are listed
 * in docs/platform_services/project_reactor_sandbox.md.
 */
class SandboxJsonUnitTests {

	private static final String GSON = "com/google/gson/Gson";
	private static final String OBJECT_MAPPER = "com/fasterxml/jackson/databind/ObjectMapper";
	private static final String OBJECT_MAPPER_3 = "tools/jackson/databind/ObjectMapper";
	private static final String PROBE = "prerna/util/SandboxProbe";
	private static final Map<String, String> DATA = Map.of("name", "sandbox");
	private static final String DATA_JSON = "{\"name\":\"sandbox\"}";

	@TempDir
	Path folder;

	@AfterEach
	void clearHeldObject() {
		SandboxProbe.heldObject = null;
	}

	@Test
	void aCallOnAMapperRunsOnItsTwin() {
		for (String[] mapper : new String[][] { { GSON, "toJson", "prerna/util/SandboxedGson" },
				{ OBJECT_MAPPER, "writeValueAsString", "prerna/util/SandboxedJackson" },
				{ "com/fasterxml/jackson/databind/json/JsonMapper", "writeValueAsString",
						"prerna/util/SandboxedJackson" },
				{ OBJECT_MAPPER_3, "writeValueAsString", "prerna/util/SandboxedJackson3" } }) {
			byte[] rewritten = SemossClassLoader
					.applyBytecodeTransformations(new SandboxProbe().method("write", writesTheHeldObject(mapper[0], mapper[1])).bytes());

			assertTrue(SandboxProbe.calls(rewritten).contains(mapper[2] + ".guarded"), mapper[0]);
		}
	}

	@Test
	void gsonWritesAndReadsJsonData() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("write", writesTheHeldObject(GSON, "toJson"))
				.method("read", readsAsTheHeldClass(GSON, "fromJson")).bytes());

		SandboxProbe.heldObject = DATA;
		assertEquals(DATA_JSON, SandboxProbe.run(probe, "write"));
		SandboxProbe.heldObject = Map.class;
		assertEquals(DATA, SandboxProbe.run(probe, "read"));
	}

	@Test
	void gsonDoesNotReachIntoAPlatformObjectOrBuildOne() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("write", writesTheHeldObject(GSON, "toJson"))
				.method("read", readsAsTheHeldClass(GSON, "fromJson")).bytes());

		SandboxProbe.heldObject = new User();
		assertThrows(JsonIOException.class, () -> SandboxProbe.run(probe, "write"));
		SandboxProbe.heldObject = User.class;
		assertThrows(JsonIOException.class, () -> SandboxProbe.run(probe, "read"));
	}

	@Test
	void gsonReadsAndWritesAProjectsOwnClass() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("write", writesTheHeldObject(GSON, "toJson"))
				.method("read", readsAsTheHeldClass(GSON, "fromJson")).bytes());

		SandboxProbe.heldObject = probe;
		Object built = SandboxProbe.run(probe, "read");
		assertTrue(probe.isInstance(built));
		SandboxProbe.heldObject = built;
		assertEquals("{}", SandboxProbe.run(probe, "write"));
	}

	@Test
	void aFilterAProjectAddsToAGsonDoesNotOpenPlatformObjects() {
		Gson open = new GsonBuilder().addReflectionAccessFilter(type -> FilterResult.ALLOW).create();

		Gson twin = SandboxedGson.guarded(open);

		assertSame(twin, SandboxedGson.guarded(open));
		assertEquals(DATA_JSON, twin.toJson(DATA));
		assertThrows(JsonIOException.class, () -> twin.toJson(new User()));
		assertThrows(JsonIOException.class, () -> twin.fromJson("{}", User.class));
	}

	@Test
	void aJacksonMapperWritesAndReadsJsonData() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("write", writesTheHeldObject(OBJECT_MAPPER, "writeValueAsString"))
						.method("read", readsAsTheHeldClass(OBJECT_MAPPER, "readValue")).bytes());

		SandboxProbe.heldObject = DATA;
		assertEquals(DATA_JSON, SandboxProbe.run(probe, "write"));
		SandboxProbe.heldObject = Map.class;
		assertEquals(DATA, SandboxProbe.run(probe, "read"));
	}

	@Test
	void aJacksonMapperDoesNotReachIntoAPlatformObjectOrBuildOne() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("write", writesTheHeldObject(OBJECT_MAPPER, "writeValueAsString"))
						.method("read", readsAsTheHeldClass(OBJECT_MAPPER, "readValue")).bytes());

		SandboxProbe.heldObject = new User();
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "write"));
		SandboxProbe.heldObject = User.class;
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "read"));
	}

	@Test
	void aJacksonMapperReadsAndWritesAProjectsOwnClass() throws Exception {
		Class<?> own = SandboxProbe.load(this.folder, new SandboxProbe().constructor("()V", mv -> {
			mv.visitVarInsn(Opcodes.ALOAD, 0);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
		}).bytes());
		ObjectMapper twin = SandboxedJackson.guarded(new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS));

		Object built = twin.readValue("{}", own);

		assertTrue(own.isInstance(built));
		assertEquals("{}", twin.writeValueAsString(built));
	}

	@Test
	void aSettingAProjectChangesOnAMapperHoldsForItsLaterCalls() {
		ObjectMapper mapper = new ObjectMapper();

		ObjectMapper returned = SandboxedJackson.guarded(mapper).enable(SerializationFeature.INDENT_OUTPUT);

		assertTrue(SandboxedJackson.guarded(mapper).isEnabled(SerializationFeature.INDENT_OUTPUT));
		assertSame(SandboxedJackson.guarded(mapper), SandboxedJackson.guarded(returned));
	}

	@Test
	void aMapperToldToReadFieldsDoesNotReachIntoAPlatformObject() {
		ObjectMapper twin = SandboxedJackson
				.guarded(new ObjectMapper().setVisibility(PropertyAccessor.FIELD, Visibility.ANY));

		assertThrows(SecurityException.class, () -> twin.writeValueAsString(new User()));
		assertThrows(SecurityException.class, () -> twin.readValue("{}", User.class));
		assertThrows(SecurityException.class, () -> twin.convertValue(new User(), Map.class));
	}

	@Test
	void aJackson3MapperFollowsTheSameRule() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe().method("write", writesTheHeldObject(OBJECT_MAPPER_3, "writeValueAsString"))
						.method("read", readsAsTheHeldClass(OBJECT_MAPPER_3, "readValue")).bytes());

		SandboxProbe.heldObject = DATA;
		assertEquals(DATA_JSON, SandboxProbe.run(probe, "write"));
		SandboxProbe.heldObject = new User();
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "write"));
		SandboxProbe.heldObject = User.class;
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "read"));
		SandboxProbe.heldObject = Map.class;
		assertEquals(DATA, SandboxProbe.run(probe, "read"));
	}

	@Test
	void aParserOrGeneratorBoundToAMapperFollowsTheSameRule() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonParser parser = mapper.getFactory().createParser("{}");
		JsonGenerator generator = mapper.getFactory().createGenerator(new StringWriter());

		assertThrows(SecurityException.class, () -> SandboxedJackson.guarded(parser).readValueAs(User.class));
		assertThrows(SecurityException.class, () -> SandboxedJackson.guarded(generator).writeObject(new User()));
		assertThrows(SecurityException.class,
				() -> SandboxedJackson.guarded((ObjectCodec) mapper).readValue(parser, User.class));
	}

	@ParameterizedTest
	@CsvSource({ "com/fasterxml/jackson/core/JsonParser, readValueAs, (Ljava/lang/Class;)Ljava/lang/Object;",
			"com/fasterxml/jackson/core/json/WriterBasedJsonGenerator, writeObject, (Ljava/lang/Object;)V",
			"com/fasterxml/jackson/core/JsonGenerator, writePOJO, (Ljava/lang/Object;)V",
			"com/fasterxml/jackson/core/ObjectCodec, readValue, (Lcom/fasterxml/jackson/core/JsonParser;Ljava/lang/Class;)Ljava/lang/Object;",
			"com/fasterxml/jackson/databind/ObjectReader, readValue, (Ljava/lang/String;)Ljava/lang/Object;" })
	void aCallThatReadsOrWritesAnObjectThroughAParsersMapperRunsOnTheTwin(String owner, String name,
			String descriptor) {
		byte[] rewritten = SemossClassLoader.applyBytecodeTransformations(
				new SandboxProbe().method("run", SandboxProbe.call(Opcodes.INVOKEVIRTUAL, owner, name, descriptor))
						.bytes());

		assertTrue(SandboxProbe.calls(rewritten).contains("prerna/util/SandboxedJackson.guarded"), owner);
	}

	@Test
	void jacksonJrIsAProjectsOwnCopy() {
		assertTrue(SemossClassLoader.isSharedWithPlatform("com.fasterxml.jackson.databind.ObjectMapper", List.of()));
		assertFalse(SemossClassLoader.isSharedWithPlatform("com.fasterxml.jackson.jr.ob.JSON", List.of()));
	}

	@ParameterizedTest
	@CsvSource({ "com/fasterxml/jackson/databind/BeanDescription", "tools/jackson/databind/BeanDescription" })
	void aMapperDoesNotCreateAnObjectOfAnyClassOnRequest(String owner) throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, owner, "instantiateBean", "(Z)Ljava/lang/Object;", false);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@CsvSource({
			"com/fasterxml/jackson/databind/node/JsonNodeFactory, pojoNode, (Ljava/lang/Object;)Lcom/fasterxml/jackson/databind/node/ValueNode;",
			"com/fasterxml/jackson/databind/node/ObjectNode, pojoNode, (Ljava/lang/Object;)Lcom/fasterxml/jackson/databind/node/ValueNode;",
			"com/fasterxml/jackson/databind/node/ObjectNode, putPOJO, (Ljava/lang/String;Ljava/lang/Object;)Lcom/fasterxml/jackson/databind/node/ObjectNode;",
			"com/fasterxml/jackson/databind/node/ArrayNode, addPOJO, (Ljava/lang/Object;)Lcom/fasterxml/jackson/databind/node/ArrayNode;",
			"com/fasterxml/jackson/databind/node/ArrayNode, insertPOJO, (ILjava/lang/Object;)Lcom/fasterxml/jackson/databind/node/ArrayNode;",
			"com/fasterxml/jackson/core/JsonGenerator, writeEmbeddedObject, (Ljava/lang/Object;)V",
			"com/fasterxml/jackson/core/json/WriterBasedJsonGenerator, writeEmbeddedObject, (Ljava/lang/Object;)V",
			"tools/jackson/databind/node/JsonNodeFactory, pojoNode, (Ljava/lang/Object;)Ltools/jackson/databind/node/ValueNode;",
			"tools/jackson/databind/node/ObjectNode, putPOJO, (Ljava/lang/String;Ljava/lang/Object;)Ltools/jackson/databind/node/ObjectNode;",
			"tools/jackson/databind/node/ArrayNode, addPOJO, (Ljava/lang/Object;)Ltools/jackson/databind/node/ArrayNode;",
			"tools/jackson/core/JsonGenerator, writeEmbeddedObject, (Ljava/lang/Object;)Ltools/jackson/core/JsonGenerator;" })
	void aJsonTreeHoldsJsonDataOnly(String owner, String name, String descriptor) throws Exception {
		Class<?> probe = SandboxProbe.loadCall(this.folder, owner, name, descriptor, false);

		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@ParameterizedTest
	@ValueSource(strings = { OBJECT_MAPPER, "com/fasterxml/jackson/databind/json/JsonMapper", OBJECT_MAPPER_3,
			"tools/jackson/databind/json/JsonMapper" })
	void aJsonMapperCannotBeExtended(String mapper) {
		byte[] subclass = new SandboxProbe("custom/sandbox/Extended", mapper).bytes();

		assertThrows(SecurityException.class, () -> SemossClassLoader.applyBytecodeTransformations(subclass));
	}

	/**
	 * A method body that creates a mapper and has it write the held object.
	 *
	 * @param mapper the internal name of the mapper class
	 * @param write  the method that writes an object as JSON text
	 * @return the body
	 */
	private static Consumer<MethodVisitor> writesTheHeldObject(String mapper, String write) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, mapper);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, mapper, "<init>", "()V", false);
			mv.visitFieldInsn(Opcodes.GETSTATIC, PROBE, "heldObject", "Ljava/lang/Object;");
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, mapper, write, "(Ljava/lang/Object;)Ljava/lang/String;", false);
		};
	}

	/**
	 * A method body that creates a mapper and has it read the JSON of {@link #DATA}
	 * as the class the test holds.
	 *
	 * @param mapper the internal name of the mapper class
	 * @param read   the method that reads JSON text as a class
	 * @return the body
	 */
	private static Consumer<MethodVisitor> readsAsTheHeldClass(String mapper, String read) {
		return mv -> {
			mv.visitTypeInsn(Opcodes.NEW, mapper);
			mv.visitInsn(Opcodes.DUP);
			mv.visitMethodInsn(Opcodes.INVOKESPECIAL, mapper, "<init>", "()V", false);
			mv.visitLdcInsn(DATA_JSON);
			mv.visitFieldInsn(Opcodes.GETSTATIC, PROBE, "heldObject", "Ljava/lang/Object;");
			mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Class");
			mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, mapper, read,
					"(Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/Object;", false);
		};
	}
}
