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

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBase;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBuilder;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.SettableBeanProperty;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.BeanSerializerBase;
import com.fasterxml.jackson.databind.ser.std.JsonValueSerializer;

/**
 * The sandbox rules for Jackson, which project reactor code shares with the
 * platform. A mapper reads and writes JSON, and every way to have it create
 * objects of the classes the data names is refused: the default typing calls,
 * the type factory's class lookups, the helper that finds and creates classes by
 * name, and the {@code @JsonTypeInfo} settings that name a class.
 *
 * <p>
 * A mapper reads and sets the members of an object by reflection, for whoever
 * hands it the object or names its class, so for project code it does that for
 * the project's own classes only. Every other value is JSON data: maps, lists,
 * text, numbers, dates, enums and the JSON tree. A platform object is turned
 * into JSON through its own methods, and built through its own constructors.
 * The rule is applied where a mapper is used: each call project code makes on a
 * mapper runs on a twin of it that has the same settings and the rule, so it
 * holds for a mapper the project builds any way it likes and for one the
 * platform hands it. The rules for Jackson 3 mappers are in
 * {@link SandboxedJackson3}. The classes Jackson does that reflection with are
 * closed to project code, so the mapper is the only way to it.
 *
 * <p>
 * A JSON tree holds JSON data only. Jackson writes a tree's text, such as in
 * {@code toString()}, with a mapper of its own, so the calls and classes that
 * put an object of any class into a tree or a token stream are closed to
 * project code: an object goes into a tree through a mapper's
 * {@code valueToTree}, which runs on the twin.
 */
public final class SandboxedJackson {

	/**
	 * Internal name of this class, which the rewritten calls name.
	 */
	static final String INTERNAL_NAME = "prerna/util/SandboxedJackson";

	/**
	 * Internal name of the Jackson 2 mapper. A call on it, or on a mapper class
	 * that extends it, runs on a twin.
	 */
	static final String OBJECT_MAPPER = "com/fasterxml/jackson/databind/ObjectMapper";

	/**
	 * Internal name of the type a Jackson 2 parser or generator knows its mapper
	 * by. A call on it runs on the twin when it is a mapper.
	 */
	static final String OBJECT_CODEC = "com/fasterxml/jackson/core/ObjectCodec";

	/**
	 * The calls on a Jackson 2 parser or generator that read or write an object
	 * through the mapper it is bound to, by the class that declares them. The
	 * parser or generator is bound to the mapper's twin first.
	 */
	static final Map<String, Set<String>> CODEC_USES = Map.of("com/fasterxml/jackson/core/JsonParser",
			Set.of("readValueAs", "readValuesAs"), "com/fasterxml/jackson/core/JsonGenerator",
			Set.of("writeObject", "writePOJO", "writeObjectField", "writePOJOField"));

	/**
	 * Packages kept out of the shared ones, so a project gets its own sandboxed
	 * copy: jackson-jr, a second library that reads and builds objects by
	 * reflection.
	 */
	static final List<String> UNSHARED_PACKAGES = List.of("com.fasterxml.jackson.jr.");

	/**
	 * Jackson packages project reactor code cannot use, as name prefixes: the
	 * classes Jackson itself reads, sets and calls the members of any class with,
	 * and builds its bean readers and writers from. A project writes its own
	 * serializers and deserializers from the classes in the {@code std} packages.
	 */
	static final List<String> BLOCKED_PACKAGES = List.of("com.fasterxml.jackson.databind.introspect.",
			"com.fasterxml.jackson.databind.deser.impl.", "com.fasterxml.jackson.databind.ser.impl.",
			"tools.jackson.databind.introspect.", "tools.jackson.databind.deser.impl.",
			"tools.jackson.databind.ser.impl.", "tools.jackson.databind.deser.bean.",
			"tools.jackson.databind.ser.bean.");

	/**
	 * Classes of {@link #BLOCKED_PACKAGES} that only hold settings and stay open.
	 */
	static final Set<String> OPEN_CLASSES = Set.of("com.fasterxml.jackson.databind.introspect.VisibilityChecker",
			"com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider",
			"com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter",
			"tools.jackson.databind.introspect.VisibilityChecker");

	/**
	 * Single Jackson classes project reactor code cannot use, by full name: the
	 * factories, providers and contexts a mapper builds its readers and writers
	 * with, which would build them without the mapper's rule, the classes that
	 * create an object or set its members for a reader, and the classes that carry
	 * an object of any class inside a JSON tree or a token stream.
	 */
	static final Set<String> BLOCKED_CLASSES = Set.of(
			"com.fasterxml.jackson.databind.node.POJONode",
			"com.fasterxml.jackson.databind.util.RawValue",
			"com.fasterxml.jackson.databind.util.TokenBuffer",
			"tools.jackson.databind.node.POJONode",
			"tools.jackson.databind.util.RawValue",
			"tools.jackson.databind.util.TokenBuffer",
			"com.fasterxml.jackson.databind.ser.BeanSerializerFactory",
			"com.fasterxml.jackson.databind.ser.BasicSerializerFactory",
			"com.fasterxml.jackson.databind.ser.SerializerFactory",
			"com.fasterxml.jackson.databind.ser.DefaultSerializerProvider",
			"com.fasterxml.jackson.databind.ser.SerializerCache",
			"com.fasterxml.jackson.databind.ser.BeanSerializer",
			"com.fasterxml.jackson.databind.ser.BeanSerializerBuilder",
			"com.fasterxml.jackson.databind.ser.PropertyBuilder",
			"com.fasterxml.jackson.databind.ser.std.BeanSerializerBase",
			"com.fasterxml.jackson.databind.deser.BeanDeserializerFactory",
			"com.fasterxml.jackson.databind.deser.BasicDeserializerFactory",
			"com.fasterxml.jackson.databind.deser.DeserializerFactory",
			"com.fasterxml.jackson.databind.deser.DefaultDeserializationContext",
			"com.fasterxml.jackson.databind.deser.DeserializerCache",
			"com.fasterxml.jackson.databind.deser.BeanDeserializer",
			"com.fasterxml.jackson.databind.deser.BeanDeserializerBase",
			"com.fasterxml.jackson.databind.deser.BeanDeserializerBuilder",
			"com.fasterxml.jackson.databind.deser.BuilderBasedDeserializer",
			"com.fasterxml.jackson.databind.deser.ValueInstantiator",
			"com.fasterxml.jackson.databind.deser.ValueInstantiators",
			"com.fasterxml.jackson.databind.deser.std.StdValueInstantiator",
			"com.fasterxml.jackson.databind.deser.std.ThrowableDeserializer",
			"com.fasterxml.jackson.databind.deser.SettableBeanProperty",
			"com.fasterxml.jackson.databind.deser.SettableAnyProperty",
			"com.fasterxml.jackson.databind.deser.CreatorProperty",
			"com.fasterxml.jackson.databind.cfg.SerializerFactoryConfig",
			"com.fasterxml.jackson.databind.cfg.DeserializerFactoryConfig",
			"tools.jackson.databind.ser.BeanSerializerFactory",
			"tools.jackson.databind.ser.BasicSerializerFactory",
			"tools.jackson.databind.ser.SerializerFactory",
			"tools.jackson.databind.ser.SerializationContextExt",
			"tools.jackson.databind.ser.SerializerCache",
			"tools.jackson.databind.ser.BeanSerializer",
			"tools.jackson.databind.ser.BeanSerializerBuilder",
			"tools.jackson.databind.ser.PropertyBuilder",
			"tools.jackson.databind.deser.BeanDeserializerFactory",
			"tools.jackson.databind.deser.BasicDeserializerFactory",
			"tools.jackson.databind.deser.DeserializerFactory",
			"tools.jackson.databind.deser.DeserializationContextExt",
			"tools.jackson.databind.deser.DeserializerCache",
			"tools.jackson.databind.deser.BeanDeserializerBuilder",
			"tools.jackson.databind.deser.ValueInstantiator",
			"tools.jackson.databind.deser.ValueInstantiators",
			"tools.jackson.databind.deser.std.StdValueInstantiator",
			"tools.jackson.databind.deser.jdk.ThrowableDeserializer",
			"tools.jackson.databind.deser.SettableBeanProperty",
			"tools.jackson.databind.deser.SettableAnyProperty",
			"tools.jackson.databind.deser.CreatorProperty",
			"tools.jackson.databind.cfg.SerializerFactoryConfig",
			"tools.jackson.databind.cfg.DeserializerFactoryConfig");

	private static final String PLATFORM_PACKAGE = "prerna.";

	/**
	 * The twin of each mapper project code has used, kept for as long as the mapper
	 * itself.
	 */
	private static final Map<ObjectMapper, ObjectMapper> TWINS = Collections.synchronizedMap(new WeakHashMap<>());

	/**
	 * Every twin, so one that a mapper's own method hands back to project code is
	 * used as it is.
	 */
	private static final Set<ObjectMapper> GUARDED = Collections
			.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

	/**
	 * The packages project reactor code shares with the platform, as name
	 * prefixes: Jackson 2 and Jackson 3.
	 */
	static final List<String> SHARED_PACKAGES = List.of("com.fasterxml.jackson.", "tools.jackson.");

	/**
	 * Single classes kept out of the shared packages, so a project gets its own
	 * sandboxed copy: the helper that finds and creates classes by name.
	 */
	static final Set<String> UNSHARED_CLASSES = Set.of("com.fasterxml.jackson.databind.util.ClassUtil",
			"tools.jackson.databind.util.ClassUtil");

	/**
	 * Calls project reactor code cannot make, by owner then method name: the type
	 * factory's class lookup, creating a bean outside a mapper, and the calls that
	 * put an object of any class into a JSON tree or a token stream, on the tree,
	 * node factory and generator classes of Jackson 2 and Jackson 3.
	 */
	static final Map<String, Set<String>> BLOCKED_CALLS = Map.ofEntries(
			Map.entry("com/fasterxml/jackson/databind/type/TypeFactory", Set.of("findClass")),
			Map.entry("tools/jackson/databind/type/TypeFactory", Set.of("findClass")),
			Map.entry("com/fasterxml/jackson/databind/BeanDescription", Set.of("instantiateBean")),
			Map.entry("tools/jackson/databind/BeanDescription", Set.of("instantiateBean")),
			Map.entry("com/fasterxml/jackson/databind/node/JsonNodeCreator", Set.of("pojoNode", "rawValueNode")),
			Map.entry("com/fasterxml/jackson/databind/node/JsonNodeFactory", Set.of("pojoNode", "rawValueNode")),
			Map.entry("com/fasterxml/jackson/databind/node/ContainerNode", Set.of("pojoNode", "rawValueNode")),
			Map.entry("com/fasterxml/jackson/databind/node/ObjectNode", Set.of("putPOJO", "putRawValue")),
			Map.entry("com/fasterxml/jackson/databind/node/ArrayNode",
					Set.of("addPOJO", "insertPOJO", "addRawValue")),
			Map.entry("com/fasterxml/jackson/core/JsonGenerator", Set.of("writeEmbeddedObject")),
			Map.entry("tools/jackson/databind/node/JsonNodeCreator", Set.of("pojoNode", "rawValueNode")),
			Map.entry("tools/jackson/databind/node/JsonNodeFactory", Set.of("pojoNode", "rawValueNode")),
			Map.entry("tools/jackson/databind/node/ContainerNode", Set.of("pojoNode", "rawValueNode")),
			Map.entry("tools/jackson/databind/node/ObjectNode", Set.of("putPOJO", "putRawValue")),
			Map.entry("tools/jackson/databind/node/ArrayNode", Set.of("addPOJO", "insertPOJO", "addRawValue")),
			Map.entry("tools/jackson/core/JsonGenerator", Set.of("writeEmbeddedObject")));

	/**
	 * Calls project reactor code cannot make on any class, by method name. Each
	 * tells a Jackson mapper to create objects of the classes that the JSON, or a
	 * string, names.
	 */
	static final Set<String> BLOCKED_CALLS_ON_ANY_CLASS = Set.of("enableDefaultTyping",
			"enableDefaultTypingAsProperty", "activateDefaultTyping", "activateDefaultTypingAsProperty",
			"setDefaultTyping", "constructFromCanonical");

	/**
	 * The Jackson annotation that picks how a type is named in JSON.
	 */
	static final String JSON_TYPE_INFO = "Lcom/fasterxml/jackson/annotation/JsonTypeInfo;";

	/**
	 * The two settings of {@link #JSON_TYPE_INFO} that name a class.
	 */
	static final Set<String> JSON_TYPE_BY_CLASS_NAME = Set.of("CLASS", "MINIMAL_CLASS");

	private SandboxedJackson() {
	}

	/**
	 * Stands in for a mapper project code is about to call: the twin with the same
	 * settings that reads and builds the project's own classes only. A setting the
	 * project changes is changed on the twin, which every later call uses too.
	 *
	 * @param mapper the mapper the project holds, or null
	 * @return its twin, or null
	 */
	public static ObjectMapper guarded(ObjectMapper mapper) {
		if (mapper == null || GUARDED.contains(mapper)) {
			return mapper;
		}
		return TWINS.computeIfAbsent(mapper, original -> {
			ObjectMapper twin = original.copy().registerModule(new ProjectClassesOnly());
			GUARDED.add(twin);
			return twin;
		});
	}

	/**
	 * Stands in for the mapper a Jackson 2 parser or generator names as its codec,
	 * before project code calls it: the twin when it is a mapper.
	 *
	 * @param codec the codec the project holds, or null
	 * @return the mapper's twin, or the same codec when it is no mapper
	 */
	public static ObjectCodec guarded(ObjectCodec codec) {
		return codec instanceof ObjectMapper mapper ? guarded(mapper) : codec;
	}

	/**
	 * Binds a parser project code is about to read an object from to the twin of
	 * its mapper.
	 *
	 * @param parser the parser the project holds, or null
	 * @return the same parser
	 */
	public static JsonParser guarded(JsonParser parser) {
		if (parser != null && parser.getCodec() instanceof ObjectMapper mapper && !GUARDED.contains(mapper)) {
			parser.setCodec(guarded(mapper));
		}
		return parser;
	}

	/**
	 * Binds a generator project code is about to write an object with to the twin
	 * of its mapper.
	 *
	 * @param generator the generator the project holds, or null
	 * @return the same generator
	 */
	public static JsonGenerator guarded(JsonGenerator generator) {
		if (generator != null && generator.getCodec() instanceof ObjectMapper mapper && !GUARDED.contains(mapper)) {
			generator.setCodec(guarded(mapper));
		}
		return generator;
	}

	/**
	 * True for a Jackson class project reactor code cannot use.
	 *
	 * @param name fully-qualified class name
	 * @return true if the class is one Jackson reaches into objects with
	 */
	static boolean isBlocked(String name) {
		int nested = name.indexOf('$');
		String outer = nested < 0 ? name : name.substring(0, nested);
		if (OPEN_CLASSES.contains(outer)) {
			return false;
		}
		if (BLOCKED_CLASSES.contains(outer)) {
			return true;
		}
		for (String blocked : BLOCKED_PACKAGES) {
			if (name.startsWith(blocked)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Refuses a class a mapper is about to read from or build for project code,
	 * unless it is the project's own or plain data.
	 *
	 * @param type         the class
	 * @param usesMembers  true when the mapper would read or set the class's
	 *                     members by reflection
	 * @param binderIsOwn  true when the code that reads or builds the class is the
	 *                     project's own serializer or deserializer
	 * @throws SecurityException when the class is not the project's own and the
	 *                           mapper would reach into it, or it is a platform
	 *                           class
	 */
	static void requireBindable(Class<?> type, boolean usesMembers, boolean binderIsOwn) {
		if (type == null || SandboxedClassLoading.isProjectClass(type) || type.isEnum() || type.isPrimitive()
				|| type.isArray()) {
			return;
		}
		if (usesMembers || (type.getName().startsWith(PLATFORM_PACKAGE) && !binderIsOwn)) {
			throw new SecurityException("A JSON mapper in a project reactor reads and builds the project's own "
					+ "classes only, not " + type.getName() + "; pass its values as a map or a list instead");
		}
	}

	/**
	 * Refuses a member a mapper is about to read or set for project code when a
	 * class outside the project declares it and it is not public.
	 *
	 * @param member the field, method or constructor, or null
	 * @throws SecurityException when the member is a non-public member of a class
	 *                           that is not the project's own
	 */
	static void requireOpenMember(Member member) {
		if (member == null) {
			return;
		}
		Class<?> declaring = member.getDeclaringClass();
		if (SandboxedClassLoading.isProjectClass(declaring)) {
			return;
		}
		if (!Modifier.isPublic(member.getModifiers()) || !Modifier.isPublic(declaring.getModifiers())) {
			throw new SecurityException("A JSON mapper in a project reactor cannot reach " + member.getName() + " of "
					+ declaring.getName() + ", which is not public");
		}
	}

	/**
	 * The Jackson module that applies the rule to a twin.
	 */
	private static final class ProjectClassesOnly extends Module {

		@Override
		public String getModuleName() {
			return "semoss-project-classes-only";
		}

		@Override
		public Version version() {
			return Version.unknownVersion();
		}

		@Override
		public void setupModule(SetupContext context) {
			context.addBeanSerializerModifier(new WritingRule());
			context.addBeanDeserializerModifier(new ReadingRule());
		}
	}

	/**
	 * Checks each class a twin is about to write, and each member it would read.
	 */
	private static final class WritingRule extends BeanSerializerModifier {

		private static final long serialVersionUID = 1L;

		@Override
		public List<BeanPropertyWriter> changeProperties(SerializationConfig config, BeanDescription beanDesc,
				List<BeanPropertyWriter> beanProperties) {
			for (BeanPropertyWriter property : beanProperties) {
				AnnotatedMember member = property.getMember();
				requireOpenMember(member == null ? null : member.getMember());
			}
			return beanProperties;
		}

		@Override
		public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc,
				JsonSerializer<?> serializer) {
			requireBindable(beanDesc.getBeanClass(),
					serializer instanceof BeanSerializerBase || serializer instanceof JsonValueSerializer,
					SandboxedClassLoading.isProjectClass(serializer.getClass()));
			return serializer;
		}
	}

	/**
	 * Checks each class a twin is about to build, and each member it would set.
	 */
	private static final class ReadingRule extends BeanDeserializerModifier {

		private static final long serialVersionUID = 1L;

		@Override
		public BeanDeserializerBuilder updateBuilder(DeserializationConfig config, BeanDescription beanDesc,
				BeanDeserializerBuilder builder) {
			Iterator<SettableBeanProperty> properties = builder.getProperties();
			while (properties.hasNext()) {
				AnnotatedMember member = properties.next().getMember();
				requireOpenMember(member == null ? null : member.getMember());
			}
			return builder;
		}

		@Override
		public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc,
				JsonDeserializer<?> deserializer) {
			requireBindable(beanDesc.getBeanClass(), deserializer instanceof BeanDeserializerBase,
					SandboxedClassLoading.isProjectClass(deserializer.getClass()));
			return deserializer;
		}
	}
}
