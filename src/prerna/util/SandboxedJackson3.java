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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import tools.jackson.core.Version;
import tools.jackson.databind.BeanDescription;
import tools.jackson.databind.DeserializationConfig;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationConfig;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.cfg.MapperBuilder;
import tools.jackson.databind.deser.BeanDeserializerBuilder;
import tools.jackson.databind.deser.SettableBeanProperty;
import tools.jackson.databind.deser.ValueDeserializerModifier;
import tools.jackson.databind.deser.bean.BeanDeserializerBase;
import tools.jackson.databind.introspect.AnnotatedMember;
import tools.jackson.databind.ser.BeanPropertyWriter;
import tools.jackson.databind.ser.ValueSerializerModifier;
import tools.jackson.databind.ser.bean.BeanSerializerBase;
import tools.jackson.databind.ser.jackson.JsonValueSerializer;

/**
 * The sandbox rules for Jackson 3 mappers, the same as {@link SandboxedJackson}
 * applies to Jackson 2: for project reactor code a mapper reads and builds the
 * project's own classes only, and every other value is JSON data. Each call
 * project code makes on a mapper runs on a twin of it that has the same settings
 * and the rule.
 */
public final class SandboxedJackson3 {

	/**
	 * Internal name of this class, which the rewritten calls name.
	 */
	static final String INTERNAL_NAME = "prerna/util/SandboxedJackson3";

	/**
	 * Internal name of the Jackson 3 mapper. A call on it, or on a mapper class
	 * that extends it, runs on a twin.
	 */
	static final String OBJECT_MAPPER = "tools/jackson/databind/ObjectMapper";

	/**
	 * The twin of each mapper project code has used, kept for as long as the mapper
	 * itself.
	 */
	private static final Map<ObjectMapper, ObjectMapper> TWINS = Collections.synchronizedMap(new WeakHashMap<>());

	/**
	 * Every twin, so one that reaches project code is used as it is.
	 */
	private static final Set<ObjectMapper> GUARDED = Collections
			.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

	private SandboxedJackson3() {
	}

	/**
	 * Stands in for a mapper project code is about to call: the twin with the same
	 * settings that reads and builds the project's own classes only.
	 *
	 * @param mapper the mapper the project holds, or null
	 * @return its twin, or null
	 */
	public static ObjectMapper guarded(ObjectMapper mapper) {
		if (mapper == null || GUARDED.contains(mapper)) {
			return mapper;
		}
		return TWINS.computeIfAbsent(mapper, original -> {
			MapperBuilder<?, ?> builder = original.rebuild();
			ObjectMapper twin = builder.addModule(new ProjectClassesOnly()).build();
			GUARDED.add(twin);
			return twin;
		});
	}

	/**
	 * The Jackson module that applies the rule to a twin.
	 */
	private static final class ProjectClassesOnly extends JacksonModule {

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
			context.addSerializerModifier(new WritingRule());
			context.addDeserializerModifier(new ReadingRule());
		}
	}

	/**
	 * Checks each class a twin is about to write, and each member it would read.
	 */
	private static final class WritingRule extends ValueSerializerModifier {

		private static final long serialVersionUID = 1L;

		@Override
		public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
				BeanDescription.Supplier beanDesc, List<BeanPropertyWriter> beanProperties) {
			for (BeanPropertyWriter property : beanProperties) {
				AnnotatedMember member = property.getMember();
				SandboxedJackson.requireOpenMember(member == null ? null : member.getMember());
			}
			return beanProperties;
		}

		@Override
		public ValueSerializer<?> modifySerializer(SerializationConfig config, BeanDescription.Supplier beanDesc,
				ValueSerializer<?> serializer) {
			SandboxedJackson.requireBindable(beanDesc.getBeanClass(),
					serializer instanceof BeanSerializerBase || serializer instanceof JsonValueSerializer,
					SandboxedClassLoading.isProjectClass(serializer.getClass()));
			return serializer;
		}
	}

	/**
	 * Checks each class a twin is about to build, and each member it would set.
	 */
	private static final class ReadingRule extends ValueDeserializerModifier {

		private static final long serialVersionUID = 1L;

		@Override
		public BeanDeserializerBuilder updateBuilder(DeserializationConfig config, BeanDescription.Supplier beanDesc,
				BeanDeserializerBuilder builder) {
			Iterator<SettableBeanProperty> properties = builder.getProperties();
			while (properties.hasNext()) {
				AnnotatedMember member = properties.next().getMember();
				SandboxedJackson.requireOpenMember(member == null ? null : member.getMember());
			}
			return builder;
		}

		@Override
		public ValueDeserializer<?> modifyDeserializer(DeserializationConfig config,
				BeanDescription.Supplier beanDesc, ValueDeserializer<?> deserializer) {
			SandboxedJackson.requireBindable(beanDesc.getBeanClass(), deserializer instanceof BeanDeserializerBase,
					SandboxedClassLoading.isProjectClass(deserializer.getClass()));
			return deserializer;
		}
	}
}
