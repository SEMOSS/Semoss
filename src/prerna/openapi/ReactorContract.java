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
package prerna.openapi;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.ReactorNames;

/**
 * What a reactor declares about itself, read for the OpenAPI document the SDKs
 * are generated from. It comes from the reactor's
 * {@link AbstractReactor#getReactorDefinition()}, the same definition its MCP
 * tool reads, with placeholders and flags for what the reactor leaves out:
 * <ul>
 * <li>inputs are the keys in {@code keysToGet} (the canonical name before any
 * alias);</li>
 * <li>a key is required when {@code keyRequired} or the definition says so;
 * with neither, it is not, the way {@code organizeKeys()} checks at
 * runtime;</li>
 * <li>a key with no declared type takes the default type, and anything with no
 * description gets a placeholder; both are flagged as inferred.</li>
 * </ul>
 * Reading a contract constructs the reactor, since its constructor sets
 * {@code keysToGet}; it never runs it.
 *
 * @param name                the name it is called by in Pixel
 * @param description         its description, or a placeholder
 * @param descriptionInferred whether the description is a placeholder
 * @param inputs              its inputs, in {@code keysToGet} order
 * @param noKeys              whether it says it takes no keys on purpose
 * @param output              its declared output, or null when undeclared
 * @param internal            its {@link Internal}, or null
 */
public record ReactorContract(String name, String description, boolean descriptionInferred, List<Input> inputs,
		boolean noKeys, Output output, Internal internal) {

	/**
	 * The methods that, overridden below {@link AbstractReactor}, mean a reactor
	 * declares something.
	 */
	private static final Map<String, List<Class<?>>> DECLARING_METHODS = Map.of("getReactorDefinition", List.of(),
			"getReactorDescription", List.of(), "getDescriptionForKey", List.of(String.class), "getKeyTypeForMCP",
			List.of(String.class));

	/** Whether each class itself overrides one of {@link #DECLARING_METHODS}. */
	private static final ClassValue<Boolean> DECLARES = new ClassValue<>() {
		@Override
		protected Boolean computeValue(Class<?> type) {
			for (Method method : type.getDeclaredMethods()) {
				List<Class<?>> parameters = DECLARING_METHODS.get(method.getName());
				if (parameters != null && !method.isBridge()
						&& Arrays.asList(method.getParameterTypes()).equals(parameters)) {
					return true;
				}
			}
			return false;
		}
	};

	/**
	 * One input.
	 *
	 * @param type         {@code String}, {@code Integer}, {@code Double},
	 *                     {@code Boolean}, {@code List}, {@code Map}, an enum, a
	 *                     record, or {@code Object} for any value
	 * @param items        the element type of a {@code List}, or {@code Void}
	 * @param typeInferred whether the type is the default rather than declared
	 */
	public record Input(String name, Class<?> type, Class<?> items, String description, boolean required,
			boolean typeInferred, boolean descriptionInferred) {
	}

	/**
	 * A declared output.
	 *
	 * @param type        the Java type, or null when the output is a JSON Schema
	 * @param items       the element type of a {@code List}, or {@code Void}
	 * @param schema      the JSON Schema the reactor gave, as JSON text, or null
	 * @param description what the output is, or null
	 */
	public record Output(Class<?> type, Class<?> items, String schema, String description) {
	}

	/** Whether anything in the contract is inferred rather than declared. */
	public boolean inferred() {
		return this.descriptionInferred || this.output == null || (this.inputs.isEmpty() && !this.noKeys)
				|| this.inputs.stream().anyMatch(i -> i.typeInferred() || i.descriptionInferred());
	}

	/** The name a reactor class is called by in Pixel. */
	public static String pixelName(Class<?> reactorClass) {
		return ReactorNames.pixelName(reactorClass.getSimpleName());
	}

	/**
	 * Whether a reactor class is a general one rather than specific to one frame
	 * type.
	 */
	public static boolean isGeneral(Class<?> reactorClass) {
		return ReactorNames.frameType(reactorClass.getPackageName()) == null;
	}

	/**
	 * Whether a reactor class declares anything about itself: it or a parent below
	 * {@link AbstractReactor} overrides {@code getReactorDefinition} or one of the
	 * deprecated description methods, or it is marked {@link Internal}.
	 */
	public static boolean isDefined(Class<?> reactorClass) {
		if (!AbstractReactor.class.isAssignableFrom(reactorClass)) {
			return false;
		}
		if (reactorClass.getAnnotation(Internal.class) != null) {
			return true;
		}
		for (Class<?> type = reactorClass; type != null && type != AbstractReactor.class; type = type.getSuperclass()) {
			if (DECLARES.get(type)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Reads a reactor's contract.
	 *
	 * @throws ReflectiveOperationException when the reactor cannot be constructed
	 * @throws IllegalArgumentException     when it is not an
	 *                                      {@code AbstractReactor}
	 * @throws IllegalStateException        when its definition has problems, such
	 *                                      as a key listed twice in
	 *                                      {@code keysToGet} or a key described
	 *                                      that is not in it
	 */
	public static ReactorContract of(Class<?> reactorClass) throws ReflectiveOperationException {
		if (!AbstractReactor.class.isAssignableFrom(reactorClass)) {
			throw new IllegalArgumentException(reactorClass.getName() + " is not an AbstractReactor");
		}
		AbstractReactor reactor = (AbstractReactor) reactorClass.getDeclaredConstructor().newInstance();
		String name = pixelName(reactorClass);

		ReactorDefinition definition = reactor.getReactorDefinition();
		List<String> problems = definition.getProblems();
		if (!problems.isEmpty()) {
			throw new IllegalStateException(String.join("; ", problems));
		}

		List<Input> inputs = new ArrayList<>();
		for (ReactorDefinition.Key key : definition.getKeys()) {
			Class<?> type = key.getTypeOrDefault();
			boolean typeInferred = key.getType() == null;
			String description = key.getDescription();
			boolean descriptionInferred = description == null || description.isBlank();
			if (descriptionInferred) {
				description = "The " + key.getName() + " input";
			}
			inputs.add(new Input(key.getName(), type, key.getItems(), description,
					Boolean.TRUE.equals(key.getRequired()), typeInferred, descriptionInferred));
		}

		String description = definition.getDescription();
		boolean descriptionInferred = description == null || description.isBlank();
		if (descriptionInferred) {
			description = name + " reactor";
		}

		ReactorDefinition.Output declared = definition.getOutput();
		Output output = declared == null ? null
				: new Output(declared.getType(), declared.getItems(),
						declared.getSchema() == null ? null : declared.getSchema().toString(),
						declared.getDescription());

		return new ReactorContract(name, description, descriptionInferred, List.copyOf(inputs), definition.isNoKeys(),
				output, reactorClass.getAnnotation(Internal.class));
	}
}
