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
package prerna.reactor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.openapi.Internal;
import prerna.openapi.PixelInput;
import prerna.openapi.PixelOutput;

/**
 * What a reactor declares about itself, read for the OpenAPI document the SDKs
 * are generated from.
 * <p>
 * Everything is inferred from what reactors already declare, so no reactor has
 * to change to be published:
 * <ul>
 * <li>inputs from {@code keysToGet} (the canonical name before any alias);</li>
 * <li>requiredness from {@code keyRequired}: with none, nothing is required,
 * the way {@code organizeKeys()} checks at runtime;</li>
 * <li>input types from {@code getKeyTypeForMCP};</li>
 * <li>descriptions from {@code getDescriptionForKey} and
 * {@code getReactorDescription}.</li>
 * </ul>
 * {@link PixelInput} refines one input and {@link PixelOutput} declares the
 * output, which cannot be inferred. Reading a contract constructs the reactor,
 * since its constructor sets {@code keysToGet}; it never runs it.
 *
 * @param name                the name it is called by in Pixel
 * @param description         its description, or a placeholder
 * @param descriptionInferred whether the description is a placeholder
 * @param inputs              its inputs, in {@code keysToGet} order
 * @param output              its declared output, or null when undeclared
 * @param internal            its {@link Internal}, or null
 */
public record ReactorContract(String name, String description, boolean descriptionInferred, List<Input> inputs,
		Output output, Internal internal) {

	/**
	 * One input.
	 *
	 * @param type         {@code String}, {@code Integer}, {@code Double},
	 *                     {@code Boolean}, {@code List}, {@code Map}, an enum, or
	 *                     {@code Object} for any value
	 * @param items        the element type of a {@code List}, or {@code Void}
	 * @param typeInferred whether the type came from {@code getKeyTypeForMCP}
	 *                     rather than a {@link PixelInput}
	 */
	public record Input(String name, Class<?> type, Class<?> items, String description, boolean required,
			boolean typeInferred, boolean descriptionInferred) {
	}

	/** A declared output, from {@link PixelOutput}. */
	public record Output(Class<?> type, Class<?> items, String description) {
	}

	/** Whether anything in the contract is inferred rather than declared. */
	public boolean inferred() {
		return this.descriptionInferred || this.output == null
				|| this.inputs.stream().anyMatch(i -> i.typeInferred() || i.descriptionInferred());
	}

	/** The name a reactor class is called by in Pixel. */
	public static String pixelName(Class<?> reactorClass) {
		return ReactorNames.pixelName(reactorClass.getSimpleName());
	}

	/** Whether a reactor class is a general one rather than specific to one frame type. */
	public static boolean isGeneral(Class<?> reactorClass) {
		return ReactorNames.frameType(reactorClass.getPackageName()) == null;
	}

	/**
	 * Reads a reactor's contract.
	 *
	 * @throws ReflectiveOperationException when the reactor cannot be constructed
	 * @throws IllegalArgumentException     when it is not an {@code AbstractReactor},
	 *                                      or declares a key twice
	 * @throws IllegalStateException        when a {@link PixelInput} names a key
	 *                                      the reactor does not declare
	 */
	public static ReactorContract of(Class<?> reactorClass) throws ReflectiveOperationException {
		if (!AbstractReactor.class.isAssignableFrom(reactorClass)) {
			throw new IllegalArgumentException(reactorClass.getName() + " is not an AbstractReactor");
		}
		AbstractReactor reactor = (AbstractReactor) reactorClass.getDeclaredConstructor().newInstance();
		String name = pixelName(reactorClass);

		Map<String, PixelInput> overrides = new LinkedHashMap<>();
		for (PixelInput input : reactorClass.getAnnotationsByType(PixelInput.class)) {
			overrides.put(input.name(), input);
		}

		List<Input> inputs = new ArrayList<>();
		Set<String> seen = new LinkedHashSet<>();
		String[] keys = reactor.keysToGet == null ? new String[0] : reactor.keysToGet;
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i] == null ? "" : keys[i].split(",", -1)[0].trim();
			if (key.isEmpty()) {
				throw new IllegalArgumentException(name + " has an empty key at position " + i + " of keysToGet");
			}
			if (!seen.add(key)) {
				throw new IllegalArgumentException(name + " declares the key \"" + key + "\" twice");
			}
			boolean required = reactor.keyRequired != null && i < reactor.keyRequired.length
					&& reactor.keyRequired[i] == 1;

			Class<?> type = typeOf(reactor.getKeyTypeForMCP(key));
			Class<?> items = Void.class;
			boolean typeInferred = true;
			String description = reactor.getDescriptionForKey(key);
			boolean descriptionInferred = description == null || description.isBlank();
			if (descriptionInferred) {
				description = "The " + key + " input";
			}

			PixelInput override = overrides.remove(key);
			if (override != null) {
				if (override.type() != Void.class) {
					type = override.type();
					items = override.items();
					typeInferred = false;
				}
				if (!override.description().isBlank()) {
					description = override.description();
					descriptionInferred = false;
				}
				if (override.required() == PixelInput.Requirement.REQUIRED) {
					required = true;
				} else if (override.required() == PixelInput.Requirement.OPTIONAL) {
					required = false;
				}
			}
			inputs.add(new Input(key, type, items, description, required, typeInferred, descriptionInferred));
		}
		if (!overrides.isEmpty()) {
			throw new IllegalStateException(reactorClass.getName() + " has @PixelInput for " + overrides.keySet()
					+ ", which are not in its keysToGet");
		}

		String description = reactor.getReactorDescription();
		boolean descriptionInferred = description == null || description.isBlank();
		if (descriptionInferred) {
			description = name + " reactor";
		}

		PixelOutput declared = reactorClass.getAnnotation(PixelOutput.class);
		Output output = declared == null ? null
				: new Output(declared.value(), declared.items(), declared.description());

		return new ReactorContract(name, description, descriptionInferred, List.copyOf(inputs), output,
				reactorClass.getAnnotation(Internal.class));
	}

	/** Maps the type a reactor reports for MCP to a Java type. */
	private static Class<?> typeOf(IReactor.MCP_KEY_TYPE type) {
		if (type == null) {
			return String.class;
		}
		return switch (type) {
		case INTEGER -> Integer.class;
		case NUMBER -> Double.class;
		case BOOLEAN -> Boolean.class;
		case ARRAY -> List.class;
		case OBJECT -> Map.class;
		default -> String.class;
		};
	}
}
