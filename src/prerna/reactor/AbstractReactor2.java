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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * A reactor that declares its inputs as a record and its output as a Java type,
 * so its contract can be read without running it.
 * <p>
 * The input record is the only place the inputs are declared. From it this class
 * fills {@link #keysToGet} and {@link #keyRequired} (so {@code organizeKeys()},
 * {@code --help} and {@code asMcpTool()} keep working), answers
 * {@link #getDescriptionForKey(String)} and {@link #getKeyTypeForMCP(String)},
 * and builds the typed input passed to {@link #run(Record)}. The return value is
 * wrapped in a {@link NounMetadata} whose data type follows from it.
 *
 * <pre>{@code
 * public class StopPixelExecutionReactor
 * 		extends AbstractReactor2<StopPixelExecutionReactor.Input, String> {
 *
 * 	public record Input(@Schema(description = "The id for the job") String id) {
 * 	}
 *
 * 	protected String run(Input input) {
 * 		return "Cancel requested for pixel job " + input.id();
 * 	}
 * }
 * }</pre>
 *
 * Record components are required unless they are annotated
 * {@code @Schema(requiredMode = NOT_REQUIRED)}; an optional component must use a
 * boxed type, and is {@code null} when it is not passed. Supported component
 * types are {@code String}, {@code Integer}/{@code int}, {@code Long}/{@code long},
 * {@code Double}/{@code double}, {@code Boolean}/{@code boolean}, enums,
 * {@code List} of those, {@code Map} and {@code Object} (any value). A reactor
 * whose output has no fixed type returns {@link NounMetadata}, or {@code Object},
 * and is annotated {@link DynamicOutput}.
 * <p>
 * The SDK generator reads {@link #inputType(Class)} and {@link #outputType(Class)}
 * from the class alone; they never construct the reactor.
 *
 * @param <I> the input record; {@link NoInput} when there are none
 * @param <O> the output type
 */
public abstract class AbstractReactor2<I extends Record, O> extends AbstractReactor {

	private final Class<? extends Record> inputRecord;
	private final RecordComponent[] inputComponents;

	protected AbstractReactor2() {
		this.inputRecord = inputType(getClass());
		this.inputComponents = this.inputRecord.getRecordComponents();

		for (RecordComponent component : this.inputComponents) {
			checkSupportedInput(component);
		}
		Type output = outputType(getClass());
		if (isDynamicOutput(output) && getClass().getAnnotation(DynamicOutput.class) == null) {
			throw new IllegalStateException(getClass().getName()
					+ " returns a value with no fixed type; annotate it with @DynamicOutput(\"why\")");
		}

		this.keysToGet = Arrays.stream(this.inputComponents).map(RecordComponent::getName).toArray(String[]::new);
		this.keyRequired = Arrays.stream(this.inputComponents).mapToInt(c -> isRequired(c) ? 1 : 0).toArray();
	}

	/**
	 * Runs the reactor.
	 *
	 * @param input the inputs, already checked: required ones are present and
	 *              every value has its declared type
	 * @return the output; a {@link NounMetadata} is returned as it is
	 */
	protected abstract O run(I input);

	/**
	 * The operation types of the returned noun. Defaults to {@code OPERATION},
	 * the same as {@code new NounMetadata(value, type)}.
	 */
	protected List<PixelOperationType> getOperationTypes() {
		return List.of(PixelOperationType.OPERATION);
	}

	@Override
	public final NounMetadata execute() {
		organizeKeys();
		O result = run(readInput());
		if (result instanceof NounMetadata noun) {
			return noun;
		}
		return new NounMetadata(toNounValue(result), dataTypeFor(result), getOperationTypes());
	}

	@Override
	protected String getDescriptionForKey(String key) {
		for (RecordComponent component : this.inputComponents) {
			if (component.getName().equals(key)) {
				Schema schema = schemaOf(component);
				if (schema != null && !schema.description().isEmpty()) {
					return schema.description();
				}
			}
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		for (RecordComponent component : this.inputComponents) {
			if (component.getName().equals(key)) {
				Class<?> type = component.getType();
				if (type == String.class || type.isEnum()) {
					return MCP_KEY_TYPE.STRING;
				} else if (isOneOf(type, Integer.class, int.class, Long.class, long.class)) {
					return MCP_KEY_TYPE.INTEGER;
				} else if (isOneOf(type, Double.class, double.class)) {
					return MCP_KEY_TYPE.NUMBER;
				} else if (isOneOf(type, Boolean.class, boolean.class)) {
					return MCP_KEY_TYPE.BOOLEAN;
				} else if (List.class.isAssignableFrom(type)) {
					return MCP_KEY_TYPE.ARRAY;
				} else if (Map.class.isAssignableFrom(type)) {
					return MCP_KEY_TYPE.OBJECT;
				}
			}
		}
		return super.getKeyTypeForMCP(key);
	}

	/*
	 * Reading the contract from the class, without constructing it
	 */

	/**
	 * Returns the input record of a reactor class.
	 *
	 * @throws IllegalArgumentException if the class does not extend
	 *                                  {@code AbstractReactor2} with concrete type
	 *                                  arguments
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends Record> inputType(Class<?> reactorClass) {
		Type input = typeArguments(reactorClass)[0];
		if (!(input instanceof Class<?> inputClass) || !inputClass.isRecord()) {
			throw new IllegalArgumentException(reactorClass.getName() + ": the input must be a record, got " + input);
		}
		return (Class<? extends Record>) inputClass;
	}

	/**
	 * Returns the declared output type of a reactor class, such as
	 * {@code String.class} or a {@code ParameterizedType} for {@code List<Foo>}.
	 */
	public static Type outputType(Class<?> reactorClass) {
		return typeArguments(reactorClass)[1];
	}

	/**
	 * Whether an output type has no fixed shape: {@link NounMetadata} or
	 * {@code Object}.
	 */
	public static boolean isDynamicOutput(Type output) {
		return output == NounMetadata.class || output == Object.class;
	}

	/**
	 * Whether an input record component is required: everything is, unless it is
	 * annotated {@code @Schema(requiredMode = NOT_REQUIRED)}.
	 */
	public static boolean isRequired(RecordComponent component) {
		Schema schema = schemaOf(component);
		return schema == null || schema.requiredMode() != Schema.RequiredMode.NOT_REQUIRED;
	}

	/**
	 * Returns the {@code @Schema} on a record component. Annotations on a
	 * component are copied to its accessor and field, which is where they are read.
	 */
	public static Schema schemaOf(RecordComponent component) {
		Schema schema = component.getAccessor().getAnnotation(Schema.class);
		if (schema != null) {
			return schema;
		}
		try {
			Field field = component.getDeclaringRecord().getDeclaredField(component.getName());
			return field.getAnnotation(Schema.class);
		} catch (NoSuchFieldException e) {
			return null;
		}
	}

	private static Type[] typeArguments(Class<?> reactorClass) {
		Class<?> current = reactorClass;
		while (current != null && current.getSuperclass() != AbstractReactor2.class) {
			current = current.getSuperclass();
		}
		if (current == null) {
			throw new IllegalArgumentException(reactorClass.getName() + " does not extend AbstractReactor2");
		}
		Type superclass = current.getGenericSuperclass();
		if (!(superclass instanceof ParameterizedType parameterized)) {
			throw new IllegalArgumentException(
					current.getName() + " must give AbstractReactor2 its input and output types");
		}
		Type[] arguments = parameterized.getActualTypeArguments();
		for (Type argument : arguments) {
			if (argument instanceof java.lang.reflect.TypeVariable<?>) {
				throw new IllegalArgumentException(current.getName()
						+ " must give AbstractReactor2 concrete input and output types, not " + argument);
			}
		}
		return arguments;
	}

	/*
	 * Building the input
	 */

	private void checkSupportedInput(RecordComponent component) {
		Class<?> type = component.getType();
		if (type.isPrimitive() && !isRequired(component)) {
			throw new IllegalStateException(this.inputRecord.getName() + "." + component.getName()
					+ " is optional, so it must use a boxed type rather than " + type.getName());
		}
		if (List.class.isAssignableFrom(type)) {
			if (!isScalar(listElementType(component))) {
				throw new IllegalStateException(this.inputRecord.getName() + "." + component.getName()
						+ ": a list input must hold strings, numbers, booleans or enums");
			}
			return;
		}
		if (!isScalar(type) && !Map.class.isAssignableFrom(type) && type != Object.class) {
			throw new IllegalStateException(this.inputRecord.getName() + "." + component.getName()
					+ ": input type " + type.getName() + " is not supported");
		}
	}

	@SuppressWarnings("unchecked")
	private I readInput() {
		Object[] values = new Object[this.inputComponents.length];
		Class<?>[] types = new Class<?>[this.inputComponents.length];
		for (int i = 0; i < this.inputComponents.length; i++) {
			types[i] = this.inputComponents[i].getType();
			values[i] = readComponent(this.inputComponents[i]);
		}
		try {
			Constructor<?> constructor = this.inputRecord.getDeclaredConstructor(types);
			constructor.setAccessible(true);
			return (I) constructor.newInstance(values);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof RuntimeException runtime) {
				throw runtime;
			}
			throw new IllegalArgumentException(e.getCause());
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Cannot build " + this.inputRecord.getName(), e);
		}
	}

	private Object readComponent(RecordComponent component) {
		String name = component.getName();
		Class<?> type = component.getType();

		if (List.class.isAssignableFrom(type)) {
			GenRowStruct values = nounsFor(name);
			if (values == null) {
				return null;
			}
			Class<?> element = listElementType(component);
			List<Object> list = new ArrayList<>();
			for (Object value : values.getAllValues()) {
				list.add(convertScalar(value, element, name));
			}
			return list;
		}

		if (Map.class.isAssignableFrom(type) || type == Object.class) {
			GenRowStruct values = nounsFor(name);
			if (values == null) {
				return null;
			}
			Object value = values.get(0);
			if (Map.class.isAssignableFrom(type) && !(value instanceof Map)) {
				throw new IllegalArgumentException(name + " must be a map");
			}
			return value;
		}

		// organizeKeys() already read scalars, named or by position, as text
		String text = this.keyValue.get(name);
		if (text == null) {
			return null;
		}
		return convertScalar(text, type, name);
	}

	/** The values passed for a key, or by position when the reactor has one input. */
	private GenRowStruct nounsFor(String name) {
		GenRowStruct values = this.store == null ? null : this.store.getGenRowStruct(name);
		if ((values == null || values.isEmpty()) && this.inputComponents.length == 1) {
			values = this.curRow;
		}
		return values == null || values.isEmpty() ? null : values;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Object convertScalar(Object value, Class<?> type, String name) {
		try {
			if (type == String.class) {
				return value.toString();
			} else if (type == Integer.class || type == int.class) {
				return value instanceof Number n && !(value instanceof Double || value instanceof Float)
						? n.intValue()
						: new BigDecimal(value.toString()).intValueExact();
			} else if (type == Long.class || type == long.class) {
				return value instanceof Number n && !(value instanceof Double || value instanceof Float)
						? n.longValue()
						: new BigDecimal(value.toString()).longValueExact();
			} else if (type == Double.class || type == double.class) {
				return value instanceof Number n ? n.doubleValue() : Double.parseDouble(value.toString());
			} else if (type == Boolean.class || type == boolean.class) {
				if (value instanceof Boolean b) {
					return b;
				}
				String text = value.toString();
				if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("false")) {
					return Boolean.parseBoolean(text);
				}
				throw new IllegalArgumentException(name + " must be true or false, got " + text);
			} else if (type.isEnum()) {
				try {
					return Enum.valueOf((Class<? extends Enum>) type, value.toString());
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException(name + " must be one of "
							+ Arrays.toString(type.getEnumConstants()) + ", got " + value);
				}
			} else if (type == Object.class) {
				return value;
			}
		} catch (NumberFormatException | ArithmeticException e) {
			throw new IllegalArgumentException(name + " must be a whole number or a number as declared, got " + value);
		}
		throw new IllegalStateException(name + ": input type " + type.getName() + " is not supported");
	}

	private static Class<?> listElementType(RecordComponent component) {
		Type generic = component.getGenericType();
		if (generic instanceof ParameterizedType parameterized
				&& parameterized.getActualTypeArguments()[0] instanceof Class<?> element) {
			return element;
		}
		throw new IllegalStateException(
				component.getDeclaringRecord().getName() + "." + component.getName() + " must be a List<...> of one type");
	}

	private static boolean isScalar(Class<?> type) {
		return type == String.class || type.isEnum() || isOneOf(type, Integer.class, int.class, Long.class, long.class,
				Double.class, double.class, Boolean.class, boolean.class);
	}

	private static boolean isOneOf(Class<?> type, Class<?>... options) {
		for (Class<?> option : options) {
			if (type == option) {
				return true;
			}
		}
		return false;
	}

	/*
	 * Wrapping the output
	 */

	private static Object toNounValue(Object result) {
		return result instanceof Enum<?> e ? e.name() : result;
	}

	private static PixelDataType dataTypeFor(Object result) {
		if (result == null) {
			return PixelDataType.NULL_VALUE;
		} else if (result instanceof String || result instanceof Enum<?>) {
			return PixelDataType.CONST_STRING;
		} else if (result instanceof Integer || result instanceof Long || result instanceof Short
				|| result instanceof Byte) {
			return PixelDataType.CONST_INT;
		} else if (result instanceof Number) {
			return PixelDataType.CONST_DECIMAL;
		} else if (result instanceof Boolean) {
			return PixelDataType.BOOLEAN;
		} else if (result instanceof Map || result instanceof Record) {
			return PixelDataType.MAP;
		} else if (result instanceof Collection || result.getClass().isArray()) {
			return PixelDataType.VECTOR;
		}
		throw new IllegalStateException("Output type " + result.getClass().getName() + " is not supported");
	}
}
