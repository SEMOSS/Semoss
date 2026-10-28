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
package prerna.contract;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.AnnotatedArrayType;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.annotations.SerializedName;

import prerna.contract.OperationContract.Diagnostic;
import prerna.contract.OperationContract.Severity;
import prerna.contract.TypeContract.*;

/** Per-read type graph; never reads field values or constructs described classes. */
final class TypeReader {
	final Map<String, ObjectType> definitions = new TreeMap<>();
	final List<Diagnostic> diagnostics = new ArrayList<>();
	private int depth;

	TypeUse read(Type type, AnnotatedType annotated, AnnotatedElement declaration, String source) {
		return read(type, annotated, declaration, source, null);
	}

	TypeUse readInline(Returns.Field[] fields, AnnotatedElement declaration, String source) {
		return read(Object.class, null, declaration, source, fields);
	}

	private TypeUse read(Type type, AnnotatedType annotated, AnnotatedElement declaration, String source,
			Returns.Field[] inlineFields) {
		Class<?> raw = raw(type);
		Description description = annotation(Description.class, declaration, annotated, raw);
		Format format = annotation(Format.class, declaration, annotated, raw);
		Dynamic dynamic = annotation(Dynamic.class, declaration, annotated, raw);
		Unverified unverified = annotation(Unverified.class, declaration, annotated, raw);
		OneOf union = annotation(OneOf.class, declaration, annotated, raw);
		boolean nullable = annotation(Nullable.class, declaration, annotated, raw) != null;
		if (dynamic != null && unverified != null) error(source, "Dynamic and Unverified cannot describe the same value");
		if (format != null) nonBlank(format.value(), source, "Format");
		String reason = dynamic != null ? dynamic.reason() : unverified != null ? unverified.reason() : "";
		Knowledge knowledge = dynamic != null ? Knowledge.DYNAMIC : unverified != null ? Knowledge.UNVERIFIED : Knowledge.VERIFIED;
		if (knowledge != Knowledge.VERIFIED) nonBlank(reason, source, "Type uncertainty reason");
		Shape shape;
		TypeUse captured = null;
		if (++depth > 128) {
			error(source, "Type expansion exceeds 128 levels; use a finite recursive reference");
			shape = new Unknown();
		} else if (inlineFields != null) {
			if (union != null) error(source, "Inline return fields cannot also declare OneOf");
			shape = inlineObject(inlineFields, source);
		} else if (union != null) {
			List<TypeUse> alternatives = new ArrayList<>();
			if (union.value().length < 2) error(source, "OneOf requires at least two distinct alternatives");
			for (Class<?> alternative : Arrays.stream(union.value()).sorted(Comparator.comparing(Class::getName)).toList()) {
				if (alternative == raw) {
					error(source, "OneOf cannot directly include its own declaration");
					continue;
				}
				TypeUse value = read(alternative, null, null, source + " alternative " + alternative.getName());
				if (alternatives.contains(value)) error(source, "Duplicate OneOf alternative");
				else alternatives.add(value);
			}
			shape = new Union(alternatives);
		} else if (raw != null && TypeRef.class.isAssignableFrom(raw)) {
			captured = capture(type, annotated, declaration == null ? raw : declaration, source);
			shape = captured.shape();
		} else {
			shape = shape(type, annotated, source);
		}
		depth--;
		if (captured != null && knowledge == Knowledge.VERIFIED) {
			knowledge = captured.knowledge();
			reason = captured.reason();
		}
		if (shape instanceof Unknown && knowledge == Knowledge.VERIFIED) {
			knowledge = Knowledge.UNVERIFIED;
			reason = "No verified wire shape for " + type.getTypeName();
		}
		if (knowledge == Knowledge.UNVERIFIED && captured == null) warning(source, reason);
		String wireFormat = format == null ? captured == null ? numericFormat(raw) : captured.format() : format.value();
		return new TypeUse(shape, nullable || captured != null && captured.nullable(),
				description == null ? captured == null ? "" : captured.description() : description.value(), wireFormat, knowledge, reason);
	}

	/** Reuse the same referenced object model as records, with an operation-local identity. */
	private Reference inlineObject(Returns.Field[] fields, String source) {
		Map<String, Property> properties = new TreeMap<>();
		for (Returns.Field field : fields) {
			String fieldSource = source + "." + field.name();
			nonBlank(field.name(), fieldSource, "Wire field name");
			TypeUse type = read(field.type(), null, null, fieldSource);
			Shape shape = type.shape();
			if (shape instanceof Empty) error(fieldSource, "An object field cannot have a void wire type");
			if (field.values().length > 0) {
				List<String> values = Arrays.stream(field.values()).sorted().toList();
				if (values.stream().distinct().count() != values.size()) error(fieldSource, "Duplicate allowed field value");
				if (shape instanceof Scalar scalar && scalar.kind() == ScalarKind.STRING) {
					if (!scalar.values().isEmpty() && !scalar.values().containsAll(values))
						error(fieldSource, "Allowed field values conflict with the enum wire values");
					shape = new Scalar(ScalarKind.STRING, values);
				} else {
					error(fieldSource, "Allowed field values require a string or enum type");
				}
			}
			if (!field.format().isEmpty()) nonBlank(field.format(), fieldSource, "Field format");
			type = new TypeUse(shape, field.nullable() || type.nullable(),
					field.description().isEmpty() ? type.description() : field.description(),
					field.format().isEmpty() ? type.format() : field.format(), type.knowledge(), type.reason());
			if (properties.putIfAbsent(field.name(), new Property(field.name(), type, field.required())) != null)
				error(fieldSource, "Duplicate inline wire field " + field.name());
		}
		definitions.put(source, new ObjectType(source, new ArrayList<>(properties.values())));
		return new Reference(source);
	}

	private TypeUse capture(Type type, AnnotatedType annotated, AnnotatedElement declaration, String source) {
		Type current = type;
		AnnotatedType currentAnnotations = annotated;
		while (raw(current) != null && raw(current) != TypeRef.class) {
			Class<?> owner = raw(current);
			Type parent = owner.getGenericSuperclass();
			if (parent == null) break;
			currentAnnotations = resolveAnnotations(owner.getAnnotatedSuperclass(), annotationVariables(current, currentAnnotations));
			current = resolve(parent, variables(current));
		}
		if (raw(current) != TypeRef.class || !(current instanceof ParameterizedType parameterized)) {
			error(source, "TypeRef must declare its type argument");
			return read(Object.class, null, declaration, source);
		}
		return read(parameterized.getActualTypeArguments()[0], annotatedArgument(currentAnnotations, 0), declaration, source);
	}

	private Shape shape(Type type, AnnotatedType annotated, String source) {
		Class<?> raw = raw(type);
		if (type instanceof TypeVariable<?> || type instanceof WildcardType) return new Unknown();
		if (raw == void.class || raw == Void.class) return new Empty();
		if (raw == String.class || raw == char.class || raw == Character.class) return new Scalar(ScalarKind.STRING, List.of());
		if (raw == boolean.class || raw == Boolean.class) return new Scalar(ScalarKind.BOOLEAN, List.of());
		if (raw == byte.class || raw == Byte.class || raw == short.class || raw == Short.class
				|| raw == int.class || raw == Integer.class || raw == long.class || raw == Long.class || raw == BigInteger.class)
			return new Scalar(ScalarKind.INTEGER, List.of());
		if (raw == float.class || raw == Float.class || raw == double.class || raw == Double.class || raw == BigDecimal.class)
			return new Scalar(ScalarKind.DECIMAL, List.of());
		if (raw != null && raw.isEnum()) {
			List<String> values = Arrays.stream(raw.getDeclaredFields()).filter(Field::isEnumConstant)
					.map(f -> wireName(f, f.getName(), source)).sorted().toList();
			if (values.stream().distinct().count() != values.size()) error(source, "Duplicate enum wire value");
			return new Scalar(ScalarKind.STRING, values);
		}
		Type component = type instanceof GenericArrayType array ? array.getGenericComponentType()
				: raw != null && raw.isArray() ? raw.getComponentType() : null;
		if (component != null) return new Sequence(read(component,
				annotated instanceof AnnotatedArrayType array ? array.getAnnotatedGenericComponentType() : null, null, source + "[]"));
		if (raw != null && Collection.class.isAssignableFrom(raw)) {
			ContainerType collection = container(type, annotated, Collection.class);
			return new Sequence(read(argument(collection.type(), 0), annotatedArgument(collection.annotated(), 0), null, source + "[]"));
		}
		if (raw != null && Map.class.isAssignableFrom(raw)) {
			ContainerType map = container(type, annotated, Map.class);
			if (argument(map.type(), 0) != String.class) {
				warning(source, "Map keys must explicitly be strings; complex-key serialization is not inferred");
				return new Unknown();
			}
			return new Dictionary(read(argument(map.type(), 1), annotatedArgument(map.annotated(), 1), null, source + ".*"));
		}
		if (raw == null || raw == Object.class || raw.getName().startsWith("java.")
				|| raw.getName().startsWith("jakarta.") || raw.isInterface()) return new Unknown();
		String id = identity(type, annotated);
		if (definitions.containsKey(id)) return new Reference(id);
		definitions.put(id, new ObjectType(id, List.of()));
		Map<String, Property> properties = new TreeMap<>();
		if (raw.isRecord()) {
			for (var field : raw.getRecordComponents()) {
				String name = wireName(field, field.getName(), source);
				try {
					Field backing = raw.getDeclaredField(field.getName());
					if (backing.isAnnotationPresent(SerializedName.class)) name = wireName(backing, name, source);
				} catch (NoSuchFieldException e) {
					throw new IllegalStateException("Record component has no backing field", e);
				}
				addProperty(properties, name, resolve(field.getGenericType(), variables(type)),
						resolveAnnotations(field.getAnnotatedType(), annotationVariables(type, annotated)), field, source);
			}
		} else {
			Type current = type;
			AnnotatedType currentAnnotations = annotated;
			while (raw(current) != null && raw(current) != Object.class) {
				Class<?> owner = raw(current);
				for (Field field : Arrays.stream(owner.getDeclaredFields()).sorted(Comparator.comparing(Field::getName)).toList()) {
					if (field.isSynthetic() || Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) continue;
					String name = wireName(field, field.getName(), source);
					addProperty(properties, name, resolve(field.getGenericType(), variables(current)),
							resolveAnnotations(field.getAnnotatedType(), annotationVariables(current, currentAnnotations)), field, source);
				}
				Type parent = owner.getGenericSuperclass();
				if (parent == null) break;
				currentAnnotations = resolveAnnotations(owner.getAnnotatedSuperclass(), annotationVariables(current, currentAnnotations));
				current = resolve(parent, variables(current));
			}
		}
		definitions.put(id, new ObjectType(id, new ArrayList<>(properties.values())));
		return new Reference(id);
	}

	private void addProperty(Map<String, Property> properties, String name, Type type, AnnotatedType annotated,
			AnnotatedElement declaration, String source) {
		if (properties.containsKey(name)) error(source, "Duplicate wire property " + name);
		properties.put(name, new Property(name, read(type, annotated, declaration, source + "." + name),
				!declaration.isAnnotationPresent(Optional.class)));
	}

	String wireName(AnnotatedElement element, String fallback, String source) {
		WireName wire = element.getAnnotation(WireName.class);
		SerializedName gson = element.getAnnotation(SerializedName.class);
		if (wire != null && gson != null && !wire.value().equals(gson.value())) error(source, "WireName conflicts with SerializedName");
		String name = wire != null ? wire.value() : gson != null ? gson.value() : fallback;
		nonBlank(name, source, "Wire name");
		return name;
	}

	void nonBlank(String value, String source, String label) {
		if (value == null || value.isBlank()) error(source, label + " must not be blank");
	}
	void warning(String source, String message) { diagnostics.add(new Diagnostic(Severity.WARNING, source, message)); }
	void error(String source, String message) { diagnostics.add(new Diagnostic(Severity.ERROR, source, message)); }
	void check() {
		diagnostics.sort(Comparator.comparing(Diagnostic::source).thenComparing(d -> d.severity().name()).thenComparing(Diagnostic::message));
		if (diagnostics.stream().anyMatch(d -> d.severity() == Severity.ERROR)) throw new ContractException(diagnostics);
	}

	private static <A extends Annotation> A annotation(Class<A> type, AnnotatedElement declaration,
			AnnotatedType annotated, Class<?> raw) {
		A value = declaration == null ? null : declaration.getAnnotation(type);
		if (value == null && annotated != null) value = annotated.getAnnotation(type);
		if (value == null && raw != null) value = raw.getDeclaredAnnotation(type);
		return value;
	}
	private static AnnotatedType annotatedArgument(AnnotatedType type, int index) {
		return type instanceof AnnotatedParameterizedType parameterized && index < parameterized.getAnnotatedActualTypeArguments().length
				? parameterized.getAnnotatedActualTypeArguments()[index] : null;
	}
	private record ContainerType(Type type, AnnotatedType annotated) {}
	/** Resolve the actual Collection<E>/Map<K,V>, not a subclass's unrelated type-parameter order. */
	private static ContainerType container(Type type, AnnotatedType annotated, Class<?> target) {
		Class<?> owner = raw(type);
		if (owner == target) return new ContainerType(type, annotated);
		List<AnnotatedType> parents = new ArrayList<>(List.of(owner.getAnnotatedInterfaces()));
		if (owner.getAnnotatedSuperclass() != null) parents.add(owner.getAnnotatedSuperclass());
		for (AnnotatedType parent : parents) {
			if (raw(parent.getType()) != null && target.isAssignableFrom(raw(parent.getType()))) {
				return container(resolve(parent.getType(), variables(type)),
						resolveAnnotations(parent, annotationVariables(type, annotated)), target);
			}
		}
		throw new IllegalArgumentException("Cannot resolve container " + type.getTypeName());
	}
	static Type argument(Type type, int index) {
		return type instanceof ParameterizedType parameterized ? parameterized.getActualTypeArguments()[index] : Object.class;
	}
	static Class<?> raw(Type type) {
		return type instanceof Class<?> value ? value : type instanceof ParameterizedType parameterized
				&& parameterized.getRawType() instanceof Class<?> value ? value : null;
	}
	private static String numericFormat(Class<?> type) {
		if (type == byte.class || type == Byte.class) return "int8";
		if (type == short.class || type == Short.class) return "int16";
		if (type == int.class || type == Integer.class) return "int32";
		if (type == long.class || type == Long.class) return "int64";
		if (type == BigInteger.class) return "big-integer";
		if (type == float.class || type == Float.class) return "float32";
		if (type == double.class || type == Double.class) return "float64";
		if (type == BigDecimal.class) return "decimal";
		return "";
	}
	static Map<TypeVariable<?>, Type> variables(Type type) {
		Map<TypeVariable<?>, Type> result = new HashMap<>();
		if (type instanceof ParameterizedType parameterized && raw(type) != null) {
			TypeVariable<?>[] names = raw(type).getTypeParameters();
			for (int i = 0; i < names.length; i++) result.put(names[i], parameterized.getActualTypeArguments()[i]);
		}
		return result;
	}

	/** Carry type-use qualifiers through generic substitution, including inherited DTOs. */
	private static Map<TypeVariable<?>, AnnotatedType> annotationVariables(Type type, AnnotatedType annotated) {
		Map<TypeVariable<?>, AnnotatedType> result = new HashMap<>();
		if (annotated instanceof AnnotatedParameterizedType parameterized && raw(type) != null) {
			TypeVariable<?>[] names = raw(type).getTypeParameters();
			AnnotatedType[] arguments = parameterized.getAnnotatedActualTypeArguments();
			for (int i = 0; i < Math.min(names.length, arguments.length); i++) result.put(names[i], arguments[i]);
		}
		return result;
	}

	private static AnnotatedType resolveAnnotations(AnnotatedType original, Map<TypeVariable<?>, AnnotatedType> variables) {
		if (original == null) return null;
		AnnotatedType bound = original.getType() instanceof TypeVariable<?> variable ? variables.get(variable) : null;
		if (bound != null) {
			Map<Class<? extends Annotation>, Annotation> qualifiers = new LinkedHashMap<>();
			for (Annotation annotation : bound.getAnnotations()) qualifiers.put(annotation.annotationType(), annotation);
			for (Annotation annotation : original.getAnnotations()) qualifiers.put(annotation.annotationType(), annotation);
			return annotatedCopy(bound, qualifiers.values().toArray(Annotation[]::new), Map.of());
		}
		return annotatedCopy(original, original.getAnnotations(), variables);
	}

	private static AnnotatedType annotatedCopy(AnnotatedType original, Annotation[] annotations,
			Map<TypeVariable<?>, AnnotatedType> variables) {
		if (original instanceof AnnotatedParameterizedType parameterized) {
			AnnotatedType[] arguments = Arrays.stream(parameterized.getAnnotatedActualTypeArguments())
					.map(argument -> resolveAnnotations(argument, variables)).toArray(AnnotatedType[]::new);
			ParameterizedType type = (ParameterizedType) original.getType();
			Type resolved = new ResolvedType(type.getRawType(), type.getOwnerType(),
					Arrays.stream(arguments).map(AnnotatedType::getType).toArray(Type[]::new));
			return new AnnotatedArguments(resolved, annotations, arguments);
		}
		if (original instanceof AnnotatedArrayType array) {
			AnnotatedType component = resolveAnnotations(array.getAnnotatedGenericComponentType(), variables);
			return new AnnotatedArray(new GenericArrayType() {
				public Type getGenericComponentType() { return component.getType(); }
				public String getTypeName() { return component.getType().getTypeName() + "[]"; }
			}, annotations, component);
		}
		return new AnnotatedValue(original.getType(), annotations);
	}

	private static class AnnotatedValue implements AnnotatedType {
		private final Type type;
		private final Annotation[] annotations;
		AnnotatedValue(Type type, Annotation[] annotations) { this.type = type; this.annotations = annotations.clone(); }
		public Type getType() { return type; }
		public AnnotatedType getAnnotatedOwnerType() { return null; }
		public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
			for (Annotation annotation : annotations) if (annotationClass.isInstance(annotation)) return annotationClass.cast(annotation);
			return null;
		}
		public Annotation[] getAnnotations() { return annotations.clone(); }
		public Annotation[] getDeclaredAnnotations() { return getAnnotations(); }
	}
	private static final class AnnotatedArguments extends AnnotatedValue implements AnnotatedParameterizedType {
		private final AnnotatedType[] arguments;
		AnnotatedArguments(Type type, Annotation[] annotations, AnnotatedType[] arguments) {
			super(type, annotations); this.arguments = arguments.clone();
		}
		public AnnotatedType[] getAnnotatedActualTypeArguments() { return arguments.clone(); }
	}
	private static final class AnnotatedArray extends AnnotatedValue implements AnnotatedArrayType {
		private final AnnotatedType component;
		AnnotatedArray(Type type, Annotation[] annotations, AnnotatedType component) {
			super(type, annotations); this.component = component;
		}
		public AnnotatedType getAnnotatedGenericComponentType() { return component; }
	}

	/** Distinct generic qualifiers must not share a definition (Box<String> vs Box<@Nullable String>). */
	private static String identity(Type type, AnnotatedType annotated) {
		if (annotated instanceof AnnotatedArrayType array) {
			Type component = type instanceof GenericArrayType generic ? generic.getGenericComponentType()
					: type instanceof Class<?> raw && raw.isArray() ? raw.getComponentType() : null;
			if (component != null) return qualifiedIdentity(component, array.getAnnotatedGenericComponentType()) + "[]";
		}
		if (type instanceof ParameterizedType parameterized) {
			List<String> arguments = new ArrayList<>();
			Type[] types = parameterized.getActualTypeArguments();
			for (int i = 0; i < types.length; i++) {
				AnnotatedType argument = annotatedArgument(annotated, i);
				arguments.add(qualifiedIdentity(types[i], argument));
			}
			return parameterized.getRawType().getTypeName() + "<" + String.join(", ", arguments) + ">";
		}
		return type.getTypeName();
	}
	private static String qualifiedIdentity(Type type, AnnotatedType annotated) {
		String qualifiers = "";
		if (annotated != null) {
			// Fixed ordering and explicit members avoid VM-dependent annotation formatting.
			if (annotated.isAnnotationPresent(Nullable.class)) qualifiers += "@Nullable ";
			Format format = annotated.getAnnotation(Format.class);
			if (format != null) qualifiers += "@Format(" + quoted(format.value()) + ") ";
			Description description = annotated.getAnnotation(Description.class);
			if (description != null) qualifiers += "@Description(" + quoted(description.value()) + ") ";
			Dynamic dynamic = annotated.getAnnotation(Dynamic.class);
			if (dynamic != null) qualifiers += "@Dynamic(" + quoted(dynamic.reason()) + ") ";
			Unverified unverified = annotated.getAnnotation(Unverified.class);
			if (unverified != null) qualifiers += "@Unverified(" + quoted(unverified.reason()) + ") ";
			OneOf union = annotated.getAnnotation(OneOf.class);
			if (union != null) qualifiers += "@OneOf(" + String.join(",", Arrays.stream(union.value()).map(Class::getName).sorted().toList()) + ") ";
		}
		return qualifiers + identity(type, annotated);
	}
	private static String quoted(String value) { return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""; }
	static Type resolve(Type type, Map<TypeVariable<?>, Type> variables) {
		if (type instanceof TypeVariable<?> variable) return variables.getOrDefault(variable, variable);
		if (type instanceof ParameterizedType parameterized) {
			Type[] args = Arrays.stream(parameterized.getActualTypeArguments()).map(arg -> resolve(arg, variables)).toArray(Type[]::new);
			return new ResolvedType(parameterized.getRawType(), parameterized.getOwnerType(), args);
		}
		if (type instanceof GenericArrayType array) {
			Type component = resolve(array.getGenericComponentType(), variables);
			return new GenericArrayType() {
				public Type getGenericComponentType() { return component; }
				public String getTypeName() { return component.getTypeName() + "[]"; }
			};
		}
		return type;
	}
	private record ResolvedType(Type rawType, Type ownerType, Type[] arguments) implements ParameterizedType {
		public Type[] getActualTypeArguments() { return arguments.clone(); }
		public Type getRawType() { return rawType; }
		public Type getOwnerType() { return ownerType; }
		public String getTypeName() {
			return rawType.getTypeName() + "<" + String.join(", ", Arrays.stream(arguments).map(Type::getTypeName).toList()) + ">";
		}
	}
}
