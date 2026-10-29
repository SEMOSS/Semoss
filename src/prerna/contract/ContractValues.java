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

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import prerna.contract.TypeContract.*;

/** Runtime binding for JSON-compatible record contracts; never used by the exporter. */
public final class ContractValues {
    private ContractValues() {}

    public static Object decode(Object value, Type type) {
        Class<?> raw = TypeReader.raw(type);
        if (value == null) {
            if (raw != null && raw.isPrimitive()) throw new IllegalArgumentException("Missing/null value for primitive " + raw.getName());
            return null;
        }
        if (raw == null) throw new IllegalArgumentException("Unresolved input type: " + type);
        if (raw.isRecord()) {
            if (raw.isInstance(value)) return value;
            if (!(value instanceof Map<?, ?> map)) throw new IllegalArgumentException("Expected object for " + raw.getName());
            var components = raw.getRecordComponents();
            Object[] values = new Object[components.length];
            Class<?>[] constructorTypes = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) {
                var component = components[i];
                String name = wireName(component);
                values[i] = decode(map.get(name), TypeReader.resolve(component.getGenericType(), TypeReader.variables(type)));
                constructorTypes[i] = component.getType();
            }
            try { return raw.getConstructor(constructorTypes).newInstance(values); }
            catch (ReflectiveOperationException error) { throw new IllegalArgumentException("Cannot construct input record " + raw.getName(), error); }
        }
        if (raw.isArray()) {
            List<?> values = list(value);
            Object result = Array.newInstance(raw.getComponentType(), values.size());
            for (int i = 0; i < values.size(); i++) Array.set(result, i, decode(values.get(i), raw.getComponentType()));
            return result;
        }
        if (Collection.class.isAssignableFrom(raw)) {
            Type element = TypeReader.argument(type, 0);
            Collection<Object> result = Set.class.isAssignableFrom(raw) ? new LinkedHashSet<>() : new ArrayList<>();
            for (Object item : list(value)) result.add(decode(item, element));
            if (!raw.isInstance(result)) throw new IllegalArgumentException("Collection needs a custom input adapter: " + raw.getName());
            return result;
        }
        if (Map.class.isAssignableFrom(raw)) {
            if (!(value instanceof Map<?, ?> map)) throw new IllegalArgumentException("Expected map");
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put((String) key, decode(item, TypeReader.argument(type, 1))));
            if (!raw.isInstance(result)) throw new IllegalArgumentException("Map needs a custom input adapter: " + raw.getName());
            return result;
        }
        if (raw.isEnum()) {
            for (Object constant : raw.getEnumConstants()) {
                if (encode(constant).equals(value)) return constant;
            }
            throw new IllegalArgumentException("Invalid enum value: " + value);
        }
        if (value instanceof Number number) {
            BigDecimal decimal = new BigDecimal(number.toString());
            try {
                if (raw == byte.class || raw == Byte.class) return decimal.byteValueExact();
                if (raw == short.class || raw == Short.class) return decimal.shortValueExact();
                if (raw == int.class || raw == Integer.class) return decimal.intValueExact();
                if (raw == long.class || raw == Long.class) return decimal.longValueExact();
                if (raw == BigInteger.class) return decimal.toBigIntegerExact();
                if (raw == BigDecimal.class) return decimal;
                if (raw == double.class || raw == Double.class) {
                    double result = decimal.doubleValue();
                    if (!Double.isFinite(result)) throw new ArithmeticException("Non-finite double");
                    return result;
                }
                if (raw == float.class || raw == Float.class) {
                    float result = decimal.floatValue();
                    if (!Float.isFinite(result)) throw new ArithmeticException("Non-finite float");
                    return result;
                }
            } catch (ArithmeticException error) { throw new IllegalArgumentException("Number is out of range for " + raw.getName(), error); }
        }
        if ((raw == char.class || raw == Character.class) && value instanceof String text && text.length() == 1) return text.charAt(0);
        if (raw == boolean.class && value instanceof Boolean || raw.isInstance(value)) return value;
        throw new IllegalArgumentException("Cannot bind " + value.getClass().getName() + " to " + type.getTypeName());
    }

    /** Produce the actual wire value, respecting record field names and absent/null semantics. */
    public static Object encode(Object value) { return encode(value, 0); }

    private static Object encode(Object value, int depth) {
        if (depth > 128) throw new IllegalArgumentException("Cyclic or excessively deep output value");
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Number) return value;
        if (value instanceof com.google.gson.JsonElement json) return jsonValue(json);
        if (value instanceof Character character) return character.toString();
        if (value instanceof Enum<?> constant) {
            try { return new TypeReader().wireName(constant.getDeclaringClass().getField(constant.name()), constant.name(), "enum"); }
            catch (NoSuchFieldException error) { throw new IllegalArgumentException(error); }
        }
        if (value instanceof Collection<?> || value.getClass().isArray()) {
            List<Object> result = new ArrayList<>();
            for (Object item : list(value)) result.add(encode(item, depth + 1));
            return result;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (var entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) throw new IllegalArgumentException("Wire map keys must be strings");
                result.put(key, encode(entry.getValue(), depth + 1));
            }
            return result;
        }
        if (value.getClass().isRecord()) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (var component : value.getClass().getRecordComponents()) {
                try {
                    Object item = component.getAccessor().invoke(value);
                    boolean nullable = component.isAnnotationPresent(Nullable.class) || component.getAnnotatedType().isAnnotationPresent(Nullable.class);
                    if (item == null && component.isAnnotationPresent(Optional.class) && !nullable) continue;
                    String name = wireName(component);
                    result.put(name, encode(item, depth + 1));
                } catch (ReflectiveOperationException error) { throw new IllegalArgumentException("Cannot read output record", error); }
            }
            return result;
        }
        // Existing DTOs must agree with their Gson wire shape. Validation catches mismatched declarations.
        return jsonValue(new Gson().toJsonTree(value));
    }

    private static String wireName(java.lang.reflect.RecordComponent component) {
        TypeReader reader = new TypeReader();
        String name = reader.wireName(component, component.getName(), component.getDeclaringRecord().getName());
        try {
            var field = component.getDeclaringRecord().getDeclaredField(component.getName());
            if (field.isAnnotationPresent(com.google.gson.annotations.SerializedName.class))
                name = reader.wireName(field, name, field.getDeclaringClass().getName());
        } catch (NoSuchFieldException error) { throw new IllegalArgumentException(error); }
        reader.check();
        return name;
    }

    private static Object jsonValue(com.google.gson.JsonElement json) {
        if (json.isJsonNull()) return null;
        if (json.isJsonObject()) {
            Map<String, Object> result = new LinkedHashMap<>();
            json.getAsJsonObject().entrySet().forEach(entry -> result.put(entry.getKey(), jsonValue(entry.getValue())));
            return result;
        }
        if (json.isJsonArray()) {
            List<Object> result = new ArrayList<>();
            json.getAsJsonArray().forEach(item -> result.add(jsonValue(item)));
            return result;
        }
        var primitive = json.getAsJsonPrimitive();
        if (primitive.isBoolean()) return primitive.getAsBoolean();
        if (primitive.isNumber()) return primitive.getAsNumber();
        return primitive.getAsString();
    }

    public static void validate(Object value, TypeUse type, Map<String, ObjectType> definitions, String path) {
        validate(value, type, definitions, path, 0);
    }

    private static void validate(Object value, TypeUse type, Map<String, ObjectType> definitions, String path, int depth) {
        if (depth > 128) throw new IllegalArgumentException(path + ": value exceeds maximum depth");
        if (value == null) {
            if (!type.nullable() && !(type.shape() instanceof Empty)) invalid(path, "null is not allowed");
            return;
        }
        switch (type.shape()) {
            case Scalar scalar -> {
                boolean valid = switch (scalar.kind()) {
                    case STRING -> value instanceof String && (scalar.values().isEmpty() || scalar.values().contains(value));
                    case BOOLEAN -> value instanceof Boolean;
                    case INTEGER -> value instanceof Number number && integer(number);
                    case DECIMAL -> value instanceof Number number && finite(number);
                };
                if (!valid) invalid(path, "expected " + scalar.kind().name().toLowerCase());
            }
            case Sequence sequence -> {
                if (!(value instanceof Collection<?>) && !value.getClass().isArray()) invalid(path, "expected array");
                int index = 0;
                for (Object item : list(value)) validate(item, sequence.element(), definitions, path + "[" + index++ + "]", depth + 1);
            }
            case Dictionary dictionary -> {
                if (!(value instanceof Map<?, ?>)) invalid(path, "expected object");
                for (var entry : ((Map<?, ?>) value).entrySet()) {
                    if (!(entry.getKey() instanceof String)) invalid(path, "expected string map key");
                    validate(entry.getValue(), dictionary.value(), definitions, path + "." + entry.getKey(), depth + 1);
                }
            }
            case Reference reference -> {
                if (!(value instanceof Map<?, ?>)) invalid(path, "expected object");
                Map<?, ?> object = (Map<?, ?>) value;
                ObjectType definition = definitions.get(reference.id());
                if (definition == null) invalid(path, "unresolved reference " + reference.id());
                for (Property property : definition.properties()) {
                    if (!object.containsKey(property.name())) {
                        if (property.required()) invalid(path + "." + property.name(), "required field is missing");
                    } else validate(object.get(property.name()), property.type(), definitions, path + "." + property.name(), depth + 1);
                }
            }
            case Union union -> {
                for (TypeUse alternative : union.alternatives()) {
                    try { validate(value, alternative, definitions, path, depth + 1); return; }
                    catch (IllegalArgumentException ignored) { /* Try the next wire alternative. */ }
                }
                invalid(path, "value matches no union alternative");
            }
            case Unknown ignored -> {
                if (type.knowledge() != Knowledge.DYNAMIC) invalid(path, "unverified shape");
            }
            case Empty ignored -> invalid(path, "expected no output");
        }
    }

    private static boolean finite(Number number) {
        try { new BigDecimal(number.toString()); return true; }
        catch (NumberFormatException error) { return false; }
    }
    private static boolean integer(Number number) {
        try { new BigDecimal(number.toString()).toBigIntegerExact(); return true; }
        catch (ArithmeticException | NumberFormatException error) { return false; }
    }
    private static void invalid(String path, String message) { throw new IllegalArgumentException(path + ": " + message); }
    private static List<?> list(Object value) {
        if (value instanceof Collection<?> collection) return new ArrayList<>(collection);
        if (!value.getClass().isArray()) throw new IllegalArgumentException("Expected array");
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < Array.getLength(value); i++) result.add(Array.get(value, i));
        return result;
    }
}
