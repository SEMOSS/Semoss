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

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.contract.ContractReader;
import prerna.contract.ContractSchemas;
import prerna.contract.ContractValues;
import prerna.contract.Invocation;
import prerna.contract.Key;
import prerna.contract.OperationContract;
import prerna.contract.TypedReactorContract;
import prerna.contract.TypeContract;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Opt-in typed execution: annotate the actual input record and execute(Input) result.
 * The legacy execute() entry point binds inputs and validates the emitted wire value.
 * Specialized legacy bases remain independent and can adopt the contract reader separately.
 */
public abstract class AbstractReactor2<I extends Record, O> extends AbstractReactor {
    private final OperationContract contract;
    private final TypedReactorContract signature;

    protected AbstractReactor2() {
        contract = ContractReader.read(getClass());
        if (contract.name().isBlank()) throw new IllegalArgumentException("AbstractReactor2 requires @Reactor(name, description)");
        signature = TypedReactorContract.inspect(getClass());
        reactorName = contract.name();
        List<String> order = contract.invocation().order().isEmpty()
                ? java.util.Arrays.stream(signature.input().getRecordComponents()).map(AbstractReactor2::keyName).toList()
                : contract.invocation().order();
        keysToGet = order.stream().map(name -> {
            var input = input(name);
            return name + (input.aliases().isEmpty() ? "" : "," + String.join(",", input.aliases()));
        }).toArray(String[]::new);
        keyRequired = order.stream().mapToInt(name -> input(name).requirement() == OperationContract.Requirement.REQUIRED ? 1 : 0).toArray();
    }

    /** Java enforces that execution consumes the declared record and returns the declared payload type. */
    protected abstract O execute(I input);

    @Override
    public final NounMetadata execute() {
        O value = execute(bindInputs());
        Object wireValue = ContractValues.encode(value);
        ContractValues.validate(wireValue, contract.outputs().getFirst().type(), contract.definitions(), "output");
        PixelDataType nounType = outputNounType(wireValue);
        // JSON_OBJECT uses the existing literal-JSON serializer, preserving explicitly nullable fields.
        Object payload = nounType == PixelDataType.JSON_OBJECT
                ? new com.google.gson.GsonBuilder().serializeNulls().create().toJsonTree(wireValue) : wireValue;
        return new NounMetadata(payload, nounType, outputOperationTypes());
    }

    /** Preserve operation flags without letting an untyped payload bypass the typed execution boundary. */
    protected prerna.sablecc2.om.PixelOperationType[] outputOperationTypes() {
        return new prerna.sablecc2.om.PixelOperationType[] { prerna.sablecc2.om.PixelOperationType.OPERATION };
    }

    private static PixelDataType outputNounType(Object value) {
        if (value instanceof String) return PixelDataType.CONST_STRING;
        if (value instanceof Boolean) return PixelDataType.BOOLEAN;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof java.math.BigInteger) return PixelDataType.CONST_INT;
        if (value instanceof Number) return PixelDataType.CONST_DECIMAL;
        return PixelDataType.JSON_OBJECT;
    }

    @SuppressWarnings("unchecked")
    private I bindInputs() {
        Map<String, Object> supplied = new LinkedHashMap<>();
        var invocation = contract.invocation();
        for (var input : contract.inputs()) {
            List<String> names = new ArrayList<>(); names.add(input.name()); names.addAll(input.aliases());
            for (String name : names) {
                GenRowStruct row = getNounStore().getGenRowStruct(name);
                if (row == null || row.isEmpty()) continue;
                if (invocation.mode() == Invocation.Mode.POSITIONAL) throw new IllegalArgumentException("Named arguments are not supported");
                if (supplied.containsKey(input.name())) throw new IllegalArgumentException("Input supplied more than once: " + input.name());
                supplied.put(input.name(), rowValue(row, input));
            }
        }
        // Only the all-nouns row is positional. curRow may still point at the last named key.
        GenRowStruct positional = getNounStore().getGenRowStruct(ALL_NOUN_STORE);
        if (positional != null && !positional.isEmpty()) {
            if (invocation.mode() == Invocation.Mode.NAMED) throw new IllegalArgumentException("Positional arguments are not supported");
            int cursor = 0;
            for (String name : invocation.order()) {
                if (supplied.containsKey(name) || cursor >= positional.size()) continue;
                if (name.equals(invocation.variadic())) {
                    List<Object> rest = new ArrayList<>();
                    while (cursor < positional.size()) rest.add(positional.get(cursor++));
                    supplied.put(name, rest);
                } else supplied.put(name, positional.get(cursor++));
            }
            if (cursor < positional.size()) throw new IllegalArgumentException("Too many positional arguments");
        }
        RecordComponent[] components = signature.input().getRecordComponents();
        Object[] values = new Object[components.length];
        Class<?>[] parameterTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            var input = input(keyName(component));
            parameterTypes[i] = component.getType();
            if (!supplied.containsKey(input.name())) {
                if (input.requirement() == OperationContract.Requirement.REQUIRED)
                    throw new IllegalArgumentException("Required input(s) missing: " + input.name());
                values[i] = null;
            } else {
                Object raw = supplied.get(input.name());
                Object wireValue = ContractValues.encode(raw);
                ContractValues.validate(wireValue, input.type(), contract.definitions(), input.name());
                values[i] = ContractValues.decode(wireValue, component.getGenericType());
            }
        }
        try { return (I) signature.input().getConstructor(parameterTypes).newInstance(values); }
        catch (ReflectiveOperationException error) { throw new IllegalArgumentException("Cannot construct reactor input record", error); }
    }

    private Object rowValue(GenRowStruct row, OperationContract.Input input) {
        if (input.type().shape() instanceof TypeContract.Sequence) {
            if (row.size() == 1 && (row.get(0) instanceof Collection<?> || row.get(0) instanceof com.google.gson.JsonArray
                    || input.type().nullable() && (row.get(0) == null || row.get(0) instanceof com.google.gson.JsonNull)
                    || row.get(0) != null && row.get(0).getClass().isArray())) return row.get(0);
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) values.add(row.get(i));
            return values;
        }
        if (row.size() != 1) throw new IllegalArgumentException("Expected one value for " + input.name());
        return row.get(0);
    }

    private static String keyName(RecordComponent component) {
        Key key = component.getAnnotation(Key.class);
        return key.name().isEmpty() ? component.getName() : key.name();
    }
    private OperationContract.Input input(String name) {
        return contract.inputs().stream().filter(value -> value.name().equals(name)).findFirst().orElseThrow();
    }
    public final OperationContract getContract() { return contract; }
    @Override public final String getReactorDescription() { return contract.description(); }
    @Override protected final String getDescriptionForKey(String key) {
        return contract.inputs().stream().filter(input -> input.name().equalsIgnoreCase(key)
                || input.aliases().stream().anyMatch(alias -> alias.equalsIgnoreCase(key)))
                .map(OperationContract.Input::description).findFirst().orElseGet(() -> super.getDescriptionForKey(key));
    }
    @Override public final JSONObject getResponseSchema() { return ContractSchemas.output(contract); }
    @Override public final JSONObject getMcpProperties() { return ContractSchemas.inputs(contract).getJSONObject("properties"); }
    @Override public final JSONObject asMcpTool() {
        JSONObject tool = super.asMcpTool();
        tool.put("inputSchema", ContractSchemas.inputs(contract).put("title", tool.getString("name") + "_Arguments"));
        return tool;
    }
}
