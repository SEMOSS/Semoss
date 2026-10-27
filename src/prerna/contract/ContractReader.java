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
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.CookieParam;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.MatrixParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

import prerna.contract.OperationContract.*;
import prerna.contract.Parameter.Binding;
import prerna.contract.TypeContract.TypeUse;

/**
 * Reads explicitly supplied operations; does not discover a deployed catalog.
 * Reactor metadata providers execute pure definition-building code, but described
 * reactors/resources are never instantiated or invoked.
 */
public final class ContractReader {
	private ContractReader() {}

	/** Read a reactor's provider or class annotations without constructing it. */
	public static OperationContract read(Class<?> reactor) {
		TypeReader types = new TypeReader();
		String source = reactor.getName();
		ReactorMetadata metadata = reactor.getDeclaredAnnotation(ReactorMetadata.class);
		String description;
		List<Input> inputs = new ArrayList<>();
		List<Output> outputs = new ArrayList<>();
		InvocationInfo invocation;
		Policy policy = policy(types, reactor, null);
		DeprecationInfo deprecation = deprecation(types, reactor, source);
		if (metadata != null) {
			if (reactor.isAnnotationPresent(Description.class) || reactor.getAnnotationsByType(Parameter.class).length > 0
					|| reactor.getAnnotationsByType(Returns.class).length > 0 || reactor.isAnnotationPresent(Invocation.class))
				types.error(source, "ReactorMetadata owns descriptions, parameters, returns and invocation; do not duplicate them in annotations");
			ReactorDefinition definition;
			try {
				definition = metadata.value().getConstructor().newInstance().definition();
				if (definition == null) throw new IllegalArgumentException("Provider returned null");
			} catch (ReflectiveOperationException | RuntimeException e) {
				types.error(source, "Cannot read metadata provider " + metadata.value().getName() + ": " + e);
				types.check();
				throw new AssertionError("Unreachable", e);
			}
			description = definition.description();
			for (ReactorDefinition.Argument argument : definition.arguments()) {
				TypeUse inputType = types.read(argument.type(), null, null, source + "." + argument.name());
				inputs.add(new Input(argument.name(), inputType.withNullable(argument.nullable() || inputType.nullable()),
						argument.description(), argument.required() ? Requirement.REQUIRED : Requirement.OPTIONAL,
						argument.aliases(), Binding.NOUN, Parameter.Encoding.DEFAULT, null));
			}
			ReactorDefinition.Result result = definition.result();
			TypeUse output = types.read(result.type(), null, null, source + " return");
			outputs.add(new Output(output.withNullable(result.nullable() || output.nullable()), -1, "", result.description()));
			invocation = definition.invocation();
			policy = new Policy(merge(policy.ignoreReasons(), definition.policy().ignoreReasons()),
					merge(policy.internalReasons(), definition.policy().internalReasons()));
			if (definition.deprecation().deprecated()) {
				if (deprecation.deprecated() && !deprecation.equals(definition.deprecation()))
					types.error(source, "Conflicting annotation and provider deprecation metadata");
				deprecation = definition.deprecation();
			}
		} else {
			description = description(reactor);
			for (Parameter parameter : reactor.getAnnotationsByType(Parameter.class)) {
				Binding binding = parameter.binding() == Binding.AUTO ? Binding.NOUN : parameter.binding();
				if (binding != Binding.NOUN) types.error(source, "Pixel parameters must use NOUN binding");
				inputs.add(input(types, parameter, null, binding, source));
			}
			outputs.addAll(outputs(types, reactor, null, source));
			Invocation declaration = reactor.getAnnotation(Invocation.class);
			invocation = declaration == null ? null : new InvocationInfo(declaration.mode(),
					List.of(declaration.order()), declaration.variadic(), declaration.canOmitParameters());
			if (invocation == null) types.warning(source, "Pixel invocation conventions are unspecified");
		}
		return finish(types, source, Kind.PIXEL, description, policy, deprecation, inputs, outputs, invocation, null);
	}

	/** Read a REST method in its declaring resource. */
	public static OperationContract read(Method method) { return read(method.getDeclaringClass(), method); }

	/** Supply the concrete resource type when reading an inherited REST method. */
	public static OperationContract read(Class<?> resource, Method method) {
		TypeReader types = new TypeReader();
		String source = method.toGenericString();
		if (!method.getDeclaringClass().isAssignableFrom(resource)) types.error(source, "Method does not belong to resource");
		List<String> verbs = Arrays.stream(method.getAnnotations()).map(a -> a.annotationType().getAnnotation(HttpMethod.class))
				.filter(a -> a != null).map(HttpMethod::value).sorted().toList();
		if (verbs.size() != 1) types.error(source, "A REST operation must declare exactly one HTTP method");
		Path base = resource.getAnnotation(Path.class);
		Path suffix = method.getAnnotation(Path.class);
		String path = joinPath(base == null ? "" : base.value(), suffix == null ? "" : suffix.value());
		Consumes consumes = method.getAnnotation(Consumes.class);
		if (consumes == null) consumes = resource.getAnnotation(Consumes.class);
		Produces produces = method.getAnnotation(Produces.class);
		if (produces == null) produces = resource.getAnnotation(Produces.class);
		RestInfo rest = new RestInfo(verbs.isEmpty() ? "" : verbs.get(0), path,
				consumes == null ? List.of() : sorted(List.of(consumes.value())),
				produces == null ? List.of() : sorted(List.of(produces.value())));
		Map<String, NativeInput> nativeInputs = new LinkedHashMap<>();
		for (Class<?> owner = resource; owner != null && owner != Object.class; owner = owner.getSuperclass()) {
			for (Field field : Arrays.stream(owner.getDeclaredFields()).sorted(Comparator.comparing(Field::getName)).toList()) {
				if (Modifier.isStatic(field.getModifiers())) continue;
				BindingName binding = binding(types, field, source);
				if (binding != null) addNative(types, nativeInputs,
						new NativeInput(field, field.getGenericType(), field.getAnnotatedType(), binding), source);
				if (field.isAnnotationPresent(BeanParam.class)) types.warning(source, "BeanParam field requires explicit flattened Parameter metadata: " + field.getName());
			}
		}
		for (java.lang.reflect.Parameter parameter : method.getParameters()) {
			if (parameter.isAnnotationPresent(Context.class)) continue;
			if (parameter.isAnnotationPresent(BeanParam.class)) {
				types.warning(source, "BeanParam requires explicit flattened Parameter metadata");
				continue;
			}
			BindingName binding = binding(types, parameter, source);
			Parameter[] declarations = parameter.getAnnotationsByType(Parameter.class);
			if (declarations.length > 1) types.error(source, "A Java parameter can have only one Parameter declaration");
			if (binding == null) binding = new BindingName(Binding.BODY,
					declarations.length == 1 && !declarations[0].name().isEmpty() ? declarations[0].name() : "body");
			addNative(types, nativeInputs, new NativeInput(parameter, parameter.getParameterizedType(), parameter.getAnnotatedType(), binding), source);
		}
		Map<String, Parameter> declarations = new LinkedHashMap<>();
		for (Parameter parameter : method.getAnnotationsByType(Parameter.class)) {
			types.nonBlank(parameter.name(), source, "Method-level parameter name");
			if (declarations.putIfAbsent(parameter.name(), parameter) != null) types.error(source, "Duplicate Parameter " + parameter.name());
		}
		List<Input> inputs = new ArrayList<>();
		for (NativeInput nativeInput : nativeInputs.values()) {
			Parameter direct = nativeInput.element().getAnnotation(Parameter.class);
			Parameter methodLevel = declarations.remove(nativeInput.binding().name());
			if (direct != null && methodLevel != null) types.error(source, "Duplicate metadata for " + nativeInput.binding().name());
			inputs.add(input(types, direct == null ? methodLevel : direct, nativeInput, nativeInput.binding().binding(), source));
		}
		for (Parameter declaration : declarations.values()) {
			if (declaration.binding() == Binding.AUTO || declaration.binding() == Binding.NOUN)
				types.error(source, "Supplemental REST parameter needs an explicit HTTP binding: " + declaration.name());
			inputs.add(input(types, declaration, null, declaration.binding(), source));
		}
		if (inputs.stream().filter(i -> i.binding() == Binding.BODY).count() > 1) types.error(source, "Only one HTTP body parameter is supported");
		for (Input input : inputs) {
			if (input.binding() == Binding.PATH && !path.matches("(?s).*\\{" + java.util.regex.Pattern.quote(input.name()) + "(?:\\s*:[^}]+)?\\}.*"))
				types.error(source, "Path parameter is absent from the resource-relative path: " + input.name());
		}
		var templateNames = java.util.regex.Pattern.compile("\\{\\s*([A-Za-z_][A-Za-z0-9_.-]*)\\s*(?=[:}])").matcher(path);
		while (templateNames.find()) {
			String name = templateNames.group(1);
			if (inputs.stream().noneMatch(input -> input.binding() == Binding.PATH && input.name().equals(name)))
				types.warning(source, "Path template input needs Parameter metadata: " + name);
		}
		DeprecationInfo deprecation = deprecation(types, method, source);
		if (!deprecation.deprecated()) deprecation = deprecation(types, resource, source);
		return finish(types, source, Kind.REST, description(method), policy(types, resource, method), deprecation,
				inputs, outputs(types, method, method, source), null, rest);
	}

	/** Read a standalone record, DTO, or named TypeRef without constructing it. */
	public static TypeContract readType(Class<?> type) {
		TypeReader reader = new TypeReader();
		TypeUse root = reader.read(type, null, null, type.getName());
		reader.check();
		return new TypeContract(root, reader.definitions, reader.diagnostics);
	}

	private static OperationContract finish(TypeReader types, String source, Kind kind, String description,
			Policy policy, DeprecationInfo deprecation, List<Input> inputs, List<Output> outputs,
			InvocationInfo invocation, RestInfo rest) {
		if (description.isBlank()) types.warning(source, "Operation description is missing");
		for (String reason : policy.ignoreReasons()) types.nonBlank(reason, source, "Ignore reason");
		for (String reason : policy.internalReasons()) types.nonBlank(reason, source, "Internal reason");
		if (deprecation.deprecated() && deprecation.reason().isBlank()) types.warning(source, "Deprecation reason is missing");
		Set<String> names = new HashSet<>();
		for (Input input : inputs) {
			types.nonBlank(input.name(), source, "Parameter name");
			if (!names.add(input.name())) types.error(source, "Duplicate parameter or alias: " + input.name());
			for (String alias : input.aliases()) {
				types.nonBlank(alias, source, "Parameter alias");
				if (!names.add(alias)) types.error(source, "Duplicate parameter or alias: " + alias);
			}
			if (kind == Kind.PIXEL && (input.name().contains(",") || input.aliases().stream().anyMatch(a -> a.contains(","))))
				types.error(source, "Pixel parameter names and aliases cannot contain commas");
			if (input.type().shape() instanceof TypeContract.Empty) types.error(source, "Input cannot have a void wire type: " + input.name());
		}
		if (invocation != null) validateInvocation(types, source, invocation, inputs);
		Set<String> responseKeys = new HashSet<>();
		for (Output output : outputs) {
			if (kind == Kind.REST && output.status() == -1) types.warning(source, "HTTP response status is unspecified");
			if (output.status() != -1 && (kind == Kind.PIXEL || output.status() < 100 || output.status() > 599))
				types.error(source, "Invalid response status " + output.status());
			if (!responseKeys.add(output.status() + ":" + output.mediaType())) types.error(source, "Duplicate response status/media type");
		}
		inputs.sort(Comparator.comparing(Input::name));
		outputs.sort(Comparator.comparingInt(Output::status).thenComparing(Output::mediaType));
		types.check();
		return new OperationContract(source, kind, description, policy, deprecation, inputs, outputs, invocation,
				rest, types.definitions, types.diagnostics);
	}

	private static void validateInvocation(TypeReader types, String source, InvocationInfo invocation, List<Input> inputs) {
		Set<String> names = new HashSet<>(inputs.stream().map(Input::name).toList());
		Set<String> order = new HashSet<>(invocation.order());
		if (order.size() != invocation.order().size()) types.error(source, "Duplicate positional argument");
		if (invocation.mode() == Invocation.Mode.NAMED && (!order.isEmpty() || !invocation.variadic().isEmpty()))
			types.error(source, "Named-only invocation cannot declare positional order or a variadic argument");
		if (invocation.mode() != Invocation.Mode.NAMED && !order.equals(names))
			types.error(source, "Positional order must name every parameter exactly once");
		if (!invocation.variadic().isEmpty() && (invocation.order().isEmpty()
				|| !invocation.order().getLast().equals(invocation.variadic())))
			types.error(source, "Variadic argument must be last in positional order");
		if (invocation.canOmitParameters() && inputs.stream().anyMatch(i -> i.requirement() != Requirement.OPTIONAL))
			types.error(source, "Cannot omit parameters when an input is required or its requiredness is unknown");
	}

	private static List<Output> outputs(TypeReader types, AnnotatedElement element, Method method, String source) {
		List<Output> outputs = new ArrayList<>();
		for (Returns returns : element.getAnnotationsByType(Returns.class)) {
			int[] statuses = returns.statuses().length == 0 ? new int[] { returns.status() }
					: Arrays.stream(returns.statuses()).sorted().toArray();
			if (returns.statuses().length > 0 && returns.status() != -1)
				types.error(source, "Returns must declare either status or statuses, not both");
			if (returns.statuses().length > 0 && Arrays.stream(statuses).anyMatch(status -> status < 100 || status > 599))
				types.error(source, "Returns statuses must be explicit HTTP status codes between 100 and 599");
			String responseSource = source + "#returns(" + Arrays.toString(statuses) + ";" + returns.mediaType() + ")";
			TypeUse type;
			if (returns.fields().length > 0) {
				if (returns.value() != Object.class) types.error(source, "Returns must declare either inline fields or a concrete value type, not both");
				type = types.readInline(returns.fields(), element, responseSource);
			} else {
				type = types.read(returns.value(), null, element, responseSource);
			}
			for (int status : statuses) {
				outputs.add(new Output(type.withNullable(returns.nullable() || type.nullable()), status, returns.mediaType(), returns.description()));
			}
		}
		if (outputs.isEmpty()) {
			TypeUse type = types.read(method == null ? Object.class : method.getGenericReturnType(),
					method == null ? null : method.getAnnotatedReturnType(), element, source + " return");
			outputs.add(new Output(type, -1, "", ""));
		}
		return outputs;
	}

	private static Input input(TypeReader types, Parameter metadata, NativeInput nativeInput, Binding binding, String source) {
		String name = nativeInput == null ? metadata.name() : nativeInput.binding().name();
		if (metadata != null && nativeInput != null) {
			if (!metadata.name().isEmpty() && !metadata.name().equals(name)) types.error(source, "Parameter name conflicts with JAX-RS: " + name);
			if (metadata.binding() != Binding.AUTO && metadata.binding() != binding) types.error(source, "Parameter binding conflicts with JAX-RS: " + name);
		}
		AnnotatedElement declaration = nativeInput == null ? null : nativeInput.element();
		DefaultValue defaultValue = declaration == null ? null : declaration.getAnnotation(DefaultValue.class);
		Requirement required = metadata != null ? (metadata.required() ? Requirement.REQUIRED : Requirement.OPTIONAL)
				: binding == Binding.PATH ? Requirement.REQUIRED : defaultValue != null
						|| declaration != null && declaration.isAnnotationPresent(Optional.class) ? Requirement.OPTIONAL : Requirement.UNKNOWN;
		if (binding == Binding.PATH && required != Requirement.REQUIRED) types.error(source, "Path parameters must be required: " + name);
		if (defaultValue != null && required == Requirement.REQUIRED) types.error(source, "Required parameter has a JAX-RS default: " + name);
		if (required == Requirement.UNKNOWN) types.warning(source + "." + name, "Input requiredness is unspecified");
		Type javaType = metadata != null && metadata.type() != void.class ? metadata.type()
				: nativeInput != null ? nativeInput.type() : Object.class;
		TypeUse type = types.read(javaType, nativeInput == null ? null : nativeInput.annotated(), declaration, source + "." + name);
		if (metadata != null && metadata.type() != void.class && nativeInput != null
				&& metadata.encoding() != Parameter.Encoding.JSON) {
			TypeUse nativeType = new TypeReader().read(nativeInput.type(), nativeInput.annotated(), declaration, source + "." + name);
			if (!(nativeType.shape() instanceof TypeContract.Unknown) && !nativeType.shape().equals(type.shape()))
				types.error(source, "Parameter type conflicts with JAX-RS Java type: " + name + "; declare JSON encoding when a string carries structured data");
		}
		if (metadata != null && metadata.nullable()) type = type.withNullable(true);
		if (binding == Binding.PATH && type.nullable()) types.error(source, "Path parameters cannot be nullable: " + name);
		if (declaration != null && declaration.isAnnotationPresent(Optional.class) && required == Requirement.REQUIRED)
			types.error(source, "Required parameter is also marked Optional: " + name);
		return new Input(name, type, metadata == null ? description(declaration) : metadata.description(), required,
				metadata == null ? List.of() : sorted(List.of(metadata.aliases())), binding,
				metadata == null ? Parameter.Encoding.DEFAULT : metadata.encoding(), defaultValue == null ? null : defaultValue.value());
	}

	private record BindingName(Binding binding, String name) {}
	private record NativeInput(AnnotatedElement element, Type type, AnnotatedType annotated, BindingName binding) {}
	private static void addNative(TypeReader types, Map<String, NativeInput> inputs, NativeInput input, String source) {
		if (inputs.putIfAbsent(input.binding().name(), input) != null) types.error(source, "Ambiguous repeated HTTP input name: " + input.binding().name());
	}
	private static BindingName binding(TypeReader types, AnnotatedElement element, String source) {
		List<BindingName> found = new ArrayList<>();
		for (Annotation annotation : element.getAnnotations()) {
			if (annotation instanceof PathParam p) found.add(new BindingName(Binding.PATH, p.value()));
			if (annotation instanceof QueryParam p) found.add(new BindingName(Binding.QUERY, p.value()));
			if (annotation instanceof HeaderParam p) found.add(new BindingName(Binding.HEADER, p.value()));
			if (annotation instanceof CookieParam p) found.add(new BindingName(Binding.COOKIE, p.value()));
			if (annotation instanceof FormParam p) found.add(new BindingName(Binding.FORM, p.value()));
			if (annotation instanceof MatrixParam p) found.add(new BindingName(Binding.MATRIX, p.value()));
		}
		if (found.size() > 1) types.error(source, "Conflicting JAX-RS bindings on one parameter");
		return found.isEmpty() ? null : found.getFirst();
	}
	private static Policy policy(TypeReader types, Class<?> owner, Method method) {
		List<String> ignored = new ArrayList<>();
		List<String> internal = new ArrayList<>();
		List<AnnotatedElement> scopes = new ArrayList<>();
		for (Class<?> c = owner; c != null && c != Object.class; c = c.getSuperclass()) scopes.add(c);
		if (method != null) scopes.add(method);
		for (AnnotatedElement scope : scopes) {
			Ignore ignore = scope.getDeclaredAnnotation(Ignore.class);
			Internal caution = scope.getDeclaredAnnotation(Internal.class);
			if (ignore != null) ignored.add(ignore.reason());
			if (caution != null) internal.add(caution.reason());
		}
		return new Policy(sorted(ignored), sorted(internal));
	}
	private static DeprecationInfo deprecation(TypeReader types, AnnotatedElement element, String source) {
		Deprecated deprecated = element.getAnnotation(Deprecated.class);
		Deprecation details = element.getAnnotation(Deprecation.class);
		if (details != null) {
			types.nonBlank(details.reason(), source, "Deprecation reason");
			if (deprecated == null) types.error(source, "Deprecation requires java.lang.Deprecated");
		}
		return new DeprecationInfo(deprecated != null, deprecated == null ? "" : deprecated.since(),
				deprecated != null && deprecated.forRemoval(), details == null ? "" : details.reason(), details == null ? "" : details.replacement());
	}
	private static String description(AnnotatedElement element) {
		Description description = element == null ? null : element.getAnnotation(Description.class);
		return description == null ? "" : description.value();
	}
	private static String joinPath(String base, String suffix) {
		return "/" + (base.replaceAll("^/+|/+$", "") + "/" + suffix.replaceAll("^/+|/+$", "")).replaceAll("^/+|/+$", "");
	}
	private static List<String> sorted(List<String> values) { return values.stream().sorted().toList(); }
	private static List<String> merge(List<String> left, List<String> right) {
		List<String> merged = new ArrayList<>(left); merged.addAll(right); return sorted(merged);
	}
}
