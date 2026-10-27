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

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import prerna.contract.OperationContract.Requirement;
import prerna.contract.TypeContract.*;

class ContractReaderUnitTests {
	@Internal(reason = "Platform operations")
	@Path("/items")
	static class Resource {
		static { initialized(); }
		Resource() { throw new AssertionError("Resource constructor must not run"); }

		@GET @Path("/{id}") @Produces("application/json")
		@Description("Read an item")
		@Returns(value = Item.class, status = 200)
		@Returns(value = String.class, status = 404)
		@Parameter(name = "id", required = true, description = "Item ID")
		@Parameter(name = "limit", type = Integer.class, description = "Maximum rows")
		public Response read(@PathParam("id") String id, @QueryParam("limit") @DefaultValue("10") int limit,
				@Context Object context) { throw new AssertionError("Endpoint must not run"); }

		@GET @Path("/legacy") @Ignore(reason = "Provider callback")
		@Deprecated(since = "5.5", forRemoval = true)
		@Deprecation(reason = "Old callback", replacement = "newCallback")
		public String legacy() { throw new AssertionError(); }

		@GET @Path("/bad")
		@Parameter(name = "query", binding = Parameter.Binding.PATH, required = true)
		public String bad(@QueryParam("query") String query) { return query; }

		@POST @Path("/form")
		@Parameter(name = "changes", type = Item[].class, binding = Parameter.Binding.FORM, encoding = Parameter.Encoding.JSON)
		public boolean form(@Context Object request) { return true; }

		@GET @Path("/unknown")
		public Response unknown(@QueryParam("q") String query) { return null; }
	}

	static int initializations;
	static void initialized() { initializations++; }
	public record Item(String id, @Optional @Nullable String label, long count) {}

	@Test void readsRestBindingsResponsesAndPolicyWithoutInitializingResource() throws Exception {
		var method = Resource.class.getMethod("read", String.class, int.class, Object.class);
		OperationContract contract = ContractReader.read(method);
		assertEquals(0, initializations);
		assertEquals("GET", contract.rest().method());
		assertEquals("/items/{id}", contract.rest().path());
		assertEquals(List.of("application/json"), contract.rest().produces());
		assertEquals(List.of(200, 404), contract.outputs().stream().map(OperationContract.Output::status).toList());
		assertTrue(contract.policy().internal());
		assertFalse(contract.policy().ignored());
		assertEquals(2, contract.inputs().size());
		assertEquals(Parameter.Binding.PATH, contract.inputs().getFirst().binding());
		assertEquals(Requirement.REQUIRED, contract.inputs().getFirst().requirement());
		assertEquals("10", contract.inputs().getLast().defaultValue());
		assertEquals(Requirement.OPTIONAL, contract.inputs().getLast().requirement());
		assertTrue(contract.diagnostics().isEmpty(), contract.diagnostics().toString());
		assertEquals(contract, ContractReader.read(method));
		assertThrows(UnsupportedOperationException.class, () -> contract.inputs().clear());
		assertThrows(UnsupportedOperationException.class, () -> contract.definitions().clear());
	}

	@Test void keepsIgnoreInternalAndDeprecationIndependent() throws Exception {
		var contract = ContractReader.read(Resource.class.getMethod("legacy"));
		assertTrue(contract.policy().ignored());
		assertTrue(contract.policy().internal());
		assertTrue(contract.deprecation().deprecated());
		assertTrue(contract.deprecation().forRemoval());
		assertEquals("5.5", contract.deprecation().since());
		assertEquals("newCallback", contract.deprecation().replacement());
	}

	@Test void rejectsConflictingNativeBindings() throws Exception {
		var error = assertThrows(ContractException.class, () -> ContractReader.read(Resource.class.getMethod("bad", String.class)));
		assertTrue(error.getMessage().contains("conflicts with JAX-RS"));
	}

	@Test void readsSupplementalFormEncodingAndReportsUnspecifiedContracts() throws Exception {
		var form = ContractReader.read(Resource.class.getMethod("form", Object.class));
		assertEquals(Parameter.Binding.FORM, form.inputs().getFirst().binding());
		assertEquals(Parameter.Encoding.JSON, form.inputs().getFirst().encoding());
		assertInstanceOf(Sequence.class, form.inputs().getFirst().type().shape());
		var unknown = ContractReader.read(Resource.class.getMethod("unknown", String.class));
		assertEquals(Requirement.UNKNOWN, unknown.inputs().getFirst().requirement());
		assertInstanceOf(Unknown.class, unknown.outputs().getFirst().type().shape());
		assertTrue(unknown.diagnostics().stream().anyMatch(d -> d.message().contains("requiredness")));
	}

	public record Node<T>(T value, @Optional @Nullable Node<T> next, List<@Nullable T> children) {}
	public static final class Nodes extends TypeRef<Map<String, List<Node<String>>>> {}
	@Nullable public static final class LongValue extends TypeRef<Long> {}
	public record Shapes(@WireName("wire_name") @Optional String name, @Nullable String explicitNull,
			@OneOf({String.class, Long.class}) Object union,
			Map<String, @Dynamic(reason = "Provider-defined data") Object> extensions,
			@Unverified(reason = "Legacy serializer needs auditing") Object unresolved,
			@Format("date-time") String timestamp) {}
	public enum Status {
		@WireName("ready") READY, WAITING;
		static { initialized(); }
	}

	@Test void resolvesGenericRecursiveTypesAndNullableElements() {
		assertEquals("int64", ContractReader.readType(LongValue.class).root().format());
		assertTrue(ContractReader.readType(LongValue.class).root().nullable());
		var contract = ContractReader.readType(Nodes.class);
		var dictionary = assertInstanceOf(Dictionary.class, contract.root().shape());
		var sequence = assertInstanceOf(Sequence.class, dictionary.value().shape());
		String id = assertInstanceOf(Reference.class, sequence.element().shape()).id();
		var properties = contract.definitions().get(id).properties();
		assertEquals(1, contract.definitions().size());
		var value = properties.stream().filter(p -> p.name().equals("value")).findFirst().orElseThrow();
		assertEquals(ScalarKind.STRING, assertInstanceOf(Scalar.class, value.type().shape()).kind());
		var next = properties.stream().filter(p -> p.name().equals("next")).findFirst().orElseThrow();
		assertFalse(next.required());
		assertTrue(next.type().nullable());
		assertEquals(id, assertInstanceOf(Reference.class, next.type().shape()).id());
		var children = properties.stream().filter(p -> p.name().equals("children")).findFirst().orElseThrow();
		assertTrue(assertInstanceOf(Sequence.class, children.type().shape()).element().nullable());
		assertTrue(contract.diagnostics().isEmpty(), contract.diagnostics().toString());
	}

	@Test void preservesWireNamesUnionsFormatsAndCertainty() {
		var contract = ContractReader.readType(Shapes.class);
		var properties = contract.definitions().get(Shapes.class.getName()).properties();
		var named = properties.stream().filter(p -> p.name().equals("wire_name")).findFirst().orElseThrow();
		assertFalse(named.required());
		assertFalse(named.type().nullable());
		var nullable = properties.stream().filter(p -> p.name().equals("explicitNull")).findFirst().orElseThrow();
		assertTrue(nullable.required());
		assertTrue(nullable.type().nullable());
		var union = properties.stream().filter(p -> p.name().equals("union")).findFirst().orElseThrow();
		assertEquals(2, assertInstanceOf(Union.class, union.type().shape()).alternatives().size());
		var extension = properties.stream().filter(p -> p.name().equals("extensions")).findFirst().orElseThrow();
		assertEquals(Knowledge.DYNAMIC, assertInstanceOf(Dictionary.class, extension.type().shape()).value().knowledge());
		assertEquals(1, contract.diagnostics().size(), contract.diagnostics().toString());
		assertEquals("Legacy serializer needs auditing", contract.diagnostics().getFirst().message());
		assertEquals(List.of("WAITING", "ready"), assertInstanceOf(Scalar.class, ContractReader.readType(Status.class).root().shape()).values());
		assertEquals(0, initializations);
	}

	@Ignore(reason = " ") static class InvalidIgnore {}
	public record InvalidUnion(@OneOf({String.class, String.class}) Object value) {}
	public record Conflicting(@Dynamic(reason = "Runtime") @Unverified(reason = "Unknown") Object value) {}
	@Test void rejectsInvalidContracts() {
		assertThrows(ContractException.class, () -> ContractReader.read(InvalidIgnore.class));
		assertThrows(ContractException.class, () -> ContractReader.readType(InvalidUnion.class));
		assertThrows(ContractException.class, () -> ContractReader.readType(Conflicting.class));
	}

	@ReactorMetadata(DefinedReactor.Metadata.class)
	public static class DefinedReactor {
		static { initialized(); }
		public DefinedReactor() { throw new AssertionError("Reactor constructor must not run"); }
		public static final class Metadata implements ReactorDefinition.Provider {
			public ReactorDefinition definition() {
				return ReactorDefinition.builder("Example")
						.parameter(new ReactorDefinition.Argument("id", String.class, true, "Identifier", List.of("identifier"), false))
						.returns(Item.class, "Item")
						.invocation(Invocation.Mode.BOTH, List.of("id"), "", false)
						.internal("Platform developers").build();
			}
		}
	}
	@Test void readsPureProviderWithoutInitializingOrConstructingReactor() {
		var contract = ContractReader.read(DefinedReactor.class);
		assertEquals(0, initializations);
		assertEquals(List.of("identifier"), contract.inputs().getFirst().aliases());
		assertEquals(List.of("id"), contract.invocation().order());
		assertFalse(contract.invocation().canOmitParameters());
		assertEquals(Requirement.REQUIRED, contract.inputs().getFirst().requirement());
		assertEquals(contract, ContractReader.read(DefinedReactor.class));
		assertTrue(contract.diagnostics().isEmpty());
	}

	@Parameter(name = "id", type = String.class, required = true)
	@Invocation(mode = Invocation.Mode.BOTH, order = {"id"}, canOmitParameters = true)
	static class InvalidInvocation {}
	@Test void refusesParameterOmissionWhenRequiredInputExists() {
		assertThrows(ContractException.class, () -> ContractReader.read(InvalidInvocation.class));
	}

	public static class BaseDto<T> { public T value; }
	public static class StringDto extends BaseDto<String> { public transient String hidden; public int count; }
	@Test void readsInheritedDtoFieldsAndIgnoresTransientFields() {
		var contract = ContractReader.readType(StringDto.class);
		var fields = contract.definitions().get(StringDto.class.getName()).properties();
		assertEquals(List.of("count", "value"), fields.stream().map(Property::name).toList());
		assertEquals(ScalarKind.STRING, assertInstanceOf(Scalar.class, fields.getLast().type().shape()).kind());
	}

	public record Box<T>(T value) {}
	public record QualifiedBoxes(Box<String> strict, Box<@Nullable String> nullable) {}
	public static class GenericRef<T> extends TypeRef<List<T>> {}
	public static final class NullableStrings extends GenericRef<@Nullable String> {}
	public static final class NullableDto extends BaseDto<@Nullable String> {}
	@Test void preservesQualifiersAcrossGenericSubstitutionAndDistinctDefinitions() {
		var list = assertInstanceOf(Sequence.class, ContractReader.readType(NullableStrings.class).root().shape());
		assertTrue(list.element().nullable());
		assertEquals(ScalarKind.STRING, assertInstanceOf(Scalar.class, list.element().shape()).kind());
		assertTrue(ContractReader.readType(NullableDto.class).definitions().get(NullableDto.class.getName())
				.properties().getFirst().type().nullable());
		var contract = ContractReader.readType(QualifiedBoxes.class);
		var fields = contract.definitions().get(QualifiedBoxes.class.getName()).properties();
		String nullableId = assertInstanceOf(Reference.class, fields.getFirst().type().shape()).id();
		String strictId = assertInstanceOf(Reference.class, fields.getLast().type().shape()).id();
		assertNotEquals(nullableId, strictId);
		assertTrue(contract.definitions().get(nullableId).properties().getFirst().type().nullable());
		assertFalse(contract.definitions().get(strictId).properties().getFirst().type().nullable());
	}

	public record GsonRecord(@com.google.gson.annotations.SerializedName("wire_value") String value) {}
	public record ConflictingWireName(@WireName("first") @com.google.gson.annotations.SerializedName("second") String value) {}
	public record Numbers(int count, double ratio, java.math.BigDecimal amount) {}
	public record Unresolved(List raw, Map<Integer, String> invalidKeys, Object unknown) {}
	@Test void distinguishesNumbersHonorsSerializerNamesAndReportsRawTypes() {
		assertEquals("wire_value", ContractReader.readType(GsonRecord.class).definitions().get(GsonRecord.class.getName())
				.properties().getFirst().name());
		assertThrows(ContractException.class, () -> ContractReader.readType(ConflictingWireName.class));
		var numbers = ContractReader.readType(Numbers.class).definitions().get(Numbers.class.getName()).properties();
		assertEquals(List.of("decimal", "int32", "float64"), numbers.stream().map(p -> p.type().format()).toList());
		assertEquals(ScalarKind.INTEGER, assertInstanceOf(Scalar.class, numbers.get(1).type().shape()).kind());
		assertEquals(ScalarKind.DECIMAL, assertInstanceOf(Scalar.class, numbers.getFirst().type().shape()).kind());
		assertFalse(ContractReader.readType(Unresolved.class).diagnostics().isEmpty());
	}

	@Description("Variadic example")
	@Parameter(name = "prefix", type = String.class)
	@Parameter(name = "values", type = String[].class)
	@Returns(String.class)
	@Invocation(mode = Invocation.Mode.POSITIONAL, order = {"prefix", "values"}, variadic = "values")
	static class Variadic {}
	@Parameter(name = "values", type = String[].class)
	@Invocation(mode = Invocation.Mode.POSITIONAL, order = {"values"}, variadic = "missing")
	static class InvalidVariadic {}
	@Deprecation(reason = "Replacement exists") static class MissingDeprecated {}
	@Test void keepsOptionalArgumentsSeparateFromOmittingTheArgumentAndValidatesVariadic() {
		var contract = ContractReader.read(Variadic.class);
		assertEquals(Invocation.Mode.POSITIONAL, contract.invocation().mode());
		assertEquals("values", contract.invocation().variadic());
		assertFalse(contract.invocation().canOmitParameters());
		assertTrue(contract.inputs().stream().allMatch(i -> i.requirement() == Requirement.OPTIONAL));
		assertThrows(ContractException.class, () -> ContractReader.read(InvalidVariadic.class));
		assertThrows(ContractException.class, () -> ContractReader.read(MissingDeprecated.class));
	}

	@Ignore(reason = "Inbound lifecycle callbacks") static class IgnoredResource {
		@GET @Path("/{id}") @Description("Callback") @Returns(value = String.class, status = 200)
		public String callback() { throw new AssertionError(); }
	}
	static class ChildResource extends IgnoredResource {}
	static class ConflictingResource {
		@GET @Parameter(name = "limit", type = String.class)
		public String type(@QueryParam("limit") int limit) { return ""; }
		@GET @Returns(value = String.class, status = 200) @Returns(value = Item.class, status = 200)
		public String status() { return ""; }
		@GET public String optional(@QueryParam("q") @Optional @Parameter(required = true) String q) { return q; }
	}
	@Test void inheritsResourcePoliciesAndDetectsIncompleteOrConflictingRestMetadata() throws Exception {
		var contract = ContractReader.read(ChildResource.class, IgnoredResource.class.getMethod("callback"));
		assertTrue(contract.policy().ignored());
		assertTrue(contract.diagnostics().stream().anyMatch(d -> d.message().contains("Path template input")));
		assertThrows(ContractException.class, () -> ContractReader.read(ConflictingResource.class.getMethod("type", int.class)));
		assertThrows(ContractException.class, () -> ContractReader.read(ConflictingResource.class.getMethod("status")));
		assertThrows(ContractException.class, () -> ContractReader.read(ConflictingResource.class.getMethod("optional", String.class)));
	}

	public static final class StringList extends java.util.ArrayList<@Nullable String> {}
	public static final class ReversedMap<V, K> extends java.util.HashMap<K, V> {}
	public static final class ReversedMapRef extends TypeRef<ReversedMap<Long, String>> {}
	public record ArrayBoxes(Box<String[]> strict, Box<@Nullable String[]> nullable) {}
	@Test void resolvesContainerInheritanceAndArrayComponentQualifiers() {
		var list = assertInstanceOf(Sequence.class, ContractReader.readType(StringList.class).root().shape());
		assertTrue(list.element().nullable());
		assertEquals(ScalarKind.STRING, assertInstanceOf(Scalar.class, list.element().shape()).kind());
		var map = assertInstanceOf(Dictionary.class, ContractReader.readType(ReversedMapRef.class).root().shape());
		assertEquals(ScalarKind.INTEGER, assertInstanceOf(Scalar.class, map.value().shape()).kind());
		var boxes = ContractReader.readType(ArrayBoxes.class);
		var properties = boxes.definitions().get(ArrayBoxes.class.getName()).properties();
		String nullableId = assertInstanceOf(Reference.class, properties.getFirst().type().shape()).id();
		String strictId = assertInstanceOf(Reference.class, properties.getLast().type().shape()).id();
		assertNotEquals(nullableId, strictId);
		assertTrue(assertInstanceOf(Sequence.class, boxes.definitions().get(nullableId).properties().getFirst().type().shape()).element().nullable());
		assertFalse(assertInstanceOf(Sequence.class, boxes.definitions().get(strictId).properties().getFirst().type().shape()).element().nullable());
	}

	static class InlineResource {
		static { initialized(); }
		InlineResource() { throw new AssertionError("Resource constructor must not run"); }
		@GET @Path("/ready") @Description("Readiness")
		@Returns(statuses = {503, 200}, description = "Readiness state", fields = {
				@Returns.Field(name = "status", type = String.class, values = {"READY", "NOT_READY"}, description = "State"),
				@Returns.Field(name = "startupComplete", type = boolean.class, description = "Startup completed")
		})
		public Response readiness() { throw new AssertionError("Endpoint must not run"); }
	}

	@Test void readsOneInlineBodyForMultipleStatusesWithoutExecutingTheResource() throws Exception {
		var method = InlineResource.class.getMethod("readiness");
		var contract = ContractReader.read(method);
		assertEquals(0, initializations);
		assertEquals(List.of(200, 503), contract.outputs().stream().map(OperationContract.Output::status).toList());
		assertEquals(contract.outputs().getFirst().type(), contract.outputs().getLast().type());
		String id = assertInstanceOf(Reference.class, contract.outputs().getFirst().type().shape()).id();
		var fields = contract.definitions().get(id).properties();
		assertEquals(List.of("startupComplete", "status"), fields.stream().map(Property::name).toList());
		assertTrue(fields.stream().allMatch(field -> field.required() && !field.type().nullable()));
		assertEquals(ScalarKind.BOOLEAN, assertInstanceOf(Scalar.class, fields.getFirst().type().shape()).kind());
		assertEquals(List.of("NOT_READY", "READY"), assertInstanceOf(Scalar.class, fields.getLast().type().shape()).values());
		assertEquals("State", fields.getLast().type().description());
		assertEquals(1, contract.definitions().size());
		assertTrue(contract.diagnostics().isEmpty(), contract.diagnostics().toString());
		assertEquals(contract, ContractReader.read(method));
		assertThrows(UnsupportedOperationException.class, fields::clear);
	}

	@Description("Inline structured Pixel output") @Invocation
	@Returns(fields = {
			@Returns.Field(name = "item", type = Item.class, required = false, nullable = true),
			@Returns.Field(name = "nodes", type = Nodes.class),
			@Returns.Field(name = "createdAt", type = String.class, format = "date-time", description = "Creation timestamp")
	})
	static class InlinePixel {}
	@Test void projectsInlineFieldsWithOptionalityNullabilityFormatsAndNestedTypeReferences() {
		var contract = ContractReader.read(InlinePixel.class);
		var schema = ContractSchemas.output(contract);
		String id = assertInstanceOf(Reference.class, contract.outputs().getFirst().type().shape()).id();
		var body = schema.getJSONObject("$defs").getJSONObject(id);
		assertEquals("/$defs/" + id.replace("~", "~0").replace("/", "~1"),
				java.net.URI.create(schema.getString("$ref")).getFragment());
		assertEquals("object", body.getString("type"));
		assertEquals(List.of("createdAt", "nodes"), body.getJSONArray("required").toList());
		var properties = body.getJSONObject("properties");
		assertEquals("date-time", properties.getJSONObject("createdAt").getString("format"));
		assertEquals("Creation timestamp", properties.getJSONObject("createdAt").getString("description"));
		assertEquals("null", properties.getJSONObject("item").getJSONArray("anyOf").getJSONObject(1).getString("type"));
		assertTrue(schema.getJSONObject("$defs").has(Item.class.getName()));
		assertEquals("object", properties.getJSONObject("nodes").getString("type"));
		assertTrue(contract.diagnostics().isEmpty(), contract.diagnostics().toString());
	}

	static class InvalidInlineResource {
		@GET @Returns(value = Item.class, fields = @Returns.Field(name = "id", type = String.class))
		public Response competingTypes() { return null; }
		@GET @Returns(fields = {@Returns.Field(name = "id", type = String.class), @Returns.Field(name = "id", type = int.class)})
		public Response duplicateFields() { return null; }
		@GET @Returns(status = 200, statuses = {200, 503}, value = String.class)
		public Response competingStatuses() { return null; }
		@GET @Returns(statuses = {200, 200}, value = String.class)
		public Response duplicateStatuses() { return null; }
		@GET @Returns(statuses = {200, -1}, value = String.class)
		public Response unspecifiedStatus() { return null; }
		@GET @Returns(fields = @Returns.Field(name = "state", type = boolean.class, values = "READY"))
		public Response nonStringValues() { return null; }
		@GET @Returns(fields = @Returns.Field(name = "state", type = String.class, values = {"READY", "READY"}))
		public Response duplicateValues() { return null; }
		@GET @Returns(fields = @Returns.Field(name = "state", type = Status.class, values = "unknown"))
		public Response invalidEnumValues() { return null; }
		@GET @Returns(fields = @Returns.Field(name = "empty", type = void.class))
		public Response voidField() { return null; }
		@GET @OneOf({String.class, Item.class}) @Returns(fields = @Returns.Field(name = "id", type = String.class))
		public Response conflictingUnion() { return null; }
	}
	@Test void rejectsContradictoryInlineDefinitions() {
		for (var method : InvalidInlineResource.class.getDeclaredMethods()) {
			assertThrows(ContractException.class, () -> ContractReader.read(method), method.getName());
		}
	}

	static class PartialInlineResource {
		@GET @Returns(status = 200)
		public Response missingBodyContract() { return null; }
		@GET @Returns(status = 200, fields = @Returns.Field(name = "unknown", type = Object.class))
		public Response unknownField() { return null; }
		@GET @Returns(value = void.class, status = 204)
		public Response noContent() { return null; }
	}
	@Test void keepsMissingTypesExplicitAndPreservesNoContent() throws Exception {
		var missing = ContractReader.read(PartialInlineResource.class.getMethod("missingBodyContract"));
		assertInstanceOf(Unknown.class, missing.outputs().getFirst().type().shape());
		assertTrue(missing.diagnostics().stream().anyMatch(d -> d.message().contains("No verified wire shape")));
		var unknown = ContractReader.read(PartialInlineResource.class.getMethod("unknownField"));
		assertTrue(unknown.diagnostics().stream().anyMatch(d -> d.source().endsWith(".unknown")));
		var empty = ContractReader.read(PartialInlineResource.class.getMethod("noContent"));
		assertInstanceOf(Empty.class, empty.outputs().getFirst().type().shape());
	}
}
