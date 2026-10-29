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

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.google.gson.Gson;
import com.google.gson.JsonParser;

import prerna.contract.*;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AbstractReactor2UnitTests {
    private static void argument(AbstractReactor reactor, String name, Object value) {
        reactor.curNoun(name);
        reactor.getCurRow().add(new NounMetadata(value, PixelDataType.CUSTOM_DATA_STRUCTURE));
    }

    @Test void existingRequiredInputBehaviorAndStringContractArePreserved() {
        var reactor = new StopPixelExecutionReactor();
        assertThrows(IllegalArgumentException.class, reactor::execute);
        assertSame(ContractReader.read(StopPixelExecutionReactor.class), reactor.getContract());
        assertEquals("StopPixelExecution", reactor.getName());
        assertEquals("Stop the current execution of a pixel job", reactor.getReactorDescription());
        assertEquals("string", reactor.getResponseSchema().getString("type"));
        assertEquals("message", reactor.getResponseSchema().getString("title"));
        assertEquals(List.of("id"), reactor.asMcpTool().getJSONObject("inputSchema").getJSONArray("required").toList());
        assertEquals(List.of("id"), reactor.getContract().invocation().order());
    }

    private static com.google.gson.JsonObject serialize(NounMetadata result) throws Exception {
        var serializer = PixelStreamUtility.class.getDeclaredMethod("processNounMetadata", prerna.om.Insight.class,
                PrintStream.class, Gson.class, NounMetadata.class, prerna.om.Pixel.class);
        serializer.setAccessible(true);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        serializer.invoke(null, null, new PrintStream(bytes), new Gson(), result, null);
        return JsonParser.parseString(bytes.toString(java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject();
    }

    @Test void namedAndPositionalCancellationStillProduceTheActualStringWireOutput() throws Exception {
        for (boolean positional : new boolean[] { false, true }) {
            var reactor = new StopPixelExecutionReactor();
            String id = "contract-test-missing-job";
            argument(reactor, positional ? AbstractReactor.ALL_NOUN_STORE : "id", id);
            NounMetadata result = reactor.execute();
            assertEquals("Pixel job " + id + " not found", result.getValue());
            assertEquals(PixelDataType.CONST_STRING, result.getNounType());
            assertEquals(List.of(PixelOperationType.OPERATION), result.getOpType());
            assertEquals(result.getValue(), serialize(result).get("output").getAsString());
        }
    }

    @Reactor(name = "TypedEcho", aliases = "EchoAlias", description = "Echo a typed input")
    @Invocation(mode = Invocation.Mode.BOTH)
    public static final class Echo extends AbstractReactor2<Echo.Input, String> {
        public record Input(@Key(name = "id", aliases = "identifier", description = "Identifier") String jobId,
                @Key(description = "Optional suffix") @Optional @Nullable String suffix) {}
        @Override @Output(name = "message", description = "Echoed identifier")
        protected String execute(Input input) { return input.jobId() + (input.suffix() == null ? "" : input.suffix()); }
    }

    @Test void actualRecordDefinesAliasesOrderAndOptionality() {
        var reactor = new Echo();
        argument(reactor, "identifier", "value");
        assertEquals("value", reactor.execute().getValue());
        assertArrayEquals(new String[] {"id,identifier", "suffix"}, reactor.keysToGet);
        assertArrayEquals(new int[] {1, 0}, reactor.keyRequired);
        assertEquals("Identifier", reactor.getDescriptionForKey("identifier"));
        assertEquals(List.of("id", "suffix"), reactor.getContract().invocation().order());
        assertEquals(List.of("EchoAlias"), reactor.getContract().aliases());
        reactor.getResponseSchema().put("type", "number");
        assertEquals("string", reactor.getResponseSchema().getString("type"));
        var mixed = new Echo();
        argument(mixed, "identifier", "first");
        argument(mixed, "all", "second");
        assertEquals("firstsecond", mixed.execute().getValue());
        var nullable = new Echo(); argument(nullable, "id", "value"); argument(nullable, "suffix", null);
        assertEquals("value", nullable.execute().getValue());
    }

    @Test void rejectsWrongTypesNullRequiredValuesDuplicateAliasesAndExtraPositionals() {
        var wrong = new Echo(); argument(wrong, "id", 12);
        assertThrows(IllegalArgumentException.class, wrong::execute);
        var nil = new Echo(); argument(nil, "id", null);
        assertThrows(IllegalArgumentException.class, nil::execute);
        var duplicate = new Echo(); argument(duplicate, "id", "one"); argument(duplicate, "identifier", "two");
        assertThrows(IllegalArgumentException.class, duplicate::execute);
        var extra = new Echo(); argument(extra, "all", "one"); argument(extra, "all", "two"); argument(extra, "all", "three");
        assertThrows(IllegalArgumentException.class, extra::execute);
    }

    public record Filter(@Description("Minimum") int min, @Description("Allowed tags") List<String> tags) {}
    public record Result(@WireName("item-id") String id, @Optional String absent,
            @Nullable String nullable, List<Filter> filters, Map<String, List<Integer>> counts) {}

    @Reactor(name = "Structured", description = "Uses actual nested input and output records")
    @Invocation(mode = Invocation.Mode.NAMED)
    public static final class Structured extends AbstractReactor2<Structured.Input, Result> {
        public record Input(@Key(description = "Filter definitions") List<Filter> filters,
                @Key(description = "Counts") Map<String, List<Integer>> counts) {}
        @Override @Output(name = "result", description = "Structured result")
        protected Result execute(Input input) { return new Result("row", null, null, input.filters(), input.counts()); }
    }

    @Test void bindsNestedRecordsAndCollectionsAndSerializesTheDeclaredWireNames() throws Exception {
        var reactor = new Structured();
        argument(reactor, "filters", List.of(Map.of("min", 2L, "tags", List.of("a"))));
        argument(reactor, "counts", Map.of("a", List.of(3L)));
        var wire = serialize(reactor.execute()).getAsJsonObject("output");
        assertEquals("row", wire.get("item-id").getAsString());
        assertFalse(wire.has("absent"));
        assertTrue(wire.has("nullable"));
        assertTrue(wire.get("nullable").isJsonNull());
        assertEquals(2, wire.getAsJsonArray("filters").get(0).getAsJsonObject().get("min").getAsInt());
        assertEquals(3, wire.getAsJsonObject("counts").getAsJsonArray("a").get(0).getAsInt());
        var invalid = new Structured();
        argument(invalid, "filters", List.of(Map.of("min", 1.5, "tags", List.of("a"))));
        argument(invalid, "counts", Map.of());
        assertThrows(IllegalArgumentException.class, invalid::execute);
        var positional = new Structured(); argument(positional, "all", List.of());
        assertThrows(IllegalArgumentException.class, positional::execute);
        var json = new Structured();
        argument(json, "filters", JsonParser.parseString("[{\"min\":2,\"tags\":[\"a\"]}]"));
        argument(json, "counts", JsonParser.parseString("{\"a\":[3]}"));
        assertEquals(wire, serialize(json.execute()).getAsJsonObject("output"));
    }

    @Reactor(name = "Variadic", description = "Joins positional values")
    @Invocation(mode = Invocation.Mode.POSITIONAL, variadic = "values")
    public static final class Variadic extends AbstractReactor2<Variadic.Input, List<String>> {
        public record Input(@Key(description = "Values to join") List<String> values) {}
        @Override @Output(name = "values", description = "Collected values")
        protected List<String> execute(Input input) { return input.values(); }
    }
    @Test void handlesVariadicInputsWithoutTreatingEveryArrayAsVariadic() throws Exception {
        var reactor = new Variadic(); argument(reactor, "all", "a"); argument(reactor, "all", "b");
        assertEquals(JsonParser.parseString("[\"a\",\"b\"]"), serialize(reactor.execute()).get("output"));
        var named = new Variadic(); argument(named, "values", List.of("a"));
        assertThrows(IllegalArgumentException.class, named::execute);
    }

    private static int initializations;
    @Reactor(name = "Uninitialized", description = "Read without running code")
    @Invocation(mode = Invocation.Mode.BOTH)
    public static final class Uninitialized extends AbstractReactor2<Uninitialized.Input, String> {
        static { initializations++; }
        public record Input(@Key(description = "Identifier") String id) {}
        public Uninitialized() { throw new AssertionError("Constructor must not run"); }
        @Override @Output(name = "message", description = "Result")
        protected String execute(Input input) { throw new AssertionError("Execution must not run"); }
    }
    @Test void deterministicReadsDoNotInitializeConstructOrExecuteReactors() {
        var contract = ContractReader.read(Uninitialized.class);
        assertSame(contract, ContractReader.read(Uninitialized.class));
        assertEquals(0, initializations);
        assertTrue(contract.diagnostics().isEmpty());
    }

    @Reactor(name = "MissingKey", description = "Incomplete input")
    @Invocation(mode = Invocation.Mode.BOTH)
    public static final class MissingKey extends AbstractReactor2<MissingKey.Input, String> {
        public record Input(String id) {}
        @Override @Output(name = "message", description = "Result")
        protected String execute(Input input) { return input.id(); }
    }
    @Reactor(name = "MissingOutput", description = "Incomplete output")
    @Invocation(mode = Invocation.Mode.BOTH)
    public static final class MissingOutput extends AbstractReactor2<MissingOutput.Input, String> {
        public record Input(@Key(description = "Identifier") String id) {}
        @Override protected String execute(Input input) { return input.id(); }
    }
    @Reactor(name = "Conflicting", description = "Duplicate metadata")
    @Parameter(name = "id", type = String.class)
    @Invocation(mode = Invocation.Mode.BOTH)
    public static final class Conflicting extends AbstractReactor2<Conflicting.Input, String> {
        public record Input(@Key(description = "Identifier") String id) {}
        @Override @Output(name = "message", description = "Result")
        protected String execute(Input input) { return input.id(); }
    }
    @Test void metadataGapsAndContradictionsFailBeforeExportOrExecution() {
        for (Class<?> type : List.of(MissingKey.class, MissingOutput.class, Conflicting.class))
            assertThrows(ContractException.class, () -> ContractReader.read(type));
    }

    @Reactor(name = "InvalidResult", description = "Invalid null in a non-null result")
    @Invocation(mode = Invocation.Mode.NAMED, canOmitParameters = true)
    public static final class InvalidResult extends AbstractReactor2<InvalidResult.Input, String> {
        public record Input() {}
        @Override @Output(name = "message", description = "Result")
        protected String execute(Input input) { return null; }
    }
    @Test void rejectsOutputValuesThatViolateTheDeclaredNullability() {
        assertThrows(IllegalArgumentException.class, () -> new InvalidResult().execute());
    }

    @Reactor(name = "NullableList", description = "Preserves explicit null lists")
    @Invocation(mode = Invocation.Mode.NAMED)
    public static final class NullableList extends AbstractReactor2<NullableList.Input, List<String>> {
        public record Input(@Key(description = "Nullable list") @Nullable List<@Nullable String> values) {}
        @Override @Output(name = "values", description = "The supplied list") @Nullable
        protected List<@Nullable String> execute(Input input) { return input.values(); }
    }
    @Test void nullableListsRemainDistinctFromListsContainingNull() throws Exception {
        var nil = new NullableList(); argument(nil, "values", null);
        assertTrue(serialize(nil.execute()).get("output").isJsonNull());
        var list = new NullableList(); argument(list, "values", java.util.Arrays.asList((String) null));
        assertEquals(JsonParser.parseString("[null]"), serialize(list.execute()).get("output"));
        var json = new NullableList(); argument(json, "values", com.google.gson.JsonNull.INSTANCE);
        assertTrue(serialize(json.execute()).get("output").isJsonNull());
    }

    @Test void javaCompilerEnforcesActualInputAndOutputTypes(@TempDir Path directory) throws Exception {
        String source = """
            import prerna.reactor.AbstractReactor2;
            public class CompilerExample extends AbstractReactor2<CompilerExample.Input, String> {
                public record Input(String id) {}
                protected String execute(Input input) { return %s; }
            }
            """;
        Path file = directory.resolve("CompilerExample.java");
        Files.writeString(file, source.formatted("input.id()"));
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new ByteArrayOutputStream();
        String[] arguments = {"-proc:none", "-classpath", System.getProperty("java.class.path"), "-d", directory.toString(), file.toString()};
        assertEquals(0, compiler.run(null, null, diagnostics, arguments), diagnostics.toString());
        Files.writeString(file, source.formatted("42"));
        assertNotEquals(0, compiler.run(null, null, diagnostics, arguments));
        Files.writeString(file, source.formatted("input.missing()"));
        assertNotEquals(0, compiler.run(null, null, diagnostics, arguments));
    }
}
