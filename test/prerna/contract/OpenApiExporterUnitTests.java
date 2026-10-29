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
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;

class OpenApiExporterUnitTests {
    static int initializations;
    static void initialized() { initializations++; }

    @Path("/items") @Internal(reason = "Platform use")
    static class Resource {
        static { initialized(); }
        Resource() { throw new AssertionError("Do not construct resources"); }
        @GET @Path("/{id}") @Produces("application/json")
        @Description("Read an item")
        @Parameter(name = "id", required = true)
        @Parameter(name = "limit", type = Integer.class)
        @Returns(value = Node.class, status = 200, description = "Found")
        @Returns(value = String.class, status = 404, description = "Missing")
        public Response read(@PathParam("id") String id, @QueryParam("limit") @DefaultValue("10") int limit) { throw new AssertionError("Do not run endpoints"); }

        @GET @Path("/empty") @Description("No body")
        @Returns(value = void.class, status = 204, description = "Empty")
        public Response empty() { throw new AssertionError(); }

        @GET @Path("/incomplete") public Response incomplete() { throw new AssertionError(); }
    }

    public record Node(@WireName("item-id") String id, @Optional @Nullable Node next,
            List<Map<String, @OneOf({String.class, Long.class}) Object>> values,
            Map<String, @Dynamic(reason = "Provider data") Object> metadata) {}

    @Description("Read a node")
    @Parameter(name = "id", aliases = "jobId", type = String.class, required = true, description = "Identifier")
    @Returns(value = Node.class, description = "The node")
    @Invocation(mode = Invocation.Mode.BOTH, order = "id")
    static class Pixel {
        static { initialized(); }
        Pixel() { throw new AssertionError("Do not construct reactors"); }
    }

    @Description("Old operation") @Returns(String.class)
    @Invocation(mode = Invocation.Mode.NAMED, canOmitParameters = true)
    @Ignore(reason = "Inbound only") @Internal(reason = "Platform only")
    @Deprecated(since = "6", forRemoval = true)
    @Deprecation(reason = "Retired", replacement = "ReadNode")
    static class Legacy {}

    @Reactor(name = "TypedEcho", aliases = "EchoAlias", description = "Echo an identifier")
    @Invocation(mode = Invocation.Mode.BOTH)
    static class Typed {
        static { initialized(); }
        public record Input(@Key(description = "Identifier") String id) {}
        @Output(name = "message", description = "Echoed identifier")
        protected String execute(Input input) { throw new AssertionError("Do not execute"); }
    }

    @Test void typedIdentityAliasesAndActualOutputTypeDriveTheExport() {
        var contract = ContractReader.read(Typed.class);
        OpenApiExporter exporter = new OpenApiExporter("Example", "1");
        exporter.addPixel(contract);
        JSONObject reactors = document(exporter).getJSONObject("x-semoss-pixels").getJSONObject("reactors");
        assertEquals(2, reactors.length());
        JSONObject pixel = reactors.getJSONObject("TypedEcho");
        assertEquals("string", pixel.getJSONObject("outputSchema").getString("type"));
        assertEquals("message", pixel.getJSONObject("outputSchema").getString("title"));
        assertEquals("id", pixel.getJSONObject("invocation").getJSONArray("order").getString(0));
        assertEquals(List.of("id"), pixel.getJSONObject("inputSchema").getJSONArray("required").toList());
        assertTrue(pixel.similar(reactors.getJSONObject("EchoAlias")));
        assertThrows(IllegalArgumentException.class, () -> exporter.addPixel("UnregisteredName", contract));
        assertEquals(0, initializations);
    }

    private static OperationContract rest() throws Exception {
        return ContractReader.read(Resource.class.getMethod("read", String.class, int.class));
    }
    private static JSONObject document(OpenApiExporter exporter) { return new JSONObject(exporter.json()); }

    @Test void emitsStandardRestBindingsStatusesAndSharedSchemasWithoutExecution() throws Exception {
        OpenApiExporter exporter = new OpenApiExporter("Example", "1");
        exporter.addRest("/health", "readItem", rest());
        JSONObject doc = document(exporter);
        assertEquals("3.1.0", doc.getString("openapi"));
        JSONObject operation = doc.getJSONObject("paths").getJSONObject("/health/items/{id}").getJSONObject("get");
        assertEquals("readItem", operation.getString("operationId"));
        assertEquals(2, operation.getJSONObject("responses").length());
        JSONObject id = operation.getJSONArray("parameters").getJSONObject(0);
        assertEquals("path", id.getString("in")); assertTrue(id.getBoolean("required"));
        JSONObject limit = operation.getJSONArray("parameters").getJSONObject(1);
        assertEquals(10, limit.getJSONObject("schema").getInt("default"));
        assertFalse(limit.getBoolean("required"));
        assertEquals("Platform use", operation.getJSONArray("x-semoss-internal").getString(0));
        assertEquals(0, initializations);
        assertFalse(exporter.json().contains("$defs"));
    }

    @Test void preservesRecursiveWireShapesAndPixelInvocation() {
        OpenApiExporter exporter = new OpenApiExporter("Example", "1");
        exporter.addPixel("ReadNode", ContractReader.read(Pixel.class));
        JSONObject doc = document(exporter);
        JSONObject pixel = doc.getJSONObject("x-semoss-pixels").getJSONObject("reactors").getJSONObject("ReadNode");
        assertEquals("BOTH", pixel.getJSONObject("invocation").getString("mode"));
        assertEquals("id", pixel.getJSONObject("invocation").getJSONArray("order").getString(0));
        assertFalse(pixel.getJSONObject("invocation").getBoolean("canOmitParameters"));
        assertTrue(pixel.getJSONObject("invocation").isNull("variadic"));
        assertEquals("id", pixel.getJSONObject("inputSchema").getJSONArray("required").getString(0));
        assertEquals("jobId", pixel.getJSONObject("inputSchema").getJSONObject("properties").getJSONObject("id").getJSONArray("x-semoss-aliases").getString(0));
        String reference = pixel.getJSONObject("outputSchema").getString("$ref");
        assertTrue(reference.startsWith("#/components/schemas/"));
        JSONObject node = doc.getJSONObject("components").getJSONObject("schemas").getJSONObject(reference.substring("#/components/schemas/".length()));
        assertTrue(node.getJSONObject("properties").has("item-id"));
        assertFalse(node.getJSONArray("required").toList().contains("next"));
        JSONObject next = node.getJSONObject("properties").getJSONObject("next");
        assertEquals(reference, next.getJSONArray("anyOf").getJSONObject(0).getString("$ref"));
        assertEquals("null", next.getJSONArray("anyOf").getJSONObject(1).getString("type"));
        assertEquals("dynamic", node.getJSONObject("properties").getJSONObject("metadata").getJSONObject("additionalProperties").getString("x-contract-status"));
        for (String key : doc.getJSONObject("components").getJSONObject("schemas").keySet()) assertTrue(key.matches("[A-Za-z0-9_.-]+"));
        assertEquals(0, initializations);
    }

    @Test void independentPoliciesSurviveExport() {
        OpenApiExporter exporter = new OpenApiExporter("Example", "1");
        exporter.addPixel("Legacy", ContractReader.read(Legacy.class));
        JSONObject pixel = document(exporter).getJSONObject("x-semoss-pixels").getJSONObject("reactors").getJSONObject("Legacy");
        assertEquals("Inbound only", pixel.getJSONArray("x-semoss-ignore").getString(0));
        assertEquals("Platform only", pixel.getJSONArray("x-semoss-internal").getString(0));
        assertTrue(pixel.getBoolean("deprecated"));
        assertTrue(pixel.getJSONObject("x-semoss-deprecation").getBoolean("forRemoval"));
        assertEquals("ReadNode", pixel.getJSONObject("x-semoss-deprecation").getString("replacement"));
    }

    @Test void exportIsStableAcrossSelectionOrder() throws Exception {
        OpenApiExporter first = new OpenApiExporter("Example", "1");
        first.addRest("/health", "readItem", rest());
        first.addPixel("ReadNode", ContractReader.read(Pixel.class));
        OpenApiExporter second = new OpenApiExporter("Example", "1");
        second.addPixel("ReadNode", ContractReader.read(Pixel.class));
        second.addRest("/health", "readItem", rest());
        assertEquals(first.json(), second.json());
        assertEquals(first.json(), first.json());
    }

    @Test void rejectsGapsAndDuplicateRoutesBeforePublishing() throws Exception {
        OpenApiExporter exporter = new OpenApiExporter("Example", "1");
        assertThrows(IllegalArgumentException.class, () -> exporter.addRest("/health", "incomplete", ContractReader.read(Resource.class.getMethod("incomplete"))));
        exporter.addRest("/health", "readItem", rest());
        assertThrows(IllegalArgumentException.class, () -> exporter.addRest("/health", "duplicate", rest()));
        assertThrows(IllegalArgumentException.class, () -> exporter.addPixel("Bad", rest()));
    }

    @Test void noContentResponsesOmitContent() throws Exception {
        OpenApiExporter exporter = new OpenApiExporter("Example", "1");
        exporter.addRest("/health", "empty", ContractReader.read(Resource.class.getMethod("empty")));
        JSONObject response = document(exporter).getJSONObject("paths").getJSONObject("/health/items/empty").getJSONObject("get").getJSONObject("responses").getJSONObject("204");
        assertFalse(response.has("content"));
    }
}
