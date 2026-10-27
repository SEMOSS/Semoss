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
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

import prerna.contract.ContractException;
import prerna.contract.ContractReader;
import prerna.contract.Description;
import prerna.contract.Invocation;
import prerna.contract.Parameter;
import prerna.contract.ReactorDefinition;
import prerna.contract.ReactorMetadata;
import prerna.contract.Returns;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AbstractReactor2UnitTests {
	@Test void existingRequiredInputBehaviorIsPreserved() {
		var reactor = new StopPixelExecutionReactor();
		assertThrows(IllegalArgumentException.class, reactor::execute);
		assertEquals(ContractReader.read(StopPixelExecutionReactor.class), reactor.getContract());
		assertEquals("Stop the current execution of a pixel job", reactor.getReactorDescription());
		assertEquals("string", reactor.getResponseSchema().getString("type"));
		assertEquals(List.of("id"), reactor.asMcpTool().getJSONObject("inputSchema").getJSONArray("required").toList());
	}

	@Test void namedAndPositionalCallsStillReturnSerializedStringOutputs() throws Exception {
		for (boolean positional : new boolean[] { false, true }) {
			var reactor = new StopPixelExecutionReactor();
			String id = "contract-test-missing-job";
			reactor.curNoun(positional ? AbstractReactor.ALL_NOUN_STORE : "id");
			reactor.getCurRow().add(new NounMetadata(id, PixelDataType.CONST_STRING));
			NounMetadata result = reactor.execute();
			assertEquals("Pixel job " + id + " not found", result.getValue());
			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			// Exercise the actual wire serializer, rather than serializing a separate fixture.
			var serializer = PixelStreamUtility.class.getDeclaredMethod("processNounMetadata", prerna.om.Insight.class,
					PrintStream.class, Gson.class, NounMetadata.class, prerna.om.Pixel.class);
			serializer.setAccessible(true);
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			serializer.invoke(null, null, new PrintStream(bytes), new Gson(), result, null);
			assertEquals(result.getValue(), JsonParser.parseString(bytes.toString(java.nio.charset.StandardCharsets.UTF_8))
					.getAsJsonObject().get("output").getAsString());
		}
	}

	@ReactorMetadata(Aliased.Metadata.class)
	public static final class Aliased extends AbstractReactor2 {
		public static final class Metadata implements ReactorDefinition.Provider {
			public ReactorDefinition definition() {
				return ReactorDefinition.builder("Alias example")
						.parameter(new ReactorDefinition.Argument("id", String.class, true, "Identifier", List.of("identifier"), false))
						.returns(String.class, "Echoed identifier")
						.invocation(Invocation.Mode.BOTH, List.of("id"), "", false).build();
			}
		}
		public NounMetadata execute() { organizeKeys(); return new NounMetadata(keyValue.get("id"), PixelDataType.CONST_STRING); }
	}
	@Test void derivesAliasEnforcementAndCanonicalMcpRequiredKeys() {
		var reactor = new Aliased();
		reactor.curNoun("identifier");
		reactor.getCurRow().add(new NounMetadata("value", PixelDataType.CONST_STRING));
		assertEquals("value", reactor.execute().getValue());
		assertEquals(List.of("id"), reactor.asMcpTool().getJSONObject("inputSchema").getJSONArray("required").toList());
		assertEquals("Identifier", reactor.getDescriptionForKey("identifier"));
		var first = reactor.getResponseSchema();
		first.put("type", "number");
		assertEquals("string", reactor.getResponseSchema().getString("type"));
	}

	@Description("Joins values in their declared positional order")
	@Parameter(name = "second", type = String.class, description = "Optional second value", nullable = true)
	@Parameter(name = "first", type = String.class, required = true, aliases = "value", description = "First value")
	@Returns(value = String.class, description = "Joined values")
	@Invocation(mode = Invocation.Mode.BOTH, order = {"second", "first"})
	public static final class Annotated extends AbstractReactor2 {
		@Override
		public NounMetadata execute() {
			organizeKeys();
			return new NounMetadata(keyValue.get("second") + ":" + keyValue.get("first"), PixelDataType.CONST_STRING);
		}
	}

	@Test void classAnnotationsDrivePositionalOrderOptionalityAndSchemas() {
		var reactor = new Annotated();
		reactor.curNoun(AbstractReactor.ALL_NOUN_STORE);
		reactor.getCurRow().add(new NounMetadata("left", PixelDataType.CONST_STRING));
		reactor.getCurRow().add(new NounMetadata("right", PixelDataType.CONST_STRING));
		assertEquals("left:right", reactor.execute().getValue());
		assertArrayEquals(new String[] {"second", "first,value"}, reactor.keysToGet);
		assertArrayEquals(new int[] {0, 1}, reactor.keyRequired);
		assertEquals("First value", reactor.getDescriptionForKey("value"));
		assertEquals("Joined values", reactor.getResponseSchema().getString("description"));
		var schema = reactor.asMcpTool().getJSONObject("inputSchema");
		assertEquals(List.of("first"), schema.getJSONArray("required").toList());
		assertEquals("null", schema.getJSONObject("properties").getJSONObject("second")
				.getJSONArray("anyOf").getJSONObject(1).getString("type"));
		assertEquals(ContractReader.read(Annotated.class), reactor.getContract());
		assertTrue(reactor.getContract().diagnostics().isEmpty());
	}

	private static int annotatedInitializations;
	@Description("Read this contract without constructing or initializing its reactor")
	@Parameter(name = "id", type = String.class, required = true)
	@Returns(String.class)
	@Invocation(mode = Invocation.Mode.BOTH, order = "id")
	public static final class Uninitialized extends AbstractReactor2 {
		static { annotatedInitializations++; }
		public Uninitialized() { throw new AssertionError("Reactor constructor must not run"); }
		@Override public NounMetadata execute() { throw new AssertionError("Operation must not run"); }
	}

	@Test void readsAnnotatedReactorWithoutInitializationOrConstruction() {
		var contract = ContractReader.read(Uninitialized.class);
		assertEquals(0, annotatedInitializations);
		assertEquals(List.of("id"), contract.invocation().order());
		assertTrue(contract.diagnostics().isEmpty());
	}

	@Description("Missing invocation")
	@Returns(String.class)
	public static final class MissingInvocation extends AbstractReactor2 {
		@Override public NounMetadata execute() { throw new AssertionError(); }
	}
	@Description("Conflicts with the provider")
	@ReactorMetadata(Aliased.Metadata.class)
	public static final class ConflictingMetadata extends AbstractReactor2 {
		@Override public NounMetadata execute() { throw new AssertionError(); }
	}

	@Test void reportsMissingInvocationAndCompetingMetadataClearly() {
		var error = assertThrows(IllegalArgumentException.class, MissingInvocation::new);
		assertTrue(error.getMessage().contains("must declare @Invocation"));
		assertThrows(ContractException.class, ConflictingMetadata::new);
	}
}
