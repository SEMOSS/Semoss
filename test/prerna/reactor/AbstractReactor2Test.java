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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import io.swagger.v3.oas.annotations.media.Schema;
import prerna.openapi.Internal;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AbstractReactor2Test {

	enum Color {
		RED, BLUE
	}

	/** Echoes its typed input back as a map, to check how each type is read. */
	static class TypesReactor extends AbstractReactor2<TypesReactor.Input, Map<String, Object>> {

		record Input(@Schema(description = "A name") String name, @Schema(description = "A count") Integer count,
				@Schema(description = "A ratio") double ratio, @Schema(description = "A flag") Boolean flag,
				@Schema(description = "A color") Color color, @Schema(description = "Some tags") List<String> tags,
				@Schema(description = "A note", requiredMode = Schema.RequiredMode.NOT_REQUIRED) String note) {
		}

		@Override
		protected Map<String, Object> run(Input input) {
			Map<String, Object> out = new java.util.HashMap<>();
			out.put("name", input.name());
			out.put("count", input.count());
			out.put("ratio", input.ratio());
			out.put("flag", input.flag());
			out.put("color", input.color());
			out.put("tags", input.tags());
			out.put("note", input.note());
			return out;
		}
	}

	static class OneInputReactor extends AbstractReactor2<OneInputReactor.Input, Integer> {

		record Input(@Schema(description = "How many") Integer count) {
		}

		@Override
		protected Integer run(Input input) {
			return input.count() * 2;
		}

		@Override
		protected List<PixelOperationType> getOperationTypes() {
			return List.of(PixelOperationType.SUCCESS);
		}
	}

	static class OptionalPrimitiveReactor extends AbstractReactor2<OptionalPrimitiveReactor.Input, String> {

		record Input(@Schema(description = "Bad", requiredMode = Schema.RequiredMode.NOT_REQUIRED) int count) {
		}

		@Override
		protected String run(Input input) {
			return "";
		}
	}

	@Internal("Testing only")
	static class InternalReactor extends AbstractReactor2<NoInput, String> {
		@Override
		protected String run(NoInput input) {
			return "internal";
		}
	}

	static class UndeclaredDynamicReactor extends AbstractReactor2<NoInput, Object> {
		@Override
		protected Object run(NoInput input) {
			return null;
		}
	}

	private static void named(AbstractReactor reactor, String key, Object... values) {
		reactor.curNoun(key);
		for (Object value : values) {
			if (value instanceof String s) {
				reactor.getCurRow().addLiteral(s);
			} else if (value instanceof Integer i) {
				reactor.getCurRow().addInteger(i);
			} else if (value instanceof Double d) {
				reactor.getCurRow().addDecimal(d);
			} else if (value instanceof Boolean b) {
				reactor.getCurRow().addBoolean(b);
			}
		}
		reactor.closeNoun(key);
	}

	private static TypesReactor typesReactorWithRequiredInputs() {
		return typesReactorWithColor("BLUE");
	}

	private static TypesReactor typesReactorWithColor(String color) {
		TypesReactor reactor = new TypesReactor();
		reactor.In();
		named(reactor, "name", "Ann");
		named(reactor, "count", 3);
		named(reactor, "ratio", 0.5);
		named(reactor, "flag", true);
		named(reactor, "color", color);
		named(reactor, "tags", "a", "b");
		return reactor;
	}

	@Test
	void declaresKeysRequirednessAndDescriptionsFromTheRecord() {
		TypesReactor reactor = new TypesReactor();

		assertArrayEquals(new String[] { "name", "count", "ratio", "flag", "color", "tags", "note" },
				reactor.keysToGet);
		JSONObject tool = reactor.asMcpTool();
		JSONObject properties = tool.getJSONObject("inputSchema").getJSONObject("properties");
		assertEquals("A count", properties.getJSONObject("count").getString("description"));
		assertEquals("integer", properties.getJSONObject("count").getString("type"));
		assertEquals("number", properties.getJSONObject("ratio").getString("type"));
		assertEquals("boolean", properties.getJSONObject("flag").getString("type"));
		assertEquals("array", properties.getJSONObject("tags").getString("type"));
		assertEquals(List.of("name", "count", "ratio", "flag", "color", "tags"),
				tool.getJSONObject("inputSchema").getJSONArray("required").toList());
	}

	@Test
	@SuppressWarnings("unchecked")
	void readsEveryInputAsItsDeclaredType() {
		NounMetadata noun = typesReactorWithRequiredInputs().execute();
		Map<String, Object> out = (Map<String, Object>) noun.getValue();

		assertEquals(PixelDataType.MAP, noun.getNounType());
		assertEquals("Ann", out.get("name"));
		assertEquals(3, out.get("count"));
		assertEquals(0.5, out.get("ratio"));
		assertEquals(true, out.get("flag"));
		assertEquals(Color.BLUE, out.get("color"));
		assertEquals(List.of("a", "b"), out.get("tags"));
		assertNull(out.get("note"));
		assertEquals(List.of(PixelOperationType.OPERATION), noun.getOpType());
	}

	@Test
	void rejectsAMissingRequiredInputTheWayOrganizeKeysDoes() {
		TypesReactor reactor = new TypesReactor();
		reactor.In();
		named(reactor, "name", "Ann");

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
		assertTrue(error.getMessage().startsWith("Required input(s) missing:"), error.getMessage());
	}

	@Test
	void rejectsAValueOfTheWrongType() {
		TypesReactor reactor = typesReactorWithColor("GREEN");

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
		assertTrue(error.getMessage().contains("color must be one of [RED, BLUE]"), error.getMessage());
	}

	@Test
	void acceptsASingleInputByPositionAndWrapsTheOutputWithItsOperationTypes() {
		OneInputReactor reactor = new OneInputReactor();
		reactor.In();
		reactor.getCurRow().addInteger(21);

		NounMetadata noun = reactor.execute();

		assertEquals(42, noun.getValue());
		assertEquals(PixelDataType.CONST_INT, noun.getNounType());
		assertEquals(List.of(PixelOperationType.SUCCESS), noun.getOpType());
	}

	@Test
	void readsTheContractFromTheClassWithoutConstructingIt() {
		assertEquals(StopPixelExecutionReactor.Input.class,
				AbstractReactor2.inputType(StopPixelExecutionReactor.class));
		assertEquals(String.class, AbstractReactor2.outputType(StopPixelExecutionReactor.class));
		assertEquals(NoInput.class, AbstractReactor2.inputType(prerna.reactor.insights.GetCurrentInsightIdReactor.class));
		assertTrue(AbstractReactor2.isDynamicOutput(AbstractReactor2.outputType(EchoReactor.class)));
		assertThrows(IllegalArgumentException.class, () -> AbstractReactor2.inputType(AbstractReactor.class));
	}

	@Test
	void failsFastOnContractsItCannotHonor() {
		assertThrows(IllegalStateException.class, OptionalPrimitiveReactor::new);
		assertThrows(IllegalStateException.class, UndeclaredDynamicReactor::new);
	}

	@Test
	void keepsThePilotReactorsContracts() {
		StopPixelExecutionReactor stop = new StopPixelExecutionReactor();
		assertArrayEquals(new String[] { "id" }, stop.keysToGet);
		assertEquals("StopPixelExecution", stop.asMcpTool().getString("name"));
		assertTrue(stop.getHelp().contains("If running the pixel synchronously"));

		EchoReactor echo = new EchoReactor();
		assertArrayEquals(new String[] { "value" }, echo.keysToGet);
	}

	@Test
	void internalIsReadableFromTheClassAndDoesNotChangeExecution() {
		Internal internal = InternalReactor.class.getAnnotation(Internal.class);
		assertEquals("Testing only", internal.value());

		InternalReactor reactor = new InternalReactor();
		reactor.In();
		assertEquals("internal", reactor.execute().getValue());
	}

	@Test
	void echoStillReturnsTheNounItWasGiven() {
		EchoReactor named = new EchoReactor();
		named.In();
		named(named, "value", 7);
		NounMetadata namedOut = named.execute();
		assertEquals(7, namedOut.getValue());
		assertEquals(PixelDataType.CONST_INT, namedOut.getNounType());

		EchoReactor positional = new EchoReactor();
		positional.In();
		positional.getCurRow().addLiteral("hi");
		NounMetadata positionalOut = positional.execute();
		assertSame(positional.getCurRow().getNoun(0), positionalOut);

		EchoReactor empty = new EchoReactor();
		empty.In();
		NounMetadata emptyOut = empty.execute();
		assertEquals("", emptyOut.getValue());
		assertEquals(PixelDataType.CONST_STRING, emptyOut.getNounType());
	}
}
