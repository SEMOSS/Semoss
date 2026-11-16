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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class ReactorDefinitionTest {

	static class DefinedReactor extends AbstractReactor {
		DefinedReactor() {
			this.keysToGet = new String[] { "ids", "count,howMany", "value", ReactorKeysEnum.LIMIT.getKey() };
			this.keyRequired = new int[] { 1, 0, 0, 0 };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().description("Finds things")
					.key("ids", key -> key.list(String.class).description("The ids to find"))
					.key("count", key -> key.type(Integer.class))
					.key("value", key -> key.type(Object.class).description("Any value"))
					.outputList(String.class, "The names found");
		}
	}

	/** Still overrides the deprecated methods. */
	static class OverridingReactor extends AbstractReactor {
		OverridingReactor() {
			this.keysToGet = new String[] { "flag" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public String getReactorDescription() {
			return "Overrides the methods";
		}

		@SuppressWarnings("deprecation")
		@Override
		protected String getDescriptionForKey(String key) {
			return key.equals("flag") ? "A flag" : super.getDescriptionForKey(key);
		}

		@SuppressWarnings("deprecation")
		@Override
		protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
			return key.equals("flag") ? MCP_KEY_TYPE.BOOLEAN : super.getKeyTypeForMCP(key);
		}
	}

	/** No keyRequired: MCP keeps treating every key as required. */
	static class NoRequiredReactor extends AbstractReactor {
		NoRequiredReactor() {
			this.keysToGet = new String[] { "a", "b,bee" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().key("b", key -> key.required(false));
		}
	}

	static class ObjectOutputReactor extends NoRequiredReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().outputSchema(new JSONObject().put("type", "object").put("properties",
					new JSONObject().put("total", new JSONObject().put("type", "integer"))));
		}
	}

	static class BareReactor extends AbstractReactor {
		BareReactor() {
			this.keysToGet = new String[] { "anything" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}
	}

	/** Its own definition names a key it does not take and says it takes none. */
	static class MistakenReactor extends AbstractReactor {
		MistakenReactor() {
			this.keysToGet = new String[] { "a" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().key("missing", "Not one of its keys").noKeys();
		}
	}

	/** Its keysToGet has an empty key and lists a key twice. */
	static class RepeatedKeyReactor extends AbstractReactor {
		RepeatedKeyReactor() {
			this.keysToGet = new String[] { "a", "", "a,alias" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}
	}

	/**
	 * Describes the keys of all its subclasses, each of which takes some of them.
	 */
	abstract static class SharedDescriptionsReactor extends AbstractReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().key("description", "The description").key("values", "The values");
		}
	}

	static class TakesDescriptionReactor extends SharedDescriptionsReactor {
		TakesDescriptionReactor() {
			this.keysToGet = new String[] { "description" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}
	}

	/**
	 * Does not take its parent's keys, so it starts from its own keys instead of
	 * super.
	 */
	static class StartsFreshReactor extends SharedDescriptionsReactor {
		StartsFreshReactor() {
			this.keysToGet = new String[] { "own" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return startDefinition().key("own", "Its own key");
		}
	}

	private static JSONObject property(IReactor reactor, String key) {
		return reactor.asMcpTool().getJSONObject("inputSchema").getJSONObject("properties").getJSONObject(key);
	}

	private static List<Object> required(IReactor reactor) {
		return reactor.asMcpTool().getJSONObject("inputSchema").getJSONArray("required").toList();
	}

	@Test
	void startsFromKeysToGetAndKeyRequired() {
		ReactorDefinition definition = new DefinedReactor().getReactorDefinition();

		assertEquals(List.of("ids", "count", "value", "limit"),
				definition.getKeys().stream().map(ReactorDefinition.Key::getName).toList());
		assertEquals(List.of("howMany"), definition.getKey("count").getAliases());
		assertTrue(definition.getKey("ids").getRequired());
		assertFalse(definition.getKey("count").getRequired());
		assertEquals("Limit to add for the query results", definition.getKey("limit").getDescription(),
				"an undescribed key keeps the shared description");
		assertNull(definition.getKey("limit").getType(), "an untyped key has no declared type");
		assertEquals(Integer.class, definition.getKey("limit").getTypeOrDefault(), "limit is an integer by default");
		assertEquals(String.class, new BareReactor().getReactorDefinition().getKey("anything").getTypeOrDefault());
		assertEquals(Integer.class, definition.getKey("count").getTypeOrDefault(), "a declared type wins");
		assertTrue(definition.getProblems().isEmpty());
	}

	@Test
	void mcpToolReadsTheDefinition() {
		DefinedReactor reactor = new DefinedReactor();
		JSONObject tool = reactor.asMcpTool();

		assertEquals("Finds things", tool.getString("description"));
		assertEquals("array", property(reactor, "ids").getString("type"));
		assertEquals("string", property(reactor, "ids").getJSONObject("items").getString("type"));
		assertEquals("The ids to find", property(reactor, "ids").getString("description"));
		assertEquals("integer", property(reactor, "count").getString("type"));
		assertEquals("string", property(reactor, "value").getString("type"), "MCP has no type for any value");
		assertEquals("integer", property(reactor, "limit").getString("type"), "an untyped key keeps the default");
		assertEquals(List.of("ids"), required(reactor));
	}

	@Test
	void mcpToolWrapsANonObjectOutput() {
		JSONObject outputSchema = new DefinedReactor().asMcpTool().getJSONObject("outputSchema");

		assertEquals("object", outputSchema.getString("type"));
		assertTrue(outputSchema.getBoolean(ReactorDefinition.WRAP_RESULT_MARKER));
		JSONObject result = outputSchema.getJSONObject("properties").getJSONObject(ReactorDefinition.WRAPPED_RESULT);
		assertEquals("array", result.getString("type"));
		assertEquals("string", result.getJSONObject("items").getString("type"));
		assertEquals("The names found", result.getString("description"));
		assertEquals(List.of(ReactorDefinition.WRAPPED_RESULT), outputSchema.getJSONArray("required").toList());
	}

	@Test
	void mcpToolKeepsAnObjectOutputAsIs() {
		JSONObject outputSchema = new ObjectOutputReactor().asMcpTool().getJSONObject("outputSchema");

		assertEquals("object", outputSchema.getString("type"));
		assertFalse(outputSchema.has(ReactorDefinition.WRAP_RESULT_MARKER));
		assertEquals("integer", outputSchema.getJSONObject("properties").getJSONObject("total").getString("type"));
	}

	@Test
	void mcpToolHasNoOutputSchemaWhenNoneIsDeclared() {
		assertFalse(new BareReactor().asMcpTool().has("outputSchema"));
	}

	@Test
	void mcpToolReadsTheDeprecatedMethodsOfAReactorThatStillOverridesThem() {
		OverridingReactor reactor = new OverridingReactor();

		assertEquals("Overrides the methods", reactor.asMcpTool().getString("description"));
		assertEquals("boolean", property(reactor, "flag").getString("type"));
		assertEquals("A flag", property(reactor, "flag").getString("description"));
	}

	@Test
	void mcpToolTreatsUnsetRequirednessAsRequired() {
		NoRequiredReactor reactor = new NoRequiredReactor();

		assertEquals(List.of("a"), required(reactor), "the definition can say a key is optional");
		JSONArray required = reactor.asMcpTool().getJSONObject("inputSchema").getJSONArray("required");
		assertFalse(required.toList().contains("b,bee"), "required lists canonical names, not aliases");
	}

	@Test
	void aReactorThatDeclaresNothingKeepsTheDefaults() {
		BareReactor reactor = new BareReactor();

		assertEquals("No description present", reactor.asMcpTool().getString("description"));
		assertEquals("string", property(reactor, "anything").getString("type"));
		assertNull(reactor.getReactorDefinition().getDescription());
		assertNull(reactor.getReactorDefinition().getOutput());
	}

	@Test
	void recordsEachProblemWithTheClassThatMadeIt() {
		ReactorDefinition definition = new MistakenReactor().getReactorDefinition();
		List<String> problems = definition.getProblems();

		assertEquals(2, problems.size(), problems.toString());
		assertEquals(MistakenReactor.class.getName() + " describes the key \"missing\", which is not in its keysToGet",
				problems.get(0));
		assertTrue(problems.get(1).startsWith(MistakenReactor.class.getName() + " says it takes no keys"),
				problems.get(1));
		assertFalse(definition.isNoKeys(), "noKeys() does not apply to a reactor with keys");
	}

	@Test
	void recordsAnEmptyOrRepeatedKeyAsAProblemOfTheReactor() {
		RepeatedKeyReactor reactor = new RepeatedKeyReactor();
		ReactorDefinition definition = reactor.getReactorDefinition();

		assertEquals(
				List.of(RepeatedKeyReactor.class.getName() + " has an empty key at position 1 of its keysToGet",
						RepeatedKeyReactor.class.getName() + " lists the key \"a\" twice in its keysToGet"),
				definition.getProblems());
		assertEquals(List.of("a"), definition.getKeys().stream().map(ReactorDefinition.Key::getName).toList());
		assertEquals("string", property(reactor, "a").getString("type"), "its MCP tool still lists the key once");
	}

	@Test
	void namesTheParentWhenItDescribesAKeyTheReactorDoesNotTake() {
		ReactorDefinition definition = new TakesDescriptionReactor().getReactorDefinition();

		assertEquals(List.of(SharedDescriptionsReactor.class.getName()
				+ " describes the key \"values\", which is not in its keysToGet"), definition.getProblems());
		assertEquals("The description", definition.getKey("description").getDescription());
		assertNull(definition.getKey("values"), "a key the reactor does not take is not added");
	}

	@Test
	void aReactorStartingFromStartDefinitionLeavesOutItsParentsDescriptions() {
		ReactorDefinition definition = new StartsFreshReactor().getReactorDefinition();

		assertTrue(definition.getProblems().isEmpty(), definition.getProblems().toString());
		assertEquals("Its own key", definition.getKey("own").getDescription());
	}

	@Test
	void helpTextReadsTheDefinition() {
		String help = new DefinedReactor().getHelp();

		assertTrue(help.contains("Description:\nFinds things"), help);
		assertTrue(help.contains("ids =\tThe ids to find"), help);
		assertTrue(help.contains("Response Schema:"), help);
	}
}
