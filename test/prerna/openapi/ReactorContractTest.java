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
package prerna.openapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.reactor.AbstractReactor;
import prerna.reactor.EchoReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.ReactorNames;
import prerna.reactor.StopPixelExecutionReactor;
import prerna.reactor.insights.GetCurrentInsightIdReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class ReactorContractTest {

	/**
	 * Declares keys the way most reactors do, describing one through the deprecated
	 * method.
	 */
	static class PlainReactor extends AbstractReactor {
		PlainReactor() {
			this.keysToGet = new String[] { "name,alias", ReactorKeysEnum.LIMIT.getKey(), "notes" };
			this.keyRequired = new int[] { 1, 0, 0 };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@SuppressWarnings("deprecation")
		@Override
		protected String getDescriptionForKey(String key) {
			return key.equals("notes") ? null : super.getDescriptionForKey(key);
		}
	}

	/** No keyRequired: organizeKeys() requires nothing. */
	static class NoRequiredReactor extends AbstractReactor {
		NoRequiredReactor() {
			this.keysToGet = new String[] { "a", "b" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public String getReactorDescription() {
			return "Has no required keys";
		}
	}

	@Internal("Testing only")
	static class DefinedReactor extends NoRequiredReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition()
					.key("a", key -> key.list(String.class).description("Some names").required(true))
					.key("b", key -> key.required(false)).outputList(Integer.class, "Some counts");
		}
	}

	static class ParentDefinedReactor extends NoRequiredReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().description("Described by the parent").key("a", "A from the parent")
					.output(String.class, null);
		}
	}

	static class ChildDefinedReactor extends ParentDefinedReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().key("a", key -> key.type(Integer.class));
		}
	}

	static class SchemaOutputReactor extends NoRequiredReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().outputSchema(new JSONObject().put("type", "array"));
		}
	}

	static class NoKeysReactor extends AbstractReactor {
		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().description("Takes nothing").noKeys().output(String.class, "Text");
		}
	}

	static class UnmarkedNoKeysReactor extends AbstractReactor {
		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().description("Has no keys, but does not say so").output(String.class,
					"Text");
		}
	}

	static class NoKeysWithKeysReactor extends NoRequiredReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().noKeys();
		}
	}

	static class MissingKeyReactor extends NoRequiredReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().key("missing", "Not one of its keys");
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

	static class TakesValuesReactor extends SharedDescriptionsReactor {
		TakesValuesReactor() {
			this.keysToGet = new String[] { "values" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}
	}

	static class OwnValuesReactor extends TakesValuesReactor {
		@Override
		public ReactorDefinition getReactorDefinition() {
			return startDefinition().key("values", "Its own values");
		}
	}

	static class TwiceReactor extends AbstractReactor {
		TwiceReactor() {
			this.keysToGet = new String[] { "a", "a,alias" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().description("Lists a key twice");
		}
	}

	static class EmptyKeyReactor extends AbstractReactor {
		EmptyKeyReactor() {
			this.keysToGet = new String[] { "a", " " };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

		@Override
		public ReactorDefinition getReactorDefinition() {
			return super.getReactorDefinition().description("Has an empty key");
		}
	}

	static class BareReactor extends AbstractReactor {
		BareReactor() {
			this.keysToGet = new String[] { "a" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}
	}

	static class ChildOfBareReactor extends BareReactor {
	}

	@Internal("Testing only")
	static class InternalBareReactor extends BareReactor {
	}

	private static ReactorContract.Input input(ReactorContract contract, String name) {
		return contract.inputs().stream().filter(i -> i.name().equals(name)).findFirst().orElseThrow();
	}

	@Test
	void infersInputsFromKeysRequirednessTypesAndDescriptions() throws Exception {
		ReactorContract contract = ReactorContract.of(PlainReactor.class);

		assertEquals("Plain", contract.name());
		assertEquals(List.of("name", "limit", "notes"),
				contract.inputs().stream().map(ReactorContract.Input::name).toList());
		assertTrue(input(contract, "name").required());
		assertFalse(input(contract, "limit").required());
		assertEquals(Integer.class, input(contract, "limit").type(), "LIMIT is an integer by default");
		assertEquals(String.class, input(contract, "name").type());
		assertTrue(input(contract, "name").typeInferred());

		assertEquals("The notes input", input(contract, "notes").description());
		assertTrue(input(contract, "notes").descriptionInferred());
		assertEquals("Plain reactor", contract.description());
		assertTrue(contract.descriptionInferred());
		assertNull(contract.output());
		assertTrue(contract.inferred());
	}

	@Test
	void treatsMissingKeyRequiredAsNothingRequiredLikeOrganizeKeys() throws Exception {
		ReactorContract contract = ReactorContract.of(NoRequiredReactor.class);

		assertFalse(input(contract, "a").required());
		assertFalse(input(contract, "b").required());
		assertEquals("Has no required keys", contract.description(), "the deprecated method is still read");
		assertFalse(contract.descriptionInferred());
	}

	@Test
	void readsTheDefinition() throws Exception {
		ReactorContract contract = ReactorContract.of(DefinedReactor.class);

		ReactorContract.Input a = input(contract, "a");
		assertEquals(List.class, a.type());
		assertEquals(String.class, a.items());
		assertEquals("Some names", a.description());
		assertTrue(a.required(), "the definition can require a key keyRequired leaves out");
		assertFalse(a.typeInferred());

		ReactorContract.Input b = input(contract, "b");
		assertFalse(b.required());
		assertTrue(b.typeInferred(), "a key without a declared type keeps the default");

		assertEquals(List.class, contract.output().type());
		assertEquals(Integer.class, contract.output().items());
		assertEquals("Some counts", contract.output().description());
		assertEquals("Testing only", contract.internal().value());
	}

	@Test
	void aSubclassAddsToItsParentsDefinition() throws Exception {
		ReactorContract contract = ReactorContract.of(ChildDefinedReactor.class);

		assertEquals("Described by the parent", contract.description());
		assertEquals("A from the parent", input(contract, "a").description());
		assertEquals(Integer.class, input(contract, "a").type(), "the child types the parent's key");
		assertFalse(input(contract, "a").typeInferred());
		assertEquals(String.class, contract.output().type());
	}

	@Test
	void keepsAJsonSchemaOutputAsGiven() throws Exception {
		ReactorContract.Output output = ReactorContract.of(SchemaOutputReactor.class).output();

		assertNull(output.type());
		assertEquals("array", new JSONObject(output.schema()).getString("type"));
	}

	@Test
	void treatsAnUnmarkedEmptyInputAsInferred() throws Exception {
		ReactorContract marked = ReactorContract.of(NoKeysReactor.class);
		assertTrue(marked.noKeys());
		assertTrue(marked.inputs().isEmpty());
		assertFalse(marked.inferred());

		ReactorContract unmarked = ReactorContract.of(UnmarkedNoKeysReactor.class);
		assertFalse(unmarked.noKeys());
		assertTrue(unmarked.inferred(), "without noKeys() an empty input is not known to be intended");
	}

	@Test
	void rejectsNoKeysOnAReactorWithKeys() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ReactorContract.of(NoKeysWithKeysReactor.class));
		assertTrue(error.getMessage().contains("says it takes no keys, but the reactor declares [a, b]"),
				error.getMessage());
	}

	@Test
	void rejectsAKeyTheReactorDoesNotDeclare() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ReactorContract.of(MissingKeyReactor.class));
		assertTrue(error.getMessage().contains("describes the key \"missing\""), error.getMessage());
	}

	@Test
	void rejectsAParentDescribingAKeyTheReactorDoesNotTakeAndNamesTheParent() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ReactorContract.of(TakesValuesReactor.class));
		assertTrue(
				error.getMessage()
						.contains(SharedDescriptionsReactor.class.getName() + " describes the key \"description\""),
				error.getMessage());
	}

	@Test
	void readsAReactorThatStartsFromStartDefinition() throws Exception {
		ReactorContract contract = ReactorContract.of(OwnValuesReactor.class);

		assertEquals(List.of("values"), contract.inputs().stream().map(ReactorContract.Input::name).toList());
		assertEquals("Its own values", input(contract, "values").description());
	}

	@Test
	void rejectsAKeyListedTwiceAndNamesTheReactor() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ReactorContract.of(TwiceReactor.class));
		assertEquals(TwiceReactor.class.getName() + " lists the key \"a\" twice in its keysToGet", error.getMessage());
	}

	@Test
	void rejectsAnEmptyKeyAndNamesTheReactor() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ReactorContract.of(EmptyKeyReactor.class));
		assertEquals(EmptyKeyReactor.class.getName() + " has an empty key at position 1 of its keysToGet",
				error.getMessage());
	}

	@Test
	void isDefinedByAnOverrideOrInternalOnTheReactorOrAParent() {
		assertFalse(ReactorContract.isDefined(BareReactor.class));
		assertFalse(ReactorContract.isDefined(ChildOfBareReactor.class));
		assertTrue(ReactorContract.isDefined(InternalBareReactor.class));
		assertTrue(ReactorContract.isDefined(PlainReactor.class), "an override of a deprecated method counts");
		assertTrue(ReactorContract.isDefined(DefinedReactor.class));
		assertTrue(ReactorContract.isDefined(ChildDefinedReactor.class));
		assertTrue(ReactorContract.isDefined(NoKeysReactor.class));
		assertFalse(ReactorContract.isDefined(String.class));
	}

	@Test
	void namesAndFrameTypesFollowReactorFactory() {
		assertEquals("StopPixelExecution", ReactorNames.pixelName("StopPixelExecutionReactor"));
		assertEquals("Plain", ReactorNames.pixelName("Plain"));
		assertEquals("r", ReactorNames.frameType("prerna.reactor.frame.r"));
		assertEquals("py", ReactorNames.frameType("prerna.reactor.frame.py.util"));
		assertEquals("rdbms", ReactorNames.frameType("prerna.reactor.frame.rdbms"));
		assertNull(ReactorNames.frameType("prerna.reactor.frame.filter"), "frame filters are general");
		assertNull(ReactorNames.frameType("prerna.reactor.insights"));
		assertTrue(ReactorContract.isGeneral(GetCurrentInsightIdReactor.class));
	}

	@Test
	void readsThePilotReactors() throws Exception {
		ReactorContract stop = ReactorContract.of(StopPixelExecutionReactor.class);
		assertEquals(List.of("id"), stop.inputs().stream().map(ReactorContract.Input::name).toList());
		assertTrue(input(stop, "id").required());
		assertEquals(String.class, input(stop, "id").type());
		assertEquals(String.class, stop.output().type());
		assertEquals("Stop the current execution of a pixel job", stop.description());

		ReactorContract echo = ReactorContract.of(EchoReactor.class);
		assertEquals(Object.class, input(echo, "value").type());
		assertEquals("The value to echo back", input(echo, "value").description());
		assertFalse(input(echo, "value").required());
		assertNull(echo.output());

		ReactorContract insight = ReactorContract.of(GetCurrentInsightIdReactor.class);
		assertTrue(insight.inputs().isEmpty());
		assertTrue(insight.noKeys());
		assertEquals(String.class, insight.output().type());
	}
}
