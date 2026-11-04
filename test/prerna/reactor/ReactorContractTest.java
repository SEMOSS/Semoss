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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.openapi.Internal;
import prerna.openapi.PixelInput;
import prerna.openapi.PixelOutput;
import prerna.reactor.insights.GetCurrentInsightIdReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class ReactorContractTest {

	/** Declares keys the way most reactors do, with no annotations. */
	static class PlainReactor extends AbstractReactor {
		PlainReactor() {
			this.keysToGet = new String[] { "name,alias", ReactorKeysEnum.LIMIT.getKey(), "notes" };
			this.keyRequired = new int[] { 1, 0, 0 };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}

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

	@PixelInput(name = "a", type = List.class, items = String.class, description = "Some names", required = PixelInput.Requirement.REQUIRED)
	@PixelInput(name = "b", required = PixelInput.Requirement.OPTIONAL)
	@PixelOutput(value = List.class, items = Integer.class, description = "Some counts")
	@Internal("Testing only")
	static class AnnotatedReactor extends NoRequiredReactor {
	}

	@PixelInput(name = "missing", type = String.class)
	static class BadOverrideReactor extends NoRequiredReactor {
	}

	static class TwiceReactor extends AbstractReactor {
		TwiceReactor() {
			this.keysToGet = new String[] { "a", "a,alias" };
		}

		@Override
		public NounMetadata execute() {
			return null;
		}
	}

	private static ReactorContract.Input input(ReactorContract contract, String name) {
		return contract.inputs().stream().filter(i -> i.name().equals(name)).findFirst().orElseThrow();
	}

	@Test
	void infersInputsFromKeysRequirednessTypesAndDescriptions() throws Exception {
		ReactorContract contract = ReactorContract.of(PlainReactor.class);

		assertEquals("Plain", contract.name());
		assertEquals(List.of("name", "limit", "notes"), contract.inputs().stream().map(ReactorContract.Input::name).toList());
		assertTrue(input(contract, "name").required());
		assertFalse(input(contract, "limit").required());
		assertEquals(Integer.class, input(contract, "limit").type(), "LIMIT is an integer for MCP");
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
		assertEquals("Has no required keys", contract.description());
		assertFalse(contract.descriptionInferred());
	}

	@Test
	void appliesPixelInputPixelOutputAndInternal() throws Exception {
		ReactorContract contract = ReactorContract.of(AnnotatedReactor.class);

		ReactorContract.Input a = input(contract, "a");
		assertEquals(List.class, a.type());
		assertEquals(String.class, a.items());
		assertEquals("Some names", a.description());
		assertTrue(a.required());
		assertFalse(a.typeInferred());

		ReactorContract.Input b = input(contract, "b");
		assertFalse(b.required());
		assertTrue(b.typeInferred(), "a @PixelInput without a type keeps the inferred one");

		assertEquals(List.class, contract.output().type());
		assertEquals(Integer.class, contract.output().items());
		assertEquals("Some counts", contract.output().description());
		assertEquals("Testing only", contract.internal().value());
	}

	@Test
	void rejectsAPixelInputForAKeyTheReactorDoesNotDeclare() {
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ReactorContract.of(BadOverrideReactor.class));
		assertTrue(error.getMessage().contains("[missing]"), error.getMessage());
	}

	@Test
	void rejectsAKeyDeclaredTwice() {
		assertThrows(IllegalArgumentException.class, () -> ReactorContract.of(TwiceReactor.class));
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
		assertFalse(input(echo, "value").required());
		assertNull(echo.output());

		ReactorContract insight = ReactorContract.of(GetCurrentInsightIdReactor.class);
		assertTrue(insight.inputs().isEmpty());
		assertEquals(String.class, insight.output().type());
	}
}
