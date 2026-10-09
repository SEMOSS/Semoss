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
package prerna.reactor.algorithms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JsonLogicReactorUnitTests {

	private JsonLogicReactor reactor;
	private Insight insight;
	private PyTranslator pyTranslator;

	@BeforeEach
	void setUp() {
		reactor = new JsonLogicReactor();
		insight = mock(Insight.class);
		pyTranslator = mock(PyTranslator.class);

		reactor.setInsight(insight);
		when(insight.getPyTranslator()).thenReturn(pyTranslator);
	}

	// -----------------------------------------------------------------------
	// Input validation
	// -----------------------------------------------------------------------

	@Test
	void testNullRuleThrowsException() {
		// organizeKeys() enforces required keys before execute() body runs
		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	void testEmptyRuleThrowsException() {
		reactor.keyValue.put("rule", "   ");
		assertThrows(SemossPixelException.class, () -> reactor.execute());
	}

	// -----------------------------------------------------------------------
	// Passing rule — result returned, no reason attached
	// -----------------------------------------------------------------------

	@Test
	void testPassingComparisonReturnsTrue() {
		reactor.keyValue.put("rule", "{\">=\":  [{\"var\": \"age\"}, 21]}");
		reactor.keyValue.put("data", "{\"age\": 25}");
		when(pyTranslator.runScript(anyString())).thenReturn("{\"result\": true, \"reason\": null}");

		NounMetadata result = reactor.execute();

		assertEquals(Boolean.TRUE, result.getValue());
		assertEquals(PixelDataType.BOOLEAN, result.getNounType());
		assertTrue(result.getAdditionalReturn().isEmpty());
	}

	// -----------------------------------------------------------------------
	// Failing rule — result returned with reason as additional return
	// -----------------------------------------------------------------------

	@Test
	void testFailedComparisonReturnsFalseWithReason() {
		reactor.keyValue.put("rule", "{\">=\":  [{\"var\": \"age\"}, 21]}");
		reactor.keyValue.put("data", "{\"age\": 18}");
		when(pyTranslator.runScript(anyString()))
				.thenReturn("{\"result\": false, \"reason\": \"Condition failed for age: 18.0 >= 21.0\"}");

		NounMetadata result = reactor.execute();
		System.out.println(result);
		assertEquals(Boolean.FALSE, result.getValue());
		assertEquals(PixelDataType.BOOLEAN, result.getNounType());
		assertEquals(1, result.getAdditionalReturn().size());
		NounMetadata reasonNoun = result.getAdditionalReturn().get(0);
		assertEquals(PixelDataType.CONST_STRING, reasonNoun.getNounType());
		assertTrue(reasonNoun.getValue().toString().contains("age"));
	}

	// -----------------------------------------------------------------------
	// String-returning rules (DISQ / QUAL / NOT_EVALUATED)
	// -----------------------------------------------------------------------

	@Test
	void testDisqResultWithReasonOp() {
		reactor.keyValue.put("rule",
				"{\"if\": [{\">=\":  [{\"var\": \"age\"}, 21]}, \"QUAL\", {\"reason\": [\"DISQ\", \"Applicant is under 21\"]}]}");
		reactor.keyValue.put("data", "{\"age\": 18}");
		when(pyTranslator.runScript(anyString()))
				.thenReturn("{\"result\": \"DISQ\", \"reason\": \"Applicant is under 21\"}");

		NounMetadata result = reactor.execute();

		assertEquals("DISQ", result.getValue());
		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
		assertFalse(result.getAdditionalReturn().isEmpty());
		assertEquals("Applicant is under 21", result.getAdditionalReturn().get(0).getValue());
	}

	@Test
	void testQualResultWithReasonOp() {
		reactor.keyValue.put("rule",
				"{\"if\": [{\">=\":  [{\"var\": \"age\"}, 21]}, {\"reason\": [\"QUAL\", \"Age requirement met\"]}, \"DISQ\"]}");
		reactor.keyValue.put("data", "{\"age\": 25}");
		when(pyTranslator.runScript(anyString()))
				.thenReturn("{\"result\": \"QUAL\", \"reason\": \"Age requirement met\"}");

		NounMetadata result = reactor.execute();

		assertEquals("QUAL", result.getValue());
		assertFalse(result.getAdditionalReturn().isEmpty());
		assertEquals("Age requirement met", result.getAdditionalReturn().get(0).getValue());
	}

	@Test
	void testNotEvaluatedBranchWithReason() {
		reactor.keyValue.put("rule",
				"{\"if\": [true, \"QUAL\", {\"reason\": [\"NOT_EVALUATED\", \"Prerequisites not met\"]}]}");
		reactor.keyValue.put("data", "{}");
		// top-level condition passes, QUAL branch taken — no reason
		when(pyTranslator.runScript(anyString()))
				.thenReturn("{\"result\": \"QUAL\", \"reason\": null}");

		NounMetadata result = reactor.execute();

		assertEquals("QUAL", result.getValue());
		assertTrue(result.getAdditionalReturn().isEmpty());
	}

	// -----------------------------------------------------------------------
	// No data provided (optional parameter)
	// -----------------------------------------------------------------------

	@Test
	void testRuleWithNoDataParam() {
		reactor.keyValue.put("rule", "{\"==\": [1, 1]}");
		// data key not set
		when(pyTranslator.runScript(anyString())).thenReturn("{\"result\": true, \"reason\": null}");

		NounMetadata result = reactor.execute();

		assertEquals(Boolean.TRUE, result.getValue());
	}

	// -----------------------------------------------------------------------
	// Numeric result
	// -----------------------------------------------------------------------

	@Test
	void testNumericResult() {
		reactor.keyValue.put("rule", "{\"+\": [{\"var\": \"x\"}, 5]}");
		reactor.keyValue.put("data", "{\"x\": 10}");
		when(pyTranslator.runScript(anyString())).thenReturn("{\"result\": 15.0, \"reason\": null}");

		NounMetadata result = reactor.execute();

		assertEquals(15.0, result.getValue());
		assertEquals(PixelDataType.CONST_DECIMAL, result.getNounType());
	}
}
