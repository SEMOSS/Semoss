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
package prerna.reactor.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class ReactorInputHelperUnitTests {

	@Test
	void resolvesValuesInsideMapArguments() {
		ReactorInputHelper helper = helper(Map.of("arg0",
				Map.of("subject", "Quarterly report", "message", "The report is attached.")));

		assertEquals("Quarterly report", helper.getMethodArgument("arg0.subject"));
		assertEquals("The report is attached.", helper.getMethodArgument("arg0.message"));
	}

	@Test
	void indexesAndProjectsAcrossListResults() {
		Map<String, Object> result = Map.of("messages",
				List.of(Map.of("subject", "First", "body", "Body one"),
						Map.of("subject", "Second", "body", "Body two")));
		ReactorInputHelper helper = helper(Map.of("result", result));

		assertEquals("Second", helper.getMethodArgument("result.messages.1.subject"));
		assertEquals(List.of("Body one", "Body two"), helper.getMethodArgument("result.messages.*.body"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void nestedReplacementCopiesTheCallersMap() {
		Map<String, Object> originalMail = Map.of("subject", "Status", "message", "Account 123");
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("arg0", originalMail);
		ReactorInputHelper helper = helper(arguments);

		assertTrue(helper.setMethodArgument("arg0.message", "Account [masked]"));

		Map<String, Object> guardedMail = (Map<String, Object>) arguments.get("arg0");
		assertNotSame(originalMail, guardedMail);
		assertEquals("Account 123", originalMail.get("message"));
		assertEquals("Account [masked]", guardedMail.get("message"));
		assertFalse(helper.setMethodArgument("arg0.missing", "value"));
	}

	private ReactorInputHelper helper(Map<String, Object> arguments) {
		NounStore nounStore = new NounStore("test");
		GenRowStruct argumentNoun = new GenRowStruct();
		argumentNoun.add(new NounMetadata(arguments, PixelDataType.MAP));
		nounStore.addNoun(PipelineReactorUtils.ARGUMENTS, argumentNoun);
		return new ReactorInputHelper(nounStore);
	}
}
