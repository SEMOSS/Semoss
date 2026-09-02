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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;

class GuardrailValueReaderUnitTests {

	@Test
	void readsTheTextOfAnInputMessage() {
		assertEquals("api_key=secret", GuardrailValueReader.screenableValue(inputMessage("api_key=secret")));
	}

	@Test
	void readsTheTextOfAResponseMessage() {
		assertEquals("the answer", GuardrailValueReader.screenableValue(ResponseMessage.text("the answer")));
	}

	@Test
	void serializesAMapSoItsFieldsCanBeRead() {
		Map<String, Object> parameters = new LinkedHashMap<>();
		parameters.put("query", "api_key=secret");
		parameters.put("limit", 10);

		// the shape listBatches and the provider parameter maps hand over
		assertEquals("{\"query\":\"api_key=secret\",\"limit\":10}", GuardrailValueReader.screenableValue(parameters));
	}

	@Test
	void joinsTextSoNothingPastTheFirstItemIsSkipped() {
		// a guardrail reads a text parameter as one value, so a list has to
		// arrive joined or only its first item would be screened
		assertEquals("one\ntwo", GuardrailValueReader.screenableValue(List.of("one", "two")));

		List<Map<String, Object>> requests = new ArrayList<>();
		requests.add(Map.of("prompt", "first"));
		requests.add(Map.of("prompt", "second"));
		assertEquals("{\"prompt\":\"first\"}\n{\"prompt\":\"second\"}", GuardrailValueReader.screenableValue(requests));
	}

	@Test
	void keepsACollectionThatIsNotAllTextAsAList() {
		List<Object> mixed = new ArrayList<>();
		mixed.add("text");
		mixed.add(7);

		assertEquals(List.of("text", 7), GuardrailValueReader.screenableValue(mixed));
	}

	@Test
	void readsEveryMessageOfAMessageList() {
		// the shape validateInputModalities hands the pipeline
		List<AbstractMessage> outboundMessages = new ArrayList<>();
		outboundMessages.add(inputMessage("first question"));
		outboundMessages.add(ResponseMessage.text("first answer"));
		outboundMessages.add(inputMessage("api_key=secret"));

		assertEquals("first question\nfirst answer\napi_key=secret",
				GuardrailValueReader.screenableValue(outboundMessages));
	}

	@Test
	void readsThePayloadOfAResponseWrapper() {
		assertEquals("the answer",
				GuardrailValueReader.screenableValue(new AskStringModelEngineResponse("the answer", 1, 2)));
	}

	@Test
	void leavesValuesThatAreAlreadyContentAlone() {
		assertEquals("plain text", GuardrailValueReader.screenableValue("plain text"));
		assertEquals(7, GuardrailValueReader.screenableValue(7));
		assertNull(GuardrailValueReader.screenableValue(null));
	}

	@Test
	void reportsNoTextForAMessageThatCarriesNone() {
		assertNull(GuardrailValueReader.messageText(null));
		assertNull(GuardrailValueReader.messageText(ResponseMessage.text(null)));
	}

	private static InputMessage inputMessage(String text) {
		Room room = new Room();
		room.setId("room-id");
		return InputMessage.builder(room).withText(text).build();
	}
}
