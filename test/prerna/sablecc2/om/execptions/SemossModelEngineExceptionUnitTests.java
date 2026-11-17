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
package prerna.sablecc2.om.execptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;

class SemossModelEngineExceptionUnitTests {

	private static final String REFERENCE = "0190f6c2-5d1e-7b3a-9c41-2f8e6a0d7b15";

	@ParameterizedTest
	@CsvSource(delimiter = '|', value = { "400|The model provider rejected the request.",
			"401|The model provider denied access.", "403|The model provider denied access.",
			"404|The model provider could not find the model.",
			"408|The model provider timed out. Try again in a moment.",
			"504|The model provider timed out. Try again in a moment.",
			"413|The request is too large for the model provider.",
			"429|The model provider is over its rate limit. Try again in a moment.",
			"500|The model provider failed. Try again in a moment.",
			"503|The model provider failed. Try again in a moment.", "418|The model provider returned an error." })
	void theMessageNamesTheCodeWhatItMeansAndTheReference(int code, String meaning) {
		assertEquals("Error code: " + code + ". " + meaning + " Reference: " + REFERENCE,
				SemossModelEngineException.providerErrorMessage(code, null, REFERENCE));
	}

	@Test
	void anErrorWithoutACodeStillGivesTheReference() {
		assertEquals("The model provider returned an error. Reference: " + REFERENCE,
				SemossModelEngineException.providerErrorMessage(0, null, REFERENCE));
	}

	@Test
	void aContextOverflowSaysTheRequestIsTooLong() {
		assertEquals("Error code: 400. The request is longer than the model can take. Reference: " + REFERENCE,
				SemossModelEngineException.providerErrorMessage(400,
						AskErrorModelEngineResponse.REASON_CONTEXT_OVERFLOW, REFERENCE));
	}

	@Test
	void theProvidersTextStaysOnTheErrorResponseAndOutOfTheMessage() {
		AskErrorModelEngineResponse providerError = new AskErrorModelEngineResponse(
				"User: arn:aws:iam::123456789012:user/semoss is not authorized to perform bedrock:InvokeModel",
				"AccessDeniedException", 403, "bedrock", "claude", "traceback");
		String message = SemossModelEngineException.providerErrorMessage(403, null, REFERENCE);

		SemossModelEngineException exception = new SemossModelEngineException(providerError, message);

		assertEquals(message, exception.getMessage());
		assertSame(providerError, exception.getErrorResponse());
		assertTrue(((AskErrorModelEngineResponse) exception.getErrorResponse()).getStringResponse()
				.contains("123456789012"));
	}

	@Test
	void aContextOverflowIsStillFoundFromTheException() {
		AskErrorModelEngineResponse providerError = new AskErrorModelEngineResponse("too long", "error", 400, "openai",
				"gpt", null);
		providerError.setReason(AskErrorModelEngineResponse.REASON_CONTEXT_OVERFLOW);

		SemossModelEngineException exception = new SemossModelEngineException(providerError,
				SemossModelEngineException.providerErrorMessage(400, providerError.getReason(), REFERENCE));

		assertTrue(SemossModelEngineException.isContextOverflow(new RuntimeException(exception)));
		assertSame(providerError, SemossModelEngineException.contextOverflowError(exception));
	}
}
