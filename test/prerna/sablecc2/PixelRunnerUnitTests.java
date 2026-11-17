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
package prerna.sablecc2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.SecretRedactor;
import prerna.usertracking.AuditSanitizer;

class PixelRunnerUnitTests {

	private static final String OWNER = "pixel-runner-model";
	private static final String KEY = "sk-live-0a1b2c3d4e5f6a7b8c9d";

	@AfterEach
	void forgetRecordedSecrets() {
		SecretRedactor.forgetSecrets(OWNER);
	}

	@Test
	void anErrorResultLosesTheSecretsItCarries() {
		recordKey();
		PixelRunner runner = new PixelRunner();
		NounMetadata error = NounMetadata.getErrorNounMessage("Traceback: OpenAI(api_key " + KEY + ") failed");

		runner.addResult("LLM(engine=\"model\", command=\"hi\");", error, true);

		NounMetadata sent = runner.getResults().get(0);
		assertEquals("Traceback: OpenAI(api_key " + AuditSanitizer.REDACTED + ") failed", sent.getValue());
		assertEquals(PixelDataType.ERROR, sent.getNounType());
		assertEquals(List.of(PixelOperationType.ERROR), sent.getOpType());
		assertEquals(List.of("Traceback: OpenAI(api_key " + AuditSanitizer.REDACTED + ") failed"),
				runner.getReturnPixelList().get(0).getErrorMessages());
	}

	@Test
	void anErrorAmongTheAdditionalReturnsLosesItsSecretsToo() {
		recordKey();
		PixelRunner runner = new PixelRunner();
		NounMetadata result = new NounMetadata("done", PixelDataType.CONST_STRING);
		result.addAdditionalReturn(NounMetadata.getErrorNounMessage("warning step failed with " + KEY));

		runner.addResult("Step();", result, true);

		NounMetadata sent = runner.getResults().get(0);
		assertEquals("done", sent.getValue());
		assertEquals("warning step failed with " + AuditSanitizer.REDACTED,
				sent.getAdditionalReturn().get(0).getValue());
	}

	@Test
	void aWarningLosesTheSecretsItCarries() {
		recordKey();
		PixelRunner runner = new PixelRunner();

		runner.addResult("CloseEngine();", NounMetadata.getWarningNounMessage("close failed with " + KEY), true);

		assertEquals("close failed with " + AuditSanitizer.REDACTED, runner.getResults().get(0).getValue());
	}

	@Test
	void aResultThatIsNotAnErrorIsSentAsItIs() {
		recordKey();
		PixelRunner runner = new PixelRunner();
		NounMetadata result = new NounMetadata("file contents with " + KEY, PixelDataType.CONST_STRING);

		runner.addResult("GetInsightAssets(filePath=\"a.py\");", result, true);

		assertSame(result, runner.getResults().get(0));
	}

	@Test
	void anErrorWithoutSecretsIsSentAsItIs() {
		PixelRunner runner = new PixelRunner();
		NounMetadata error = NounMetadata.getErrorNounMessage("Engine 1234 does not exist");

		runner.addResult("Engine();", error, true);

		assertSame(error, runner.getResults().get(0));
	}

	private static void recordKey() {
		Properties smss = new Properties();
		smss.setProperty("OPEN_AI_KEY", KEY);
		SecretRedactor.recordSecrets(OWNER, smss);
	}
}
