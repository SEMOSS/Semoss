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
package prerna.engine.impl.model.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.usage.ModelUsageBilledTo;

public class ModelEngineInferenceLogsWorkerBillingUnitTests {

	@Test
	void eachRowCarriesTheSourceBilledAndTheResponseRowTheTimeAndCost() {
		AbstractModelEngine engine = mock(AbstractModelEngine.class);
		when(engine.getSmssProp()).thenReturn(new CaseInsensitiveProperties());
		when(engine.getCatalogSubType(any())).thenReturn("OPEN_AI");
		when(engine.getEngineId()).thenReturn("engine-id");
		when(engine.keepInputOutput()).thenReturn(false);
		User user = mock(User.class);
		AccessToken token = mock(AccessToken.class);
		when(user.getPrimaryLoginToken()).thenReturn(token);
		when(user.getPrimaryLogin()).thenReturn(AuthProvider.NATIVE);
		when(token.getId()).thenReturn("eli");
		ZonedDateTime inputTime = ZonedDateTime.now();
		ZonedDateTime outputTime = inputTime.plusSeconds(2);

		ModelEngineInferenceLogsWorker worker = new ModelEngineInferenceLogsWorker("m1", "t1", "ask", engine, "insight",
				null, null, user, "session", "room", null, "prompt", null, 10, inputTime, "answer", 20, outputTime, 10,
				20, null, null, null)
				.withBilling(new ModelUsageBilledTo(ModelUsageBilledTo.TEAM, "Ops", "CUSTOM"), 1234L);

		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.doModelIsRegistered("engine-id")).thenReturn(true);
			logs.when(() -> ModelInferenceLogsUtils.doCheckRoomExists("room")).thenReturn(true);
			worker.run();
			// the INPUT row: no response time and no cost, so each call counts them once
			logs.verify(() -> ModelInferenceLogsUtils.doRecordMessage(eq("m1"), eq("t1"), eq("INPUT"), isNull(),
					eq("ask"), eq(10), eq(10), isNull(), isNull(), isNull(), isNull(), isNull(),
					any(ZonedDateTime.class), eq("engine-id"), eq("insight"), eq("session"), eq("room"), eq("eli"),
					eq("NATIVE"), any(), any(), eq("TEAM"), eq("Ops"), eq("CUSTOM"), isNull()));
			logs.verify(() -> ModelInferenceLogsUtils.doRecordMessage(eq("t1"), eq("t1"), eq("RESPONSE"), isNull(),
					eq("ask"), eq(20), isNull(), eq(20), isNull(), isNull(), isNull(), eq(2000.0),
					any(ZonedDateTime.class), eq("engine-id"), eq("insight"), eq("session"), eq("room"), eq("eli"),
					eq("NATIVE"), any(), any(), eq("TEAM"), eq("Ops"), eq("CUSTOM"), eq(1234L)));
		}
	}
}
