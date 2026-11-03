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
package prerna.engine.impl.model;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.usage.ModelUsageReport;
import prerna.engine.impl.model.usage.ModelUsageRequest;
import prerna.engine.impl.model.usage.ModelUsageTicket;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.om.ThreadStore;

public class EmbeddedModelEngine extends AbstractPythonModelEngine {

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.EMBEDDED;
	}

	protected List<String> keywordExtractionCall(Object input, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();

		StringBuilder callMaker = new StringBuilder(varName);
		String inputAsString = PyUtils.determineStringType(input);
		callMaker.append(".keyword_extraction(input = ").append(inputAsString);
		if (parameters != null && !parameters.isEmpty()) {
			callMaker.append(", **").append(PyUtils.determineStringType(parameters));
		}
		callMaker.append(")");

		List<String> output = (List<String>) pyTranslator.runDirectPy(callMaker.toString());
		return output;
	}

	public List<String> keywordExtraction(Object input, Insight insight, Map<String, Object> parameters) {
		// usage limits: the call reports no tokens, so it counts its compute time;
		// the input is estimated only when a token or cost rule applies
		ModelUsageTicket usageTicket = orNoLimits(
				ModelUsageRestrictionUtility.admit(insight == null ? null : insight.getUser(), this,
						usageRequestFor(ModelUsageRequest.embeddings(() -> ModelUsageRequest.estimateTokens(input)))));
		ZonedDateTime inputTime = ZonedDateTime.now();
		List<String> keywordExtractionResponse;
		try {
			keywordExtractionResponse = keywordExtractionCall(input, insight, parameters);
		} catch (RuntimeException e) {
			usageTicket.release();
			throw e;
		}
		ZonedDateTime outputTime = ZonedDateTime.now();
		Long costMicros = usageTicket
				.bill(new ModelUsageReport(0, 0, 0, 0, 0, ModelUsageReport.millisBetween(inputTime, outputTime))
						.asEmbeddings());

		// @formatter:off
		if (inferenceLogsEnbaled) {
			String messageId = GUID.v7().toUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/messageId,
					/*transactionId*/messageId,
					/*messageMethod*/"textKeywords", 
					/*engine*/this,
					/*insightId*/insight.getInsightId(),
					/*projectContextId*/insight.getContextProjectId(),
					/*projectId*/insight.getProjectId(),
					/*user*/insight.getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/ThreadStore.getInsightId(),
					/*context*/null,
					/*prompt*/input + "",
					/*fullPrompt*/null,
					/*promptTokens*/null,
					/*inputTime*/inputTime, 
					/*response*/PyUtils.determineStringType(keywordExtractionResponse),
					/*responseTokens*/null,
					/*outputTime*/outputTime
			).withBilling(usageTicket.billedTo(), costMicros));
			inferenceRecorder.start();
		}
		// @formatter:on	
		return keywordExtractionResponse;
	}

}
