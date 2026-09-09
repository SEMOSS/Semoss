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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;

import com.google.gson.GsonBuilder;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.project.api.IProject;
import prerna.util.Utility;

public class ModelEngineInferenceLogsWorker implements Runnable {

	public static final String INPUT = "INPUT";
	public static final String RESPONSE = "RESPONSE";

	private String messageId;
	private String transactionId;
	private String messageMethod;
	private IEngine engine;
	private String insightId;
	private String projectContextId;
	private String projectId;
	private User user;
	private String sessionId;
	private String roomId;
	private String context;
	private String prompt;
	private Object fullPrompt;
	private Integer promptTokens;
	private ZonedDateTime inputTime;
	private String response;
	private Integer responseTokens;
	private ZonedDateTime responseTime;
	// per-transaction granular tokens (nullable when provider/cache unavailable)
	private Integer inputTokens;
	private Integer outputTokens;
	private Integer cacheReadTokens;
	private Integer cacheCreationTokens;
	// thinking tokens are recorded on the RESPONSE row only (assistant output)
	private Integer thinkingTokens;

	// @formatter:off
    public ModelEngineInferenceLogsWorker(
		String messageId,
		String transactionId,
		String messageMethod,
		IEngine engine,
		String insightId,
	    String projectContextId,
	    String projectId,
	    User user,
		String sessionId,
		String roomId,
	   	String context,
	   	String prompt,
	   	Object fullPrompt,
	   	Integer promptTokens,
	   	ZonedDateTime inputTime,
	   	String response,
	   	Integer responseTokens,
	   	ZonedDateTime responseTime
	) {
    	this(messageId, transactionId, messageMethod, engine, insightId, projectContextId, projectId, user,
    			sessionId, roomId, context, prompt, fullPrompt, promptTokens, inputTime, response, responseTokens,
    			responseTime, null, null, null, null, null);
    }

    public ModelEngineInferenceLogsWorker(
		String messageId,
		String transactionId,
		String messageMethod,
		IEngine engine,
		String insightId,
	    String projectContextId,
	    String projectId,
	    User user,
		String sessionId,
		String roomId,
	   	String context,
	   	String prompt,
	   	Object fullPrompt,
	   	Integer promptTokens,
	   	ZonedDateTime inputTime,
	   	String response,
	   	Integer responseTokens,
	   	ZonedDateTime responseTime,
	   	Integer inputTokens,
	   	Integer outputTokens,
	   	Integer cacheReadTokens,
	   	Integer cacheCreationTokens,
	   	Integer thinkingTokens
	) {
    	this.messageId = messageId;
    	this.transactionId = transactionId;
    	this.messageMethod = messageMethod;
    	this.engine = engine;
    	this.insightId = insightId;
    	this.projectContextId = projectContextId;
    	this.projectId = projectId;
    	this.user = user;
    	this.sessionId = sessionId;
    	this.roomId = roomId;
    	this.context = context;
        this.prompt = prompt;
        if(this.prompt != null) {
        	this.prompt = this.prompt.trim();
        }
        this.fullPrompt = fullPrompt;
        this.promptTokens = promptTokens;
        this.inputTime = inputTime;
        this.response = response;
        this.responseTokens = responseTokens;
        this.responseTime = responseTime;
        this.inputTokens = inputTokens;
        this.outputTokens = outputTokens;
        this.cacheReadTokens = cacheReadTokens;
        this.cacheCreationTokens = cacheCreationTokens;
        this.thinkingTokens = thinkingTokens;
    }
    // @formatter:on

	@Override
	public void run() {
		String agentType = engine.getCatalogSubType(engine.getSmssProp());

		// assumption, if project level, then they will be inferencing through a saved
		// insight or SetContext
		String projectId = this.projectContextId;
		if (projectId == null) {
			projectId = this.projectId;
		}
		String projectName = null;
		if (projectId != null) {
			IProject project = Utility.getProject(projectId);
			if(project != null) {
				projectName = project.getProjectName();
			}
		}

		AccessToken userToken = user.getPrimaryLoginToken();
		String userId = userToken.getId();
		String userName = userToken.getName();
		String userUsername = userToken.getUsername();
		String userEmail = userToken.getEmail();

		// try to get the user's actual name otherwise try for username or email address
		if (userName == null) {
			if (userUsername != null) {
				userName = userUsername;
			} else {
				userName = userEmail;
			}
		}

		if (prompt == null || prompt.isEmpty()) {
			if (fullPrompt instanceof Collection && ((Collection) fullPrompt).size() == 1) {
				Object promptObj = ((Collection) fullPrompt).iterator().next();
				if (promptObj instanceof String) {
					prompt = (String) promptObj;
				} else {
					prompt = new GsonBuilder().disableHtmlEscaping().create().toJson(promptObj);
				}
			} else {
				prompt = new GsonBuilder().disableHtmlEscaping().create().toJson(fullPrompt);
			}
		}

		Duration duration = Duration.between(inputTime, responseTime);
		long millisecondsDifference = duration.toMillis();
		Double millisecondsDouble = (double) millisecondsDifference;

		// TODO this needs to be moved to wherever we "publish" a new LLM/agent
		if (!ModelInferenceLogsUtils.doModelIsRegistered(engine.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(engine.getEngineId(), engine.getEngineName(), null, agentType,
					user.getPrimaryLoginToken().getId());
		}

		// @formatter:off
		if (!ModelInferenceLogsUtils.doCheckRoomExists(roomId)) {
			String roomName = prompt.substring(0, Math.min(prompt.length(), 100));
			ModelInferenceLogsUtils.doCreateNewConversation(
				insightId,
				roomId,
				roomName, 
				null, 
				userId,
				userName,
				userEmail,
				agentType,
				engine.getEngineId(),
				true, 
				projectId, 
				projectName,
				null
			);
		}
		// @formatter:on

		if (this.context != null) {
			// set the context for the room / insight
			ModelInferenceLogsUtils.setRoomContext(roomId, userId, context);
		}

		boolean keepInputOutput = false;
		// TODO: ADD TO INTERFACE SO NOT DOING THIS DUMB CASTING
		if (engine instanceof AbstractModelEngine) {
			keepInputOutput = ((AbstractModelEngine) engine).keepInputOutput();
		} else if (engine instanceof AbstractVectorDatabaseEngine) {
			keepInputOutput = ((AbstractVectorDatabaseEngine) engine).keepInputOutput();
		} else if (engine instanceof PGVectorDatabaseEngine) {
			keepInputOutput = ((PGVectorDatabaseEngine) engine).keepInputOutput();
		}

		// @formatter:off
		// Granular tokens split by row type so SUM(<col>) gives correct totals without
		// filtering by MESSAGE_TYPE. INPUT row carries prompt-side counts (input + cache),
		// RESPONSE row carries assistant-side counts (output + thinking).
		String messageBody = keepInputOutput ? this.prompt : null;
		String responseBody = keepInputOutput ? this.response : null;

		Double inputBudget = null;
		Double responseBudget = null;
		if (engine instanceof AbstractModelEngine) {
			AbstractModelEngine ame = (AbstractModelEngine) engine;
			Double inCredit = ame.getInputTokenCredit();
			if (inCredit != null && this.inputTokens != null) {
				int cacheRead     = this.cacheReadTokens     != null ? this.cacheReadTokens     : 0;
				int cacheCreation = this.cacheCreationTokens != null ? this.cacheCreationTokens : 0;
				int newTokens     = this.inputTokens - cacheRead - cacheCreation;
				double readMult   = this.cacheReadMultiplierOrDefault(ame);
				double writeMult  = this.cacheWriteMultiplierOrDefault(ame);
				inputBudget = newTokens * inCredit
						+ cacheRead      * inCredit * readMult
						+ cacheCreation  * inCredit * writeMult;
			}
			Double outCredit = ame.getOutputTokenCredit();
			if (outCredit != null && (this.outputTokens != null || this.thinkingTokens != null)) {
				int out      = this.outputTokens  != null ? this.outputTokens  : 0;
				int thinking = this.thinkingTokens != null ? this.thinkingTokens : 0;
				responseBudget = (out + thinking) * outCredit;
			}
		}

		ModelInferenceLogsUtils.doRecordMessage(
			this.messageId,
			this.transactionId,
			INPUT,
			messageBody,
			this.messageMethod,
			this.promptTokens,
			this.inputTokens,
			null, // output tokens belong on the RESPONSE row
			this.cacheReadTokens,
			this.cacheCreationTokens,
			null, // thinking tokens belong on the RESPONSE row
			millisecondsDouble,
			this.inputTime,
			this.engine.getEngineId(),
			insightId,
			this.sessionId,
			this.roomId,
			userId,
			userName,
			userEmail,
			inputBudget
		);
		ModelInferenceLogsUtils.doRecordMessage(
			this.transactionId,
			this.transactionId,
			RESPONSE,
			responseBody,
			this.messageMethod,
			this.responseTokens,
			null, // input tokens belong on the INPUT row
			this.outputTokens,
			null, // cache_read belongs on the INPUT row
			null, // cache_creation belongs on the INPUT row
			this.thinkingTokens,
			millisecondsDouble,
			this.responseTime,
			this.engine.getEngineId(),
			insightId,
			this.sessionId,
			this.roomId,
			userId,
			userName,
			userEmail,
			responseBudget
		);
		// @formatter:on
	}

	private double cacheReadMultiplierOrDefault(AbstractModelEngine ame) {
		Double m = ame.getCacheReadMultiplier();
		return m != null ? m : 1.0;
	}

	private double cacheWriteMultiplierOrDefault(AbstractModelEngine ame) {
		Double m = ame.getCacheWriteMultiplier();
		return m != null ? m : 1.0;
	}
}
