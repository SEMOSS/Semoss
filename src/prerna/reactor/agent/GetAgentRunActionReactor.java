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
package prerna.reactor.agent;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunActionStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Look up a single pending HITL action by its action id, scoped to the
 * logged-in user. A portal opened via {@code ?actionId=<id>} calls this on
 * load to get everything it needs to prefill the approve/decline form, so the
 * URL never has to carry runId/roomId/toolCallId/parentMessageId/args.
 */
public class GetAgentRunActionReactor extends AbstractReactor {

	private static final String ACTION_ID_KEY = "actionId";
	private static final Gson GSON = new Gson();

	public GetAgentRunActionReactor() {
		this.keysToGet = new String[] { ACTION_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String actionId = StringUtils.trimToNull(this.keyValue.get(ACTION_ID_KEY));
		if (actionId == null) {
			throw new IllegalArgumentException("actionId is required");
		}
		String userId = this.insight != null ? this.insight.getUserId() : null;
		if (userId == null || userId.trim().isEmpty() || "-1".equals(userId)) {
			throw new SecurityException("Must be logged in to look up an agent action");
		}
		Map<String, Object> action = new AgentRunActionStore().getPendingActionById(actionId, userId);
		if (action == null) {
			throw new IllegalArgumentException("No pending agent action found for actionId=" + actionId);
		}
		// Parse the stored JSON strings into objects so the FE gets real maps.
		action.put("toolArgs", parseJson(action.get("toolArgs")));
		action.put("toolMeta", parseJson(action.get("toolMeta")));
		action.put("editedArgs", parseJson(action.get("editedArgs")));
		return new NounMetadata(action, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Object parseJson(Object value) {
		if (value instanceof String && !((String) value).trim().isEmpty()) {
			try {
				return GSON.fromJson((String) value, Object.class);
			} catch (Exception e) {
				return value;
			}
		}
		return value;
	}

	@Override
	public String getReactorDescription() {
		return "Look up a single pending HITL agent action by actionId (scoped to the logged-in user) so a portal can prefill its approve/decline form.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION_ID_KEY)) {
			return "The AGENT_RUN_ACTION id to look up.";
		}
		return super.getDescriptionForKey(key);
	}
}
