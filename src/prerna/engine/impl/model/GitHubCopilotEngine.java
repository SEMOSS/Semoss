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

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;

/**
 * GitHub Copilot models called with the asking user's own GitHub login, so each
 * request is billed to that user's Copilot subscription. The engine holds no API
 * key: the user's token rides along on every ask as the github_token parameter.
 */
public class GitHubCopilotEngine extends AbstractPythonModelEngine {

	public static final String GITHUB_TOKEN = "github_token";

	@Override
	public AskModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
			Map<String, Object> parameters) {
		User user = insight == null ? null : insight.getUser();
		AccessToken token = user == null ? null : user.getAccessToken(AuthProvider.GITHUB);
		if (token == null || token.getAccess_token() == null) {
			throw new IllegalArgumentException(
					"Connect your GitHub account (Settings > My Profile > Connect GitHub) to use GitHub Copilot models");
		}
		// copied, not mutated: message parameter maps are persisted with the room
		Map<String, Object> withToken = parameters == null ? new HashMap<>() : new HashMap<>(parameters);
		withToken.put(GITHUB_TOKEN, token.getAccess_token());
		return super.askCall(inputMessage, insight, roomId, withToken);
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.GITHUB_COPILOT;
	}
}
