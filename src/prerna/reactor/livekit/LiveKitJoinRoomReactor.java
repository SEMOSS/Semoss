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
package prerna.reactor.livekit;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.livekit.LiveKitController;
import io.livekit.server.AccessToken;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;

public class LiveKitJoinRoomReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(LiveKitJoinRoomReactor.class);

	
	public LiveKitJoinRoomReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				"operation",
				"roomId",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
		};
		this.keyRequired = new int[] { 1, 1, 0 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String roomId = this.keyValue.get("roomId");
		if (roomId == null || roomId.isEmpty()) {
			roomId = UUID.randomUUID().toString();
		}
		
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String operation = this.keyValue.get("operation");
		
		
		User user = this.insight.getUser();
		String userId = User.getSingleLogginName(user);
		
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		Map<String, Object> paramMap = getParamMap();
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		LiveKitController controller = new LiveKitController();
		try {
		AccessToken token = controller.joinRoom(userId, userId, roomId, engineId, operation, this.insight, paramMap);
		
		String jwt = token.toJwt();
		
		HashMap<String, String> result = new HashMap<>();
		
		result.put("jwt", jwt);
		result.put("room_id", roomId);
		result.put("insightId", this.insight.getInsightId());
		
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		
		} catch (Exception e) {
			String errorMsg = "Failed to list LiveKit rooms: " + e.getMessage();
			classLogger.error(errorMsg, e);
			
			return getError(errorMsg);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
	
	@Override
	public String getReactorDescription() {
		return "Create or join a LiveKit Room.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("roomId")) {
			return "This is the room ID of an existing room. To join a new room simply do not pass this param or pass as an empty string.";
		}

		return super.getDescriptionForKey(key);
	}
	
	
}
