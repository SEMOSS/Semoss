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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.ClaudeCodeManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClaudeCodeReactor extends AbstractReactor {

	public ClaudeCodeReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(),
				"allowedTools",
				"permissionMode",
				"mcps"
		};
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String command = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String permissionMode = this.keyValue.get("permissionMode");
		List<Map<String, String>> mcps = getMcps();
	    GenRowStruct grs = this.store.getGenRowStruct("allowedTools");
	    List<String> allowedTools = (grs != null && !grs.isEmpty()) 
	        ? grs.getAllStrValues() 
	        : new ArrayList<>();
		
		User user = this.insight.getUser();
		ClaudeCodeManager manager = new ClaudeCodeManager();

		String response = manager.query(this.insight, user, engineId, projectId, command, context, roomId, allowedTools, permissionMode, mcps);
		
		return new NounMetadata(response, PixelDataType.CONST_STRING,
				PixelOperationType.OPERATION);

	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, String>> getMcps() {
		List<NounMetadata> mapInputs = null;
		GenRowStruct mapGrs = this.store.getGenRowStruct("mcps");
		if (mapGrs != null && !mapGrs.isEmpty()) {
			mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
		}
		if (mapInputs == null || mapInputs.isEmpty()) {
			mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		}
		if (mapInputs == null || mapInputs.isEmpty()) {
			return null;
		}
		List<Map<String, String>> mcps = new ArrayList<>();
		for (NounMetadata noun : mapInputs) {
			mcps.add((Map<String, String>) noun.getValue());
		}
		return mcps;
	}

}
