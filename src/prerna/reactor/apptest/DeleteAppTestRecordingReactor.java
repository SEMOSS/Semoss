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
package prerna.reactor.apptest;

import java.io.IOException;
import java.util.Map;

import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteAppTestRecordingReactor extends AbstractAppTestReactor {
	public DeleteAppTestRecordingReactor() {
		this.keysToGet = new String[] { "project", "recordingId" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String target = targetProject();
		try {
			return new NounMetadata(Map.of("deleted", AppTestRecordings.delete(AppTestRecordings.folder(target), getString("recordingId"))),
					PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to delete app test recording", e);
		}
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Delete a saved app test recording. Requires app edit access; do not delete tests to hide a failure.")
				.key("project", "Required target app project id")
				.key("recordingId", "Recording UUID returned by ListAppTestRecordings")
				.output(Map.class, "Whether the requested recording was deleted");
	}
}
