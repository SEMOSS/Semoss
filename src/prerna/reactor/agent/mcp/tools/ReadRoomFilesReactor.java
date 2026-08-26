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
package prerna.reactor.agent.mcp.tools;

import prerna.util.files.SemossParsedFile;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Get parsed text from SemossParsedFile */
public class ReadRoomFilesReactor extends AbstractReactor {

	public ReadRoomFilesReactor() {
		this.keysToGet = new String[] { "fileNames" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<String> fileNames = getFileNames();
		Map<String, String> fileContentsMap = new ConcurrentHashMap<>();
		List<String> failedFiles = new CopyOnWriteArrayList<>();

		fileNames.parallelStream().forEach(fileName -> {
			try {
				File targetFile = Paths.get(insight.getInsightFolder(), fileName).toFile();
				SemossParsedFile semossParsedFile = new SemossParsedFile(targetFile);
				String extractedContent = semossParsedFile.getExtractedContents();
				fileContentsMap.put(fileName, extractedContent);
			} catch (IOException e) {
				failedFiles.add(fileName);
			}
		});

		if (failedFiles.size() == fileNames.size()) {
			return NounMetadata
					.getErrorNounMessage("Error reading or parsing all files: " + String.join(", ", failedFiles));
		}

		NounMetadata result = new NounMetadata(fileContentsMap, PixelDataType.MAP);
		if (!failedFiles.isEmpty()) {
			result.addAdditionalReturn(
					NounMetadata.getWarningNounMessage(
							"Failed to read or parse some files: " + String.join(", ", failedFiles)));
		}
		return result;
	}

	/**
	 * Gets the list of file names from the input.
	 *
	 * @return List of file names
	 */
	public List<String> getFileNames() {
		List<String> inputStrings = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct("fileNames");
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++)
				inputStrings.add(grs.get(i).toString());
			return inputStrings;
		}
		int size = this.curRow.size();
		for (int i = 0; i < size; i++)
			inputStrings.add(this.curRow.get(i).toString());
		return inputStrings;
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves the parsed text content from specified BYOD files in a room.";
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (key.equals("fileNames")) {
			return "The list of file names to read and extract content from.";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals("fileNames")) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}
}