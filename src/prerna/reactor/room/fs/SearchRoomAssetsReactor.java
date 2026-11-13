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
package prerna.reactor.room.fs;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import prerna.auth.User;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class SearchRoomAssetsReactor extends AbstractRoomAssetsReactor {

	public SearchRoomAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.OPTIONS.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = requireUser();
		String roomFolder = getRoomFolder(user);

		String relativeFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		String rawTerm = this.keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		List<String> optionsList = getNounAsStringList(ReactorKeysEnum.OPTIONS.getKey());

		if (relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if (!relativeFilePath.isEmpty() && !relativeFilePath.startsWith("/")) {
				relativeFilePath = "/" + relativeFilePath;
			}
		}

		int baseLen = roomFolder.length();
		File rootDir = new File(roomFolder + (relativeFilePath != null ? relativeFilePath : ""));
		if (!rootDir.isDirectory()) {
			throw new IllegalArgumentException(
					"The path " + relativeFilePath + " is not a directory in the room's folder");
		}

		String expr;
		if (optionsList.stream().anyMatch(s -> "regex".equalsIgnoreCase(s))) {
			expr = rawTerm;
		} else {
			expr = Pattern.quote(rawTerm);
			if (optionsList.stream().anyMatch(s -> "word".equalsIgnoreCase(s))) {
				expr = "\\b" + expr + "\\b";
			}
		}
		int flags = optionsList.stream().anyMatch(s -> "case".equalsIgnoreCase(s)) ? 0 : Pattern.CASE_INSENSITIVE;
		final Pattern pattern;
		try {
			pattern = Pattern.compile(expr, flags);
		} catch (PatternSyntaxException e) {
			throw new IllegalArgumentException("Invalid search pattern: " + e.getDescription());
		}

		List<Map<String, Object>> results = FileSystemUtil.search(user, rootDir, pattern, baseLen);
		return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Recursively search the files and directories in a room's folder, filtered by search term and optional searching flags.")
				.key(ReactorKeysEnum.FILE_PATH, "The directory to search from, relative to the room's folder.")
				.key(ReactorKeysEnum.SEARCH, "The search term used to filter file and directory names")
				.key(ReactorKeysEnum.OPTIONS, key -> key.list(String.class).description("""
						A list of zero or more search flags to modify matching behavior.
						       Valid values are:
						          "case"  - perform a case-sensitive match
						          "word"  - match only whole words
						          "regex" - treat the search term as a full Java regular expression
						       If omitted, defaults to a case-insensitive file search.
						"""))
				.outputList(Map.class,
						"One entry per match, directories first: name, path (directories end with '/'), type (directory or the file extension) and lastModified");
	}

}
