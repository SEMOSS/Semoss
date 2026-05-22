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
package prerna.reactor.insights.fs;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class SearchInsightAssetsReactor extends AbstractReactor {

	private DateTimeFormatter dateTimeFormatter;

	public SearchInsightAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SEARCH.getKey(),
				ReactorKeysEnum.OPTIONS.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = insight.getUser();
		this.dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss").withZone(user.getZoneId());

		String relativeFilePath = keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		String rawTerm = keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		List<String> optionsList = getNounAsStringList(ReactorKeysEnum.OPTIONS.getKey());

		// Normalize relative path
		if (relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if (!relativeFilePath.isEmpty() && !relativeFilePath.startsWith("/")) {
				relativeFilePath = "/" + relativeFilePath;
			}
		}

		String filePath = this.insight.getInsightFolder();
		int baseLen = filePath.length();
		String searchRoot = filePath + (relativeFilePath != null ? relativeFilePath : "");

		File rootDir = new File(searchRoot);
		if (!rootDir.exists() || !rootDir.isDirectory()) {
			throw new IllegalArgumentException("Invalid assets directory: " + searchRoot);
		}

		// Build the searching Pattern
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

		// Recursive search
		List<Map<String, Object>> results = FileSystemUtil.search(rootDir, pattern, baseLen, dateTimeFormatter);

		return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Recursively search files and directories within the insight's assets folder, filtered by search term and optional searching flags.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to search files from. This relative path should assume the file path till insight's folder and relative path should start onwards";
		} else if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "The search term used to filter file and directory names";
		} else if (key.equals(ReactorKeysEnum.OPTIONS.getKey())) {
			return """
						A list of zero or more search flags to modify matching behavior.
					       Valid values are:
					          "case"  - perform a case-sensitive match
					          "word"  - match only whole words
					          "regex" - treat the search term as a full Java regular expression
					       If omitted, defaults to a case-insensitive file search.
					""";
		}
		return super.getDescriptionForKey(key);
	}

}
