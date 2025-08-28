/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.project;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.io.FilenameUtils;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class SearchAppAssetsReactor extends AbstractReactor {

	private DateTimeFormatter dateTimeFormatter;

	public SearchAppAssetsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.OPTIONS.getKey()};
		this.keyRequired = new int[]{1, 1, 1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = insight.getUser();
		this.dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss").withZone(user.getZoneId());

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String relativeFilePath = keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// Normalize relative path
		if (relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if (!relativeFilePath.isEmpty() && !relativeFilePath.startsWith("/")) {
				relativeFilePath = "/" + relativeFilePath;
			}
		}

		String filePath = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		int baseLen = filePath.length();
		String searchRoot = filePath + (relativeFilePath != null ? relativeFilePath : "");

		File rootDir = new File(searchRoot);
		if (!rootDir.exists() || !rootDir.isDirectory()) {
			throw new IllegalArgumentException("Invalid assets directory: " + searchRoot);
		}

		String rawTerm = keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		List<String> optionsList = getNounAsStringList(ReactorKeysEnum.OPTIONS.getKey());

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
		List<Map<String, Object>> results = new ArrayList<>();
		searchRecursive(rootDir, pattern, baseLen, results);

		return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * @param dir
	 * @param pattern
	 * @param baseLen
	 * @param results
	 */
	private void searchRecursive(File dir, Pattern pattern, int baseLen, List<Map<String, Object>> results) {
		File[] entries = dir.listFiles();
		if (entries == null)
			return;

		for (File f : entries) {
			String name = f.getName();
			// skip hidden directory
			if (f.isDirectory() && name.startsWith("."))
				continue;
			// build relative path
			String rel = f.getAbsolutePath().substring(baseLen).replace('\\', '/');
			// match
			if (pattern.matcher(name).find()) {
				Map<String, Object> meta = createMeta(f, rel, f.isDirectory());
				results.add(meta);
			}
			// recurse
			if (f.isDirectory()) {
				searchRecursive(f, pattern, baseLen, results);
			}
		}
	}

	/**
	 * @param f
	 * @param relativePath
	 * @param isDir
	 * @return
	 */
	private Map<String, Object> createMeta(File f, String relativePath, boolean isDir) {
		Map<String, Object> map = new HashMap<>();
		map.put("name", f.getName());
		map.put("path", relativePath);
		map.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
		map.put("type", isDir ? "directory" : FilenameUtils.getExtension(f.getName()));
		return map;
	}

	@Override
	public String getReactorDescription() {
		return "Recursively search files and directories within the project's assets folder, filtered by search term and optional searching flags.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to search files from. This relative path should assume the file path till project's app_root folder and relative path should start onwards";
		} else if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "The search term used to filter file and directory names";
		} else if (key.equals(ReactorKeysEnum.OPTIONS.getKey())) {
			return """
					A list of zero or more search flags to modify matching behavior.
					       Valid values are:
					          "case"  – perform a case-sensitive match
					          "word"  – match only whole words
					          "regex" – treat the search term as a full Java regular expression
					       If omitted, defaults to a case-insensitive file search.
					""";
		}
		return super.getDescriptionForKey(key);
	}
}
