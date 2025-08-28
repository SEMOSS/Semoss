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
package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetEngineFilesReactor extends AbstractEngineFileReactor {

	private static final Logger classLogger = LogManager.getLogger(GetEngineFilesReactor.class);

	public GetEngineFilesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		validateUserAndEngineAccess(user);

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}

		String enginePath = getSpecificEngineBaseFolder(engineId);
		Map<String, Object> responseData = null;
		try {
			responseData = getEngineFiles(enginePath);
		} catch (IOException e) {
			classLogger.error("Error processing files", e);
			throw new RuntimeException("File processing failed: " + e.getMessage(), e);
		}

		return new NounMetadata(responseData, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
	}

	/**
	 * @param enginePath
	 * @return
	 * @throws IOException
	 */
	private Map<String, Object> getEngineFiles(String enginePath) throws IOException {
		String engineSubPath = Utility
				.normalizePath(this.keyValue.getOrDefault(ReactorKeysEnum.FILE_PATH.getKey(), ""));
		if (!engineSubPath.startsWith("/") && !engineSubPath.startsWith("\\")) {
			engineSubPath = "/" + engineSubPath;
		}

		String engineFilePath = enginePath + engineSubPath;
		File target = new File(engineFilePath);

		if (!target.exists())
			return new HashMap<>();
		Map<String, Object> result = new HashMap<>();
		if (target.isFile()) {
			Map<String, String> fileData = new HashMap<>();
			fileData.put("fileName", target.getName());
			fileData.put("content", readFileContent(target));

			Map<String, Object> singleFileMap = new HashMap<>();
			singleFileMap.put("files", Arrays.asList(fileData));
			result.put(target.getParentFile().getName(), singleFileMap);
		} else if (target.isDirectory()) {
			File[] files = target.listFiles(File::isFile); // Only immediate files

			if (files != null && files.length > 0) {
				List<Map<String, String>> fileList = new ArrayList<>();
				for (File file : files) {
					Map<String, String> fileData = new HashMap<>();
					fileData.put("fileName", file.getName());
					fileData.put("content", readFileContent(file));
					fileList.add(fileData);
				}
				Map<String, Object> folderMap = new HashMap<>();
				folderMap.put("files", fileList);
				result.put(target.getName(), folderMap);
			}
		}

		return result;
	}
}
