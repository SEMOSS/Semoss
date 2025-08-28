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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetAppAssetsBase64Reactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAppAssetsBase64Reactor.class);

	public GetAppAssetsBase64Reactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
		this.keyRequired = new int[]{1, 1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String filePath = this.keyValue.get(this.keysToGet[1]);
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass a filePath for the file to retrieve");
		}
		filePath = filePath.replace("\\", "/");
		if (!filePath.startsWith("/")) {
			filePath = "/" + filePath;
		}
		filePath = Utility.normalizePath(filePath);

		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());

		String output = null;
		// just read the current file
		String assetFilePath = assetFolder + filePath;
		File assetFile = new File(assetFilePath);
		if (!assetFile.exists()) {
			throw new IllegalArgumentException("The filePath " + filePath + " does not exist");
		}
		if (!assetFile.isFile()) {
			throw new IllegalArgumentException("The filePath " + filePath + " exists but is not a file");
		}
		try {
			byte[] bytes = Files.readAllBytes(Paths.get(assetFilePath));
			output = Base64.getEncoder().encodeToString(bytes);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to read file " + filePath);
		}

		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve the contents of an image file as base64 encoded string in the projects assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the image file to get the contents as a base64 encoded string. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		}
		return super.getDescriptionForKey(key);
	}
}
