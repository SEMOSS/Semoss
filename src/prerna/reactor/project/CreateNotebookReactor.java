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
package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Creates a new NOTEBOOK-type Project and always scaffolds a sample
 * {@code .ipynb} file at the project's {@code version/assets/public/main.ipynb}.
 * This is the notebook analogue of
 * {@link prerna.reactor.agent.skill.CreateSkillReactor}: {@code CreateProject}
 * rejects the NOTEBOOK type because it does not perform the starter-file
 * scaffold that this reactor adds.
 */
public class CreateNotebookReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateNotebookReactor.class.getName();

	// fixed name of the scaffolded notebook under version/assets/public
	private static final String NOTEBOOK_FILE_NAME = "main.ipynb";

	public CreateNotebookReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.GLOBAL.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		this.organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		String projectName = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (!Utility.validateName(projectName)) {
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey()) + "");

		NounMetadata warning = null;
		if (global) {
			if (AbstractSecurityUtils.adminOnlyProjectSetPublic() && !SecurityAdminUtils.userIsAdmin(user)) {
				warning = NounMetadata.getWarningNounMessage(
						"Public access can only be enabled by administrators. This item will be created as private.");
				global = false;
			}
		}

		IProject project = ProjectHelper.generateNewProject(projectName, IProject.PROJECT_TYPE.NOTEBOOK, global, null,
				null, user, logger);
		String projectId = project.getProjectId();

		try {
			String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
			// always scaffold at <assets>/public/main.ipynb
			File publicDir = new File(Utility.normalizePath(assetsFolder + "/" + Constants.PUBLIC_ASSETS_FOLDER));
			if (!publicDir.exists() && !publicDir.mkdirs()) {
				throw new IllegalStateException(
						"Failed to create public assets folder: " + publicDir.getAbsolutePath());
			}
			Path notebookFile = publicDir.toPath().resolve(NOTEBOOK_FILE_NAME);
			Files.write(notebookFile, buildSampleNotebook(projectName).getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			logger.error("Failed to scaffold sample notebook for project '{}' (id {})", projectName, projectId, e);
			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("Project created but failed to scaffold the sample notebook file"));
		}

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(user, projectId);
		NounMetadata retNoun = new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP,
				PixelOperationType.MARKET_PLACE_ADDITION);
		if (warning != null) {
			retNoun.addAdditionalReturn(warning);
		}
		return retNoun;
	}

	// minimal nbformat-4 notebook: a title markdown cell and a sample Python code cell
	private static String buildSampleNotebook(String projectName) {
		JsonArray cells = new JsonArray();

		JsonObject markdownCell = new JsonObject();
		markdownCell.addProperty("cell_type", "markdown");
		markdownCell.addProperty("id", Utility.getRandomString(8));
		markdownCell.add("metadata", new JsonObject());
		JsonArray markdownSource = new JsonArray();
		markdownSource.add("# " + projectName + "\n");
		markdownSource.add("\n");
		markdownSource.add("Sample notebook.");
		markdownCell.add("source", markdownSource);
		cells.add(markdownCell);

		JsonObject codeCell = new JsonObject();
		codeCell.addProperty("cell_type", "code");
		codeCell.addProperty("id", Utility.getRandomString(8));
		codeCell.add("execution_count", JsonNull.INSTANCE);
		codeCell.add("metadata", new JsonObject());
		codeCell.add("outputs", new JsonArray());
		JsonArray codeSource = new JsonArray();
		codeSource.add("print('Hello from " + projectName + "')");
		codeCell.add("source", codeSource);
		cells.add(codeCell);

		JsonObject kernelspec = new JsonObject();
		kernelspec.addProperty("display_name", "Python 3");
		kernelspec.addProperty("language", "python");
		kernelspec.addProperty("name", "python3");
		JsonObject languageInfo = new JsonObject();
		languageInfo.addProperty("name", "python");
		JsonObject metadata = new JsonObject();
		metadata.add("kernelspec", kernelspec);
		metadata.add("language_info", languageInfo);

		JsonObject notebook = new JsonObject();
		notebook.add("cells", cells);
		notebook.add("metadata", metadata);
		notebook.addProperty("nbformat", 4);
		notebook.addProperty("nbformat_minor", 5);

		return new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(notebook);
	}

	@Override
	public String getReactorDescription() {
		return "Creates a new NOTEBOOK-type Project and scaffolds a sample public/main.ipynb file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this notebook project. Note: the project ID is randomly generated";
		}
		return super.getDescriptionForKey(key);
	}

}
