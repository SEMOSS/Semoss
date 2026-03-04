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
package prerna.reactor.codeexec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class LoadPyFromFileProjectPyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(LoadPyFromFileReactor.class);

	public LoadPyFromFileProjectPyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.ALIAS.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String appFolder = null;
		String space = keyValue.get(keysToGet[2]);

		if (space != null && !space.isEmpty() && !space.equals(AssetUtility.INSIGHT_SPACE_KEY)
				&& !space.equals(AssetUtility.USER_SPACE_KEY)) {
			appFolder = AssetUtility.getProjectAssetsFolder(space) + "/" + Constants.PY_BASE_FOLDER;
			appFolder = appFolder.replace("\\", "/");
		} else {
			throw new IllegalArgumentException("Project space is needed");
		}

		// this also validates
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		String alias = keyValue.get(keysToGet[1]);
		if (!PyUtils.isValidPythonVariableName(alias)) {
			throw new IllegalArgumentException("Provided alias " + alias + " is not a valid python variable name");
		}

		IProject project = Utility.getProject(space);
		try {
			String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='"
					+ filePath + "', search='" + appFolder + "')";
			project.getProjectPyTranslator().runScript(script);
			return new NounMetadata("Variable set " + alias, PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error("Unable to load python file as module. Error: " + e.getMessage(), e);
			throw new SemossPixelException("Unable to load python file as module. Error: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Load a python file as a variable (alias input) to reference as a class object into the project's python process";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ALIAS.getKey())) {
			return "A valid string input for a python variable name";
		}
		return super.getDescriptionForKey(key);
	}
}
