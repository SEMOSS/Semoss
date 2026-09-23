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

import java.util.ArrayList;
import java.util.List;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractProjectPyReactor extends AbstractReactor {

	public AbstractProjectPyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CODE.getKey(), ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String disable_terminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			}
		}

		// check if py terminal is disabled
		String disable_py_terminal = Utility.getDIHelperProperty(Constants.DISABLE_PY_TERMINAL);
		if (disable_py_terminal != null && !disable_py_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_py_terminal)) {
				throw new IllegalArgumentException("Python terminal has been disabled.");
			}
		}

		if (!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}

		organizeKeys();
		String projectId = resolveContextEngineId(this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()));

		String code = getDecodedCode();

		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		PyTranslator projectPyTranslator = project.getProjectPyTranslator();
		Object output = projectPyTranslator.runScript(code);

		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output + "", PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

	@Override
	public String getReactorDescription() {
		return "Run Python code in the project's dedicated python process";
	}

	/**
	 * Decode the code string
	 * 
	 * @return The decoded code string
	 */
	protected abstract String getDecodedCode();
}
