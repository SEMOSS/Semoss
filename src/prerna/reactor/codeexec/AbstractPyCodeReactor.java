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

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.ICodeExecution;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.Variable.LANGUAGE;
import prerna.reactor.frame.py.AbstractPyFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractPyCodeReactor extends AbstractPyFrameReactor implements ICodeExecution {

	// the code that was executed
	protected String code = null;

	public AbstractPyCodeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CODE.getKey() };
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
		this.code = getDecodedCode();
		this.code = fillVars(this.code);
		if (this.code.startsWith("sns.")) {
			return new NounMetadata("Please use PyPlot to plot your chart", PixelDataType.CONST_STRING);
		}

		PyTranslator pyTranslator = this.insight.getPyTranslator();

		NounMetadata execNoun = null;
		Object output = pyTranslator.runScript(this.code);
		if (output instanceof String) {
			execNoun = new NounMetadata(output, PixelDataType.CONST_STRING);
		} else {
			execNoun = new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}
		List<NounMetadata> outputs = new ArrayList<>(2);
		outputs.add(execNoun);

		boolean smartSync = (insight.getProperty("SMART_SYNC") != null)
				&& insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		// forcing smart sync to true
		smartSync = true;
		if (smartSync) {
			// if this returns true
			if (smartSync(pyTranslator)) {
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME,
						PixelOperationType.FRAME_HEADERS_CHANGE));
			}
		}
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

	@Override
	public String getExecutedCode() {
		return this.code;
	}

	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.PYTHON;
	}

	@Override
	public boolean isUserScript() {
		return true;
	}

	@Override
	public String getReactorDescription() {
		return "Run Python code in the user's dedicated python process";
	}

	/**
	 * Decode the code string from the pixel
	 * 
	 * @return The decoded code string
	 */
	protected abstract String getDecodedCode();
}
