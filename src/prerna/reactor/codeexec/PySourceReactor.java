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

import java.io.File;
import java.util.List;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;

import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.py.AbstractPyFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class PySourceReactor extends AbstractPyFrameReactor {

	public PySourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String disable_terminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			}
		}

		// check if script sourcing terminal is disabled
		String disable_script_scource = Utility.getDIHelperProperty(Constants.DISABLE_SCRIPT_SOURCE);
		if (disable_script_scource != null && !disable_script_scource.isEmpty()) {
			if (Boolean.parseBoolean(disable_script_scource)) {
				throw new IllegalArgumentException("Script Sourcing has been disabled.");
			}
		}

		String relativePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		// strip leading separators so we don't get a double slash after concat with
		// assetFolder
		while (relativePath.startsWith("/") || relativePath.startsWith("\\")) {
			relativePath = relativePath.substring(1);
		}
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);

		String path = assetFolder + "/" + relativePath;
		path = path.replace("\\", "/");

		// strict script source, we will check if its .r/.R or .py/.Py
		String strict_script_source = Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE);
		if (Boolean.parseBoolean(strict_script_source)) {
			String extension = FilenameUtils.getExtension(path);
			if (!extension.equalsIgnoreCase("Py")) {
				throw new IllegalArgumentException(
						"Only user code with extensions .py or .PY may be sourced by this reactor");
			}
		}

		// if we have a chroot, mount the project for that user.
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			// get the app_root folder for the project
			this.insight.getUser().getUserSymlinkHelper().symlinkFolder(assetFolder);
		}

		File file = new File(path);
		String name = file.getName();
		name = name.replaceAll(".py", "");

		String assetOutput = assetFolder + "/" + name + ".output";

		PyTranslator pyt = this.insight.getPyTranslator();
		pyt.runScript("smssutil.runwrapper('" + path + "', '" + assetOutput + "', '" + assetOutput + "', globals())");

		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(true, PixelDataType.BOOLEAN));

		boolean smartSync = (insight.getProperty("SMART_SYNC") != null)
				&& insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		if (smartSync) {
			// if this returns true
			if (smartSync(pyt)) {
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME,
						PixelOperationType.FRAME_HEADERS_CHANGE));
			}
		}

		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}
