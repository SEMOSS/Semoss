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
package prerna.reactor.frame.py;

import java.io.File;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class PyLoadReactor extends AbstractReactor {

	public PyLoadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String path = Utility.getBaseFolder() + "/Py/" + relativePath;
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);

		// if the file is not there try in the insight
		// if(!file.exists())
		path = assetFolder + "/" + relativePath;
		path = path.replace("\\", "/");

		File file = new File(path);
		String name = file.getName();
		name = name.replaceAll(".py", "");

		String assetOutput = assetFolder + "/" + name + ".output";

		PyTranslator pyt = this.insight.getPyTranslator();

		// pyt.runScript(name + " = smssutil.runwrapper('smss', '" + path + "')");
		pyt.runScript(name + " = smssutil.loadScript('smss', '" + path + "')");

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
