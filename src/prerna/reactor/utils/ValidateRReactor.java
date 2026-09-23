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
package prerna.reactor.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ValidateRReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ValidateRReactor.class);

	public ValidateRReactor() {
		this.keysToGet = new String[] { "script" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String result = "";

		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(getLogger(this.getClass() + ""));
		rJavaTranslator.startR();

		String baseFolder = Utility.getBaseFolder();
		String rFolder = baseFolder + "/R/util/smssutil.r";
		rFolder = rFolder.replace("\\", "/");

		rJavaTranslator.runR("source('" + rFolder + "');");

		try {
			String rScript = insight.getInsightFolder() + "/" + keyValue.get(keysToGet[0]);
			rScript = rScript.replace("\\", "/");
			Object outObject = rJavaTranslator.executeR("canLoad('" + rScript + "')");

			String output = "";
			if (outObject instanceof org.rosuda.REngine.REXPString) {
				output = ((org.rosuda.REngine.REXPString) outObject).asString();
			} else if (outObject instanceof org.rosuda.JRI.REXP) {
				output = ((org.rosuda.JRI.REXP) outObject).asString();
			}

			output = output.replace("\"", "");

			if (output.length() == 0) {
				result = keyValue.get(keysToGet[0]) + " : All Libraries available";
			} else {
				StringBuilder library = new StringBuilder(keyValue.get(keysToGet[0])).append(":  Missing Libraries [")
						.append(output).append("]");
				result = library.toString();
			}
		} catch (Exception e) {
			classLogger.error("Failed to validate R libraries for script {}", keyValue.get(keysToGet[0]), e);
		}
		return new NounMetadata(result, PixelDataType.CONST_STRING);
	}
}
