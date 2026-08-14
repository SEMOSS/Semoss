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
package prerna.reactor;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Variable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddVarReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AddVarReactor.class);

	public AddVarReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.FRAME.getKey(),
				ReactorKeysEnum.EXPRESSION.getKey(), ReactorKeysEnum.LANGUAGE.getKey(),
				ReactorKeysEnum.FORMAT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String name = (String) this.getNounStore().getGenRowStruct(this.keysToGet[0]).get(0);
		// this should be a list of strings
		List frames = this.getNounStore().getGenRowStruct(this.keysToGet[1]).getAllValues();
		String expression = (String) this.getNounStore().getGenRowStruct(this.keysToGet[2]).get(0);
		String language = (String) this.getNounStore().getGenRowStruct(this.keysToGet[3]).get(0);

		Variable var = new Variable();
		var.setName(name);
		var.setExpression(expression);
		var.setFrames(frames);

		if (this.getNounStore().getGenRowStruct(this.keysToGet[4]) != null) {
			String format = (String) this.getNounStore().getGenRowStruct(this.keysToGet[4]).get(0);
			if (format != null) {
				var.setFormat(format);
			}
		}

		if (language != null) {
			if (language.equalsIgnoreCase("r")) {
				var.setLanguage(Variable.LANGUAGE.R);
				// try to execute in R and see if the expression works
				try {
					String newExpression = "tryCatch(" + expression + ", error=function(e) { 'error'})";
					String obj = this.insight.getRJavaTranslator(this.getClass().getCanonicalName())
							.runRAndReturnOutput(newExpression) + "";
					if (obj != null && (obj.toString().contains("error")
							|| obj.contains("java.lang.IllegalArgumentException"))) {
						return NounMetadata.getErrorNounMessage("Expression has error, please correct " + expression);
					}
				} catch (Exception e) {
					classLogger.error("Error occurred executing expression {}", expression, e);
				}
			} else if (language.equalsIgnoreCase("python")) {
				var.setLanguage(Variable.LANGUAGE.PYTHON);
			}
		}

		// add the variable
		boolean success = insight.addVariable(var);
		NounMetadata retNoun = null;
		if (success) {
			retNoun = new NounMetadata(name, PixelDataType.CONST_STRING, PixelOperationType.ADD_VARIABLE);
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Variable Set : " + name));
		} else {
			retNoun = NounMetadata.getErrorNounMessage("One or more of the frames this variable uses is not available");
		}
		return retNoun;
	}

}
