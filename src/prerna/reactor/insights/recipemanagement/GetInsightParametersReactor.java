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
package prerna.reactor.insights.recipemanagement;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.parsers.ParamStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightParametersReactor extends AbstractInsightParameterReactor {

	private static final Logger classLogger = LogManager.getLogger(GetInsightParametersReactor.class);

	@Override
	public NounMetadata execute() {
		VarStore varStore = this.insight.getVarStore();
		// loop through all the parameters
		// and return the parameter list
		List<String> parameterKeys = varStore.getInsightParameterKeys();
		List<ParamStruct> paramList = new Vector<>();
		for (String paramName : parameterKeys) {
			NounMetadata paramNoun = varStore.get(paramName);
			if (paramNoun != null) {
				paramList.add((ParamStruct) paramNoun.getValue());
			} else {
				classLogger.info("Unable to find parameter name = " + paramName);
			}
		}

		NounMetadata pStructNoun = new NounMetadata(paramList, PixelDataType.PARAM_STRUCT);
		return pStructNoun;
	}

}
