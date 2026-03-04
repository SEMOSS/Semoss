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
package prerna.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SemanticDescription extends AbstractRFrameReactor {

	public SemanticDescription() {
		this.keysToGet = new String[] { "input" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		String[] packages = { "WikidataR", "WikipediR", "curl", "httr", "jsonlite" };
		this.rJavaTranslator.checkPackages(packages);
		String input = this.keyValue.get(this.keysToGet[0]);
		StringBuilder rsb = new StringBuilder();

		// r temp variables
		String random = Utility.getRandomString(5);
		String rFindItem = "findItem" + random;
		// resulting frame
		String rFrame = "SemanticMeaning";
		// resulting frame header names
		String url = "Url";
		String semanticMeaning = "SemanticMeaning";
		// do wiki look up
		// remove results
		rsb.append("rm(" + rFrame + ");\n");
		rsb.append("library(WikidataR);\n");
		rsb.append(rFindItem + "<-find_item('" + input + "');\n");
		rsb.append(rFrame + "<-data.frame(Reduce('rbind',lapply(" + rFindItem
				+ ",function(x) cbind(x$url,ifelse(length(x$description)==0,NA,x$description)))));\n");
		rsb.append("if(exists('" + rFrame + "')) { \n");
		// rename columns
		rsb.append(RSyntaxHelper.asDataTable(rFrame, rFrame) + "\n");
		// remove frame if empty
		rsb.append("if(nrow(SemanticMeaning) == 0) {\nrm(SemanticMeaning)\n} else {\n");
		rsb.append("colnames(" + rFrame + ") <- c('" + url + "', '" + semanticMeaning + "'); \n");
		rsb.append(rFrame + "$" + url + "<-gsub('//',''," + rFrame + "$" + url + "); \n");
		rsb.append(rFrame + "$" + semanticMeaning + " <- as.character(" + rFrame + "$" + semanticMeaning + ");\n");
		rsb.append("}}\n");
		// r temp variable clean up
		rsb.append("rm(" + rFindItem + ");");

		this.rJavaTranslator.runR(rsb.toString());
		this.addExecutedCode(rsb.toString());

		String frameExists = "exists('" + rFrame + "')";
		boolean nullResults = this.rJavaTranslator.getBoolean(frameExists);
		if (!nullResults) {
			NounMetadata noun = new NounMetadata("Unable to view your results", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		RDataTable returnTable = createNewFrameFromVariable(rFrame);
		this.insight.setDataMaker(returnTable);
		return new NounMetadata(returnTable, PixelDataType.FRAME);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(this.keysToGet[0])) {
			return "The input to look up description.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
