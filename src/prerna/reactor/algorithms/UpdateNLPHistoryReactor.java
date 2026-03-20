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
package prerna.reactor.algorithms;

import java.io.File;
import java.util.List;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class UpdateNLPHistoryReactor extends AbstractRFrameReactor {

	/**
	 * Returns predicted next word of an NLP Search as String array
	 */

	protected static final String CLASS_NAME = UpdateNLPHistoryReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public UpdateNLPHistoryReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// otherwise, proceed with the reactor
		String[] packages = new String[] { "data.table", "stringr", "stringdist", "udpipe", "tokenizers", "openNLP",
				"openNLPmodels.en" };
		this.rJavaTranslator.checkPackages(packages);

		// Generate string to initialize R console
		StringBuilder sb = new StringBuilder();
		String baseFolder = Utility.getBaseFolder();
		String wd = "wd" + Utility.getRandomString(5);
		sb.append(wd + "<- getwd();");
		sb.append(("setwd(\"" + baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts\");")
				.replace("\\", "/"));
		sb.append("source(\"nli_db.R\");");
		sb.append("source(\"word_vectors.R\");");
		sb.append(RSyntaxHelper.loadPackages(packages));
		this.rJavaTranslator.runR(sb.toString());

		// get all id's of a user
		List<String> allIds = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		;

		// get matrix of data from local master
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(allIds);

		// run script
		generateAndRunScript(allTableCols);

		// reset working directory
		this.rJavaTranslator.runR("setwd(\"" + wd + "\");");

		// return data to the front end
		File historyRDS = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts"
				+ DIR_SEPARATOR + "nli_training.rds");
		if (historyRDS.exists()) {
			String message = "NLP History successfully updated";
			return new NounMetadata(message, PixelDataType.CONST_STRING);
		} else {
			String message = "Something went wrong...";
			return new NounMetadata(message, PixelDataType.CONST_STRING);
		}
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/**
	 * Generate and run the script, returning array of strings
	 * 
	 * @param query
	 * @param allTableCols
	 * @return
	 */
	private void generateAndRunScript(List<Object[]> allTableCols) {
		// Getting database info
		String rTempTable = "NaturalLangTable" + Utility.getRandomString(8);
		StringBuilder rsb = new StringBuilder();

		String rAppIds = "c(";
		String rTableNames = "c(";
		String rColNames = "c(";
		String rColTypes = "c(";

		int colCount = allTableCols.size();
		// create R vector of appid, tables, and columns
		for (int i = 0; i < colCount; i++) {
			Object[] entry = allTableCols.get(i);
			String appId = entry[0].toString();
			String table = entry[1].toString();
			String column = entry[2].toString();
			String dataType = entry[3].toString();
			if (i == 0) {
				rAppIds += "'" + appId + "'";
				rTableNames += "'" + table + "'";
				rColNames += "'" + column + "'";
				rColTypes += "'" + dataType + "'";
			} else {
				rAppIds += ",'" + appId + "'";
				rTableNames += ",'" + table + "'";
				rColNames += ",'" + column + "'";
				rColTypes += ",'" + dataType + "'";
			}
		}

		rAppIds += ")";
		rTableNames += ")";
		rColNames += ")";
		rColTypes += ")";

		rsb.append(rTempTable + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = "
				+ rAppIds + ", Datatype = " + rColTypes + ", stringsAsFactors = FALSE);");
		rsb.append("refresh_nlidb_history(\"nlq_history.txt\"," + rTempTable + " , filename = \"nli_training.rds\" "
				+ ");");
		this.rJavaTranslator.runR(rsb.toString());

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + rTempTable + " ); gc();");
	}
}