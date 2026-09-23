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
package prerna.util;

import java.io.File;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.util.sql.RdbmsTypeEnum;

public class RecreateInsightsDatabaseFromMosfetFiles {

//	public static void main(String[] args) throws Exception {
//		String mainDirectory = null;
//		String connUrl = null;
//		if(args.length == 0) {
//			mainDirectory = "C:/Users/mahkhalil/Desktop/review123/nogit_version_no_cache";
//			connUrl = "jdbc:h2:C:\\Users\\mahkhalil\\Desktop\\review123\\insights_database";
//		} else {
//			mainDirectory = args[0];
//			connUrl = args[1];
//		}
//		build(mainDirectory, connUrl);
//	}

	private static void build(String mainDirectory, String connectionUrl) throws Exception {
		Properties insightSmssProp = new Properties();
		insightSmssProp.put(Constants.CONNECTION_URL, connectionUrl);
		insightSmssProp.put(Constants.USERNAME, "sa");
		insightSmssProp.put(Constants.PASSWORD, "");
		insightSmssProp.put(Constants.DRIVER, RdbmsTypeEnum.H2_DB.getDriver());
		insightSmssProp.put(Constants.RDBMS_TYPE, RdbmsTypeEnum.H2_DB.getLabel());
		try (IRDBMSEngine insightEngine = new RDBMSNativeEngine()) {
			insightEngine.setBasic(true);
			insightEngine.open(insightSmssProp);
			SmssUtilities.runInsightCreateTableQueries(insightEngine);

			// main directory has insight folders inside of it
			File mainD = new File(Utility.normalizePath(mainDirectory));
			File[] mainDFiles = mainD.listFiles();
			INSIGHT_FOLDER: for (File insightFolder : mainDFiles) {
				// only care about insight folders
				if (!insightFolder.isDirectory()) {
					continue;
				}

				// get all the files inside the insight folder
				File[] insightFiles = insightFolder.listFiles();
				if (insightFiles.length == 0) {
					System.out.println("Insight had no mosfet = " + insightFolder.getName());
					continue INSIGHT_FOLDER;
				}

				boolean hasMosfet = false;
				for (File insightFile : insightFiles) {
					String fName = insightFile.getName();
					if (fName.equals(".mosfet")) {
						System.out.println("Loading mosfet file");
						hasMosfet = true;
						// we have a mosfet file to load
						MosfetFile mosfet = MosfetFile.generateFromFile(insightFile);

						String projectId = mosfet.getProjectId();
						String id = mosfet.getRdbmsId();
						String insightName = mosfet.getInsightName();
						String layout = mosfet.getLayout();
						List<String> recipe = mosfet.getRecipe();
						boolean global = mosfet.isGlobal();
						boolean cacheable = mosfet.isCacheable();
						int cacheMinutes = mosfet.getCacheMinutes();
						String cacheCron = mosfet.getCacheCron();
						ZonedDateTime cachedOn = mosfet.getCachedOn();
						boolean cacheEncrypt = mosfet.isCacheEncrypt();
						String schemaName = mosfet.getSchemaName();

						InsightAdministrator admin = new InsightAdministrator(insightEngine);
						// just put the recipe into an array
						admin.addInsight(id, insightName, layout, recipe, global, cacheable, cacheMinutes, cacheCron,
								cachedOn, cacheEncrypt, schemaName);
					} else {
						System.out.println("Found file in insight = " + fName);
					}
				}

				if (!hasMosfet) {
					System.out.println("Insight had folder but no mosfet = " + insightFolder.getName());
				}
			}
		}
	}

}
