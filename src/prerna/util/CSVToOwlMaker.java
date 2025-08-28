/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.database.upload.rdbms.RDBMSEngineCreationHelper;

public class CSVToOwlMaker {

	private static final Logger classLogger = LogManager.getLogger(CSVToOwlMaker.class);

	/**
	 * Create OWL from CSV OWLEngine will be released at end of method
	 *
	 * @param owlEngine
	 * @param csvFile
	 * @param owlFileLocation
	 * @param dbType
	 * @param addUniqueId
	 * @throws Exception
	 */
	public void makeFlatOwl(WriteOWLEngine owlEngine, String csvFile, String owlFileLocation,
			IDatabaseEngine.DATABASE_TYPE dbType, boolean addUniqueId) throws Exception {
		try {
			// get the headers + types + additional types
			// based on the csv parsing
			// and then generate a new OWL file

			CSVFileHelper helper = new CSVFileHelper();
			// parse and collect headers
			helper.parse(csvFile);
			helper.collectHeaders();

			String[] headers = helper.getHeaders();
			Object[][] typePredictions = helper.predictTypes();

			String fileName = Utility.getOriginalFileName(csvFile);
			String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();

			owlEngine.addConcept(cleanTableName, null, null);
			if (addUniqueId) {
				String identityColumn = cleanTableName + "_UNIQUE_ROW_ID";
				owlEngine.addProp(cleanTableName, identityColumn, "LONG", null);
			}

			for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
				String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
				owlEngine.addProp(cleanTableName, cleanHeader, typePredictions[headerIndex][0].toString(),
						(String) typePredictions[headerIndex][1]);
			}

			try {
				owlEngine.commit();
				owlEngine.export();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} finally {
			owlEngine.close();
		}
	}
}
